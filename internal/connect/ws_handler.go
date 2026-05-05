package connect

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"likegochat/internal/common/proto/authpb" // 替换为你的实际 protobuf 路径

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	// 读入消息最大为4KB
	maxMessageSize = 4096
)

var upgrader = websocket.Upgrader{
	// WebSocket读/写缓冲区大小均设置1KB
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// 生产环境需要改成白名单校验，这里就不处理了
		return true
	},
}

// ServerContext全局依赖注入容器
type ServerContext struct {
	// redis客户端
	Registry *Registry
	// Logic层的gRPC客户端
	AuthClient authpb.AuthServiceClient
}

// Client 封装单个用户的物理长连接
type Client struct {
	UserID int64
	Conn   *websocket.Conn
	Send   chan []byte
	Ctx    *ServerContext
	// 用户在线时加入的Room
	roomsMu sync.Mutex
	rooms   map[int64]struct{}
	// 关闭连接信号，避免重复close Send
	closeOnce sync.Once
	done      chan struct{}
}

// ConnectionManager 本机长连接池管理器
type ConnectionManager struct {
	Clients map[int64]*Client
	Lock    sync.RWMutex
}
type RoomCmd struct {
	Op     string `json:"op"`
	RoomID int64  `json:"room_id"`
}

var DefaultManager = &ConnectionManager{
	Clients: make(map[int64]*Client),
}

// 需要处理相同userID创建新连接覆盖旧连接的情况：
// 不能直接覆盖m.Clients中的原连接，因为旧连接的readPump还在运行，继续占用资源，需要关闭旧连接
func (m *ConnectionManager) AddClient(userID int64, client *Client) {
	var old *Client
	// 查询旧连接是否存在
	m.Lock.Lock()
	if cc, ok := m.Clients[userID]; ok {
		old = cc
	}
	m.Clients[userID] = client
	m.Lock.Unlock()
	// 关闭旧连接：注意重复Add的幂等性
	if old != nil && old != client {
		old.closeConn()
	}

}

// 按照userID直接删除，不考虑误删新连接的问题
func (m *ConnectionManager) RemoveClient(userID int64) {
	m.RemoveClientIfMatch(userID, nil)
}

// 关闭done通道和ws连接
func (c *Client) closeConn() {
	c.closeOnce.Do(func() {
		close(c.done)
		c.Conn.Close()
	})
}

// expect为nil时无条件删除；否则仅在当前映射匹配expect时删除。
func (m *ConnectionManager) RemoveClientIfMatch(userID int64, expect *Client) bool {
	var c *Client

	m.Lock.Lock()
	if cc, ok := m.Clients[userID]; ok {
		// 如果expect为nil或当前userID对应的连接和expect不同时，不进行删除
		if expect != nil && cc != expect {
			m.Lock.Unlock()
			return false
		}
		c = cc
		delete(m.Clients, userID)
	}
	m.Lock.Unlock()
	// 锁外释放，减少锁竞争
	if c != nil {
		c.closeConn()
		return true
	}
	return false
}

// 非阻塞发送，避免推送消息时因为c.Send缓冲区满时一直阻塞在c.Send <- payload
func (c *Client) trySend(payload []byte) bool {

	select {
	case <-c.done:
		return false
	case c.Send <- payload:
		return true
	default:
		return false
	}
}

// ServeWS 处理HTTP请求并升级为WebSocket
func ServeWS(serverContext *ServerContext, w http.ResponseWriter, r *http.Request) {
	// 提取 Session ID
	sessionID := r.Header.Get("Authorization")
	if sessionID == "" {
		http.Error(w, "empty session_id ", http.StatusUnauthorized)
		return
	}

	// 跨层鉴权：通过gRPC调用Logic层
	rpcCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	verifyReply, err := serverContext.AuthClient.Verify(rpcCtx, &authpb.VerifyRequest{SessionId: sessionID})
	if err != nil {
		log.Printf("invalid session: %v", err)
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}

	userID := verifyReply.UserId

	// 鉴权通过，接管底层TCP升级为WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade websocket failed: %v", err)
		return
	}

	client := &Client{
		UserID: userID,
		Conn:   conn,
		Send:   make(chan []byte, 256),
		Ctx:    serverContext,
		rooms:  make(map[int64]struct{}),
		done:   make(chan struct{}),
	}

	// 加入connect节点连接池并注册到Redis
	// 细节是应该在节点连接时先注册本地连接再注册Redis，保证task层推送消息时，ws连接一定存在
	DefaultManager.AddClient(userID, client)
	err = serverContext.Registry.RegisterUser(r.Context(), userID)
	if err != nil {
		log.Printf("Redis注册connect节点%s失败: %v", serverContext.Registry.ServerID, err)
		// 注意删除旧连接
		DefaultManager.RemoveClientIfMatch(userID, client)
		return
	}

	// 启动全双工读写协程处理消息的接收/发送
	go client.writePump()
	go client.readPump()
}

func (c *Client) readPump() {
	defer func() {
		// 仅当前连接仍是userID的有效映射时才做下线清理，避免旧连接误删新连接状态
		if removed := DefaultManager.RemoveClientIfMatch(c.UserID, c); removed {
			_ = c.Ctx.Registry.UnregisterUser(context.Background(), c.UserID)
		}
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("连接异常断开: %v", err)
			}
			c.leaveAllRooms()
			break
		}
		// 读用户输入
		in := &RoomCmd{}
		err = json.Unmarshal(message, &in)
		if err != nil {
			// 用户输入反序列化失败
			log.Println("bad request")
			continue
		}
		switch in.Op {
		case "joinRoom":
			if in.RoomID <= 0 {
				continue
			}
			DefaultRoomManager.JoinRoom(in.RoomID, c.UserID, c.Send)
			c.addRoom(in.RoomID)
		case "leaveRoom":
			if in.RoomID <= 0 {
				continue
			}
			DefaultRoomManager.LeaveRoom(in.RoomID, c.UserID, c.Send)
			c.removeRoom(in.RoomID)
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case <-c.done:
			return
		case message, ok := <-c.Send: // 收到需要向客户端推送的消息

			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Conn.NextWriter(websocket.BinaryMessage)
			if err != nil {
				log.Printf("ws socket Write failed:%s", err.Error())
				return
			}
			// 把数据刷到底层net.Conn，写缓冲满时会自动分片
			w.Write(message)
			// 可以继续优化的一个点：这里继续写c.Send的数据，但这样做需要再包装一层协议来处理应用层的“粘包”

			if err := w.Close(); err != nil {
				log.Printf("ws socket Send failed:%s", err.Error())
				return
			}
		case <-ticker.C: // 发送心跳包
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) addRoom(roomID int64) {
	c.roomsMu.Lock()
	c.rooms[roomID] = struct{}{}
	c.roomsMu.Unlock()
}

// 维护用户连接的房间roomID
func (c *Client) removeRoom(roomID int64) {
	c.roomsMu.Lock()
	delete(c.rooms, roomID)
	c.roomsMu.Unlock()
}

func (c *Client) leaveAllRooms() {

	c.roomsMu.Lock()
	roomIDs := make([]int64, 0, len(c.rooms))
	for key := range c.rooms {
		roomIDs = append(roomIDs, key)
	}
	c.rooms = make(map[int64]struct{})
	c.roomsMu.Unlock()
	for _, roomID := range roomIDs {
		DefaultRoomManager.LeaveRoom(roomID, c.UserID, c.Send)
	}

}
