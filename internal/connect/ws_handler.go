package connect

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"likegochat/internal/common/proto/authpb" // 替换为你的实际 protobuf 路径

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 4096
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // 生产环境需严格校验跨域
	},
}

// ServerContext 全局依赖注入容器
type ServerContext struct {
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
}

// ConnectionManager 本机长连接池管理器
type ConnectionManager struct {
	Clients map[int64]*Client
	Lock    sync.RWMutex
}

var DefaultManager = &ConnectionManager{
	Clients: make(map[int64]*Client),
}

func (m *ConnectionManager) AddClient(userID int64, client *Client) {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	m.Clients[userID] = client
}

func (m *ConnectionManager) RemoveClient(userID int64) {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	if c, ok := m.Clients[userID]; ok {
		c.Conn.Close()
		close(c.Send)
		delete(m.Clients, userID)
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
	}

	// 4. 加入本机连接池并注册到 Redis
	DefaultManager.AddClient(userID, client)
	err = serverContext.Registry.RegisterUser(r.Context(), userID)
	if err != nil {
		log.Printf("Redis注册connect节点%s失败: %v", serverContext.Registry.ServerID, err)
		DefaultManager.RemoveClient(userID)
		return
	}

	// 5. 启动全双工读写协程
	go client.writePump()
	go client.readPump()
}

func (c *Client) readPump() {
	defer func() {
		DefaultManager.RemoveClient(c.UserID)
		c.Ctx.Registry.UnregisterUser(context.Background(), c.UserID)
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
			break
		}
		// 此处为扩展点：将收到的 message 发往 Kafka，由 Task 层处理。
		// 例：c.Ctx.KafkaProducer.Send(message)
		_ = message
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
		case message, ok := <-c.Send: // 收到 gRPC 传来的推送消息
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Conn.NextWriter(websocket.BinaryMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// 批量写入缓冲区优化性能
			n := len(c.Send)
			for i := 0; i < n; i++ {
				w.Write(<-c.Send)
			}

			if err := w.Close(); err != nil {
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
