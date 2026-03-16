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
	Registry   *Registry
	AuthClient authpb.AuthServiceClient // Logic层的gRPC客户端
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

// ServeWS 处理 HTTP 请求并升级为 WebSocket
func ServeWS(ctx *ServerContext, w http.ResponseWriter, r *http.Request) {
	// 1. 提取 Session ID
	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		http.Error(w, "缺少 session_id 参数", http.StatusUnauthorized)
		return
	}

	// 2. 跨层鉴权：通过 gRPC 调用 Logic 层
	rpcCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	verifyReply, err := ctx.AuthClient.Verify(rpcCtx, &authpb.VerifyRequest{SessionId: sessionID})
	if err != nil || verifyReply == nil {
		log.Printf("Session 鉴权失败: %v", err)
		http.Error(w, "Token 无效或已过期", http.StatusUnauthorized)
		return
	}

	userID := verifyReply.UserId

	// 3. 鉴权通过，接管底层 TCP 升级为 WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("协议升级失败: %v", err)
		return
	}

	client := &Client{
		UserID: userID,
		Conn:   conn,
		Send:   make(chan []byte, 256),
		Ctx:    ctx,
	}

	// 4. 加入本机连接池并注册到 Redis
	DefaultManager.AddClient(userID, client)
	err = ctx.Registry.RegisterUser(context.Background(), userID)
	if err != nil {
		log.Printf("Redis 注册失败: %v", err)
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
