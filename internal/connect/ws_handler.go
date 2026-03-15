package connect

import (
	"sync"

	"github.com/gorilla/websocket"
)

type Client struct {
	UserID int64
	Conn   *websocket.Conn
	Send   chan []byte
}

type ConnectionManager struct {
	Clients map[int64]*Client
	Lock    sync.RWMutex
}

var DefaultManager = &ConnectionManager{
	Clients: make(map[int64]*Client),
}

func (m *ConnectionManager) AddClient(userID int64, client *Client) {
	m.Lock.Lock()
	m.Clients[userID] = client
	m.Lock.Unlock()
}

func (m *ConnectionManager) RemoveClient(userID int64) {
	m.Lock.Lock()
	if c, ok := m.Clients[userID]; ok {
		c.Conn.Close()
		close(c.Send)
		delete(m.Clients, userID)
	}
	m.Lock.Unlock()
}
