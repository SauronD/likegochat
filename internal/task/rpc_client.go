package task

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"likegochat/internal/common/proto/connectpb"
)

// ConnectClientPool 管理到所有存活 Connect 节点的 gRPC 连接
type ConnectClientPool struct {
	clients map[string]connectpb.ConnectServiceClient
	lock    sync.RWMutex
}

// NewConnectClientPool 初始化连接池
func NewConnectClientPool() *ConnectClientPool {
	return &ConnectClientPool{
		clients: make(map[string]connectpb.ConnectServiceClient),
	}
}

// GetClient 根据 redis 中查出的 serverID 获取或新建 gRPC 客户端
func (p *ConnectClientPool) GetClient(serverID string) (connectpb.ConnectServiceClient, error) {
	// 1. 优先读锁获取，提升并发性能
	p.lock.RLock()
	client, exists := p.clients[serverID]
	p.lock.RUnlock()

	if exists {
		return client, nil
	}

	// 2. 缓存未命中，加写锁创建新连接
	p.lock.Lock()
	defer p.lock.Unlock()

	// 3. 双重检查机制 (Double-Check)，防止并发协程重复创建
	if client, exists = p.clients[serverID]; exists {
		return client, nil
	}

	// 4. 建立底层 TCP/HTTP2 连接
	conn, err := grpc.Dial(serverID, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	newClient := connectpb.NewConnectServiceClient(conn)
	p.clients[serverID] = newClient

	return newClient, nil
}
