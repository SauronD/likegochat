package task

import (
	"errors"
	"fmt"
	"sync"

	"likegochat/internal/common"
	"likegochat/internal/common/proto/connectpb"

	"google.golang.org/grpc"
)

type pooledConnectClient struct {
	serverID string
	client   connectpb.ConnectServiceClient
	conn     *grpc.ClientConn
}

// ConnectClientPool 管理到所有存活 Connect 节点的 gRPC 连接
type ConnectClientPool struct {
	clients map[string]*pooledConnectClient
	lock    sync.RWMutex
}

// NewConnectClientPool 初始化连接池
func NewConnectClientPool() *ConnectClientPool {
	return &ConnectClientPool{
		clients: make(map[string]*pooledConnectClient),
	}
}

// GetClient 根据 redis 中查出的 serverID 获取或新建 gRPC 客户端
func (p *ConnectClientPool) GetClient(serverID string) (connectpb.ConnectServiceClient, error) {
	// 1. 优先读锁获取，提升并发性能
	p.lock.RLock()
	entry, exists := p.clients[serverID]
	p.lock.RUnlock()

	if exists {
		return entry.client, nil
	}

	// 2. 缓存未命中，加写锁创建新连接
	p.lock.Lock()
	defer p.lock.Unlock()

	// 3. 双重检查机制 (Double-Check)，防止并发协程重复创建
	if entry, exists = p.clients[serverID]; exists {
		return entry.client, nil
	}

	// 4. 建立底层 TCP/HTTP2 连接
	client, conn, err := common.NewConnectClinet(serverID)
	if err != nil {
		return nil, err
	}
	p.clients[serverID] = &pooledConnectClient{serverID, client, conn}
	return client, nil
}
func (p *ConnectClientPool) CloseClient(serverID string) error {
	p.lock.Lock()
	entry, exists := p.clients[serverID]
	if exists {
		delete(p.clients, serverID)
	}
	p.lock.Unlock()
	if !exists {
		return nil
	}
	return entry.conn.Close()

}

func (p *ConnectClientPool) CloseAll() error {
	p.lock.Lock()
	entries := make([]*pooledConnectClient, 0, len(p.clients))
	for serverID, entry := range p.clients {
		entries = append(entries, entry)
		delete(p.clients, serverID)
	}
	p.lock.Unlock()
	errs := []error{}
	for _, entry := range entries {
		if err := entry.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close grpc conn for server %s: %w", entry.serverID, err))
		}
	}
	return errors.Join(errs...)
}
