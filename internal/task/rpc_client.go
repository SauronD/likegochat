package task

import (
	"likegochat/internal/common/proto/connectpb"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConnectClientPool struct {
	clients map[string]connectpb.ConnectServiceClient
	lock    sync.RWMutex
}

var DefaultClientPool = &ConnectClientPool{
	clients: make(map[string]connectpb.ConnectServiceClient),
}

// GetClient 根据目标地址获取或创建一个 gRPC 客户端
func (p *ConnectClientPool) GetClient(serverAddress string) (connectpb.ConnectServiceClient, error) {
	p.lock.RLock()
	client, exists := p.clients[serverAddress]
	p.lock.RUnlock()

	if exists {
		return client, nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	// 并发安全检测（防止其他协程已创建）
	if client, exists = p.clients[serverAddress]; exists {
		return client, nil
	}

	conn, err := grpc.Dial(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	newClient := connectpb.NewConnectServiceClient(conn)
	p.clients[serverAddress] = newClient
	return newClient, nil
}
