package common

import (
	"likegochat/internal/common/proto/connectpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// grpc客户端创建到connect节点的grpc连接
func NewConnectClinet(addr string) (connectpb.ConnectServiceClient, *grpc.ClientConn, error) {
	// 采用明文传输，不配置TLS
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return connectpb.NewConnectServiceClient(conn), conn, nil
}
