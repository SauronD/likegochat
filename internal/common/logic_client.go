package common

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	authpb "likegochat/internal/common/proto/authpb"
)

// logic层认证服务grpc客户端创建
func NewAuthClient(addr string) (authpb.AuthServiceClient, *grpc.ClientConn, error) {
	// grpc明文传输
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	return authpb.NewAuthServiceClient(conn), conn, nil
}
