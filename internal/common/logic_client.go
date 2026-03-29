package common

import (
	"likegochat/internal/common/proto/authpb"
	"likegochat/internal/common/proto/chatpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// logic层信息传输服务grpc客户端创建
func NewChatClient(addr string) (chatpb.ChatServiceClient, *grpc.ClientConn, error) {
	// grpc明文传输
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	return chatpb.NewChatServiceClient(conn), conn, nil
}

// logic层信息传输服务grpc客户端创建
func NewGrouptClient(addr string) (chatpb.GroupServiceClient, *grpc.ClientConn, error) {
	// grpc明文传输
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	return chatpb.NewGroupServiceClient(conn), conn, nil
}
