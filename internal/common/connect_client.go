package common

import (
	"likegochat/internal/common/proto/connectpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewConnectClinet(addr string) (connectpb.ConnectServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return connectpb.NewConnectServiceClient(conn), conn, nil
}
