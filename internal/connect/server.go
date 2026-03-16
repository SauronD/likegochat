package connect

import (
	"context"

	"likegochat/internal/common/proto/connectpb" // 替换为你的实际 protobuf 路径
)

// GrpcServer 实现 Task 调用 Connect 层的接口
type GrpcServer struct {
	connectpb.UnimplementedConnectServiceServer
}

// PushMsg 将二进制数据透传给对应用户的 WebSocket 发送通道
func (s *GrpcServer) PushMsg(ctx context.Context, req *connectpb.PushMsgRequest) (*connectpb.PushMsgReply, error) {
	DefaultManager.Lock.RLock()
	client, exists := DefaultManager.Clients[req.ToUserId]
	DefaultManager.Lock.RUnlock()

	if exists {
		// 写入通道，由写协程实际发送
		client.Send <- req.Payload
		return &connectpb.PushMsgReply{Success: true}, nil
	}

	return &connectpb.PushMsgReply{Success: false}, nil
}
