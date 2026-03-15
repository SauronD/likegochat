package connect

import (
	"context"
	"likegochat/internal/common/proto/connectpb"
)

type GrpcServer struct {
	connectpb.UnimplementedConnectServiceServer
}

// PushMsg 接收来自 Task 层的调用
func (s *GrpcServer) PushMsg(ctx context.Context, req *connectpb.PushMsgRequest) (*connectpb.PushMsgReply, error) {
	DefaultManager.Lock.RLock()
	client, exists := DefaultManager.Clients[req.ToUserId]
	DefaultManager.Lock.RUnlock()

	if exists {
		// 用户物理连接在当前节点，执行写入
		client.Send <- req.Payload
		return &connectpb.PushMsgReply{Success: true}, nil
	}

	// 用户不在当前节点（可能刚断开），返回失败
	return &connectpb.PushMsgReply{Success: false}, nil
}
