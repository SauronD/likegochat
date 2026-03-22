package connect

import (
	"context"

	"likegochat/internal/common/proto/connectpb" // 替换为你的实际 protobuf 路径
)

// GrpcServer实现Task调用Connect层的单人/多人聊天服务
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

// BroadcastRoom 群聊信息，向当前节点的指定group的用户连接发送信息
func (s *GrpcServer) BroadcastRoom(ctx context.Context, req *connectpb.BroadcastRoomRequest) (*connectpb.BroadcastRoomReply, error) {
	fanout := DefaultRoomManager.BroadcastRoom(req.GroupId, req.FromUserId, req.Payload)
	return &connectpb.BroadcastRoomReply{
		Success: true,
		Fanout:  int32(fanout),
	}, nil
}
