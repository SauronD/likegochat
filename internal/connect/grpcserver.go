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

	if !exists {
		return &connectpb.PushMsgReply{Success: false}, nil
	}
	if !client.trySend(req.Payload) {
		return &connectpb.PushMsgReply{Success: false}, nil
	}
	// if exists {
	// 	// 写入通道，由写协程实际发送
	// 	client.Send <- req.Payload
	// 	return &connectpb.PushMsgReply{Success: true}, nil
	// }

	return &connectpb.PushMsgReply{Success: true}, nil
}

func (s *GrpcServer) PushMsgToUsers(ctx context.Context, req *connectpb.PushAllRequest) (*connectpb.PushMsgReply, error) {
	clients := make([]*Client, 0, len(req.UserIds))
	DefaultManager.Lock.RLock()
	for i := range req.UserIds {
		client, exists := DefaultManager.Clients[req.UserIds[i]]
		if !exists {
			continue
		}
		clients = append(clients, client)
	}
	DefaultManager.Lock.RUnlock()

	fail := 0
	for _, client := range clients {
		// select {
		// case client.Send <- req.Payload:
		// default:
		// 	// 当前用户的发送缓冲已满，放弃发送
		// 	fail++
		// }
		if !client.trySend(req.Payload) {
			fail++
		}
	}
	return &connectpb.PushMsgReply{Success: fail == 0}, nil

}

// BroadcastRoom 群聊信息，向当前节点的指定group的用户连接发送信息
func (s *GrpcServer) BroadcastRoom(ctx context.Context, req *connectpb.BroadcastRoomRequest) (*connectpb.BroadcastRoomReply, error) {
	fanout := DefaultRoomManager.BroadcastRoom(req.RoomId, req.FromUserId, req.Payload)
	return &connectpb.BroadcastRoomReply{
		Success: true,
		Fanout:  int32(fanout),
	}, nil
}
