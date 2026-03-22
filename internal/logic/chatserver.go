package logic

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"likegochat/internal/common/proto/chatpb"
)

// 每毫秒内的本地序号，和毫秒时间戳拼接后生成消息ID。
var localMsgSeq atomic.Uint64

func nextMessageID(nowMilli int64) int64 {
	const seqMask uint64 = (1 << 20) - 1 // 每毫秒最多2^20-1个message序号
	seq := localMsgSeq.Add(1) & seqMask
	return (nowMilli << 20) | int64(seq)
}

type ChatServer struct {
	chatpb.UnimplementedChatServiceServer
	KafkaProducer  sarama.SyncProducer
	ChatTopic      string
	GroupChatTopic string
	Store          *Store
}

func (s *ChatServer) SendMessage(ctx context.Context, req *chatpb.SendMessageRequest) (*chatpb.SendMessageReply, error) {
	// 1. 业务前置校验（示例）
	if req.FromUserId == req.ToUserId {
		return nil, status.Error(codes.InvalidArgument, "不能给自己发送消息")
	}

	// 2. 构建底层存储的 Message 模型

	now := time.Now().UnixMilli()
	msgID := nextMessageID(now)

	chatMsg := &chatpb.Message{
		MsgId:      msgID,
		FromUserId: req.FromUserId,
		ToUserId:   req.ToUserId,
		Content:    req.Content,
		MsgType:    req.MsgType,
		CreateTime: now,
	}

	// 3. 序列化为 Protobuf 二进制流
	payload, err := proto.Marshal(chatMsg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "消息序列化失败: %v", err)
	}

	// 4. 组装 Kafka 消息
	kMsg := &sarama.ProducerMessage{
		Topic: s.ChatTopic,
		// 使用目标用户的 ID 作为 Key，确保发给同一个用户的消息落入同一个Kafka分区，保证严格的局部有序性
		Key:   sarama.StringEncoder(strconv.FormatInt(req.ToUserId, 10)),
		Value: sarama.ByteEncoder(payload),
	}

	// 5. 同步阻塞发送至 Kafka 磁盘
	partition, offset, err := s.KafkaProducer.SendMessage(kMsg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "写入消息队列失败: %v", err)
	}

	// 可选：记录日志用于排查
	_ = partition
	_ = offset

	// 6. 返回成功确认给 API 层
	return &chatpb.SendMessageReply{
		MsgId:     msgID,
		Timestamp: now,
	}, nil
}

func (s *ChatServer) SendGroupMessage(ctx context.Context, req *chatpb.SendGroupMessageRequest) (*chatpb.SendGroupMessageReply, error) {
	if req.GroupId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "group_id 无效")
	}
	if len(req.Content) == 0 {
		return nil, status.Error(codes.InvalidArgument, "content 不能为空")
	}
	if s.GroupChatTopic == "" {
		return nil, status.Error(codes.FailedPrecondition, "group chat topic 未配置")
	}

	if s.Store != nil {
		ok, err := s.Store.IsGroupMember(ctx, req.GroupId, req.FromUserId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "群成员校验失败: %v", err)
		}
		if !ok {
			return nil, status.Error(codes.PermissionDenied, "发送者不在该群")
		}
	}

	now := time.Now().UnixMilli()
	msgID := nextMessageID(now)

	groupMsg := &chatpb.GroupMessage{
		MsgId:       msgID,
		FromUserId:  req.FromUserId,
		GroupId:     req.GroupId,
		Content:     req.Content,
		MsgType:     req.MsgType,
		CreateTime:  now,
		RoutingMode: req.RoutingMode,
	}

	payload, err := proto.Marshal(groupMsg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "群消息序列化失败: %v", err)
	}

	kMsg := &sarama.ProducerMessage{
		Topic: s.GroupChatTopic,
		Key:   sarama.StringEncoder(strconv.FormatInt(req.GroupId, 10)),
		Value: sarama.ByteEncoder(payload),
	}
	if _, _, err = s.KafkaProducer.SendMessage(kMsg); err != nil {
		return nil, status.Errorf(codes.Internal, "写入群消息队列失败: %v", err)
	}

	return &chatpb.SendGroupMessageReply{
		MsgId:     msgID,
		Timestamp: now,
	}, nil
}
