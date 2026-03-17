package logic

import (
	"context"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"likegochat/internal/common/proto/chatpb"
)

type ChatServer struct {
	chatpb.UnimplementedChatServiceServer
	KafkaProducer sarama.SyncProducer
	ChatTopic     string
	// DB *gorm.DB // 预留：此处可以注入 DB 用于前置校验（如检查好友关系）
}

func (s *ChatServer) SendMessage(ctx context.Context, req *chatpb.SendMessageRequest) (*chatpb.SendMessageReply, error) {
	// 1. 业务前置校验（示例）
	if req.FromUserId == req.ToUserId {
		return nil, status.Error(codes.InvalidArgument, "不能给自己发送消息")
	}

	// 2. 构建底层存储的 Message 模型
	now := time.Now().UnixMilli()
	// 实际工程中，此处应调用分布式 ID 生成算法（如雪花算法）获取唯一的 MsgID
	var msgID int64 = now

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
		// 使用目标用户的 ID 作为 Key，确保发给同一个用户的消息落入同一个 Kafka 分区，保证严格的局部有序性
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
