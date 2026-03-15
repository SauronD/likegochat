package task

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"likegochat/internal/common/proto/chatpb" // 之前的聊天消息 pb
	"likegochat/internal/common/proto/connectpb"
)

type ChatMessageConsumer struct {
	DB  *gorm.DB
	RDB *redis.Client
}

func (c *ChatMessageConsumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (c *ChatMessageConsumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *ChatMessageConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for kMsg := range claim.Messages() {
		var msg chatpb.Message
		proto.Unmarshal(kMsg.Value, &msg)

		// 1. 业务持久化（写入 MySQL，此时不影响逻辑层，完全解耦）
		// 此处调用你原有的 DB 模型写入逻辑
		// c.DB.Create(...)

		// 2. 路由寻址：从 Redis 获取目标用户的 ServerID
		key := fmt.Sprintf("user_server:%d", msg.ToUserId)
		serverID, err := c.RDB.Get(context.Background(), key).Result()

		if err == nil && serverID != "" {
			// 3. 目标用户在线，获取对应的 Connect 节点 gRPC 客户端
			grpcClient, err := DefaultClientPool.GetClient(serverID)
			if err == nil {
				// 4. 发起跨进程调用，通知 Connect 层推发消息
				grpcClient.PushMsg(context.Background(), &connectpb.PushMsgRequest{
					ToUserId: msg.ToUserId,
					Payload:  kMsg.Value, // 将原始字节流直接透传，减少二次序列化开销
				})
			}
		}

		// 5. 提交 Offset，确认消息已消费完毕
		session.MarkMessage(kMsg, "")
	}
	return nil
}
