package task

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"likegochat/internal/common/proto/chatpb"
	"likegochat/internal/common/proto/connectpb"
)

// ChatConsumer 包含处理消息所需的全部外部依赖资源
type ChatConsumer struct {
	DB         *gorm.DB
	RDB        *redis.Client
	ClientPool *ConnectClientPool
}

// Setup 消费组启动前的钩子函数
func (c *ChatConsumer) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Kafka 消费者已准备就绪")
	return nil
}

// Cleanup 消费组退出后的清理函数
func (c *ChatConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Kafka 消费者正在退出清理")
	return nil
}

// ConsumeClaim 核心业务处理循环
func (c *ChatConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for kMsg := range claim.Messages() {
		// 1. 反序列化业务数据 (仅为了读取 ToUserId 和持久化)
		var chatMsg chatpb.Message
		if err := proto.Unmarshal(kMsg.Value, &chatMsg); err != nil {
			log.Printf("反序列化 Protobuf 失败: %v", err)
			session.MarkMessage(kMsg, "")
			continue
		}

		// 2. 数据落库持久化 (不阻塞后续推送流程)
		// 注意：此处假设你的 DB 模型与 protobuf 结构对应，实际开发中需进行对象转换
		// err := c.DB.Create(&chatMsg).Error
		// if err != nil {
		// 	log.Printf("消息存入 MySQL 失败: %v", err)
		// }

		// 3. 查路由：目标用户当前连在哪个 Connect 节点？
		// 前缀必须与 Connect 层保持严格一致
		routingKey := fmt.Sprintf("user_server:%d", chatMsg.ToUserId)
		serverID, err := c.RDB.Get(context.Background(), routingKey).Result()

		// 4. 执行物理推送
		if err == nil && serverID != "" {
			// 获取对应的 gRPC 客户端
			grpcClient, err := c.ClientPool.GetClient(serverID)
			if err == nil {
				// 发起跨进程调用。注意：这里直接透传 kMsg.Value 字节流，避免二次序列化
				rpcCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err = grpcClient.PushMsg(rpcCtx, &connectpb.PushMsgRequest{
					ToUserId: chatMsg.ToUserId,
					Payload:  kMsg.Value,
				})
				cancel()

				if err != nil {
					log.Printf("gRPC 推送至节点 %s 失败: %v", serverID, err)
				}
			} else {
				log.Printf("获取节点 %s 的 gRPC 客户端失败: %v", serverID, err)
			}
		}

		// 5. 提交 Offset，确认消费完成
		session.MarkMessage(kMsg, "")
	}
	return nil
}
