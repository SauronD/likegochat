package task

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"likegochat/internal/common"
	"likegochat/internal/common/proto/chatpb"
	"likegochat/internal/common/proto/connectpb"
)

const (
	// 每个用户-连接的connect节点
	userServerKeyPrefix string = "user_server:"
	// 所有在线的connect节点
	connectNodesKeyPrefix string = "connect_nodes"

	groupMembersKeyPrefix string = "group_members"

	// 小群走本地推送：查群的所有存活用户，再查每个用户连接的connect节点进行发送
	RoutingModeSmall int32 = 1
	// 大群节点广播：推送到所有connect节点上，每个节点通过groupid查到维护的bucket内并推送到所有连接的ws
	RoutingModeLarge int32 = 2
)

// ChatConsumer 包含处理消息所需的全部外部依赖资源
type ChatConsumer struct {
	DB  *gorm.DB
	RDB *redis.Client
	// connect层grpc客户端
	ClientPool           *ConnectClientPool
	SmallGroupMaxMembers int
}
type SingleChatHander struct {
	Base *ChatConsumer
}

func NewSingleChatHandler(base *ChatConsumer) *SingleChatHander {
	return &SingleChatHander{base}
}

// Setup 消费组启动前的钩子函数
func (h *SingleChatHander) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Kafka 消费者已准备就绪")
	return nil
}

// Cleanup 消费组退出后的清理函数
func (h *SingleChatHander) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Kafka 消费者正在退出清理")
	return nil
}

// ConsumeClaim 核心业务处理循环
func (h *SingleChatHander) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for kMsg := range claim.Messages() {
		// 1. 反序列化业务数据 (仅为了读取 ToUserId 和持久化)
		var chatMsg chatpb.Message
		if err := proto.Unmarshal(kMsg.Value, &chatMsg); err != nil {
			log.Printf("反序列化 Protobuf 失败: %v", err)
			session.MarkMessage(kMsg, "")
			continue
		}

		// 2. 数据落库持久化
		persistCtx, persistCancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := h.Base.persistMessage(persistCtx, &chatMsg)
		persistCancel()
		if err != nil {
			log.Printf("消息存入 MySQL 失败: %v", err)
		}

		// 3. 查路由：目标用户当前连在哪个 Connect 节点？
		// 前缀必须与 Connect 层保持严格一致
		routingKey := fmt.Sprintf("user_server:%d", chatMsg.ToUserId)
		serverID, err := h.Base.RDB.Get(context.Background(), routingKey).Result()

		// 4. 执行物理推送
		if err == nil && serverID != "" {
			// 获取对应的 gRPC 客户端
			grpcClient, err := h.Base.ClientPool.GetClient(serverID)
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

// 群聊消息消费Handler
type GroupChatHandler struct {
	Base *ChatConsumer
}

func NewGroupChatHandler(base *ChatConsumer) *GroupChatHandler {
	return &GroupChatHandler{Base: base}
}

func (h *GroupChatHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("[group] Kafka 消费者已准备就绪")
	return nil
}

func (h *GroupChatHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("[group] Kafka 消费者正在退出清理")
	return nil
}
func (h *GroupChatHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for kMsg := range claim.Messages() {
		if err := h.Base.handleGroupMessage(kMsg); err != nil {
			log.Printf("[group] 消息处理失败 topic=%s partition=%d offset=%d err=%v",
				kMsg.Topic, kMsg.Partition, kMsg.Offset, err)
		}
		session.MarkMessage(kMsg, "")
	}
	return nil
}

// ------------- 群聊处理 + 分流 -------------
func (c *ChatConsumer) handleGroupMessage(kMsg *sarama.ConsumerMessage) error {
	var gm chatpb.GroupMessage
	if err := proto.Unmarshal(kMsg.Value, &gm); err != nil {
		return err
	}

	// 由上游决定路由模式，不在 task 层查 group_members
	switch gm.GetRoutingMode() {
	case RoutingModeSmall:
		// 小群：使用消息里携带的目标用户列表
		return c.handleSmallGroupMessage(context.Background(), &gm, kMsg.Value)
	case RoutingModeLarge:
		// 大群：直接节点广播（不查 Redis 群成员）
		return c.handleLargeGroupMessage(context.Background(), &gm, kMsg.Value)
	default:
		return fmt.Errorf("unknown route_mode: %v", gm.GetRoutingMode())
	}
}
func (c *ChatConsumer) handleSmallGroupMessage(ctx context.Context, gm *chatpb.GroupMessage, payload []byte) error {
	// 小群：Redis查群的所有在线成员，再按 user_server:<uid> 精准推送
	memberIDs, err := c.loadGroupOnlineMemberIDs(ctx, gm.GroupId)
	if err != nil {
		return err
	}

	// memberIDs为空不一定是没有在线用户，也有可能是redis的LRU删除了这个集合，因此：
	// 调logic grpc找到群的所有用户，在redis里找到所有存活用户写回redis:
	if len(memberIDs) == 0 {
		// logic处理
	}

	g, gctx := errgroup.WithContext(ctx)

	sem := make(chan struct{}, 100)

	for _, uid := range memberIDs {
		targetUID := uid
		if targetUID == gm.FromUserId {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-gctx.Done():
			return gctx.Err()
		}

		g.Go(func() error {
			defer func() { <-sem }()
			serverID, err := c.getUserServerID(gctx, targetUID)
			if err != nil || serverID == "" {
				return nil
			}
			if err := c.pushToUser(gctx, serverID, targetUID, payload); err != nil {
				log.Printf("small group push failed group=%d user=%d err=%v", gm.GroupId, targetUID, err)
			}
			return nil
		})

	}
	return g.Wait()
}

func (c *ChatConsumer) handleLargeGroupMessage(ctx context.Context, gm *chatpb.GroupMessage, payload []byte) error {
	// 查询所有connect节点进行广播
	nodeIDs, err := c.loadGroupNodeIDs(ctx)
	if err != nil {
		return err
	}
	if len(nodeIDs) == 0 {
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)

	// 针对gRPC这种非CPU计算任务，并发度可以直接给到100
	sem := make(chan struct{}, 100)

	for _, sid := range nodeIDs {
		// 修正 2：在循环体内进行显式的值拷贝，切断闭包内存共享地址
		targetSid := sid

		// 修正 1：在启动协程前获取信号量，严格控制在内存中存活的 Goroutine 数量
		select {
		case sem <- struct{}{}:
		case <-gctx.Done():
			return gctx.Err()
		}

		g.Go(func() error {
			// 协程退出时释放令牌
			defer func() { <-sem }()

			// 使用拷贝后的 targetSid，确保物理指向正确
			client, err := c.ClientPool.GetClient(targetSid)
			if err != nil {
				// 获取不到客户端不应该阻断大群的广播，记录日志并返回 nil
				log.Printf("get client fail sid=%s err=%v", targetSid, err)
				return nil
			}
			rpcCtx, cancel := context.WithTimeout(gctx, 800*time.Millisecond)
			defer cancel()

			_, err = client.BroadcastRoom(rpcCtx, &connectpb.BroadcastRoomRequest{
				GroupId:    gm.GroupId,
				FromUserId: gm.FromUserId,
				Payload:    payload,
			})
			if err != nil {
				log.Printf("broadcast fail sid=%s err=%v", targetSid, err)
			}

			return nil
		})
	}

	return g.Wait()
}
func (c *ChatConsumer) persistMessage(ctx context.Context, chatMsg *chatpb.Message) error {
	if c.DB == nil {
		return fmt.Errorf("db is nil")
	}
	dbMsg := &common.Message{
		MsgID:       chatMsg.MsgId,
		ClientMSGID: chatMsg.MsgId,
		FromUserID:  chatMsg.FromUserId,
		ToUserID:    chatMsg.ToUserId,
		Content:     string(chatMsg.Content),
		MsgType:     chatMsg.MsgType,
		Status:      0,
		CreateTime:  time.UnixMilli(chatMsg.CreateTime),
	}
	return c.DB.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(dbMsg).Error
}
func (c *ChatConsumer) pushToUser(ctx context.Context, serverID string, userID int64, payload []byte) error {
	grpcClient, err := c.ClientPool.GetClient(serverID)
	if err != nil {
		return err
	}
	rpcCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	_, err = grpcClient.PushMsg(rpcCtx, &connectpb.PushMsgRequest{
		ToUserId: userID,
		Payload:  payload,
	})
	return err
}

func (c *ChatConsumer) getUserServerID(ctx context.Context, userID int64) (string, error) {
	key := fmt.Sprintf("%s%d", userServerKeyPrefix, userID)
	serverID, err := c.RDB.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return serverID, err
}

// 查找一个群的所有在线用户：一个群的所有成员+用户是否在线进行判断
func (c *ChatConsumer) loadGroupOnlineMemberIDs(ctx context.Context, groupID int64) ([]int64, error) {
	memberKey := fmt.Sprintf("%s%d", groupMembersKeyPrefix, groupID)
	rawMembers, err := c.RDB.SMembers(ctx, memberKey).Result()
	if err != nil {
		return nil, err
	}
	if len(rawMembers) == 0 {
		return []int64{}, nil
	}

	userIDs := make([]int64, 0, len(rawMembers))
	routeKeys := make([]string, 0, len(rawMembers))

	for _, s := range rawMembers {
		uid, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			continue
		}
		userIDs = append(userIDs, uid)
		routeKeys = append(routeKeys, fmt.Sprintf("%s%d", userServerKeyPrefix, uid))
	}

	if len(routeKeys) == 0 {
		return []int64{}, nil
	}

	// 一次 MGET 批量判断在线态，避免 N 次 GET
	vals, err := c.RDB.MGet(ctx, routeKeys...).Result()
	if err != nil {
		return nil, err
	}

	onlineIDs := make([]int64, 0, len(userIDs))
	for i, v := range vals {
		if v == nil {
			continue // user_server:<uid> 不存在，离线
		}
		switch sv := v.(type) {
		case string:
			if sv == "" {
				continue
			}
		case []byte:
			if len(sv) == 0 {
				continue
			}
		}
		onlineIDs = append(onlineIDs, userIDs[i])
	}

	return onlineIDs, nil
}

func (c *ChatConsumer) loadGroupNodeIDs(ctx context.Context) ([]string, error) {
	return c.RDB.SMembers(ctx, connectNodesKeyPrefix).Result()
}
