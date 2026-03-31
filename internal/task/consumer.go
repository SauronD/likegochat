package task

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"

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
	connectNodesKeyPrefix string = "connect_nodes:"
	// 一个群的所有用户
	groupMembersKeyPrefix string = "group_members:"

	// 小群走本地推送：查群的所有存活用户，再查每个用户连接的connect节点进行发送
	RoutingModeSmall int32 = 1
	// 大群节点广播：推送到所有connect节点上，每个节点通过groupid查到维护的bucket内并推送到所有连接的ws
	RoutingModeLarge int32 = 2
)

// ChatConsumer 包含处理消息所需的全部外部依赖资源
type ChatConsumer struct {
	RDB *redis.Client
	// connect层grpc客户端
	ClientPool *ConnectClientPool
	// logic层grpc客户端
	LogicClient chatpb.GroupServiceClient
	// 物理级并发拦截器，利用进程堆内存收敛 gRPC 请求
	sg singleflight.Group
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
		// 反序列化业务数据(仅为了读取ToUserId和持久化)
		var chatMsg chatpb.Message
		if err := proto.Unmarshal(kMsg.Value, &chatMsg); err != nil {
			log.Printf("反序列化 Protobuf 失败: %v", err)
			session.MarkMessage(kMsg, "")
			continue
		}

		// 查目标用户当前连在哪个Connect节点
		routingKey := fmt.Sprintf("user_server:%d", chatMsg.ToUserId)
		ctx, cancel := context.WithTimeout(session.Context(), 100*time.Millisecond)
		serverID, err := h.Base.RDB.Get(ctx, routingKey).Result()
		cancel()
		// 执行物理推送
		if err == nil && serverID != "" {
			// 获取对应的connect节点的gRPC客户端
			grpcClient, err := h.Base.ClientPool.GetClient(serverID)
			if err == nil {
				// 发起grpc调用:注意直接传kMsg.Value字节流，避免二次序列化
				rpcCtx, cancel := context.WithTimeout(session.Context(), 1*time.Second)
				_, err = grpcClient.PushMsg(rpcCtx, &connectpb.PushMsgRequest{
					ToUserId: chatMsg.ToUserId,
					Payload:  kMsg.Value,
					MsgType:  chatMsg.MsgType,
				})
				cancel()

				if err != nil {
					log.Printf("gRPC推送至节点%s失败: %v", serverID, err)
				}
			} else {
				log.Printf("获取节点%s的gRPC客户端失败: %v", serverID, err)
			}
		}

		// 4. 提交 Offset，确认消费完成
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

// 消费者处理标准流程：
func (h *GroupChatHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	ctx := session.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			for {
				cctx, cancel := context.WithTimeout(session.Context(), 2*time.Second)

				if err := h.Base.handleGroupMessage(cctx, msg); err != nil {
					log.Printf("[group] 消息处理失败 topic=%s partition=%d offset=%d err=%v",
						msg.Topic, msg.Partition, msg.Offset, err)

					cancel()
					if strings.Contains(err.Error(), "proto Unmarshal failed") {
						break
					}

					timer := time.NewTimer(1 * time.Second)
					select {
					case <-ctx.Done():
						timer.Stop()
						return nil
					case <-timer.C:
						continue
					}
				}
				cancel()
				break
			}
			session.MarkMessage(msg, "")

		}
	}
}

// 小群聊天处理
func (c *ChatConsumer) handleGroupMessage(ctx context.Context, kMsg *sarama.ConsumerMessage) error {
	var gm chatpb.GroupMessage
	if err := proto.Unmarshal(kMsg.Value, &gm); err != nil {
		return fmt.Errorf("proto Unmarshal failed:%s", err.Error())
	}
	return c.handleSmallGroupMessage(ctx, &gm, kMsg.Value)
}
func (c *ChatConsumer) handleSmallGroupMessage(ctx context.Context, gm *chatpb.GroupMessage, payload []byte) error {
	// 小群：Redis查群的所有在线成员，再按 user_server:<uid> 精准推送
	nodeTargetMap, err := c.loadGroupOnlineMemberIDs(ctx, gm.GroupId)
	if err != nil {
		errMsg := err.Error()

		// 物理防线 A：绝对匹配丢弃信号，直接中断协程内存生命周期
		if errMsg == "group has no members" || errMsg == "group has no online users" {
			return nil
		}

		// 物理防线 B：触发底层的 gRPC 回源机制
		if errMsg == "group has no members OR redis cache missed" {
			// 构建底层内存锁的唯一标识键
			flightKey := fmt.Sprintf("refresh_group_%d", gm.GroupId)
			// 执行并发收敛：
			// 1000 个协程执行到这里，底层 Go 调度器只允许 1 个协程真正发起 gRPC 网络 I/O。
			// 另外 999 个协程将被物理挂起，等待第一个协程的内存指针返回。
			v, rpcErr, _ := c.sg.Do(flightKey, func() (interface{}, error) {
				return c.LogicClient.RefreshGroupMembersCache(ctx, &chatpb.GroupMembersRequest{GroupId: gm.GroupId})
			})

			if rpcErr != nil {
				return rpcErr // 网络断开或 Logic 内部报错，向上阻断
			}
			// 内存态类型断言：将 interface{} 还原为真实的 Protobuf 结构体指针
			reply := v.(*chatpb.GroupMembersReply)

			if len(reply.GroupMembers) == 0 {
				return nil // Logic 确认 MySQL 物理磁盘中该群已空，丢弃
			}

			// 物理防线 C：使用回源拿到的真实 UID，调用独立函数重新进行 MGET 路由寻址
			nodeTargetMap, err = c.getOnlineRoutingMap(ctx, reply.GroupMembers)
			if err != nil {
				return err
			}
		} else {
			// 未知错误（如 Redis 物理机宕机引发的 EOF 错误）
			return err
		}
	}

	// 最终检验：如果经过上述所有流转，路由表依然为空，终止下发
	if len(nodeTargetMap) == 0 {
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	// connect节点数有多少？
	sem := make(chan struct{}, 10)

	for srvID, targetUIDs := range nodeTargetMap {
		serverID := srvID

		// 在内存中剔除发送者自己
		var finalUIDs []int64
		for _, uid := range targetUIDs {
			if uid != gm.FromUserId {
				finalUIDs = append(finalUIDs, uid)
			}
		}

		if len(finalUIDs) == 0 {
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-gctx.Done():
			return gctx.Err()
		}

		g.Go(func() error {
			defer func() { <-sem }()

			// 一次 gRPC 调用，将payload连同所有目标UID发给单个Connect节点
			if err := c.pushToConnect(gctx, serverID, gm.GroupId, finalUIDs, payload, gm.MsgType); err != nil {
				log.Printf("batch push to node %s failed: %v", serverID, err)
			}
			return nil
		})
	}
	return g.Wait()
}

type RoomChatHandler struct {
	Base *ChatConsumer
}

func NewRoomChatHandler(base *ChatConsumer) *RoomChatHandler {
	return &RoomChatHandler{Base: base}
}

func (h *RoomChatHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("[group] Kafka 消费者已准备就绪")
	return nil
}

func (h *RoomChatHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("[group] Kafka 消费者正在退出清理")
	return nil
}
func (h *RoomChatHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	ctx := session.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case kMsg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			ctx, cancel := context.WithTimeout(session.Context(), 2*time.Second)
			if err := h.Base.handleRoomMessage(ctx, kMsg); err != nil {
				log.Printf("[group] 消息处理失败 topic=%s partition=%d offset=%d err=%v",
					kMsg.Topic, kMsg.Partition, kMsg.Offset, err)
			}
			cancel()
			// room消息丢就丢了，不进行重新处理
			session.MarkMessage(kMsg, "")
		}
	}

}

// 房间消息聊天
func (c *ChatConsumer) handleRoomMessage(ctx context.Context, kMsg *sarama.ConsumerMessage) error {
	var gm chatpb.RoomMessage
	if err := proto.Unmarshal(kMsg.Value, &gm); err != nil {
		return err
	}
	return c.handleRoomBroadcast(ctx, &gm, kMsg.Value)
}
func (c *ChatConsumer) handleRoomBroadcast(ctx context.Context, gm *chatpb.RoomMessage, playload []byte) error {
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
				RoomId:     gm.RoomId,
				FromUserId: gm.FromUserId,
				Payload:    playload,
				MsgType:    gm.MsgType,
			})
			if err != nil {
				log.Printf("broadcast fail sid=%s err=%v", targetSid, err)
			}

			return nil
		})
	}

	return g.Wait()
}

// 取connect节点grpc client连接并调用connect grpc服务向单个用户推送消息
func (c *ChatConsumer) pushToUser(ctx context.Context, serverID string, userID int64, payload []byte, msgType int32) error {
	grpcClient, err := c.ClientPool.GetClient(serverID)
	if err != nil {
		return err
	}
	rpcCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	_, err = grpcClient.PushMsg(rpcCtx, &connectpb.PushMsgRequest{
		ToUserId: userID,
		Payload:  payload,
		MsgType:  msgType,
	})
	return err
}

// 取connect节点grpc client连接并调用connect grpc服务向多个用户推送消息
func (c *ChatConsumer) pushToConnect(ctx context.Context, serverID string, groupID int64, toUserIDs []int64, playload []byte, msgType int32) error {
	grpcClient, err := c.ClientPool.GetClient(serverID)
	if err != nil {
		return err
	}

	rpcCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	_, err = grpcClient.PushMsgToUsers(rpcCtx, &connectpb.PushAllRequest{
		GroupId: groupID,
		UserIds: toUserIDs,
		Payload: playload,
		MsgType: msgType,
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
func (c *ChatConsumer) loadGroupOnlineMemberIDs(ctx context.Context, groupID int64) (map[string][]int64, error) {
	memberKey := fmt.Sprintf("%s%d", groupMembersKeyPrefix, groupID)
	rawMembers, err := c.RDB.SMembers(ctx, memberKey).Result()
	if err != nil {
		return nil, err
	}
	if len(rawMembers) == 0 {
		return nil, errors.New("group has no members OR redis cache missed")
	}
	if len(rawMembers) == 1 && rawMembers[0] == "-1" {
		// 返回一个极其明确的特定错误，直接中断执行流
		return nil, errors.New("group has no members")
	}

	userIDs := make([]int64, 0, len(rawMembers))
	for _, s := range rawMembers {
		uid, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			continue // 忽略脏数据
		}
		userIDs = append(userIDs, uid)
	}

	if len(userIDs) == 0 {
		return nil, errors.New("group has no online users")
	}

	// 绝对物理分层：将组装好的 UID 切片直接压入独立函数的执行栈
	return c.getOnlineRoutingMap(ctx, userIDs)
}

func (c *ChatConsumer) loadGroupNodeIDs(ctx context.Context) ([]string, error) {
	return c.RDB.SMembers(ctx, connectNodesKeyPrefix).Result()
}

type PersistConsumer struct {
	DB *gorm.DB
}

func NewPersistGroupHandler(db *gorm.DB) *PersistConsumer {
	return &PersistConsumer{db}
}

// 单人聊天消息持久化消费
func (c *PersistConsumer) persistMessage(ctx context.Context, chatMsg *chatpb.Message) error {
	if c.DB == nil {
		return fmt.Errorf("db is nil")
	}
	dbMsg := &common.Message{
		MsgID:       chatMsg.MsgId,
		ClientMSGID: chatMsg.ClientMsgId,
		FromUserID:  chatMsg.FromUserId,
		ToUserID:    chatMsg.ToUserId,
		Content:     string(chatMsg.Content),
		MsgType:     chatMsg.MsgType,
		Status:      0,
		CreateTime:  time.UnixMilli(chatMsg.CreateTime),
	}
	return c.DB.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(dbMsg).Error
}

// 群组聊天消息持久化
func (c *PersistConsumer) persistGroupMessage(ctx context.Context, chatMsg *chatpb.GroupMessage) error {
	if c.DB == nil {
		return fmt.Errorf("db is nil")
	}
	dbMsg := &common.GroupMessage{
		MsgID:       chatMsg.MsgId,
		ClientMSGID: chatMsg.ClientMsgId,
		FromUserID:  chatMsg.FromUserId,
		GroupID:     chatMsg.GroupId,
		Content:     string(chatMsg.Content),
		MsgType:     chatMsg.MsgType,
		Status:      0,
		CreateTime:  time.UnixMilli(chatMsg.CreateTime),
	}
	return c.DB.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(dbMsg).Error
}

func (c *PersistConsumer) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Kafka 消费者已准备就绪")
	return nil
}

func (c *PersistConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Kafka 消费者正在退出清理")
	return nil
}
func (c *PersistConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			// 针对单条消息开启物理重试死循环
			for {
				// 必须在重试循环内部重新申请堆内存，重置2秒生命周期
				cctx, cancel := context.WithTimeout(ctx, 2*time.Second)

				var processErr error
				var badMessage bool

				switch msg.Topic {
				case "chat_messages":
					var chatMsg chatpb.Message
					if err := proto.Unmarshal(msg.Value, &chatMsg); err != nil {
						log.Printf("单聊消息反序列化失败，直接物理抹除 (Offset: %d): %v", msg.Offset, err)
						badMessage = true
						break // 跳出 switch
					}
					processErr = c.persistMessage(cctx, &chatMsg)

				case "group_chat_messages":
					var chatMsg chatpb.GroupMessage
					if err := proto.Unmarshal(msg.Value, &chatMsg); err != nil {
						log.Printf("[致命异常] 群聊消息反序列化失败，直接物理抹除 (Offset: %d): %v", msg.Offset, err)
						badMessage = true
						break // 跳出 switch
					}
					processErr = c.persistGroupMessage(cctx, &chatMsg)
				}

				cancel()

				// 消息序列化失败，不可重试
				if badMessage {
					break
				}

				// 持久化成功
				if processErr == nil {
					break
				}

				// 持久化消息失败，
				log.Printf("[重试触发] 消息持久化失败，当前协程进入物理休眠等待重试 (Topic: %s, Offset: %d): %v", msg.Topic, msg.Offset, processErr)

				// 等待，防止短时间内高频发包打爆宕机中的 MySQL
				timer := time.NewTimer(1 * time.Second)
				select {
				case <-ctx.Done():
					// 在休眠期间，如果收到了操作系统的 SIGTERM 终止进程信号，
					// 必须立刻回收定时器内存并退出，绝对不允许强制提交游标
					timer.Stop()
					return nil
				case <-timer.C:
					// 1秒物理时间到，进入下一轮for循环重新查库
				}
			}
			// 提交消息游标
			session.MarkMessage(msg, "")
		}
	}
}

// 独立出的物理寻址函数：输入UID切片，输出节点与UID的映射关系
func (c *ChatConsumer) getOnlineRoutingMap(ctx context.Context, userIDs []int64) (map[string][]int64, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	routeKeys := make([]string, 0, len(userIDs))
	for _, uid := range userIDs {
		routeKeys = append(routeKeys, fmt.Sprintf("%s%d", userServerKeyPrefix, uid))
	}

	vals, err := c.RDB.MGet(ctx, routeKeys...).Result()
	if err != nil {
		return nil, err
	}

	nodeTargetMap := make(map[string][]int64)
	for i, v := range vals {
		if v == nil {
			continue
		}
		var serverID string
		switch sv := v.(type) {
		case string:
			if sv == "" {
				continue
			}
			serverID = sv
		case []byte:
			if len(sv) == 0 {
				continue
			}
			serverID = string(sv)
		default:
			continue
		}
		nodeTargetMap[serverID] = append(nodeTargetMap[serverID], userIDs[i])
	}
	return nodeTargetMap, nil
}
