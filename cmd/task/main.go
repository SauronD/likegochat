package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"likegochat/internal/common" // 替换为你的通用包路径
	"likegochat/internal/task"

	"github.com/IBM/sarama"
)

func main() {

	// 1. 读取全局配置
	cfg, err := common.LoadConfig("configs/dev.toml")
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}
	common.InitLogger(cfg.Logger.LogFilePath,
		cfg.Logger.LogFileSize,
		cfg.Logger.LogFileBackups,
		cfg.Logger.LogFileAge,
		cfg.Logger.LogFileLevel,
	)

	// 2. 初始化 Redis
	rdb, err := common.OpenRedis(cfg.Redis.RedisAddr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		log.Fatalf("Redis 连接失败: %v", err)
	}

	// 3. 初始化 MySQL (假设你的 common 提供了此方法)
	db, err := common.OpenMySQL(cfg.MySQL.DSN)
	if err != nil {
		log.Fatalf("MySQL 连接失败: %v", err)
	}

	// 4. 初始化 gRPC 客户端池
	clientPool := task.NewConnectClientPool()
	defer func() {
		if err := clientPool.CloseAll(); err != nil {
			log.Printf("关闭 Connect 客户端池失败: %v", err)
		}
	}()
	// logic层grpc客户端
	logicClient, coon, err := common.NewGrouptClient(cfg.Logic.GRPCAddr)
	if err != nil {
		log.Printf("logic grpc客户端创建失败:%s", err)
		return
	}
	defer coon.Close()

	// 5. 组装消费者实例
	chatConsumer := &task.ChatConsumer{
		RDB:         rdb,
		ClientPool:  clientPool,
		LogicClient: logicClient,
	}

	// 6. 初始化 Kafka 消费者组客户端
	singleCG, err := newConsumerGroup(cfg, cfg.Kafka.SinglechatConsumerGroup)
	if err != nil {
		log.Fatalln(err)
	}
	defer singleCG.Close()
	groupCG, err := newConsumerGroup(cfg, cfg.Kafka.GroupchatConsumerGroup)
	if err != nil {
		log.Fatalln(err)
	}
	defer groupCG.Close()

	roomCG, err := newConsumerGroup(cfg, cfg.Kafka.RoomchatConsumerGroup)
	if err != nil {
		log.Fatalln(err)
	}
	persistCG, err := newConsumerGroup(cfg, cfg.Kafka.PersistchatConsumerGroup)
	if err != nil {
		log.Fatalln(err)
	}
	defer persistCG.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	singlechat := task.NewSingleChatHandler(chatConsumer)
	groupchat := task.NewGroupChatHandler(chatConsumer)
	roomchat := task.NewGroupChatHandler(chatConsumer)
	persistchat := task.NewPersistGroupHandler(db)
	// 每个task层进程开启一个协程，作为消费组的一员对相应topic消息进行消费
	go consumeLoop(ctx, singleCG, []string{cfg.Kafka.ChatTopic}, singlechat)
	go consumeLoop(ctx, groupCG, []string{cfg.Kafka.GroupChatTopic}, groupchat)
	go consumeLoop(ctx, roomCG, []string{cfg.Kafka.RoomChatTopic}, roomchat)
	go consumeLoop(ctx, persistCG, []string{cfg.Kafka.ChatTopic, cfg.Kafka.GroupChatTopic}, persistchat)
	log.Printf("Task 层服务已启动，正在监听Kafka Topic: %s", cfg.Kafka.ChatTopic)

	// 8. 优雅退出机制
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("收到退出信号，Task 节点正在关闭...")
}
func newConsumerGroup(cfg *common.Config, groupID string) (sarama.ConsumerGroup, error) {
	sc := sarama.NewConfig()

	v, err := sarama.ParseKafkaVersion(cfg.Kafka.Version)
	if err != nil {
		return nil, err
	}
	sc.Version = v

	sc.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	sc.Consumer.Offsets.Initial = sarama.OffsetNewest

	return sarama.NewConsumerGroup(cfg.Kafka.KafkaBrokers, groupID, sc)
}
func consumeLoop(ctx context.Context, consumerGroup sarama.ConsumerGroup, topics []string, handler sarama.ConsumerGroupHandler) {
	for {
		// Consume 方法会阻塞执行，直到传入的 ctx 被 cancel，或者发生了不可恢复的错误
		if err := consumerGroup.Consume(ctx, topics, handler); err != nil {
			log.Printf("消费组运行异常: %v", err)
		}
		// 判断上下文是否被取消，如果是则退出循环
		if ctx.Err() != nil {
			return
		}
	}
}
