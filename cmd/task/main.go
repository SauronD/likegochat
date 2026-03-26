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
	// 5. 组装消费者实例
	chatConsumer := &task.ChatConsumer{
		DB:         db, // 替换为实际的 db 实例
		RDB:        rdb,
		ClientPool: clientPool,
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	singlechat := task.NewSingleChatHandler(chatConsumer)
	groupchat := task.NewGroupChatHandler(chatConsumer)
	// 每个task层进程开启一个协程，作为消费组的一员对三个topic消息进行消费
	go consumeLoop(ctx, singleCG, cfg.Kafka.ChatTopic, singlechat)
	go consumeLoop(ctx, groupCG, cfg.Kafka.GroupChatTopic, groupchat)

	log.Printf("Task 层服务已启动，正在监听 Kafka Topic: %s", cfg.Kafka.ChatTopic)

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
func consumeLoop(ctx context.Context, consumerGroup sarama.ConsumerGroup, topic string, handler sarama.ConsumerGroupHandler) {
	for {
		// Consume 方法会阻塞执行，直到传入的 ctx 被 cancel，或者发生了不可恢复的错误
		if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
			log.Printf("消费组运行异常: %v", err)
		}
		// 判断上下文是否被取消，如果是则退出循环
		if ctx.Err() != nil {
			return
		}
	}
}
