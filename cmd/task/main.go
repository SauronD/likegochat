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

	// 5. 组装消费者实例
	chatConsumer := &task.ChatConsumer{
		DB:         db, // 替换为实际的 db 实例
		RDB:        rdb,
		ClientPool: clientPool,
	}

	// 6. 初始化 Kafka 消费者组客户端
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest // 从最新消息开始消费

	consumerGroup, err := sarama.NewConsumerGroup(cfg.Kafka.KafkaBrokers, cfg.Kafka.ConsumerGroup, saramaConfig)
	if err != nil {
		log.Fatalf("创建 Kafka 消费组失败: %v", err)
	}
	defer consumerGroup.Close()

	// 7. 启动消费协程
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			// Consume 方法会阻塞执行，直到传入的 ctx 被 cancel，或者发生了不可恢复的错误
			if err := consumerGroup.Consume(ctx, []string{cfg.Kafka.ChatTopic}, chatConsumer); err != nil {
				log.Printf("消费组运行异常: %v", err)
			}
			// 判断上下文是否被取消，如果是则退出循环
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Printf("Task 层服务已启动，正在监听 Kafka Topic: %s", cfg.Kafka.ChatTopic)

	// 8. 优雅退出机制
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("收到退出信号，Task 节点正在关闭...")
}
