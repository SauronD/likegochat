package main

import (
	"database/sql"
	"log"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	"likegochat/internal/common"
	authpb "likegochat/internal/common/proto/authpb"
	"likegochat/internal/common/proto/chatpb"
	"likegochat/internal/logic"
)

func main() {

	cfg, err := loadConfig()
	if err != nil {
		log.Printf("load config: %s\n", err.Error())
		return
	}
	// 确保退出前刷入磁盘
	defer common.Logger.Sync()

	// 连接数据库
	db, sqlDB, err := initMysql(cfg)
	if err != nil {
		common.Logger.Error("init mysql failed", zap.Error(err))
		return
	}
	defer sqlDB.Close()
	// 连接Redis
	rdb, err := initRedis(cfg)
	if err != nil {
		common.Logger.Error("open redis failed:", zap.Error(err))
		return
	}
	defer rdb.Close()

	// 数据库访问层
	store := &logic.Store{
		DB:         db,
		RDB:        rdb,
		SessionTTL: time.Duration(cfg.Session.TTLsec) * time.Second,
	}

	if err = buildServices(cfg, store); err != nil {
		common.Logger.Error("service failed", zap.Error(err))
	}

}

func loadConfig() (*common.Config, error) {
	cfg, err := common.LoadConfig("configs/dev.toml")
	if err != nil {
		return nil, err
	}
	common.InitLogger(cfg.Logger.LogFilePath,
		cfg.Logger.LogFileSize,
		cfg.Logger.LogFileBackups,
		cfg.Logger.LogFileAge,
		cfg.Logger.LogFileLevel)
	return cfg, nil
}

func initMysql(cfg *common.Config) (*gorm.DB, *sql.DB, error) {
	db, err := common.OpenMySQL(cfg.MySQL.DSN)
	if err != nil {
		return nil, nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	return db, sqlDB, nil
}

func initRedis(cfg *common.Config) (*redis.Client, error) {
	rdb, err := common.OpenRedis(cfg.Redis.RedisAddr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		return nil, err
	}
	return rdb, nil
}
func buildServices(cfg *common.Config, store *logic.Store) error {
	// 用户认证服务
	authService := &logic.AuthServer{
		Store: store,
	}
	// chat服务
	producer, err := logic.InitProducer(cfg.Kafka.KafkaBrokers, cfg.Kafka.Version)
	if err != nil {
		common.Logger.Error("init kafka producer failed", zap.Error(err))
		return err
	}
	defer producer.Close()
	chatService := &logic.ChatServer{
		KafkaProducer:  producer,
		ChatTopic:      cfg.Kafka.ChatTopic,
		GroupChatTopic: cfg.Kafka.GroupChatTopic,
		Store:          store,
	}
	// 群组管理服务

	// 监听50001，注册服务，运行gRPC server
	lis, err := net.Listen("tcp", cfg.Logic.GRPCAddr)
	if err != nil {
		common.Logger.Error("Listen grpc server port faild", zap.Error(err))
		return err
	}
	gs := grpc.NewServer(grpc.UnaryInterceptor(common.ZapGrpcLogger()))
	authpb.RegisterAuthServiceServer(gs, authService)
	chatpb.RegisterChatServiceServer(gs, chatService)
	chatpb.RegisterGroupServiceServer(gs, authService)
	log.Println("logic grpc listening on:", cfg.Logic.GRPCAddr)
	err = gs.Serve(lis)
	if err != nil {
		return err
	}
	return nil
}
