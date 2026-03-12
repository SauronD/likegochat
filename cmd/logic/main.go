package main

import (
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	"likegochat/internal/common"
	authpb "likegochat/internal/common/proto/authpb"
	"likegochat/internal/logic"
)

func main() {
	cfg, err := common.LoadConfig("configs/dev.toml")
	if err != nil {
		log.Fatal(err)
	}

	// 连接数据库
	db, err := common.OpenMySQL(cfg.MySQL.DSN)
	if err != nil {
		log.Fatal("open mysql failed:", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("get sql db failed:", err)
	}
	defer sqlDB.Close()

	// 连接Redis
	rdb, err := common.OpenRedis(cfg.Redis.RedisAddr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		log.Fatalf("open redis failed:%s", err.Error())
	}
	defer rdb.Close()

	// 数据库访问层
	store := &logic.Store{
		DB:         db,
		RDB:        rdb,
		SessionTTL: time.Duration(cfg.Session.TTLsec),
	}

	// 用户认证服务
	srv := &logic.AuthServer{
		Store:      store,
		SessionTTL: time.Duration(cfg.Session.TTLsec) * time.Second,
	}
	// 监听50001，注册服务，运行gRPC server
	lis, err := net.Listen("tcp", cfg.Logic.GRPCAddr)
	if err != nil {
		log.Fatal(err)
	}
	gs := grpc.NewServer()
	authpb.RegisterAuthServiceServer(gs, srv)

	log.Println("logic grpc listening on", cfg.Logic.GRPCAddr)
	log.Fatal(gs.Serve(lis))
}
