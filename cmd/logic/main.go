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

	// JWT管理对象
	jm := &common.JWTManager{
		Issuer:    cfg.JWT.Issuer,
		Audience:  cfg.JWT.Audience,
		Secret:    []byte(cfg.JWT.Secret),
		AccessTTL: time.Duration(cfg.JWT.AccessTTLSec) * time.Second,
	}

	// 数据库访问层
	store := &logic.Store{DB: db}

	// 用户认证服务
	srv := &logic.AuthServer{
		Store:      store,
		JWT:        jm,
		SessionTTL: time.Duration(cfg.JWT.SessionTTLSec) * time.Second,
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
