package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"likegochat/internal/common"
	"likegochat/internal/common/proto/connectpb"
	"likegochat/internal/connect"
)

func main() {
	cfg, err := common.LoadConfig("configs/dev.toml")
	if err != nil {
		log.Fatal(err)
	}
	// 1. 初始化 Redis 客户端
	rdb, err := common.OpenRedis(cfg.Redis.RedisAddr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		log.Fatalf("Redis 连接失败: %v", err)
	}

	// 2. 初始化路由注册器
	registry := &connect.Registry{
		RDB:      rdb,
		ServerID: cfg.Connect.ConnectServerAddr,
	}

	// 3. 建立连接至 Logic 层的 gRPC 客户端 (用于鉴权)
	authClient, logicConn, err := common.NewAuthClient(cfg.Logic.GRPCAddr)
	if err != nil {
		log.Fatalf("connect logic grpc server failed:%s", err.Error())
	}
	defer logicConn.Close()

	// 4. 组装全局依赖上下文
	serverCtx := &connect.ServerContext{
		Registry:   registry,
		AuthClient: authClient,
	}

	// 5. 启动接收Task层调用的gRPC server端
	go func() {
		lis, err := net.Listen("tcp", cfg.Connect.ConnectGRPCAddr)
		if err != nil {
			log.Fatalf("gRPC 端口监听失败: %v", err)
		}
		grpcServer := grpc.NewServer()
		connectpb.RegisterConnectServiceServer(grpcServer, &connect.GrpcServer{})

		log.Printf("Connect 层内部 gRPC 服务已启动，监听 %s", cfg.Connect.ConnectGRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC 服务运行失败: %v", err)
		}
	}()

	// 6. 启动对外的 HTTP/WebSocket 服务
	go func() {
		http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			connect.ServeWS(serverCtx, w, r)
		})
		log.Printf("Connect 层外部 WebSocket 服务已启动，监听 %s", cfg.Connect.ConnectHTTPAddr)
		if err := http.ListenAndServe(cfg.Connect.ConnectHTTPAddr, nil); err != nil {
			log.Fatalf("HTTP 服务运行失败: %v", err)
		}
	}()

	// 7. 阻塞并实现优雅退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("收到退出信号，系统正在关闭...")
	// 此处后续可补充针对已建立 WebSocket 的清理和断开逻辑
}
