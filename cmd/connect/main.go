package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"likegochat/internal/common/proto/authpb"
	"likegochat/internal/common/proto/connectpb"
	"likegochat/internal/connect"
)

var (
	httpPort  = flag.String("http_port", ":8001", "对外提供 WebSocket 服务的端口")
	grpcPort  = flag.String("grpc_port", ":9001", "对内接收 Task 层推送的 gRPC 端口")
	redisAddr = flag.String("redis_addr", "127.0.0.1:6379", "Redis 缓存地址")
	serverID  = flag.String("server_id", "127.0.0.1:9001", "当前 Connect 节点的网络寻址 ID")
	logicAddr = flag.String("logic_addr", "127.0.0.1:9000", "Logic 层的 gRPC 接口地址")
)

func main() {
	flag.Parse()

	// 1. 初始化 Redis 客户端
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Redis 连接失败: %v", err)
	}

	// 2. 初始化路由注册器
	registry := &connect.Registry{
		RDB:      rdb,
		ServerID: *serverID,
	}

	// 3. 建立连接至 Logic 层的 gRPC 客户端 (用于鉴权)
	logicConn, err := grpc.Dial(*logicAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("无法连接至 Logic 层: %v", err)
	}
	defer logicConn.Close()
	authClient := authpb.NewAuthServiceClient(logicConn)

	// 4. 组装全局依赖上下文
	serverCtx := &connect.ServerContext{
		Registry:   registry,
		AuthClient: authClient,
	}

	// 5. 启动接收 Task 层调用的 gRPC 服务
	go func() {
		lis, err := net.Listen("tcp", *grpcPort)
		if err != nil {
			log.Fatalf("gRPC 端口监听失败: %v", err)
		}
		grpcServer := grpc.NewServer()
		connectpb.RegisterConnectServiceServer(grpcServer, &connect.GrpcServer{})

		log.Printf("Connect 层内部 gRPC 服务已启动，监听 %s", *grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC 服务运行失败: %v", err)
		}
	}()

	// 6. 启动对外的 HTTP/WebSocket 服务
	go func() {
		http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			connect.ServeWS(serverCtx, w, r)
		})
		log.Printf("Connect 层外部 WebSocket 服务已启动，监听 %s", *httpPort)
		if err := http.ListenAndServe(*httpPort, nil); err != nil {
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
