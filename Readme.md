likegochat/
├── cmd/
│   ├── api/                 # HTTP 网关：注册/登录/发消息等（对外）
│   │   └── main.go
│   ├── logic/               # 业务核心：鉴权、路由、消息生产（gRPC server）
│   │   └── main.go
│   ├── connect/             # 长连接：WebSocket/TCP（gRPC client 调 logic 验证）
│   │   └── main.go
│   └── task/                # 消费队列：Kafka consumer -> 调 connect 投递
│       └── main.go
│
├── internal/
│   ├── api/                 # api 服务用到的 handler/client（少量文件）
│   │   ├── handler_auth.go
│   │   ├── handler_msg.go
│   │   └── logic_client.go
│   │
│   ├── logic/               # logic 服务实现（少量文件）
│   │   ├── auth.go          # 注册/登录/校验（bcrypt + 单端 session）
│   │   ├── message.go       # 发消息：写 Kafka（或先内存）
│   │   ├── rpc.go           # gRPC server 注册
│   │   └── store.go         # MySQL/Redis 的简单封装（Repo 集中在这）
│   │
│   ├── connect/             # connect 服务实现
│   │   ├── ws.go            # websocket accept/read/write
│   │   ├── session.go       # 连接会话管理（user->conn）
│   │   └── logic_client.go  # 调 logic.Verify
│   │
│   ├── task/                # task 服务实现
│   │   ├── consumer.go      # Kafka consumer
│   │   └── dispatcher.go    # 调 connect 投递
│   │
│   └── common/              # 所有服务共享的最小公共代码（控制在少数文件）
│       ├── config.go        # 读取配置（toml/yaml/flag 任选）
│       ├── db.go            # MySQL 连接
│       ├── kafka.go         # Kafka 连接（后期启用）
│       ├── jwt.go           # JWT（后期启用）
│       └── proto/           # protoc 生成的 pb.go（统一放这，简单）
│           └── ...
│
├── proto/                   # *.proto 原始定义（auth/message/connect）
│   ├── auth.proto
│   ├── logic.proto
│   └── connect.proto
│
├── sql/                     # DDL 与索引（学习项目直接放这最直观）
│   ├── schema.sql
│   └── seed.sql
│
├── configs/                  # 一套配置即可（dev），别搞 prod/dev 多层
│   └── dev.toml
│
├── scripts/                  # 启动脚本（可选，但很利于面试演示）
│   ├── up_deps.sh           # 起 mysql/kafka 等（可选）
│   └── run_all.sh           # 一键启动四个服务
│
├── docker-compose.yml        # 可选：依赖（mysql/kafka/etcd/redis）
├── Makefile                  # proto / build / run
├── go.mod
└── README.md                 # 重点写：架构图 + 时序图 + 启动方式