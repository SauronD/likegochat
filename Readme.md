M0 公共基础（先做）
目标：先把配置、协议、工具打底。
参考文件：config.go、logic.go(proto)、connect.go(proto)、task.go(proto)、redis.go、commom.go
你实现：配置加载、统一错误码、消息结构体、Redis/ID 工具。
验收：服务能读配置并启动，协议结构可被各模块复用。

M1 用户与鉴权（logic + db）
目标：先有注册/登录/鉴权闭环。
参考文件：db.go、user.go(dao)、rpc.go(logic)（Register/Login/CheckAuth/Logout）
你实现：用户表、密码校验、token session（Redis）。
验收：注册后可登录，token 可验可注销。

M2 API 网关（HTTP）
目标：把用户和消息入口开放成 REST。
参考文件：chat.go(api)、router.go、user.go(handler)、push.go(handler)、rpc.go(api)
你实现：Gin 路由、中间件鉴权、调用 logic RPC。
验收：Postman 可完成登录、发消息接口调用。

M3 长连接接入（connect，先 websocket）
目标：用户能建立长连接并收消息。
参考文件：connect.go、websocket.go、server.go、channel.go、room.go、bucket.go、operator.go
你实现：连接建立、按用户入 bucket、房间广播。
验收：两个浏览器进入同房间可实时互发。

M4 消息异步分发（logic -> redis queue -> task -> connect）
目标：解耦发送与推送，形成生产级链路。
参考文件：publish.go、queue.go、push.go(task)、rpc.go(task)、rpc.go(connect)
你实现：消息入 Redis 队列、task 消费、RPC 推送到 connect。
验收：API 发消息不直接推连接，task 挂了会堆积，恢复后继续消费。

M5 服务发现与多实例
目标：支持 connect/logic 水平扩容。
参考文件：rpc.go(api)、rpc.go(connect)、publish.go(logic)、rpc.go(task)、common.toml(dev)
你实现：etcd 注册发现、task 动态感知 connect 实例。
验收：新增 connect 实例无需重启全链路。

M6 TCP 接入（可选加分）
目标：展示协议设计能力。
参考文件：server_tcp.go、stickpackage.go
你实现：TCP 粘包拆包、建连鉴权、与 websocket 共用投递链路。
验收：TCP 客户端与 WebSocket 客户端能同房间互通。

M7 前端与部署收尾
目标：能演示、能交付。
参考文件：site.go、run.sh、reload.sh、gochat_api.ini
你实现：静态站点、Docker Compose/脚本、一键启动文档。
验收：新机器按 README 10 分钟跑通。