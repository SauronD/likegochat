
登录机制：
1、/api/login登录拿到sessionID
2、用sessionID去连接ws，并将userID-connect_serverID添加到redis中




群聊机制：


1、小群聊天
本质单人聊天广播：需要task层查到所有人连接的connect节点并进行推送


2、聊天室功能：roomID唯一键
ws连接到connect节点时，显式传roomID进行连接，在当前connect层bucket内注册
发送消息：调logic层grpc，推送Kafka，task层处理广播到所有connect节点

消息离线机制：
每类消息单独一个consumer group来做持久化




项目经历
分层式分布式即时通讯系统（LikeGoChat）
技术栈：Go、gRPC、WebSocket、Kafka、Redis、MySQL、GORM、Protobuf、Singleflight、Lua、Zap

项目描述：
基于 API + Logic + Task + Connect 四层架构实现高并发 IM 系统，覆盖单聊、小群聊与房间广播三类消息场景。系统通过 Kafka 解耦消息生产与分发、Redis 承担会话与在线路由、MySQL 进行离线消息持久化，支持 Connect 节点横向扩展与多节点实时推送。

个人职责：

认证与会话体系：基于 Redis 双键索引（user->session / session->user）+ Lua 脚本实现会话原子切换，完成“单端登录”与登录挤占场景治理，避免并发登录导致会话状态错乱。
实时消息主链路：设计 HTTP -> gRPC -> Kafka -> Task -> Connect -> WebSocket 端到端投递流程，按 to_user_id/group_id 进行分区键路由，保证同会话消息局部有序。
群聊路由与缓存一致性：实现 Redis 群成员缓存 + MySQL 回源机制；结合 singleflight 收敛并发回源请求，并通过空值占位（-1）与双删策略降低缓存穿透、击穿和脏读风险。
多节点推送优化：在 Task 层构建“节点->用户列表”批量路由映射，使用 errgroup + semaphore 做限流并发下发，减少逐用户 RPC 调用开销。
Connect 网关与房间广播：实现 WebSocket 长连接管理、心跳保活、慢连接保护（非阻塞发送）；基于分桶 RoomManager（哈希分片）完成房间级广播，提升并发广播吞吐。
离线持久化与幂等：按消息类型拆分 Kafka 消费组，落库时使用唯一键与 OnConflict DoNothing 保证幂等写入，支持异常重试与消费位点安全提交。

v2功能：
1、安全问题：
登录验证、SSL、跨越安全问题
2、更多功能：
离线消息推送、文件上传/下载、多媒体(语音聊天...)、...





<!-- 高并发场景 -->
1、WebSocket连接
大量用户同时上线/重连，集中打在ServeWS、连接池 map、Redis在线路由注册上（connect层）。

2、单聊消息洪峰
大量/api/send同时进入，Logic同步写Kafka，Task再并发路由到各connect节点。

3、群聊fanout
一条群消息会拆成“按节点批量推送”，群越大、在线人数越多，并发RPC越高。

4、房间广播fanout
一条room消息会广播到所有connect节点，再在每个节点对本地房间成员广播，放大最明显。

5、热点用户/热点房间
同一userID或roomID的频繁join/leave/broadcast会集中争用同一把锁(bucket锁)与通道缓冲。

6、缓存 miss/重建风暴
群成员缓存失效时，多请求同时回源MySQL/Redis；singleflight做抑制。

7、慢连接堆积
下游客户端写入慢时，Send 缓冲很快打满，进入丢弃分支，触发大量非阻塞失败路径。

8、多topic消费叠加
task进程同时消费single/group/room/persist，多条管线并行时会叠加CPU、网络、Redis压力。