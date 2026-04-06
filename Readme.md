
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