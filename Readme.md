
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