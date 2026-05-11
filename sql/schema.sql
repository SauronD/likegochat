CREATE DATABASE IF NOT EXISTS likegochat DEFAULT CHARACTER SET utf8mb4;
USE likegochat;

-- 用户表：只存账号与密码哈希
CREATE TABLE IF NOT EXISTS users (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(64) NOT NULL,
  password_hash VARCHAR(100) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 会话表
-- revoked_at = NULL 表示“当前有效会话”
CREATE TABLE IF NOT EXISTS user_sessions (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  session_id CHAR(36) NOT NULL,
  issued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP NOT NULL,
  revoked_at TIMESTAMP NULL,

  device_id VARCHAR(64) NULL,
  ip VARCHAR(45) NULL,
  user_agent VARCHAR(255) NULL,

  UNIQUE KEY uk_session_id (session_id),
  KEY idx_user_active (user_id, revoked_at),
  CONSTRAINT fk_sessions_user FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 单人聊天信息表
CREATE TABLE IF NOT EXISTS messages(
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '数据库自增主键',
  msg_id BIGINT NOT NULL COMMENT '消息ID,服务器端接收时产生',
  -- 消息幂等性保证，对同一消息的去重处理
  client_msg_id BIGINT NOT NULL COMMENT '客户端发送消息ID',
  from_user_id BIGINT NOT NULL COMMENT '发送方用户ID',
  to_user_id BIGINT NOT NULL COMMENT '接收方用户ID',
  content TEXT  NOT NULL COMMENT '消息内容，多媒体(图片/文件等)存引用',
  msg_type TINYINT NOT NULL  DEFAULT 1 COMMENT '消息类型',
  `status` TINYINT NOT NULL DEFAULT 0 COMMENT '消息状态，比如：正常、删除、撤回',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '服务端写入时间',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '服务端更新时间',
  PRIMARY KEY (id),
  UNIQUE KEY uk_msg_id (msg_id),
  -- 客户端重试去重
  UNIQUE KEY uk_from_client_msg (from_user_id, to_user_id,client_msg_id),
  KEY idx_to_user_created (to_user_id,created_at,id),
  KEY idx_from_user_created (from_user_id, created_at, id),
  -- 双人会话查询
  KEY idx_pair_created (from_user_id, to_user_id, created_at, id)

)ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT '单人聊天信息表';

-- 群组表
CREATE TABLE IF NOT EXISTS `groups`(
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  group_name VARCHAR(30) NOT NULL COMMENT '群名称',
  group_status TINYINT NOT NULL DEFAULT 0 COMMENT '群状态:0正常;1封禁',
  owner_user_id BIGINT UNSIGNED NOT NULL COMMENT '群主用户id',
  member_count INT UNSIGNED NOT NULL DEFAULT 1 COMMENT '群人数',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '群创建时间',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '群组表信息更新时间', 

  PRIMARY KEY (id),
  UNIQUE KEY (group_name),
  KEY `owner` (owner_user_id)
)ENGINE=InnoDB DEFAULT CHARACTER SET=UTF8MB4 COLLATE=utf8mb4_0900_ai_ci COMMENT '群组信息表';


-- 群组成员表
CREATE TABLE IF NOT EXISTS `group_members`(
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  group_id BIGINT UNSIGNED NOT NULL COMMENT '群id',
  user_id BIGINT UNSIGNED NOT NULL COMMENT '用户id',
  user_role TINYINT NOT NULL DEFAULT 0 COMMENT '用户权限:0普通;1管理员;2群主',
  user_status TINYINT NOT NULL DEFAULT 0 COMMENT '用户状态:0活跃;1退群;2被踢',
  joined_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入群时间',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '群组成员信息更新时间',
  PRIMARY KEY (id),
  UNIQUE KEY `uk_group_user` (group_id,user_id),
  -- 查询某个群里所有活跃的用户
  KEY `idx_group_status` (group_id,user_status),
  -- 查询某个用户的所有活跃的群
  KEY `idx_user_status` (user_id,user_status)
)ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT '群组成员信息表';

-- 群组聊天信息表
CREATE TABLE IF NOT EXISTS `group_messages`(
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '数据库自增主键',
  -- 离线消息拉取，由客户端上传其每个group的最后一条持久化消息msg_id
  msg_id BIGINT NOT NULL COMMENT '消息ID,服务器端接收时产生',
  client_msg_id BIGINT NOT NULL COMMENT '客户端发送消息ID',
  from_user_id BIGINT NOT NULL COMMENT '发送方用户ID',
  group_id BIGINT UNSIGNED NOT NULL COMMENT '群id',
  content TEXT  NOT NULL COMMENT '消息内容，多媒体(图片/文件等)存引用',
  msg_type TINYINT NOT NULL  DEFAULT 1 COMMENT '消息类型',
  `status` TINYINT NOT NULL DEFAULT 0 COMMENT '消息状态，比如：正常、删除、撤回',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '服务端写入时间',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '服务端更新时间',
  PRIMARY KEY (id),
  UNIQUE KEY uk_msg_id (msg_id),
  -- 客户端重试去重
  UNIQUE KEY uk_from_client_msg (from_user_id, group_id,client_msg_id),
  KEY idx_from_user_created (from_user_id, created_at, id),
  KEY idx_group_created (group_id, created_at, id),
  -- 拉取离线群组消息的索引:
  KEY idx_group_msgid (group_id,msg_id)
)ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT '群组聊天消息表';