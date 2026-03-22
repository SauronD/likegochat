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

-- 会话表：实现“单端登录”的核心
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

CREATE TABLE IF NOT EXISTS messages(
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '数据库自增主键',
  msg_id BIGINT NOT NULL COMMENT '消息ID,服务器端接收时产生',
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
  -- 客户端重试去重（可选）
  UNIQUE KEY uk_from_client_msg (from_user_id, client_msg_id),
  KEY idx_to_user_created (to_user_id,created_at,id),
  KEY idx_from_user_created (from_user_id, created_at, id),
  -- 双人会话查询（不建会话表时常用）  
  KEY idx_pair_created (from_user_id, to_user_id, created_at, id)

)ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT '单人聊天信息表';