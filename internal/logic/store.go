package logic

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type Store struct {
	// mysql数据库连接
	DB *gorm.DB
	// redis连接
	RDB *redis.Client
	// session在服务器端存活时间
	SessionTTL time.Duration
}

type User struct {
	ID           int64     `gorm:"column:id;primaryKey;autoIncrement"`
	Username     string    `gorm:"column:username"`
	PasswordHash string    `gorm:"column:password_hash"`
	CreatTime    time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (User) TableName() string {
	return "users"
}

type UserSession struct {
	ID        int64  `gorm:"column:id;primaryKey;autoIncrement"`
	UserID    int64  `gorm:"column:user_id"`
	SessionID string `gorm:"column:session_id"`
	// 登录时间，注意不能设置为指针，因为gorm插入时会换成NULL，和数据库NOT NULL冲突
	IssuedAt *time.Time `gorm:"column:issued_at;autoCreateTime"`
	// session过期时间
	ExpiresAt time.Time `gorm:"column:expires_at"`
	// session撤销时间，在数据库中可以为NULL，因此需要用指针来区别
	RevokedAt *time.Time `gorm:"column:revoked_at"`
	IP        string     `gorm:"column:ip"`
	UserAgent string     `gorm:"column:user_agent"`
}

// gorm结构体绑定表的方法：实现TableName()方法
func (UserSession) TableName() string {
	return "user_sessions"
}

func (s *Store) CreateUser(ctx context.Context, username, passwordHash string) (int64, error) {
	u := &User{
		Username:     username,
		PasswordHash: passwordHash,
	}
	// gorm创建数据：(*gorm.DB).Create(*any)
	// sql:insert into users values (username,password_hash) values (u.Username,u.PasswordHash);
	if err := s.DB.WithContext(ctx).Create(u).Error; err != nil {
		return 0, err
	}
	return u.ID, nil
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	u := &User{}
	// sql: select * from users where username= username;
	err := s.DB.WithContext(ctx).
		Where("username = ?", username).
		Take(u).Error
	if err != nil {
		return nil, err
	}
	return u, nil
}

const redisSessPrefix string = "sess:"
const redisUserPrefix string = "user_sess:"

// Redis key:
// sess:<session_id> -> user_id

func sessionKey(sessionID string) string {
	return redisSessPrefix + sessionID
}

// Redis key:
// user_sess:<user_id> -> session_id
func userSessionKey(userID int64) string {
	return redisUserPrefix + strconv.FormatInt(userID, 10)
}

// 单端登录：撤销用户的session
func (s *Store) RevokeActiveSessionForUser(ctx context.Context, userID int64) error {
	oldSid, err := s.RDB.Get(ctx, userSessionKey(userID)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	if err := s.RDB.Del(ctx, sessionKey(oldSid)).Err(); err != nil {
		return err
	}
	if err := s.RDB.Del(ctx, userSessionKey(userID)).Err(); err != nil {
		return err
	}
	return nil
}

// 创建一个用户的session
func (s *Store) CreateSession(ctx context.Context, userID int64, sessionID string) error {
	pipe := s.RDB.Pipeline()
	pipe.Set(ctx, userSessionKey(userID), sessionID, s.SessionTTL)
	pipe.Set(ctx, sessionKey(sessionID), strconv.FormatInt(userID, 10), s.SessionTTL)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	return nil
}

// 登录时撤销旧session，创建一个新session：
func (s *Store) RefreshSession(ctx context.Context, userID int64, sessionID string) error {
	// Lua脚本实现原子操作：
	// KEYS[1]: user_sid_key (存储 user -> sid)
	// KEYS[2]: sid_info_prefix (sid -> userId 的前缀)
	// ARGV[1]: new_sid
	// ARGV[2]: user_id_str
	// ARGV[3]: ttl_seconds
	script := `
        -- 1. 查找并删除旧的session->userid
        local oldSid = redis.call("get", KEYS[1])
        if oldSid then
            redis.call("del", KEYS[2] .. oldSid)
        end
        
        -- 2. 设置新的 user -> sid 映射
        redis.call("set", KEYS[1], ARGV[1], "EX", ARGV[3])
        
        -- 3. 设置新的 sid -> userId 映射
        redis.call("set", KEYS[2] .. ARGV[1], ARGV[2], "EX", ARGV[3])
        
        return 1
    `
	return s.RDB.Eval(ctx, script,
		[]string{userSessionKey(userID), redisSessPrefix},
		sessionID,
		strconv.FormatInt(userID, 10),
		int(s.SessionTTL.Seconds()),
	).Err()
}

// 检查session是否有效
func (s *Store) IsSessionActive(ctx context.Context, sessionID string) (int64, error) {
	val, err := s.RDB.Get(ctx, sessionKey(sessionID)).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, errors.New("invalid session")
		}
		return 0, err
	}
	userID, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}
	activeSessionID, err := s.RDB.Get(ctx, userSessionKey(userID)).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, errors.New("session expired or revoked")
		}
		return 0, err
	}
	if activeSessionID != sessionID {
		return 0, errors.New("session revoked by another device")
	}
	return userID, nil
}

// 退出登录：删除session
func (s *Store) DeleteSession(ctx context.Context, sessionID string) error {

	val, err := s.RDB.Get(ctx, sessionKey(sessionID)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	userID, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	// 登出的两次删除操作也必须是原子性的：
	// KEYS[1]: session_id_key
	// KEYS[2]: user_sid_key
	// ARGV[1]: 当前请求删除的 sessionID
	script := `
        -- 无论如何，先删掉具体的 session 数据
        redis.call("del", KEYS[1])
        
        -- 检查当前 user 指向的 session 是否还是我这个 session
        -- 如果是，说明没有其他设备抢占，安全删除 user 映射
        local currentSid = redis.call("get", KEYS[2])
        if currentSid == ARGV[1] then
            redis.call("del", KEYS[2])
        end
        return 1
    `

	return s.RDB.Eval(ctx, script,
		[]string{sessionKey(sessionID), userSessionKey(userID)},
		sessionID,
	).Err()
}
func (s *Store) IsGroupMember(ctx context.Context, groupID, userID int64) (bool, error) {
	key := fmt.Sprintf("group_members:%d", groupID)
	return s.RDB.SIsMember(ctx, key, userID).Result()
}
