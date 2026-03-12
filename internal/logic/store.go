package logic

import (
	"context"
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

// Redis key:
// sess:<session_id> -> user_id

func sessionKey(sessionID string) string {
	return "sess:" + sessionID
}

// Redis key:
// user_sess:<user_id> -> session_id
func userSessionKey(userID int64) string {
	return "user_sess:" + strconv.FormatInt(userID, 10)
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

// 检查session是否有效
func (s *Store) IsSessionActive(ctx context.Context, sessionID string) (bool, int64, error) {
	val, err := s.RDB.Get(ctx, sessionKey(sessionID)).Result()
	if err != nil {
		if err == redis.Nil {
			return false, 0, nil
		}
		return false, 0, err
	}
	userID, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return false, 0, err
	}

	// 再次查询userID->sessionID是否正确：
	curSID, err := s.RDB.Get(ctx, userSessionKey(userID)).Result()
	if err != nil {
		if err == redis.Nil {
			return false, 0, nil
		}
		return false, 0, err
	}
	if curSID != sessionID {
		return false, 0, nil
	}
	return true, userID, nil
}

// 登出：删除session
func (s *Store) DeleteSession(ctx context.Context, sessionID string) error {

	val, err := s.RDB.Get(ctx, sessionKey(sessionID)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	userID, err := strconv.ParseInt(val, 10, 64)

	if err := s.RDB.Del(ctx, sessionKey(sessionID)).Err(); err != nil {
		return err
	}
	if err := s.RDB.Del(ctx, userSessionKey(userID)).Err(); err != nil {
		return err
	}
	return nil
}
