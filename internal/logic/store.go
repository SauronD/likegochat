package logic

import (
	"context"
	"time"

	"gorm.io/gorm"
)

type Store struct {
	DB *gorm.DB
}

type User struct {
	ID           int64  `gorm:"column:id;primaryKey;autoIncrement"`
	Username     string `gorm:"column:username"`
	PasswordHash string `gorm:"column:password_hash"`
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

// 单端登录：撤销旧会话（revoked_at = NULL）
func (s *Store) RevokeActiveSessions(ctx context.Context, tx *gorm.DB, userID int64) error {
	now := time.Now()
	// update user_sessions set revoked_at=now where user_id = userID AND revoked_at IS NULL;
	return tx.WithContext(ctx).
		Model(&UserSession{}).
		Where("user_id = ? AND revoked_at IS NULL", userID).
		Update("revoked_at", now).Error
}

func (s *Store) CreateSession(ctx context.Context, tx *gorm.DB, userID int64, sessionID string, expiresAt time.Time, ip, ua string) error {
	session := &UserSession{
		UserID:    userID,
		SessionID: sessionID,
		ExpiresAt: expiresAt,
		IP:        ip,
		UserAgent: ua,
	}
	// sql:
	// insert into user_sessions (username,password_hash) values
	// (session.UserID,session.SessionID,session.ExpiresAt,session.IP,session.UserAgent);
	return tx.WithContext(ctx).Create(session).Error
}

// Verify 时检查 session 是否仍有效（解决 JWT 无法撤销的问题）
func (s *Store) IsSessionActive(ctx context.Context, userID int64, sessionID string) (bool, error) {
	var cnt int64

	err := s.DB.WithContext(ctx).
		Model(&UserSession{}).
		Where("user_id = ? AND session_id = ? AND revoked_at IS NULL AND expires_at > ?", userID, sessionID, time.Now()).
		Count(&cnt).Error
	if err != nil {
		return false, err
	}

	return cnt > 0, nil
}
