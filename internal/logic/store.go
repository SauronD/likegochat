package logic

import (
	"context"
	"database/sql"
	"time"
)

type Store struct {
	DB *sql.DB
}

type User struct {
	ID           int64
	Username     string
	PasswordHash string
}

func (s *Store) CreateUser(ctx context.Context, username, passwordHash string) (int64, error) {
	res, err := s.DB.ExecContext(ctx,
		`INSERT INTO users(username, password_hash) VALUES(?, ?)`,
		username, passwordHash,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	u := &User{}
	err := s.DB.QueryRowContext(ctx,
		`SELECT id, username, password_hash FROM users WHERE username=?`,
		username,
	).Scan(&u.ID, &u.Username, &u.PasswordHash)
	if err != nil {
		return nil, err
	}
	return u, nil
}

// 单端登录：撤销旧会话（revoked_at=NULL）
func (s *Store) RevokeActiveSessions(ctx context.Context, tx *sql.Tx, userID int64) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE user_sessions SET revoked_at=NOW()
		  WHERE user_id=? AND revoked_at IS NULL`,
		userID,
	)
	return err
}

func (s *Store) CreateSession(ctx context.Context, tx *sql.Tx, userID int64, sessionID string, expiresAt time.Time, ip, ua string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO user_sessions(user_id, session_id, expires_at, ip, user_agent)
		 VALUES(?, ?, ?, ?, ?)`,
		userID, sessionID, expiresAt, ip, ua,
	)
	return err
}

// Verify 时检查 session 是否仍有效（解决 JWT 无法撤销的问题）
func (s *Store) IsSessionActive(ctx context.Context, userID int64, sessionID string) (bool, error) {
	var cnt int
	err := s.DB.QueryRowContext(ctx,
		`SELECT COUNT(1)
		   FROM user_sessions
		  WHERE user_id=? AND session_id=? AND revoked_at IS NULL AND expires_at > NOW()`,
		userID, sessionID,
	).Scan(&cnt)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}
