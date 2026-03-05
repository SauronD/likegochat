package logic

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"likegochat/internal/common"
	authpb "likegochat/internal/common/proto/authpb"
)

type AuthServer struct {
	authpb.UnimplementedAuthServiceServer

	Store      *Store
	JWT        *common.JWTManager
	SessionTTL time.Duration
}

func (a *AuthServer) Register(ctx context.Context, req *authpb.RegisterRequest) (*authpb.RegisterReply, error) {
	if req.GetUsername() == "" || req.GetPassword() == "" {
		return nil, errors.New("username/password required")
	}

	// 关键原理：bcrypt 生成不可逆 hash，存 hash 不存明文
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	id, err := a.Store.CreateUser(ctx, req.Username, string(hash))
	if err != nil {
		return nil, err
	}
	return &authpb.RegisterReply{UserId: id}, nil
}

func (a *AuthServer) Login(ctx context.Context, req *authpb.LoginRequest) (*authpb.LoginReply, error) {
	if req.GetUsername() == "" || req.GetPassword() == "" {
		return nil, errors.New("username/password required")
	}

	u, err := a.Store.GetUserByUsername(ctx, req.Username)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("invalid credentials")
		}
		return nil, err
	}

	// 关键原理：bcrypt Compare 用 hash 校验明文是否匹配
	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(req.Password)); err != nil {
		return nil, errors.New("invalid credentials")
	}

	// 关键原理：单端登录必须用事务保证一致性
	// revoke 旧 session + create 新 session，要么一起成功，要么一起失败
	tx, err := a.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := a.Store.RevokeActiveSessions(ctx, tx, u.ID); err != nil {
		return nil, err
	}

	sessionID := uuid.NewString()
	sessionExpires := time.Now().Add(a.SessionTTL)

	if err := a.Store.CreateSession(ctx, tx, u.ID, sessionID, sessionExpires, req.GetIp(), req.GetUserAgent()); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	// 关键原理：JWT 是 access token（短期），里面带 sub=user_id 和 sid=session_id
	access, expiresIn, err := a.JWT.SignAccessToken(u.ID, sessionID)
	if err != nil {
		return nil, err
	}

	return &authpb.LoginReply{
		UserId:      u.ID,
		AccessToken: access,
		ExpiresIn:   expiresIn,
	}, nil
}

func (a *AuthServer) Verify(ctx context.Context, req *authpb.VerifyRequest) (*authpb.VerifyReply, error) {
	token := req.GetAccessToken()
	if token == "" {
		return &authpb.VerifyReply{Ok: false, Reason: "missing_token"}, nil
	}

	claims, err := a.JWT.ParseAccessToken(token)
	if err != nil {
		return &authpb.VerifyReply{Ok: false, Reason: "invalid_token"}, nil
	}

	userID, err := strconv.ParseInt(claims.Subject, 10, 64)
	if err != nil {
		return &authpb.VerifyReply{Ok: false, Reason: "bad_sub"}, nil
	}
	if claims.SessionID == "" {
		return &authpb.VerifyReply{Ok: false, Reason: "missing_sid"}, nil
	}

	// 关键原理：JWT 通过 ≠ 当前 session 有效
	// 必须查 session 表：如果用户新登录，旧 sid 就会被 revoked，从而这里会失败（实现“踢旧端”）
	ok, err := a.Store.IsSessionActive(ctx, userID, claims.SessionID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &authpb.VerifyReply{Ok: false, Reason: "revoked_or_expired"}, nil
	}

	return &authpb.VerifyReply{Ok: true, UserId: userID}, nil
}
