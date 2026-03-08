package logic

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"likegochat/internal/common"
	authpb "likegochat/internal/common/proto/authpb"
)

// AuthServer 是 gRPC 的认证服务实现。
// 它对应 proto 里的 AuthService，负责处理：
// 1. Register：注册
// 2. Login：登录
// 3. Verify：校验 access token 是否仍然有效
type AuthServer struct {
	authpb.UnimplementedAuthServiceServer

	// Store 负责数据库访问：
	// - 查用户
	// - 创建用户
	// - 撤销旧 session
	// - 创建新 session
	// - 校验 session 是否仍有效
	Store *Store

	// JWT 是 token 工具：
	// - 登录成功时生成 access token
	// - Verify 时解析 token
	JWT *common.JWTManager

	// SessionTTL 是服务端 session 的有效期。
	// 注意它和 JWT 的 AccessTTL 不是一回事：
	//
	// - AccessTTL：access token 的有效期，通常较短，比如 15 分钟
	// - SessionTTL：服务端登录会话的有效期，通常较长，比如 30 天
	//
	// 为什么要分成两个时间：
	// - access token 短期，降低泄露风险
	// - session 较长期，用来表示“这次登录”在服务端是否仍然有效
	SessionTTL time.Duration
}

// Register 处理注册逻辑。
//
// 整体流程：
// 1. 校验用户名和密码是否为空
// 2. 用 bcrypt 生成密码哈希
// 3. 把用户名和密码哈希写入 users 表
//
// 关键原则：
// - 永远不存明文密码
// - 数据库里存的是 password_hash
func (a *AuthServer) Register(ctx context.Context, req *authpb.RegisterRequest) (*authpb.RegisterReply, error) {
	// 最基础的参数校验：用户名和密码不能为空
	if req.GetUsername() == "" || req.GetPassword() == "" {
		return nil, errors.New("username/password required")
	}

	// bcrypt.GenerateFromPassword 会把明文密码转换成不可逆的哈希字符串。
	//
	// 这里使用 bcrypt.DefaultCost，适合学习项目和一般场景。
	// 这样数据库里保存的是 hash，而不是明文密码。
	//
	// 举例：
	// 明文密码：123456
	// 存库后可能类似：
	// $2a$10$...
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	// 调用 store 把用户写入数据库
	id, err := a.Store.CreateUser(ctx, req.Username, string(hash))
	if err != nil {
		return nil, err
	}

	// 注册成功后返回 user_id
	return &authpb.RegisterReply{UserId: id}, nil
}

// Login 处理登录逻辑。
//
// 整体流程：
// 1. 校验用户名和密码是否为空
// 2. 根据用户名查用户
// 3. 用 bcrypt 校验密码是否正确
// 4. 开启事务
// 5. 撤销当前用户旧的 active session（单端登录）
// 6. 创建新的 session
// 7. 提交事务
// 8. 生成 JWT access token 并返回
//
// 为什么要有事务：
// 单端登录的关键语义是：
// “旧 session 失效 + 新 session 生效”必须是一个原子操作。
// 如果其中一步成功、另一部失败，会导致会话状态不一致。
func (a *AuthServer) Login(ctx context.Context, req *authpb.LoginRequest) (*authpb.LoginReply, error) {
	// 参数校验：用户名和密码不能为空
	if req.GetUsername() == "" || req.GetPassword() == "" {
		return nil, errors.New("username/password required")
	}

	// 先根据用户名查用户。
	// 这里查出来的主要信息有：
	// - user id
	// - password_hash
	u, err := a.Store.GetUserByUsername(ctx, req.Username)
	if err != nil {
		// 在 database/sql 里，查不到通常是 sql.ErrNoRows
		// 在 GORM 里，查不到通常是 gorm.ErrRecordNotFound
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 为了安全，不要告诉前端“用户名不存在”
			// 统一返回 invalid credentials，避免泄露用户是否存在的信息
			return nil, errors.New("invalid credentials")
		}
		return nil, err
	}

	// bcrypt.CompareHashAndPassword 用来验证：
	// “用户输入的明文密码” 是否能匹配数据库中的 password_hash。
	//
	// 注意：这里并不是把输入密码加密后直接字符串比较，
	// 因为 bcrypt 的 hash 里包含 salt 等信息。
	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(req.Password)); err != nil {
		return nil, errors.New("invalid credentials")
	}

	// 到这里说明：用户名存在，且密码正确。
	// 接下来进入“单端登录”的核心逻辑：
	//
	// 旧端失效 + 新端生效
	//
	// 这两步必须放在同一个事务中，保证原子性。
	tx := a.Store.DB.Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}

	// defer rollback 是一种常见写法：
	// - 如果后面任何一步 return 了，事务会被回滚
	// - 如果后面 Commit 成功，Rollback 不会再真正回滚已提交的事务
	//
	// 这样做的好处是：少写很多错误分支的回滚代码。
	defer tx.Rollback()

	// 第一步：撤销当前用户所有还处于 active 状态的 session
	//
	// 这里是“单端登录”的关键：
	// 新登录时，把旧登录对应的 session 全部标记 revoked。
	// 以后旧 token 即使签名正确，也会因为 session 已撤销而 Verify 失败。
	if err := a.Store.RevokeActiveSessions(ctx, tx, u.ID); err != nil {
		return nil, err
	}

	// 为这次新登录生成一个全新的 session_id。
	//
	// 这个 session_id 会同时：
	// 1. 存到 user_sessions 表里
	// 2. 写到 JWT 的 sid(claim) 里
	//
	// 这样 Verify 时可以做到：
	// “JWT 解析通过” + “session 仍有效” 才算真正登录有效。
	sessionID := uuid.NewString()

	// 计算服务端 session 的过期时间
	sessionExpires := time.Now().Add(a.SessionTTL)

	// 第二步：创建新的 session
	if err := a.Store.CreateSession(ctx, tx, u.ID, sessionID, sessionExpires, req.GetIp(), req.GetUserAgent()); err != nil {
		return nil, err
	}

	// 提交事务。
	//
	// 这里要注意：GORM 的 Commit() 返回的是 *gorm.DB，
	// 所以要检查的是 .Error
	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	// 事务提交成功后，说明：
	// - 旧 session 已撤销
	// - 新 session 已创建
	//
	// 接下来生成 access token（JWT）。
	//
	// 这个 token 通常是给客户端后续请求携带的，时间较短。
	// token 中会包含：
	// - sub：user_id
	// - sid：session_id
	// - exp：过期时间
	access, expiresIn, err := a.JWT.SignAccessToken(u.ID, sessionID)
	if err != nil {
		return nil, err
	}

	// 返回登录结果：
	// - user_id
	// - access_token
	// - expires_in（秒）
	return &authpb.LoginReply{
		UserId:      u.ID,
		AccessToken: access,
		ExpiresIn:   expiresIn,
	}, nil
}

// Verify 校验 access token 是否仍然有效。
//
// 注意：这里不是“只验 JWT 签名”。
// 真正的校验流程是两步：
//
// 1. 解析 JWT：
//   - 签名是否正确
//   - token 是否过期
//   - claims 是否合法
//
// 2. 校验服务端 session：
//   - sid 对应的 session 是否还存在
//   - 是否已被撤销（revoked_at）
//   - 是否已过服务端 session 有效期
//
// 为什么必须做第 2 步：
// 因为 JWT 本身是无状态的，签发后在过期前都可能看起来合法。
// 如果不查 session 表，就无法实现：
// - 单端登录踢旧端
// - 主动登出
// - 服务端强制失效某次登录
func (a *AuthServer) Verify(ctx context.Context, req *authpb.VerifyRequest) (*authpb.VerifyReply, error) {
	token := req.GetAccessToken()
	if token == "" {
		return &authpb.VerifyReply{
			Ok:     false,
			Reason: "missing_token",
		}, nil
	}

	// 第一步：解析 JWT。
	//
	// 这一步会验证：
	// - token 格式
	// - 签名
	// - exp 是否过期
	claims, err := a.JWT.ParseAccessToken(token)
	if err != nil {
		return &authpb.VerifyReply{
			Ok:     false,
			Reason: "invalid_token",
		}, nil
	}

	// JWT 标准 claims 里的 sub（subject）这里约定存 user_id。
	// 它本质是字符串，所以这里要转回 int64。
	userID, err := strconv.ParseInt(claims.Subject, 10, 64)
	if err != nil {
		return &authpb.VerifyReply{
			Ok:     false,
			Reason: "bad_sub",
		}, nil
	}

	// sid 是我们自定义的 claim，表示这次登录对应的 session_id。
	// 没有 sid 的 token，无法和服务端 session 对应起来。
	if claims.SessionID == "" {
		return &authpb.VerifyReply{
			Ok:     false,
			Reason: "missing_sid",
		}, nil
	}

	// 第二步：查数据库，确认这个 session 当前仍然有效。
	//
	// 这是 JWT + Session 组合的核心：
	// - JWT 负责“客户端携带凭证”
	// - Session 负责“服务端控制该凭证是否仍有效”
	//
	// 如果用户在别处重新登录，
	// 旧 session 会被 RevokeActiveSessions 撤销，
	// 那么这里就会返回 false，实现“踢旧端”。
	ok, err := a.Store.IsSessionActive(ctx, userID, claims.SessionID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &authpb.VerifyReply{
			Ok:     false,
			Reason: "revoked_or_expired",
		}, nil
	}

	// 两步都通过，说明：
	// - token 本身合法
	// - 这次登录对应的 session 仍然有效
	return &authpb.VerifyReply{
		Ok:     true,
		UserId: userID,
	}, nil
}
