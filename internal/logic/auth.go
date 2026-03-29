package logic

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"likegochat/internal/common"
	authpb "likegochat/internal/common/proto/authpb"
	"likegochat/internal/common/proto/chatpb"
)

// AuthServer 是 gRPC 的认证服务实现。
// 它对应 proto 里的 AuthService，负责处理：
// 1. Register：注册
// 2. Login：登录
// 3. Verify：校验 access token 是否仍然有效
type AuthServer struct {
	authpb.UnimplementedAuthServiceServer
	chatpb.UnimplementedGroupServiceServer

	// Store 负责数据库/Redis访问：
	// - 查用户
	// - 创建用户
	// - 撤销旧 session
	// - 创建新 session
	// - 校验 session 是否仍有效
	Store *Store
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
	// 用户名和密码不能为空
	if req.GetUsername() == "" || req.GetPassword() == "" {
		return nil, errors.New("username/password required")
	}

	// bcrypt哈希算法，内部随机加盐，工作因子10
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	// 写入数据库
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

	if req.GetUsername() == "" || req.GetPassword() == "" {
		return nil, errors.New("empty username or password")
	}

	user := &common.User{}

	err := a.Store.DB.WithContext(ctx).Where("username = ?", req.Username).Take(user).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("invalid password or username")
		}
		return nil, err
	}
	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password))
	if err != nil {
		return nil, errors.New("invalid password or username")
	}
	// 撤销session和创建新session的操作必须是一个原子操作，否则有如下情况：
	// 1、一个客户端2正在登录，在删除了旧登录的session，
	// 2、另外一个客户端1也在登录，并提前一步创建好了session1，Set userid->sessionid1
	// 3、客户端2删除了userid->sessionid1，导致客户端1瞬间掉线
	sessionID := uuid.New().String()
	err = a.Store.RefreshSession(ctx, user.ID, sessionID)
	if err != nil {
		return nil, fmt.Errorf("refresh seesion faild:%s", err.Error())
	}
	// 查询用户加入的小组：登录是低频操作，可以接受MySQL查询
	groups, err := a.Store.ListUserSmallGroups(ctx, user.ID)
	if err != nil {
		return nil, fmt.Errorf("load small group failed:%s", err.Error())
	}

	smallGroups := make([]*authpb.SmallGroupInfo, 0, len(groups))
	for i := range groups {
		smallGroups = append(smallGroups, &authpb.SmallGroupInfo{
			GroupId:     groups[i].GroupID,
			GroupName:   groups[i].GroupName,
			MemberCount: groups[i].MemberCount,
		})
	}
	return &authpb.LoginReply{
		UserId:      user.ID,
		SessionId:   sessionID,
		SmallGroups: smallGroups,
	}, nil
}

// Verify 校验sesssion是否仍然有效。
func (a *AuthServer) Verify(ctx context.Context, req *authpb.VerifyRequest) (*authpb.VerifyReply, error) {

	if req.GetSessionId() == "" {
		return nil, errors.New("empty session")
	}

	if userID, err := a.Store.IsSessionActive(ctx, req.SessionId); err == nil {
		return &authpb.VerifyReply{
			UserId: userID,
		}, nil
	} else {
		return nil, err
	}

}

func (a *AuthServer) Logout(ctx context.Context, req *authpb.LogoutRequest) (*authpb.LogoutReply, error) {
	if req.GetSessionId() == "" {
		return nil, errors.New("empty sessionID")
	}

	err := a.Store.DeleteSession(ctx, req.SessionId)
	if err != nil {
		return nil, err
	}
	return &authpb.LogoutReply{
		Ok: true,
	}, nil
}

type UserSmallGroup struct {
	GroupID     int64  `gorm:"column:group_id"`
	GroupName   string `gorm:"column:group_name"`
	MemberCount int32  `gorm:"column:member_count"`
}

func (s *Store) ListUserSmallGroups(ctx context.Context, userID int64) ([]UserSmallGroup, error) {
	var groups []UserSmallGroup
	// err := s.DB.WithContext(ctx).
	// 	Table("group_members AS gm").
	// 	Select("g.id AS group_id, g.group_name, g.member_count").
	// 	Joins("JOIN `groups` AS g ON g.id = gm.group_id").
	// 	Where("gm.user_id = ? AND gm.user_status = 0 AND g.group_status = 0", userID).
	// 	Order("gm.joined_at DESC").
	// 	Scan(&groups).Error

	err := s.DB.Debug().WithContext(ctx).
		Table("group_members").
		Joins("LEFT JOIN `groups` ON group_members.group_id=`groups`.id").
		Select("group_members.group_id,`groups`.group_name,`groups`.member_count").
		Where("group_members.id=? AND `groups`.group_status=? AND group_members.user_status=?", userID, 0, 0).
		Order("group_members.joined_at DESC").
		Scan(&groups).Error

	if err != nil {
		return nil, err
	}
	return groups, nil
}
