package logic

import (
	"context"
	"errors"
	"fmt"
	"likegochat/internal/common"
	"likegochat/internal/common/proto/authpb"
	"likegochat/internal/common/proto/chatpb"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (a *AuthServer) AddGroup(ctx context.Context, req *authpb.AddGroupRequest) (*authpb.AddGroupReply, error) {
	// TODU:实现加入群聊的功能，注意：1、检查user是否已经是group成员；2、若不是，写MySQL并redis;3、返回；注意并发情况的性能与容错
	// mysql事务：检查+写入
	groupID := req.GetGroupId()
	userID := req.GetUserId()
	now := time.Now()
	// 写入redis
	// 是否需要回写 Redis（幂等场景也尝试回写，修复缓存漂移）
	inserted := false

	err := a.Store.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1) 锁住群记录，确认群存在且可用
		var grp common.Group
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND group_status = 0", groupID).
			Take(&grp).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return status.Error(codes.NotFound, "group not found or inactive")
			}
			return status.Errorf(codes.Internal, "query group failed: %v", err)
		}
		// 2) 直接插入成员关系；若已存在则不做任何更新（不改活跃状态）
		gm := &common.GroupMember{
			GroupID:    groupID,
			UserID:     userID,
			UserRole:   0,
			UserStatus: 0,
			JoinAt:     &now,
			UpdateAt:   &now,
		}

		res := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "group_id"}, {Name: "user_id"}},
			DoNothing: true,
		}).Create(gm)
		if res.Error != nil {
			return status.Errorf(codes.Internal, "insert group member failed: %v", res.Error)
		}

		// 已存在成员关系：直接返回，不修改 user_status
		if res.RowsAffected == 0 {
			return nil
		}

		inserted = true

		// 3) 仅在新插入时递增群人数
		if err := tx.Model(&common.Group{}).
			Where("id = ?", groupID).
			UpdateColumn("member_count", gorm.Expr("member_count + 1")).Error; err != nil {
			return status.Errorf(codes.Internal, "increase member_count failed: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 4) 仅新加入时回写 Redis
	if inserted {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		a.doubleDeleteGroupMembersCache(ctx, groupID)
		cancel()
	}

	return &authpb.AddGroupReply{Ok: true}, nil
}

func (a *AuthServer) doubleDeleteGroupMembersCache(ctx context.Context, groupID int64) error {
	key := fmt.Sprintf("%s%d", redisGroupUserPrefix, groupID)

	// 第一次删除：
	if err := a.Store.RDB.Del(ctx, key).Err(); err != nil {
		log.Printf("delayed cache 1st delete failed key=%s err=%v", key, err)
	}
	// 等待一段时间后的二次删除
	go func(key string) {
		timer := time.NewTimer(200 * time.Millisecond)
		defer timer.Stop()
		<-timer.C
		// 延迟异步操作需要独立于原请求的context
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		if err := a.Store.RDB.Del(ctx, key).Err(); err != nil {
			log.Printf("delayed cache 2nd delete failed key=%s err=%v", key, err)
		}
	}(key)
	return nil
}

func (a *AuthServer) RefreshGroupMembersCache(ctx context.Context, req *chatpb.GroupMembersRequest) (*chatpb.GroupMembersReply, error) {
	if req.GetGroupId() == 0 {
		return nil, fmt.Errorf("invalid groupID")
	}
	groupMembers := []int64{}
	// 查MySQL:
	err := a.Store.DB.Debug().WithContext(ctx).
		Table("group_members").
		Select("user_id").
		Where("group_id=? AND user_status=0", req.GroupId).
		Scan(&groupMembers).Error
	if err != nil {
		return nil, err
	}

	// 写redis:
	key := fmt.Sprintf("group_members:%d", req.GroupId)
	pipe := a.Store.RDB.Pipeline()
	if len(groupMembers) == 0 {
		// 强行在内存中写入一个 -1 的占位符，阻断后续回源
		pipe.Del(ctx, key)
		pipe.SAdd(ctx, key, -1)
		// 设置一个极短的过期时间（例如 5 分钟）。
		// 因为这可能是一个新群，只是碰巧还没人加，不需要锁死 24 小时
		pipe.Expire(ctx, key, 5*time.Minute)
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("redis write null cache failed: %v", err)
		}
		return &chatpb.GroupMembersReply{GroupMembers: []int64{}}, nil
	}

	args := make([]interface{}, 0, len(groupMembers))
	for _, id := range groupMembers {
		args = append(args, id)
	}
	// 并发情况下需要再删除一次保留了脏数据
	pipe.Del(ctx, key)
	pipe.SAdd(ctx, key, args...)
	pipe.Expire(ctx, key, 24*time.Hour)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	return &chatpb.GroupMembersReply{
		GroupMembers: groupMembers,
	}, nil

}

func (a *AuthServer) CreateGroup(ctx context.Context, req *authpb.CreateGroupRequest) (*authpb.CreateGroupReply, error) {

	if len(req.GroupName) == 0 || len(req.GroupName) > 30 {
		return nil, fmt.Errorf("invalid groupName")
	}
	if req.GetUserId() <= 0 {
		return nil, fmt.Errorf("invalid userid")
	}

	now := time.Now()

	group := &common.Group{
		Name:        req.GetGroupName(),
		Status:      0,
		OwnerUserID: req.UserId,
		MemberCount: 1,
		CreatedAt:   &now,
		UpdatedAt:   &now,
	}
	groupMember := &common.GroupMember{
		UserID:     req.UserId,
		UserRole:   2,
		UserStatus: 0,
		JoinAt:     &now,
		UpdateAt:   &now,
	}

	err := a.Store.DB.WithContext(ctx).Debug().Transaction(func(tx *gorm.DB) error {
		// 写入群信息表
		if err := tx.Debug().Table("groups").Create(group).Error; err != nil {
			return err
		}
		groupMember.GroupID = group.ID
		// 写入群-成员表
		if err := tx.Debug().Table("group_members").Create(groupMember).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &authpb.CreateGroupReply{GroupId: group.ID}, nil
}
