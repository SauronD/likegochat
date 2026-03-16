package connect

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const UserServerKeyPrefix = "user_server:"

type Registry struct {
	RDB      *redis.Client
	ServerID string // 例如 "192.168.1.10:9001"
}

// RegisterUser 用户上线，绑定物理节点
func (r *Registry) RegisterUser(ctx context.Context, userID int64) error {
	key := fmt.Sprintf("%s%d", UserServerKeyPrefix, userID)
	// 设置过期时间防止僵尸数据
	return r.RDB.Set(ctx, key, r.ServerID, 24*time.Hour).Err()
}

// UnregisterUser 用户下线，解绑物理节点
func (r *Registry) UnregisterUser(ctx context.Context, userID int64) error {
	key := fmt.Sprintf("%s%d", UserServerKeyPrefix, userID)
	return r.RDB.Del(ctx, key).Err()
}
