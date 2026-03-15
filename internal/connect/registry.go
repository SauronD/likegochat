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
	ServerID string // 当前 Connect 节点的 gRPC 地址，例如 "10.0.0.5:9000"
}

func (r *Registry) RegisterUser(ctx context.Context, userID int64) error {
	key := fmt.Sprintf("%s%d", UserServerKeyPrefix, userID)
	// 设置 24 小时过期时间，防止异常断线导致的脏数据
	return r.RDB.Set(ctx, key, r.ServerID, 24*time.Hour).Err()
}

func (r *Registry) UnregisterUser(ctx context.Context, userID int64) error {
	key := fmt.Sprintf("%s%d", UserServerKeyPrefix, userID)
	return r.RDB.Del(ctx, key).Err()
}
