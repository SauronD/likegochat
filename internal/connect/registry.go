package connect

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// 在线用户绑定的connect节点：用于消息推送和用户是否在线
	UserServerKeyPrefix = "user_server:"
	// 所有在线connect节点
	ConnectNodesKeyPrefix = "connect_nodes:"
)

type Registry struct {
	RDB *redis.Client
	// connect节点绑定的地址，用户和task层用这个地址来绑定一个connect节点
	ServerID string
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

// RegisterConnectNode connect节点启动，注册到redis set中
func (r *Registry) RegisterConnectNode(ctx context.Context) error {
	if r.ServerID == "" {
		return fmt.Errorf("empty server id")
	}
	return r.RDB.SAdd(ctx, ConnectNodesKeyPrefix, r.ServerID).Err()
}

// UnregisterConnectNode connect节点关闭，从redis set中移除
func (r *Registry) UnregisterConnectNode(ctx context.Context) error {
	if r.ServerID == "" {
		return nil
	}
	return r.RDB.SRem(ctx, ConnectNodesKeyPrefix, r.ServerID).Err()
}
