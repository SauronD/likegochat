package logic

import (
	"context"
	"errors"
	"fmt"
	"likegochat/internal/common"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

const (
	redisSessPrefix string = "sess:"
	redisUserPrefix string = "user_sess:"
	// group的所有成员，用于小群聊天的鉴权和小群在线用户的筛选
	redisGroupUserPrefix string = "group_members:"
)

type Store struct {
	// mysql数据库连接
	DB *gorm.DB
	// redis连接
	RDB *redis.Client
	// session在服务器端存活时间
	SessionTTL time.Duration
	// 收敛并发向MySQL的请求
	sg singleflight.Group
}

func (s *Store) CreateUser(ctx context.Context, username, passwordHash string) (int64, error) {
	u := &common.User{
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

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*common.User, error) {
	u := &common.User{}
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
	return redisSessPrefix + sessionID
}

// Redis key:
// user_sess:<user_id> -> session_id
func userSessionKey(userID int64) string {
	return redisUserPrefix + strconv.FormatInt(userID, 10)
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
	pipe := s.RDB.TxPipeline()
	pipe.Set(ctx, userSessionKey(userID), sessionID, s.SessionTTL)
	pipe.Set(ctx, sessionKey(sessionID), strconv.FormatInt(userID, 10), s.SessionTTL)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	return nil
}

// 登录时撤销旧session，创建一个新session：
func (s *Store) RefreshSession(ctx context.Context, userID int64, sessionID string) error {
	// Lua脚本实现原子操作：
	// KEYS[1]: user_sid_key (存储 user -> sid)
	// KEYS[2]: sid_info_prefix (sid -> userId 的前缀)
	// ARGV[1]: new_sid
	// ARGV[2]: user_id_str
	// ARGV[3]: ttl_seconds
	script := `
        -- 1.查找并删除旧的session->userid
        local oldSid = redis.call("get", KEYS[1])
        if oldSid then
            redis.call("del", KEYS[2] .. oldSid)
        end
        
        -- 2.直接覆盖新的user->sid映射
        redis.call("set", KEYS[1], ARGV[1], "EX", ARGV[3])
        
        -- 3.直接覆盖新的sid->user映射
        redis.call("set", KEYS[2] .. ARGV[1], ARGV[2], "EX", ARGV[3])
        
        return 1
    `
	return s.RDB.Eval(ctx, script,
		[]string{userSessionKey(userID), redisSessPrefix},
		sessionID,
		strconv.FormatInt(userID, 10),
		int(s.SessionTTL.Seconds()),
	).Err()
}

// 检查session是否有效
func (s *Store) IsSessionActive(ctx context.Context, sessionID string) (int64, error) {
	val, err := s.RDB.Get(ctx, sessionKey(sessionID)).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, errors.New("invalid session")
		}
		return 0, err
	}
	userID, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}
	activeSessionID, err := s.RDB.Get(ctx, userSessionKey(userID)).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, errors.New("session expired or revoked")
		}
		return 0, err
	}
	if activeSessionID != sessionID {
		return 0, errors.New("session revoked by another device")
	}
	return userID, nil
}

// 退出登录：删除session
func (s *Store) DeleteSession(ctx context.Context, sessionID string) error {

	val, err := s.RDB.Get(ctx, sessionKey(sessionID)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	userID, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	// 登出的两次删除操作也必须是原子性的：
	// KEYS[1]: session_id_key
	// KEYS[2]: user_sid_key
	// ARGV[1]: 当前请求删除的 sessionID
	script := `
        -- 先删掉当前session，表示当前登录状态不存在
        redis.call("del", KEYS[1])
        
        -- 检查当前user指向的session是否还是当前session
        -- 如果是，说明没有其他设备抢占，安全删除user映射，否则不做处理
        local currentSid = redis.call("get", KEYS[2])
        if currentSid == ARGV[1] then
            redis.call("del", KEYS[2])
        end
        return 1
    `

	return s.RDB.Eval(ctx, script,
		[]string{sessionKey(sessionID), userSessionKey(userID)},
		sessionID,
	).Err()
}

// 查询userID是否在groupID群里，注意
func (s *Store) IsGroupMember(ctx context.Context, groupID, userID int64) (bool, error) {
	key := fmt.Sprintf("%s%d", redisGroupUserPrefix, groupID)

	// 检查redis中数据是否存在
	isMember, err := s.RDB.SIsMember(ctx, key, userID).Result()
	if err != nil {
		return false, err
	}
	if isMember {
		return true, nil
	}

	// 确认是因为不在小群里，还是因为没同步到redis
	exists, err := s.RDB.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if exists > 0 {
		// 缓存键存在（可能是真实成员，或者是-1占位符）说明不在小群里
		return false, nil
	}

	// redis中没有缓存，到MySQL中查询后添加到redis中
	flightKey := fmt.Sprintf("fallback_group_member_%d", groupID)
	// 用singleflight收敛并发查询MySQL操作
	v, err, _ := s.sg.Do(flightKey, func() (interface{}, error) {
		var members []int64
		dbErr := s.DB.WithContext(ctx).Table("group_members").
			Select("user_id").
			Where("group_id = ? AND user_status = 0", groupID).
			Scan(&members).Error
		if dbErr != nil {
			return nil, dbErr
		}

		pipe := s.RDB.TxPipeline()
		pipe.Del(ctx, key)

		if len(members) == 0 {
			// 写入-1占位符，缓解缓存穿透，注意过期时间应该短一些
			pipe.SAdd(ctx, key, -1)
			pipe.Expire(ctx, key, 5*time.Minute)
		} else {
			args := make([]interface{}, 0, len(members))
			for _, id := range members {
				args = append(args, id)
			}
			pipe.SAdd(ctx, key, args...)
			pipe.Expire(ctx, key, 24*time.Hour)
		}

		if _, pipeErr := pipe.Exec(ctx); pipeErr != nil {
			// Redis写入失败仅打印日志，不阻断本次发信的真实性判断
			log.Printf("redis cache rebuild failed in IsGroupMember: %v", pipeErr)
		}

		// 将切片转化为map用于判断userID是否在群组中
		memberMap := make(map[int64]struct{}, len(members))
		for _, id := range members {
			memberMap[id] = struct{}{}
		}
		return memberMap, nil
	})

	if err != nil {
		return false, err
	}
	// 对每个并发协程判断其userID是否存在于小群中
	memberMap := v.(map[int64]struct{})
	_, ok := memberMap[userID]

	return ok, nil
}
