package connect

import (
	"encoding/binary"
	"sync"

	"github.com/dgryski/go-farm"
)

const defaultBucketNum = 64

var DefaultRoomManager = NewRoomManager(defaultBucketNum)

type RoomBucket struct {
	mu    sync.RWMutex
	rooms map[int64]*Room
}

type RoomManager struct {
	// 保存RoomBucket而不是*RoomBucket，是为了利用CPU的空间局部性优化
	buckets []RoomBucket
}

func NewRoomManager(bucketNum int) *RoomManager {
	if bucketNum <= 0 {
		bucketNum = defaultBucketNum
	}
	buckets := make([]RoomBucket, bucketNum)
	for i := range buckets {
		buckets[i].rooms = make(map[int64]*Room)
	}
	return &RoomManager{buckets: buckets}
}

func (m *RoomManager) bucketByRoomID(roomID int64) *RoomBucket {
	// 1. 在栈内存上分配 8 字节空间 (零逃逸)
	var b [8]byte

	// 2. 底层内联转换：将 int64 写入字节数组 (物理效果等同于你之前的 8 行位移)
	binary.LittleEndian.PutUint64(b[:], uint64(roomID))

	// 3. 调用 FarmHash 极速算出 32 位散列值
	hashVal := farm.Hash32(b[:])

	// 4. 定位物理桶索引
	idx := int(hashVal % uint32(len(m.buckets)))

	return &m.buckets[idx]
}

func (m *RoomManager) getOrCreateRoom(roomID int64) *Room {
	b := m.bucketByRoomID(roomID)

	// 先读锁查
	b.mu.RLock()
	r, ok := b.rooms[roomID]
	b.mu.RUnlock()
	if ok {
		return r
	}

	// 未命中再写锁创建（双检）
	b.mu.Lock()
	defer b.mu.Unlock()
	if r, ok = b.rooms[roomID]; ok {
		return r
	}
	r = NewRoom()
	b.rooms[roomID] = r
	return r
}

func (m *RoomManager) JoinRoom(groupID, userID int64, send chan []byte) {
	r := m.getOrCreateRoom(groupID)
	r.Join(userID, send)
}

func (m *RoomManager) LeaveRoom(groupID, userID int64) {
	b := m.bucketByRoomID(groupID)

	b.mu.RLock()
	r, ok := b.rooms[groupID]
	b.mu.RUnlock()
	if !ok {
		return
	}

	empty := r.Leave(userID)
	if !empty {
		return
	}

	// 房间空了再尝试删除（双检，防止并发 Join）
	b.mu.Lock()
	defer b.mu.Unlock()
	r2, ok := b.rooms[groupID]
	if !ok {
		return
	}
	if r2 != r {
		return
	}
	// 再确认一次是否仍为空
	r2.mu.RLock()
	isEmpty := r2.members.Len() == 0
	r2.mu.RUnlock()
	if isEmpty {
		delete(b.rooms, groupID)
	}
}

func (m *RoomManager) BroadcastRoom(roomID, fromUserID int64, payload []byte) int {
	b := m.bucketByRoomID(roomID)

	b.mu.RLock()
	r, ok := b.rooms[roomID]
	b.mu.RUnlock()
	if !ok {
		return 0
	}
	return r.Broadcast(fromUserID, payload)
}
