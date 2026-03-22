package connect

import (
	"container/list"
	"sync"
)

// roomNode只保存广播必需信息：用户ID + ws发送通道。
type roomNode struct {
	userID int64
	send   chan []byte
}

// 每个bucket内的一个room
type Room struct {
	mu sync.RWMutex
	// element.Value: *roomNode
	members *list.List
	// userID -> element
	index map[int64]*list.Element
}

func NewRoom() *Room {
	return &Room{
		members: list.New(),
		index:   make(map[int64]*list.Element),
	}
}

// Join 加入房间；同用户重连时替换 send 通道。
func (r *Room) Join(userID int64, send chan []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if elem, ok := r.index[userID]; ok {
		node := elem.Value.(*roomNode)
		node.send = send
		return
	}

	elem := r.members.PushBack(&roomNode{
		userID: userID,
		send:   send,
	})
	r.index[userID] = elem
}

// Leave 退出房间，返回房间是否为空（便于上层做回收）。
func (r *Room) Leave(userID int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	elem, ok := r.index[userID]
	if !ok {
		return r.members.Len() == 0
	}

	r.members.Remove(elem)
	delete(r.index, userID)
	return r.members.Len() == 0
}

// Broadcast 给房间内成员广播，默认排除发送者。
// 非阻塞发送，通道满时跳过该连接，避免阻塞整个房间。
func (r *Room) Broadcast(fromUserID int64, payload []byte) int {
	r.mu.RLock()
	targets := make([]chan []byte, 0, r.members.Len())
	for e := r.members.Front(); e != nil; e = e.Next() {
		node := e.Value.(*roomNode)
		if node.userID == fromUserID {
			continue
		}
		targets = append(targets, node.send)
	}
	r.mu.RUnlock()

	sent := 0
	for _, ch := range targets {
		select {
		case ch <- payload:
			sent++
		default:
			// 慢连接保护：通道满则丢弃当前推送，防止拖垮广播路径
		}
	}
	return sent
}
