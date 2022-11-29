package comet

import (
	"sync"

	"github.com/Terry-Mao/goim/api/protocol"
	"github.com/Terry-Mao/goim/internal/comet/errors"
)

// Logic Server 通过 RPC 调用，将广播的消息发给 Room.Push, 数据会被暂存在 vers, ops, msgs 里，
// 每个 Room 在初始化时会开启一个 groutine 用来处理暂存的消息，达到 Batch Num 数量或是延迟一定时间后，将消息批量 Push 到 Channel 消息通道。

// Room is a room and store channel room info.
type Room struct {
	ID        string // 房间号
	rLock     sync.RWMutex
	next      *Channel // 该房间的所有客户端的Channel  是一个双向链表，复杂度为o(1)，效率比较高。
	drop      bool     // 标示房间是否存活
	Online    int32    // 房间的channel数量，即房间的在线用户的多少  dirty read is ok
	AllOnline int32
}

// NewRoom new a room struct, store channel room info.
func NewRoom(id string) (r *Room) {
	r = new(Room)
	r.ID = id
	r.drop = false
	r.next = nil
	r.Online = 0
	return
}

// Put put channel into the room.
func (r *Room) Put(ch *Channel) (err error) {
	r.rLock.Lock()
	if !r.drop {
		if r.next != nil {
			r.next.Prev = ch
		}
		ch.Next = r.next
		ch.Prev = nil
		r.next = ch // insert to header
		r.Online++
	} else {
		err = errors.ErrRoomDroped
	}
	r.rLock.Unlock()
	return
}

// Del delete channel from the room.
func (r *Room) Del(ch *Channel) bool {
	r.rLock.Lock()
	if ch.Next != nil {
		// if not footer
		ch.Next.Prev = ch.Prev
	}
	if ch.Prev != nil {
		// if not header
		ch.Prev.Next = ch.Next
	} else {
		r.next = ch.Next
	}
	ch.Next = nil
	ch.Prev = nil
	r.Online--
	r.drop = r.Online == 0
	r.rLock.Unlock()
	return r.drop
}

// Push push msg to the room, if chan full discard it.
// 遍历房间内的channel的链表，将消息放到channel的发送队列中，又回到了channel消息单发的逻辑。
func (r *Room) Push(p *protocol.Proto) {
	r.rLock.RLock()
	for ch := r.next; ch != nil; ch = ch.Next {
		_ = ch.Push(p)
	}
	r.rLock.RUnlock()
}

// Close close the room.
func (r *Room) Close() {
	r.rLock.RLock()
	for ch := r.next; ch != nil; ch = ch.Next {
		ch.Close()
	}
	r.rLock.RUnlock()
}

// OnlineNum the room all online.
func (r *Room) OnlineNum() int32 {
	if r.AllOnline > 0 {
		return r.AllOnline
	}
	return r.Online
}
