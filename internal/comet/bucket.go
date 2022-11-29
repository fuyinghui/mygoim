package comet

import (
	"sync"
	"sync/atomic"

	pb "github.com/Terry-Mao/goim/api/comet"
	"github.com/Terry-Mao/goim/api/protocol"
	"github.com/Terry-Mao/goim/internal/comet/conf"
)

// 房间信息推送流程：
//	job通过rpc的方式发送msg到comet
//	comet内部将消息推送到至bucket.routines，bucket.routines以轮询的方式选出一个proto.BoardcastRoomArg信道进行消息转发
//	bucket.routines's worker 消费消息，根据msg'roomid从roomsMap(bucket.rooms)选出该room的所有客户端Channels，再将消息一一转发至client'Channel
//	dispatchWorker消费client'Channel消息，通过websocket或者tcp协议发送到客户端
//
//单播推送流程：
//	单播相对于”房播（群播）”，简化了房间检索的步骤。
//	job通过rpc的方式发送msg到comet
//	comet内部根据消息的subKey，从buckets中找出对应的channel，将消息转发到对应的Channel
//	dispatchWorker消费client'Channel消息，通过websocket或者tcp协议发送到客户端

// Bucket is a channel holder.
// 维护当前消息通道和房间的信息
// Bucket is a channel holder.
type Bucket struct {
	c     *conf.Bucket
	cLock sync.RWMutex // protect the channels for chs
	// 维护一个长连接用户，只能对应一个 Room.
	// 推送的消息可以在 Room 内广播，也可以推送到指定的 Channel(维护一个长连接用户).
	chs map[string]*Channel // map sub key to a channel
	// room
	// 可以理解为房间，群组或是一个 Group. 这个房间内维护 N 个 Channel, 即长连接用户。在该 Room 内广播消息，会发送给房间内的所有 Channel.
	rooms       map[string]*Room            // bucket room channels
	routines    []chan *pb.BroadcastRoomReq // 节点房间的总信道数(用于广播给bucket下所有的room)
	routinesNum uint64                      // 处理routines信道的goroutine个数，用于消费房播的信道消息

	ipCnts map[string]int32
}

// NewBucket new a bucket struct. store the key with im channel.
func NewBucket(c *conf.Bucket) (b *Bucket) {
	b = new(Bucket)
	b.chs = make(map[string]*Channel, c.Channel)
	b.ipCnts = make(map[string]int32)
	b.c = c
	b.rooms = make(map[string]*Room, c.Room)
	b.routines = make([]chan *pb.BroadcastRoomReq, c.RoutineAmount)
	for i := uint64(0); i < c.RoutineAmount; i++ {
		c := make(chan *pb.BroadcastRoomReq, c.RoutineSize)
		b.routines[i] = c
		go b.roomproc(c)
	}
	return
}

// ChannelCount channel count in the bucket
func (b *Bucket) ChannelCount() int {
	return len(b.chs)
}

// RoomCount room count in the bucket
func (b *Bucket) RoomCount() int {
	return len(b.rooms)
}

// RoomsCount get all room id where online number > 0.
func (b *Bucket) RoomsCount() (res map[string]int32) {
	var (
		roomID string
		room   *Room
	)
	b.cLock.RLock()
	res = make(map[string]int32)
	for roomID, room = range b.rooms {
		if room.Online > 0 {
			res[roomID] = room.Online
		}
	}
	b.cLock.RUnlock()
	return
}

// ChangeRoom change ro room
func (b *Bucket) ChangeRoom(nrid string, ch *Channel) (err error) {
	var (
		nroom *Room
		ok    bool
		oroom = ch.Room
	)
	// change to no room
	if nrid == "" {
		if oroom != nil && oroom.Del(ch) {
			b.DelRoom(oroom)
		}
		ch.Room = nil
		return
	}
	b.cLock.Lock()
	if nroom, ok = b.rooms[nrid]; !ok {
		nroom = NewRoom(nrid)
		b.rooms[nrid] = nroom
	}
	b.cLock.Unlock()
	if oroom != nil && oroom.Del(ch) {
		b.DelRoom(oroom)
	}

	if err = nroom.Put(ch); err != nil {
		return
	}
	ch.Room = nroom
	return
}

// Put put a channel according with sub key.
func (b *Bucket) Put(rid string, ch *Channel) (err error) {
	var (
		room *Room
		ok   bool
	)
	b.cLock.Lock()
	// close old channel
	if dch := b.chs[ch.Key]; dch != nil {
		dch.Close()
	}
	b.chs[ch.Key] = ch
	if rid != "" {
		if room, ok = b.rooms[rid]; !ok {
			room = NewRoom(rid)
			b.rooms[rid] = room
		}
		ch.Room = room
	}
	b.ipCnts[ch.IP]++
	b.cLock.Unlock()
	if room != nil {
		err = room.Put(ch)
	}
	return
}

// Del delete the channel by sub key.
func (b *Bucket) Del(dch *Channel) {
	room := dch.Room
	b.cLock.Lock()
	if ch, ok := b.chs[dch.Key]; ok {
		if ch == dch {
			delete(b.chs, ch.Key)
		}
		// ip counter
		if b.ipCnts[ch.IP] > 1 {
			b.ipCnts[ch.IP]--
		} else {
			delete(b.ipCnts, ch.IP)
		}
	}
	b.cLock.Unlock()
	if room != nil && room.Del(dch) {
		// if empty room, must delete from bucket
		b.DelRoom(room)
	}
}

// Channel get a channel by sub key.
func (b *Bucket) Channel(key string) (ch *Channel) {
	b.cLock.RLock()
	ch = b.chs[key]
	b.cLock.RUnlock()
	return
}

// Broadcast push msgs to all channels in the bucket.
// 广播，直接从bucket.chs中遍历
func (b *Bucket) Broadcast(p *protocol.Proto, op int32) {
	var ch *Channel
	b.cLock.RLock()
	for _, ch = range b.chs {
		// 如果不在该channel的监听队列中，那么该消息不会发给该客户端
		// 这个监听队列，是在建立连接是发送的 “accepts” 字段中取得的
		// 譬如accpets = [1000, 1001, 1002], 但是op = 1003， 那么就不会发送
		//
		// 值得注意的是，这个op是从BroadcastReq.ProtoOp取得，BroadcastReq.ProtoOp又是从pushMsg.Operation取得
		// 也就是说 op = grpc.BroadcastReq.ProtoOp = proto.Op = PushMsg.Operation = 从发送消息接口产生。
		//
		if !ch.NeedPush(op) {
			continue
		}
		_ = ch.Push(p)
	}
	b.cLock.RUnlock()
}

// Room get a room by roomid.
func (b *Bucket) Room(rid string) (room *Room) {
	b.cLock.RLock()
	room = b.rooms[rid]
	b.cLock.RUnlock()
	return
}

// DelRoom delete a room by roomid.
func (b *Bucket) DelRoom(room *Room) {
	b.cLock.Lock()
	delete(b.rooms, room.ID)
	b.cLock.Unlock()
	room.Close()
}

// BroadcastRoom broadcast a message to specified room
func (b *Bucket) BroadcastRoom(arg *pb.BroadcastRoomReq) {
	// 这里取模选中一个goroutine来执行任务
	num := atomic.AddUint64(&b.routinesNum, 1) % b.c.RoutineAmount
	// b.routines 是 b.c.RoutineAmount 数量的
	// 有 b.c.RoutineSize 缓冲大小的 chan *grpc.BroadcastRoomReq
	b.routines[num] <- arg
}

// Rooms get all room id where online number > 0.
func (b *Bucket) Rooms() (res map[string]struct{}) {
	var (
		roomID string
		room   *Room
	)
	res = make(map[string]struct{})
	b.cLock.RLock()
	for roomID, room = range b.rooms {
		if room.Online > 0 {
			res[roomID] = struct{}{}
		}
	}
	b.cLock.RUnlock()
	return
}

// IPCount get ip count.
func (b *Bucket) IPCount() (res map[string]struct{}) {
	var (
		ip string
	)
	b.cLock.RLock()
	res = make(map[string]struct{}, len(b.ipCnts))
	for ip = range b.ipCnts {
		res[ip] = struct{}{}
	}
	b.cLock.RUnlock()
	return
}

// UpRoomsCount update all room count
func (b *Bucket) UpRoomsCount(roomCountMap map[string]int32) {
	var (
		roomID string
		room   *Room
	)
	b.cLock.RLock()
	for roomID, room = range b.rooms {
		room.AllOnline = roomCountMap[roomID]
	}
	b.cLock.RUnlock()
}

// roomproc
func (b *Bucket) roomproc(c chan *pb.BroadcastRoomReq) {
	for {
		arg := <-c
		if room := b.Room(arg.RoomID); room != nil {
			room.Push(arg.Proto)
		}
	}
}
