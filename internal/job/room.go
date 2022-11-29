package job

import (
	"errors"
	"time"

	"github.com/Terry-Mao/goim/api/protocol"
	"github.com/Terry-Mao/goim/internal/job/conf"
	"github.com/Terry-Mao/goim/pkg/bytes"
	log "github.com/golang/glog"
)

var (
	// ErrComet commet error.
	ErrComet = errors.New("comet rpc is not available")
	// ErrCometFull comet chan full.
	ErrCometFull = errors.New("comet proto chan full")
	// ErrRoomFull room chan full.
	ErrRoomFull = errors.New("room proto chan full")

	roomReadyProto = new(protocol.Proto)
)

// getRoom(roomID) -> room.Push() -> p -> room.proto
//
//	|
//	|---> NewRoom(batch, duration)
//			|
//			|---> go room.pushproc() -> p <- room.proto
//
// Room room.
type Room struct {
	c     *conf.Room
	job   *Job                 // 绑定job，为了追溯Room所属的Job
	id    string               // 房间ID
	proto chan *protocol.Proto //有缓冲channel
}

// NewRoom new a room struct, store channel room info.
func NewRoom(job *Job, id string, c *conf.Room) (r *Room) {
	r = &Room{
		c:     c,
		id:    id,
		job:   job,
		proto: make(chan *protocol.Proto, c.Batch*2),
	}
	go r.pushproc(c.Batch, time.Duration(c.Signal))
	return
}

// Push push msg to the room, if chan full discard it.
func (r *Room) Push(op int32, msg []byte) (err error) {
	var p = &protocol.Proto{
		Ver:  1,
		Op:   op,
		Body: msg,
	}
	select {
	case r.proto <- p:
	default:
		err = ErrRoomFull
	}
	return
}

// pushproc merge proto and push msgs in batch.
func (r *Room) pushproc(batch int, sigTime time.Duration) {
	var (
		n    int
		last time.Time
		p    *protocol.Proto
		buf  = bytes.NewWriterSize(int(protocol.MaxBodySize))
	)
	log.Infof("start room:%s goroutine", r.id)
	td := time.AfterFunc(sigTime, func() {
		select {
		case r.proto <- roomReadyProto:
		default:
		}
	})
	defer td.Stop()
	for {
		if p = <-r.proto; p == nil {
			// 如果创建了room，但是读到空包
			break // exit
		} else if p != roomReadyProto {
			// merge buffer ignore error, always nil
			p.WriteTo(buf)
			if n++; n == 1 {
				// 如果是第一个数据包，则重置定时器，并继续读取后续数据包
				last = time.Now()
				td.Reset(sigTime)
				continue
			} else if n < batch {
				// 后续的数据包，不会重置定时器，但是如果时间仍在第一个数据包的 sigTime 时间间隔内
				// 简单说，定时器还没到时间
				if sigTime > time.Since(last) {
					continue
				}
			}
			// 累计的数据包数量已经超过了batch, 执行发送动作
		} else {
			// 定时器到读到了roomReadyProto
			// 如果buf已经被重置了，则跳出循环执行清理动作；否则执行发送消息
			if n == 0 {
				break
			}
		}
		// 发送房间内的消息
		_ = r.job.broadcastRoomRawBytes(r.id, buf.Buffer())
		// TODO use reset buffer
		// after push to room channel, renew a buffer, let old buffer gc
		buf = bytes.NewWriterSize(buf.Size())
		n = 0
		// 如果配置了房间最大闲置时间，则重新设定定时器
		// 也就是说，如果房间被创建后，处理完了该房间的消息，并不是直接跳出循环清理房间
		// 而是，会阻塞等待下一次的消息再来，如果在 “1m / r.c.Idle” 时间内没有来，则会跳出循环清理掉该房间
		// 如果在 “1m / r.c.Idle” 内有消息，则会重新设定定时器为sigTime，并为proto计数
		if r.c.Idle != 0 {
			td.Reset(time.Duration(r.c.Idle))
		} else {
			td.Reset(time.Minute)
		}
	}
	r.job.delRoom(r.id)
	log.Infof("room:%s goroutine exit", r.id)
}

func (j *Job) delRoom(roomID string) {
	j.roomsMutex.Lock()
	delete(j.rooms, roomID)
	j.roomsMutex.Unlock()
}

func (j *Job) getRoom(roomID string) *Room {
	j.roomsMutex.RLock()
	room, ok := j.rooms[roomID]
	j.roomsMutex.RUnlock()
	if !ok {
		j.roomsMutex.Lock()
		if room, ok = j.rooms[roomID]; !ok {
			room = NewRoom(j, roomID, j.c.Room)
			j.rooms[roomID] = room
		}
		j.roomsMutex.Unlock()
		log.Infof("new a room:%s active:%d", roomID, len(j.rooms))
	}
	return room
}
