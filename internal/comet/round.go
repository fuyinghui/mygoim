package comet

import (
	"github.com/Terry-Mao/goim/internal/comet/conf"
	"github.com/Terry-Mao/goim/pkg/bytes"
	"github.com/Terry-Mao/goim/pkg/time"
)

// RoundOptions round options.
type RoundOptions struct {
	Timer        int
	TimerSize    int
	Reader       int
	ReadBuf      int
	ReadBufSize  int
	Writer       int
	WriteBuf     int
	WriteBufSize int
}

// Round used for connection round-robin get a reader/writer/timer for split big lock.
type Round struct {
	readers []bytes.Pool // 读缓存对象池
	writers []bytes.Pool // 写缓存对象池
	timers  []time.Timer // timer对象
	options RoundOptions
}

// NewRound new a round struct.
func NewRound(c *conf.Config) (r *Round) {
	var i int
	r = &Round{
		options: RoundOptions{
			Reader:       c.TCP.Reader,      // 多少个Pool对象用于读使用,默认是32
			ReadBuf:      c.TCP.ReadBuf,     // 初始化读内存的时候，分配多少块内存		1K
			ReadBufSize:  c.TCP.ReadBufSize, // 初始化读内存的时候，每块内存分配多少大小  8K
			Writer:       c.TCP.Writer,
			WriteBuf:     c.TCP.WriteBuf,
			WriteBufSize: c.TCP.WriteBufSize,
			Timer:        c.Protocol.Timer,
			TimerSize:    c.Protocol.TimerSize,
		}}
	// 初始化读缓存池对象们
	r.readers = make([]bytes.Pool, r.options.Reader)
	for i = 0; i < r.options.Reader; i++ {
		r.readers[i].Init(r.options.ReadBuf, r.options.ReadBufSize)
	}
	// 初始化写缓存池对象们
	r.writers = make([]bytes.Pool, r.options.Writer)
	for i = 0; i < r.options.Writer; i++ {
		r.writers[i].Init(r.options.WriteBuf, r.options.WriteBufSize)
	}
	// timer
	r.timers = make([]time.Timer, r.options.Timer)
	for i = 0; i < r.options.Timer; i++ {
		r.timers[i].Init(r.options.TimerSize)
	}
	return
}

// Timer get a timer.
func (r *Round) Timer(rn int) *time.Timer {
	return &(r.timers[rn%r.options.Timer])
}

// Reader get a reader memory buffer.
func (r *Round) Reader(rn int) *bytes.Pool {
	return &(r.readers[rn%r.options.Reader])
}

// Writer get a writer memory buffer pool.
func (r *Round) Writer(rn int) *bytes.Pool {
	return &(r.writers[rn%r.options.Writer])
}
