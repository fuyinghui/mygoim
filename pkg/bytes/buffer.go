package bytes

import (
	"sync"
)

// Buffer buffer.
type Buffer struct {
	buf  []byte
	next *Buffer // next free buffer
}

// Bytes bytes.
func (b *Buffer) Bytes() []byte {
	return b.buf
}

// Pool is a buffer pool.
type Pool struct {
	lock sync.Mutex
	free *Buffer // 内部未使用Buffer
	max  int     // max = num * size
	num  int     // 总共多少块内存
	size int     // 每块多少字节内存
}

// NewPool new a memory buffer pool struct.
func NewPool(num, size int) (p *Pool) {
	p = new(Pool)
	p.init(num, size)
	return
}

// Init init the memory buffer.
func (p *Pool) Init(num, size int) {
	p.init(num, size)
}

// init init the memory buffer.
func (p *Pool) init(num, size int) {
	p.num = num
	p.size = size
	p.max = num * size
	p.grow()
}

// grow grow the memory buffer size, and update free pointer.
func (p *Pool) grow() {
	var (
		i   int
		b   *Buffer
		bs  []Buffer
		buf []byte
	)
	// 分配大内存
	buf = make([]byte, p.max)
	// 对应多少块Buffer？
	bs = make([]Buffer, p.num)
	p.free = &bs[0]
	// Buffer 对应上面的buf
	b = p.free
	for i = 1; i < p.num; i++ {
		b.buf = buf[(i-1)*p.size : i*p.size]
		b.next = &bs[i]
		b = b.next
	}
	b.buf = buf[(i-1)*p.size : i*p.size]
	b.next = nil
}

// Get get a free memory buffer.
func (p *Pool) Get() (b *Buffer) {
	p.lock.Lock()
	if b = p.free; b == nil {
		// 使用完以后再次分配
		p.grow()
		b = p.free
	}
	p.free = b.next
	p.lock.Unlock()
	return
}

// Put put back a memory buffer to free.
func (p *Pool) Put(b *Buffer) {
	p.lock.Lock()
	b.next = p.free
	p.free = b
	p.lock.Unlock()
}
