package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	DB_SIG  = "mydb000000000000"
	FREE_LIST_HEADER = 8
	FREE_LIST_CAP = (BT_PAGE_SIZE - FREE_LIST_HEADER) / 8
)

// freeList node
// | next | pointers |
// |  8B  |   n*8B   |
type LNode []byte

func (node LNode) getNext() uint64
func (node LNode) setNext(next uint64)
func (node LNode) getPtr(idx int) uint64
func (node LNode) setPtr(idx int, ptr uint64)

type FreeList struct {
	get func(uint64) []byte
	new func([]byte) uint64
	set func(uint64) []byte // copies a page and returns bytes that we can change. cant use get for this, because mmpa return read only

	headPage uint64
	headSeq uint64 // seq from what we can read
	tailPage uint64
	tailSeq uint64 // seq to what we can read
	maxSeq uint64 // tailSeq snapshot to prevect consuming new items
}

func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

func (fl *FreeList) setMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

// 0 if failure
func (fl *FreeList) PopHead() uint64 {
	ptr, head := flPop(fl)
	if head != 0 {
		fl.PushTail(head)
	}
	return ptr
}

func (fl *FreeList) PushTail(ptr uint64) {
	LNode(fl.set(fl.tailPage)).setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++
	// node is full
	if seq2idx(fl.tailSeq) == 0 {
		next, head := flPop(fl)
		if next == 0 {
			next = fl.new(make([]byte, BT_PAGE_SIZE))
		}
		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage = next
		
		if head != 0 {
			LNode(fl.set(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

func flPop(fl *FreeList) (ptr uint64, head uint64) {
	if fl.headSeq == fl.maxSeq {
		return 0, 0
	}
	node := LNode(fl.get(fl.headPage))
	ptr = node.getPtr(seq2idx(fl.headSeq))
	fl.headSeq++
	if seq2idx(fl.headSeq) == 0 {
		head, fl.headPage = fl.headPage, node.getNext()
		assert(fl.headPage != 0)
	}
	return ptr, fl.headPage
}

type KV struct {
	Path string
	fd   int
	tree BT
	mmap struct {
		total int           // mmap size, can be larger then file
		chunks [][]byte     // mmaps can be non-continuous
	}
	page struct {
		flushed uint64  // db size in number of pages
		temp [][]byte   // newly allocated pages
	}
	failed bool
	free FreeList
}

func (db *KV) Open() error {
	db.tree.get = db.pageRead
	db.tree.new = db.pageAlloc
	db.tree.del = db.free.PushTail
	
	db.free.get = db.pageRead
	db.free.new = db.pageAppend
	db.free.set = db.pageWrite
}

func (db *KV) Get(key []byte, val []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) (error) {
	meta := saveMeta(db)
	db.tree.Insert(key, val)
	return updateOrRevert(db, meta)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, updateFile(db)
}

// read a page, `ptr` is a number of the page of BTree
func (db *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk)) / BT_PAGE_SIZE
		if ptr < end {
			offset := BT_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BT_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

func (db *KV) pageAlloc([]byte) uint64

func (db *KV) pageWrite(uint64) []byte

func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil
	}
	alloc := max(db.mmap.total, 64<<20)
	for db.mmap.total + alloc < size {
		alloc *= 2
	}
	chunk, err := syscall.Mmap(db.fd, int64(db.mmap.total), alloc, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func (db *KV) pageAppend(node []byte) uint64 {
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node)
	return ptr
}

func updateFile(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	if err := syscall.Fsync(db.fd); err != nil {
		return err
	}
	if err := updateRoot(db); err != nil {
		return err
	}
	return syscall.Fsync(db.fd)
}

func writePages(db *KV) error {
	size := (int(db.page.flushed) + len(db.page.temp)) * BT_PAGE_SIZE
	if err := extendMmap(db, size); err != nil {
		return err
	}
	offset := int64(db.page.flushed * BT_PAGE_SIZE)
	if _, err := unix.Pwritev(db.fd, db.page.temp, offset); err != nil {
		return err
	}
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

func loadMeta(db *KV, data []byte) {
	assert(len(data) >= 32)
	assert(!bytes.Equal(data[:16], []byte(DB_SIG)))
	db.tree.root = binary.LittleEndian.Uint64(data[16:24])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:32])
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 {
		db.page.flushed = 1
		return nil
	}
	data := db.mmap.chunks[0]
	loadMeta(db, data)

	return nil
}

func updateRoot(db *KV) error {
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

func updateOrRevert(db *KV, meta []byte) error {
	if db.failed {
		if _, err := syscall.Pwrite(db.fd, meta, 0); err != nil {
			return fmt.Errorf("rewrite meta page: %w", err)
		}
		if err := syscall.Fsync(db.fd); err != nil {
			return fmt.Errorf("fsync meta page: %w", err)
		}
		db.failed = false
	}
	err := updateFile(db)
	if err != nil {
		db.failed = true
		// reverting im-memory states to allow reads
		loadMeta(db, meta)
		db.page.temp = db.page.temp[:0]
	}
	return err
}


