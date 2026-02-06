package btree

import (
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

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
}

func (db *KV) Open() error {
	db.tree.get = db.pageRead
	db.tree.new = db.pageAppend
	db.tree.del = func(uint64) {}
	// ...
}

func (db *KV) Get(key []byte, val []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) (error) {
	db.tree.Insert(key, val)
	return updateFile(db)
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

func updateRoot(db *KV) error

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
