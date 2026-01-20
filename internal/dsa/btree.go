package dsa

import (
	"bytes"
	"encoding/binary"
)

const (
	HEADER = 4
	BT_PAGE_SIZE = 4096
	BT_MAX_KEY_SIZE = 1000
	BT_MAX_VAL_SIZE = 3000

	BN_NODE = 1
	BN_LEAF = 2
)

type BN []byte // B-tree node

func assert(condition bool) {
    if !condition {
        panic("assertion failed")
    }
}

func init() {
	node1max := HEADER + 8 + 2 + 4 + BT_MAX_KEY_SIZE + BT_MAX_VAL_SIZE
	assert(node1max <= BT_PAGE_SIZE)
}

type BT struct {
	root uint64

	get func(uint64) []byte
	new func([]byte) uint64
	del func(uint64)
}

func (node BN) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BN) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BN) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BN) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BN) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

func offsetPos(node BN, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BN) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BN) setOffset(idx uint16, offset uint16) {
	assert(1 <= idx && idx <= node.nkeys())
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

func (node BN) kvPos(idx uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BN) getKey(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BN) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

func (node BN) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// Find first key less than or equal to given
func nodeLookupLE(node BN, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}
