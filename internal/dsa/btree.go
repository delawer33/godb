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

// Find first key less than or equal to given TODO: binary search
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

// leaves

func leafInsert(new BN, old BN, idx uint16, key []byte, val []byte) {
	new.setHeader(BN_LEAF, old.nkeys() + 1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)	
}

func leadUpdate(new BN, old BN, idx uint16m key []byte, val []byte) {
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	if idx+1 < old.nkeys() {
		nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
	}
}

func nodeAppendKV(new BN, idx uint16, ptr uint64, key []byte, val []byte) {
	new.setPtr(idx, ptr)
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

func nodeAppendRange(new BN, old BN, dstNew uint16, srcOld uint16, n uint16) {
	assert(srcOld+n <= old.nkeys())
	assert(dstNew+n <= new.nkeys())
	if n == 0 {
		return
	}
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}
	start := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new[new.kvPos(dstNew):], old[start:end])
}

func nodeReplaceKidN(tree *BT, new BN, old BN, idx uint16, kids ...BN) {
	inc := uint16(len(kids))
	new.setHeader(BN_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-idx-1)
}

func nodeSplit2(left BN, right BN, old BN) {
	nkeys := old.nkeys()
	splitIdx := nkeys / 2
	if splitIdx == 0 {
		splitIdx = 1
	}
	btype := old.btype()
	left.setHeader(btype, splitIdx)
	right.setHeader(btype, nkeys-splitIdx)

	nodeAppendRange(left, old, 0, 0, splitIdx)
	nodeAppendRange(right, old, 0, splitIdx, nkeys-splitIdx)
}

func nodeSplit3(old BN) (uint16, [3]BN) {
	if old.nbytes() <= BT_PAGE_SIZE {
		old = old[:BT_PAGE_SIZE]
		return 1, [3]BN{old}
	}
	left := BN(make([]byte, 2*BT_PAGE_SIZE))
	right := BN(make([]byte, BT_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BT_PAGE_SIZE {
		left = left[:BT_PAGE_SIZE]
		return 2, [3]BN{left, right}
	}
	leftleft := BN(make([]byte, BT_PAGE_SIZE))
	middle := BN(make([]byte, BT_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BT_PAGE_SIZE)
	return 3, [3]BN{leftleft, middle, right}
}

// insert a KV into a node, the result might be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BT, node BN, key []byte, val []byte) BN {
	new := BN(make([]byte, 2*BT_PAGE_SIZE))
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BN_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx+1, key, val)
		}
	case BN_NODE:
		 nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node")
	}
	return new
}

func nodeInsert(tree *BT, new BN, node BN, idx uint16, key []byte, val []byte) {
	kptr := node.getPtr(idx)
	knode := treeInsert(tree, tree.get(kptr), key, val)
	nsplit, split := nodeSplit3(knode)
	tree.del(kptr)
	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}



