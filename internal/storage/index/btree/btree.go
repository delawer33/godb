package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"
)

const (
	HEADER          = 4
	BT_PAGE_SIZE    = 4096
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

func (node BN) kvSize(idx uint16) uint16 {
	pos := node.kvPos(idx)
	var next uint16

	if idx+1 < node.nkeys() {
		next = node.kvPos(idx + 1)
	} else {
		next = node.nbytes()
	}

	return next - pos
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

func leafInsert(new BN, old BN, idx uint16, key []byte, val []byte) {
	new.setHeader(BN_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new BN, old BN, idx uint16, key []byte, val []byte) {
	new.setHeader(BN_LEAF, old.nkeys())
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

// check how many bytes it will take to copy `count` KV's 
// from `from` to new node. Only for leaf nodes
func leafSizeFor(old BN, from, count uint16) uint16 {
	assert(old.btype() == BN_LEAF)
	size := uint16(HEADER)
	size += (count + 1) * 2

	if count == 0 {
		return size
	}

	start := old.kvPos(from)
	end := old.nbytes()
	if from+count < old.nkeys() {
		end = old.kvPos(from + count)
	}

	size += end - start
	return size
}

func nodeSplit2(left BN, right BN, old BN) {
	n := old.nkeys()
	btype := old.btype()

	bestIdx := uint16(0)
	bestMax := uint16(^uint16(0))

	for i := uint16(1); i < n; i++ {
		ls := leafSizeFor(old, 0, i)
		rs := leafSizeFor(old, i, n-i)

		if ls > BT_PAGE_SIZE || rs > BT_PAGE_SIZE {
			continue
		}

		max := ls
		if rs > max {
			max = rs
		}

		if max < bestMax {
			bestMax = max
			bestIdx = i
		}
	}

	assert(bestIdx > 0)

	left.setHeader(btype, bestIdx)
	right.setHeader(btype, n-bestIdx)

	nodeAppendRange(left, old, 0, 0, bestIdx)
	nodeAppendRange(right, old, 0, bestIdx, n-bestIdx)
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

func (tree *BT) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		root := BN(make([]byte, BT_PAGE_SIZE))
		root.setHeader(BN_LEAF, 2)

		// dummy
		nodeAppendKV(root, 0, 0, nil, nil)

		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}
	node := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		// add new level
		root := BN(make([]byte, BT_PAGE_SIZE))
		root.setHeader(BN_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

func (tree *BT) Delete(key []byte) bool {
	if tree.root == 0 {
		return false
	}
	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated) == 0 {
		return false
	}
	tree.del(tree.root)
	if updated.nkeys() == 0 && updated.btype() == BN_NODE {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

func leafDelete(new BN, old BN, idx uint16) {
	assert(idx < old.nkeys())
	new.setHeader(BN_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)

	if idx+1 < old.nkeys() {
		nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx-1)
	}
}

func nodeMerge(new BN, left BN, right BN) {
	assert(left.btype() == right.btype())
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

func shouldMerge(tree *BT, node BN, idx uint16, updated BN) (int, BN) {
	if updated.nbytes() > BT_PAGE_SIZE/4 {
		return 0, BN{}
	}
	if idx > 0 {
		sibling := BN(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BT_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nkeys() {
		sibling := BN(tree.get(node.getPtr(idx + 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BT_PAGE_SIZE {
			return 1, sibling
		}
	}
	return 0, BN{}
}

func nodeReplace2Kid(new BN, old BN, idx uint16, ptr uint64, key []byte) {
	// asserts of type and idx???
	new.setHeader(BN_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)

	if idx+2 < old.nkeys() {
		nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-idx-2)
	}
}

func treeDelete(tree *BT, node BN, key []byte) BN {
	if node.btype() == BN_LEAF {
		idx := nodeLookupLE(node, key)
		if !bytes.Equal(node.getKey(idx), key) {
			return nil
		}
		new := BN(make([]byte, BT_PAGE_SIZE))
		leafDelete(new, node, idx)
		return new
	}
	idx := nodeLookupLE(node, key)
	return nodeDelete(tree, node, idx, key)
}

func nodeDelete(tree *BT, node BN, idx uint16, key []byte) BN {
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BN{}
	}
	tree.del(kptr)

	new := BN(make([]byte, BT_PAGE_SIZE))
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0:
		merged := BN(make([]byte, BT_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0:
		merged := BN(make([]byte, BT_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0)
		new.setHeader(BN_NODE, 0)
	case mergeDir == 0 && updated.nkeys() > 0:
		nodeReplaceKidN(tree, new, node, idx, updated)
		if idx > 0 {
			pos := new.kvPos(idx)
			firstKey := updated.getKey(0)
			binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(firstKey)))
			binary.LittleEndian.PutUint16(new[pos+2:], 0)
			copy(new[pos+4:], firstKey)
		}
	}
	return new
}


func treeGet(tree *BT, node BN, key []byte) ([]byte, bool) {
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BN_LEAF:
		if bytes.Equal(node.getKey(idx), key) {
			return node.getVal(idx), true
		}
		return nil, false

	case BN_NODE:
		ptr := node.getPtr(idx)
		return treeGet(tree, tree.get(ptr), key)

	default:
		panic("bad node type")
	}
}


func (tree *BT) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}
	return treeGet(tree, tree.get(tree.root), key)
}

// In-memory Btree
type C struct {
	tree  BT
	ref   map[string]string
	pages map[uint64]BN
}

func NewC() *C {
	pages := map[uint64]BN{}
	return &C{
		tree: BT{
			get: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				assert(ok)
				return node
			},
			new: func(node []byte) uint64 {
				assert(BN(node).nbytes() <= BT_PAGE_SIZE)
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				assert(pages[ptr] == nil)
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				assert(pages[ptr] != nil)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (node BN) String() string {
	var buf bytes.Buffer

	btype := node.btype()
	nkeys := node.nkeys()

	var typeStr string
	switch btype {
	case BN_LEAF:
		typeStr = "LEAF"
	case BN_NODE:
		typeStr = "NODE"
	default:
		typeStr = fmt.Sprintf("UNKNOWN(%d)", btype)
	}

	fmt.Fprintf(&buf, "=== %s Node (nkeys=%d, nbytes=%d, total_size=%d) ===\n",
		typeStr, nkeys, node.nbytes(), len(node))

	if nkeys > 0 {
		fmt.Fprintf(&buf, "Pointers: [")
		for i := uint16(0); i < nkeys; i++ {
			ptr := node.getPtr(i)
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "%d", ptr)
		}
		buf.WriteString("]\n")

		fmt.Fprintf(&buf, "Offsets:  [0")
		for i := uint16(1); i <= nkeys; i++ {
			offset := node.getOffset(i)
			fmt.Fprintf(&buf, ", %d", offset)
		}
		buf.WriteString("]\n")

		fmt.Fprintf(&buf, "KV pairs:\n")
		for i := uint16(0); i < nkeys; i++ {
			key := node.getKey(i)
			val := node.getVal(i)
			kvPos := node.kvPos(i)

			fmt.Fprintf(&buf, "  [%2d] @pos=%d: ", i, kvPos)

			if len(key) == 0 {
				buf.WriteString("KEY: <empty>")
			} else if len(key) <= 50 {
				fmt.Fprintf(&buf, "KEY: %q", string(key))
			} else {
				fmt.Fprintf(&buf, "KEY: %q... (%d bytes)",
					string(key[:50]), len(key))
			}

			if btype == BN_LEAF {
				if len(val) == 0 {
					buf.WriteString(", VAL: <empty>")
				} else if len(val) <= 30 {
					fmt.Fprintf(&buf, ", VAL: %q", string(val))
				} else {
					fmt.Fprintf(&buf, ", VAL: %q... (%d bytes)",
						string(val[:30]), len(val))
				}
			} else {
				buf.WriteString(", VAL: <nil>")
			}
			buf.WriteByte('\n')
		}
	} else {
		buf.WriteString("(empty node)\n")
	}

	if len(node) > 0 {
		fmt.Fprintf(&buf, "\nRaw data (first 100 bytes):\n")
		limit := 100
		if len(node) < limit {
			limit = len(node)
		}
		for i := 0; i < limit; i += 16 {
			end := i + 16
			if end > limit {
				end = limit
			}

			for j := i; j < end; j++ {
				fmt.Fprintf(&buf, "%02x ", node[j])
				if j == i+7 {
					buf.WriteByte(' ')
				}
			}

			for j := end; j < i+16; j++ {
				buf.WriteString("   ")
				if j == i+7 {
					buf.WriteByte(' ')
				}
			}
			buf.WriteString(" | ")
			for j := i; j < end; j++ {
				b := node[j]
				if b >= 32 && b <= 126 {
					buf.WriteByte(b)
				} else {
					buf.WriteByte('.')
				}
			}
			buf.WriteByte('\n')
		}
	}

	return buf.String()
}
