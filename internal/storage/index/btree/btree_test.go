package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// check if all keys are sorted
func assertSorted(t *testing.T, node BN) {
	bn := BN(node)
	nkeys := bn.nkeys()

	for i := uint16(1); i < nkeys; i++ {
		prevKey := bn.getKey(i - 1)
		currKey := bn.getKey(i)

		cmp := bytes.Compare(prevKey, currKey)
		if cmp > 0 {
			t.Errorf("Keys not sorted in node type=%d: index %d: prev=%q, curr=%q",
				bn.btype(), i, string(prevKey), string(currKey))
			fmt.Println(node.String())
			panic(0)
		}
	}
}

// check if node size is in bounds
func isStableNode(node BN) bool {
	bn := BN(node)
	return bn.nbytes() <= BT_PAGE_SIZE
}

// check if Btree has valid structure
func verifyTreeStructure(t *testing.T, c *C) {
	for ptr, node := range c.pages {
		bn := BN(node)

		// header
		if bn.btype() != BN_LEAF && bn.btype() != BN_NODE {
			t.Errorf("Invalid node type: %d", bn.btype())
		}

		// nkeys in root
		if bn.nkeys() == 0 && ptr == c.tree.root {
			// can't be 0 if there are pages except root
			if len(c.pages) > 1 {
				t.Errorf("Root has 0 keys but tree has other nodes")
			}
		}

		// pointers and keys
		if bn.btype() == BN_NODE {
			for i := uint16(0); i < bn.nkeys(); i++ {
				childPtr := bn.getPtr(i)
				if _, exists := c.pages[childPtr]; !exists && childPtr != 0 {
					t.Errorf("Child pointer %d not found in pages", childPtr)
				}

				// check first key in child node is equal to key in parent
				childNode := BN(c.tree.get(childPtr))
				nodeKey, childKey := bn.getKey(i), childNode.getKey(0)
				if !bytes.Equal(nodeKey, childKey) {
					t.Errorf("Child first key is not equal to his key in parent")
				}
			}
		}

		// page size
		if len(node) > BT_PAGE_SIZE {
			t.Errorf("Node %d exceeds page size: %d > %d", ptr, len(node), BT_PAGE_SIZE)
		}
	}
}

// check if node size and offsets are correct
func assertNodeSize(t *testing.T, node BN) {
	bn := BN(node)

	if bn.nbytes() > uint16(len(node)) {
		t.Errorf("nbytes()=%d exceeds node length=%d", bn.nbytes(), len(node))
	}

	nkeys := bn.nkeys()
	for i := uint16(1); i <= nkeys; i++ {
		offset := bn.getOffset(i)
		if i > 0 {
			prevOffset := bn.getOffset(i - 1)
			if offset <= prevOffset && i > 1 {
				t.Errorf("Offset not increasing: offset[%d]=%d <= offset[%d]=%d",
					i, offset, i-1, prevOffset)
			}
		}
		if offset > uint16(len(node)) {
			t.Errorf("Offset out of bounds: offset[%d]=%d > node length=%d",
				i, offset, len(node))
		}
	}
}

func TestNodeSortedKeys(t *testing.T) {
	c := NewC()

	keys := []string{"z", "a", "m", "f", "b", "x", "c"}
	for _, key := range keys {
		c.add(key, "value_"+key)
	}

	root := BN(c.tree.get(c.tree.root))
	assertSorted(t, root)

	for _, node := range c.pages {
		assertSorted(t, node)
	}
}

func TestNodeSizeLimit(t *testing.T) {
	c := NewC()

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%04d", i)
		val := fmt.Sprintf("value_%04d", i)
		c.add(key, val)

		for ptr, node := range c.pages {
			bn := BN(node)
			if len(node) > BT_PAGE_SIZE {
				t.Errorf("Node %d (type=%d, nkeys=%d) exceeds page size: %d > %d",
					ptr, bn.btype(), bn.nkeys(), len(node), BT_PAGE_SIZE)
			}
			if bn.nbytes() > uint16(len(node)) {
				t.Errorf("Node %d: nbytes()=%d > actual length=%d",
					ptr, bn.nbytes(), len(node))
			}
		}
	}

	// Additional
	for ptr, node := range c.pages {
		bn := BN(node)

		if bn.btype() != BN_LEAF && bn.btype() != BN_NODE {
			t.Errorf("Node %d has invalid type: %d", ptr, bn.btype())
		}

		nkeys := bn.nkeys()
		for i := uint16(1); i <= nkeys; i++ {
			offset := bn.getOffset(i)
			if i > 0 {
				prevOffset := bn.getOffset(i - 1)
				if offset < prevOffset && i > 1 {
					t.Errorf("Node %d: offset[%d]=%d < offset[%d]=%d",
						ptr, i, offset, i-1, prevOffset)
				}
			}
		}
	}

	fmt.Printf("Total nodes created: %d\n", len(c.pages))
}

func TestSplitMaintainsSortOrder(t *testing.T) {
	c := NewC()

	// keys for several splits
	keys := make([]string, 0)
	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("key_%d_%08d", rand.Intn(100), i)
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		c.add(key, "value_"+key)

		// check nodes after each insert
		for _, node := range c.pages {
			assertSorted(t, node)
		}
	}

	verifyTreeStructure(t, c)
}

func TestDeleteMaintainsSortOrder(t *testing.T) {
	c := NewC()

	keys := []string{}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key_%04d", i)
		keys = append(keys, key)
		c.add(key, "value_"+key)
	}

	// delete in random order

	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for _, key := range keys[:100] {
		success := c.tree.Delete([]byte(key))
		if !success {
			t.Errorf("Failed to delete key: %s", key)
		}

		// check sorting after each deletion
		for _, node := range c.pages {
			assertSorted(t, node)
		}
	}

	verifyTreeStructure(t, c)
}

func TestBTreeConsistency(t *testing.T) {
	c := NewC()

	operations := []struct {
		op  string
		key string
		val string
	}{
		{"add", "apple", "fruit"},
		{"add", "banana", "fruit"},
		{"add", "cherry", "fruit"},
		{"add", "date", "fruit"},
		{"delete", "banana", ""},
		{"add", "elderberry", "fruit"},
		{"add", "fig", "fruit"},
		{"delete", "apple", ""},
		{"add", "grape", "fruit"},
	}

	for _, op := range operations {
		switch op.op {
		case "add":
			c.add(op.key, op.val)
		case "delete":
			c.tree.Delete([]byte(op.key))
		}

		for _, node := range c.pages {
			assertSorted(t, node)
			assertNodeSize(t, node)
		}
	}

	verifyTreeStructure(t, c)
}

func TestLargeKeysAndValues(t *testing.T) {
	c := NewC()

	largeKey := string(make([]byte, BT_MAX_KEY_SIZE-10))
	largeVal := string(make([]byte, BT_MAX_VAL_SIZE-10))

	c.add(largeKey, largeVal)

	for _, node := range c.pages {
		assertNodeSize(t, node)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s_%d", largeKey[:100], i)
		val := fmt.Sprintf("%s_%d", largeVal[:100], i)
		c.add(key, val)

		for _, node := range c.pages {
			assertSorted(t, node)
			assertNodeSize(t, node)
		}
	}
}

func TestRandomOperations(t *testing.T) {
	c := NewC()

	expected := make(map[string]string)

	for i := 0; i < 1000; i++ {
		op := rand.Intn(3)
		key := fmt.Sprintf("key_%d", rand.Intn(100))

		switch op {
		// Add/Update
		case 0, 1:
			val := fmt.Sprintf("val_%d", rand.Intn(1000))
			c.add(key, val)
			expected[key] = val
		// Delete
		case 2:
			c.tree.Delete([]byte(key))
			delete(expected, key)
		}

		if i%100 == 0 {
			for _, node := range c.pages {
				assertSorted(t, node)
				assertNodeSize(t, node)
			}
		}
	}

	verifyTreeStructure(t, c)
}

func TestGetBasic(t *testing.T) {
	c := NewC()

	c.add("a", "1")
	c.add("b", "2")
	c.add("c", "3")

	val, ok := c.tree.Get([]byte("a"))
	if !ok || string(val) != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1, true", val, ok)
	}

	val, ok = c.tree.Get([]byte("b"))
	if !ok || string(val) != "2" {
		t.Fatalf("Get(b) = %q, %v; want 2, true", val, ok)
	}

	val, ok = c.tree.Get([]byte("c"))
	if !ok || string(val) != "3" {
		t.Fatalf("Get(c) = %q, %v; want 3, true", val, ok)
	}
}

func TestGetNotFound(t *testing.T) {
	c := NewC()

	c.add("a", "1")
	c.add("b", "2")

	if val, ok := c.tree.Get([]byte("c")); ok || val != nil {
		t.Fatalf("Get(c) = %q, %v; want nil, false", val, ok)
	}
}

func TestGetAfterUpdate(t *testing.T) {
	c := NewC()

	c.add("key", "v1")
	c.add("key", "v2")

	val, ok := c.tree.Get([]byte("key"))
	if !ok || string(val) != "v2" {
		t.Fatalf("Get(key) = %q, %v; want v2, true", val, ok)
	}
}

func TestGetAfterDelete(t *testing.T) {
	c := NewC()

	c.add("a", "1")
	c.add("b", "2")

	ok := c.tree.Delete([]byte("a"))
	if !ok {
		t.Fatal("Delete(a) failed")
	}

	if val, ok := c.tree.Get([]byte("a")); ok || val != nil {
		t.Fatalf("Get(a) after delete = %q, %v; want nil, false", val, ok)
	}

	val, ok := c.tree.Get([]byte("b"))
	if !ok || string(val) != "2" {
		t.Fatalf("Get(b) = %q, %v; want 2, true", val, ok)
	}
}

func TestGetDoesNotModifyTree(t *testing.T) {
	c := NewC()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		c.add(key, "val")
	}

	before := len(c.pages)

	for i := 0; i < 100; i++ {
		c.tree.Get([]byte(fmt.Sprintf("key_%d", i)))
	}

	after := len(c.pages)
	if before != after {
		t.Fatalf("Get modified tree: pages before=%d after=%d", before, after)
	}
}

func TestGetRandomOperations(t *testing.T) {
	c := NewC()
	ref := map[string]string{}

	for i := 0; i < 1000; i++ {
		op := rand.Intn(3)
		key := fmt.Sprintf("key_%d", rand.Intn(100))

		switch op {
		case 0, 1:
			val := fmt.Sprintf("val_%d", rand.Intn(1000))
			c.add(key, val)
			ref[key] = val
		case 2:
			c.tree.Delete([]byte(key))
			delete(ref, key)
		}

		if i%50 == 0 {
			for k, v := range ref {
				val, ok := c.tree.Get([]byte(k))
				if !ok || string(val) != v {
					t.Fatalf("Get(%s) = %q, %v; want %s, true",
						k, val, ok, v)
				}
			}
		}
	}
}

// benchmarks TODO

func BenchmarkBTreeInsert(b *testing.B) {
	c := NewC()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("value_%d", i)
		c.add(key, val)
	}
}
