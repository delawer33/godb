package dsa

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)


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
		
		// pointers
		if bn.btype() == BN_NODE {
			for i := uint16(0); i < bn.nkeys(); i++ {
				childPtr := bn.getPtr(i)
				if _, exists := c.pages[childPtr]; !exists && childPtr != 0 {
					t.Errorf("Child pointer %d not found in pages", childPtr)
				}
			}
		}
		
		// page size
		if len(node) > BT_PAGE_SIZE {
			t.Errorf("Node %d exceeds page size: %d > %d", ptr, len(node), BT_PAGE_SIZE)
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

// TODO: Falls (problem: keys are not sorted after some deletions, also in some tests deletion fails)
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
