// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cmdq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func (n *node) verify() {
	if root := n.parent == nil; !root {
		if n.count < minCmds || maxCmds < n.count {
			panic(fmt.Sprintf("command count %d outside range [%d,%d]", n.count, minCmds, maxCmds))
		}
	}
	for i := int16(1); i < n.count; i++ {
		if cmp(n.cmds[i-1], n.cmds[i]) >= 0 {
			panic(fmt.Sprintf("cmds are not sorted @ %d: %v >= %v",
				i, n.cmds[i-1], n.cmds[i]))
		}
	}
	if !n.leaf {
		for i := int16(0); i < n.count; i++ {
			prev := n.children[i]

			if cmp(prev.cmds[prev.count-1], n.cmds[i]) >= 0 {
				panic(fmt.Sprintf("cmds are not sorted @ %d: %v >= %v",
					i, n.cmds[i], prev.cmds[prev.count-1]))
			}
			next := n.children[i+1]
			if cmp(n.cmds[i], next.cmds[0]) >= 0 {
				panic(fmt.Sprintf("cmds are not sorted @ %d: %v >= %v",
					i, n.cmds[i], next.cmds[0]))
			}
		}
		for i := int16(0); i <= n.count; i++ {
			if n.children[i].pos != i {
				panic(fmt.Sprintf("child has incorrect pos: %d != %d/%d", n.children[i].pos, i, n.count))
			}
			if n.children[i].parent != n {
				panic(fmt.Sprintf("child does not point to parent: %d/%d", i, n.count))
			}
			n.children[i].verify()
		}
	}
}

// Verify asserts that the tree's structural invariants all hold.
func (t *btree) Verify() {
	if t.root == nil {
		return
	}
	t.root.verify()
}

func key(i int) roachpb.Key {
	return []byte(fmt.Sprintf("%04d", i))
}

func newCmd(k roachpb.Key) *cmd {
	return &cmd{span: roachpb.Span{Key: k}}
}

func checkIter(t *testing.T, it iterator, start, end int) {
	t.Helper()
	i := start
	for it.First(); it.Valid(); it.Next() {
		cmd := it.Cmd()
		expected := key(i)
		if !expected.Equal(cmd.span.Key) {
			t.Fatalf("expected %s, but found %s", expected, cmd.span.Key)
		}
		i++
	}
	if i != end {
		t.Fatalf("expected %d, but at %d", end, i)
	}

	for it.Last(); it.Valid(); it.Prev() {
		i--
		cmd := it.Cmd()
		expected := key(i)
		if !expected.Equal(cmd.span.Key) {
			t.Fatalf("expected %s, but found %s", expected, cmd.span.Key)
		}
	}
	if i != start {
		t.Fatalf("expected %d, but at %d: %+v parent=%p", start, i, it, it.n.parent)
	}
}

func TestBTree(t *testing.T) {
	var tr btree

	// With degree == 16 (max-items/node == 31) we need 513 items in order for
	// there to be 3 levels in the tree. The count here is comfortably above
	// that.
	const count = 768
	// Add keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Set(newCmd(key(i)))
		tr.Verify()
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), 0, i+1)
	}
	// Delete keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(newCmd(key(i)))
		tr.Verify()
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), i+1, count)
	}

	// Add keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Set(newCmd(key(count - i)))
		tr.Verify()
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), count-i, count+1)
	}
	// Delete keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(newCmd(key(count - i)))
		tr.Verify()
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), 1, count-i)
	}
}

func TestBTreeSeek(t *testing.T) {
	const count = 513

	var tr btree
	for i := 0; i < count; i++ {
		tr.Set(newCmd(key(i * 2)))
	}

	it := tr.MakeIter()
	for i := 0; i < 2*count-1; i++ {
		it.SeekGE(newCmd(key(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		cmd := it.Cmd()
		expected := key(2 * ((i + 1) / 2))
		if !expected.Equal(cmd.span.Key) {
			t.Fatalf("%d: expected %s, but found %s", i, expected, cmd.span.Key)
		}
	}
	it.SeekGE(newCmd(key(2*count - 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	for i := 1; i < 2*count; i++ {
		it.SeekLT(newCmd(key(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		cmd := it.Cmd()
		expected := key(2 * ((i - 1) / 2))
		if !expected.Equal(cmd.span.Key) {
			t.Fatalf("%d: expected %s, but found %s", i, expected, cmd.span.Key)
		}
	}
	it.SeekLT(newCmd(key(0)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}
}

func randomKey(rng *rand.Rand, b []byte) []byte {
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

func BenchmarkIterSeekGE(b *testing.B) {
	for _, count := range []int{16, 128, 1024} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			var keys [][]byte
			var tr btree

			for i := 0; i < count; i++ {
				key := []byte(fmt.Sprintf("%05d", i))
				keys = append(keys, key)
				tr.Set(newCmd(key))
			}

			rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
			it := tr.MakeIter()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[rng.Intn(len(keys))]
				it.SeekGE(newCmd(k))
				if testing.Verbose() {
					if !it.Valid() {
						b.Fatal("expected to find key")
					}
					if !bytes.Equal(k, it.Cmd().span.Key) {
						b.Fatalf("expected %s, but found %s", k, it.Cmd().span.Key)
					}
				}
			}
		})
	}
}

func BenchmarkIterSeekLT(b *testing.B) {
	for _, count := range []int{16, 128, 1024} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			var keys [][]byte
			var tr btree

			for i := 0; i < count; i++ {
				key := []byte(fmt.Sprintf("%05d", i))
				keys = append(keys, key)
				tr.Set(newCmd(key))
			}

			rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
			it := tr.MakeIter()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j := rng.Intn(len(keys))
				k := keys[j]
				it.SeekLT(newCmd(k))
				if testing.Verbose() {
					if j == 0 {
						if it.Valid() {
							b.Fatal("unexpected key")
						}
					} else {
						if !it.Valid() {
							b.Fatal("expected to find key")
						}
						k := keys[j-1]
						if !bytes.Equal(k, it.Cmd().span.Key) {
							b.Fatalf("expected %s, but found %s", k, it.Cmd().span.Key)
						}
					}
				}
			}
		})
	}
}

func BenchmarkIterNext(b *testing.B) {
	buf := make([]byte, 64<<10)
	var tr btree

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	for i := 0; i < len(buf); i += 8 {
		key := randomKey(rng, buf[i:i+8])
		tr.Set(newCmd(key))
	}

	it := tr.MakeIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.First()
		}
		it.Next()
	}
}

func BenchmarkIterPrev(b *testing.B) {
	buf := make([]byte, 64<<10)
	var tr btree

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	for i := 0; i < len(buf); i += 8 {
		key := randomKey(rng, buf[i:i+8])
		tr.Set(newCmd(key))
	}

	it := tr.MakeIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.Last()
		}
		it.Prev()
	}
}
