// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package interval

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/sync/errgroup"
)

var btreeMinDegree = flag.Int("btree_min_degree", DefaultBTreeMinimumDegree, "B-Tree minimum degree")

func init() {
	seed := timeutil.Now().Unix()
	rand.Seed(seed)
}

// perm returns a random permutation of intervals whose range start is in the
// range [0, n).
func perm(n uint32) (out items) {
	for _, i := range rand.Perm(int(n)) {
		u := uint32(i)
		iv := makeMultiByteInterval(u, u+1, u)
		out = append(out, iv)
	}
	return
}

// rang returns an ordered list of intervals in the range [m, n].
func rang(m, n uint32) (out items) {
	for u := m; u <= n; u++ {
		iv := makeMultiByteInterval(u, u+1, u)
		out = append(out, iv)
	}
	return
}

// all extracts all items from a tree in order as a slice.
func all(t *btree) (out items) {
	t.Do(func(a Interface) bool {
		out = append(out, a)
		return false
	})
	return
}

func makeMultiByteInterval(start, end, id uint32) *Interval {
	return &Interval{Range{toBytes(start), toBytes(end)}, uintptr(id)}
}

func toBytes(n uint32) Comparable {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, n); err != nil {
		panic(fmt.Sprintf("binary.Write error: %s", err))
	}
	return Comparable(buf.Bytes())
}

type Interval struct {
	r  Range
	id uintptr
}

func (iv *Interval) Range() Range {
	return iv.r
}

func (iv *Interval) ID() uintptr {
	return iv.id
}

func (iv *Interval) String() string {
	return fmt.Sprintf("%v-%d", iv.Range(), iv.ID())
}

func (items items) Len() int {
	return len(items)
}

func (items items) Less(i, j int) bool {
	return Compare(items[i], items[j]) <= 0
}

func (items items) Swap(i, j int) {
	items[i], items[j] = items[j], items[i]
}

func (children children) Len() int {
	return len(children)
}

func (children children) Less(i, j int) bool {
	return children[i].Range.Start.Compare(children[j].Range.Start) <= 0
}

func (children children) Swap(i, j int) {
	children[i], children[j] = children[j], children[i]
}

// describe returns a string description of the tree. The format is similar to
// https://en.wikipedia.org/wiki/Newick_format
func (tree *btree) describe() string {
	if tree.isEmpty() {
		return ";"
	}
	return tree.root.String()
}

var _ = (*btree).describe

func (n node) String() string {
	var buf bytes.Buffer
	n.describe(&buf)
	return buf.String()
}

func (n *node) describe(buf *bytes.Buffer) {
	if len(n.children) == 0 {
		for idx, i := range n.items {
			if idx != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(i.(*Interval).String())
		}
	}
	for i, c := range n.children {
		buf.WriteString("(")
		c.describe(buf)
		buf.WriteString(fmt.Sprintf(":%s", c.Range))
		buf.WriteString(")")
		if i < len(n.children)-1 {
			buf.WriteString(n.items[i].(*Interval).String())
		}
	}
}

func (n *node) isKeyInRange(t *testing.T, min, max Comparable) bool {
	for _, i := range n.items {
		start := i.Range().Start
		if min != nil && start.Compare(min) < 0 {
			return false
		}
		if max != nil && start.Compare(max) > 0 {
			return false
		}
	}
	oldMin, oldMax := min, max
	for i, c := range n.children {
		min, max := oldMin, oldMax
		if i != 0 {
			min = n.items[i-1].Range().Start
		}
		if i != len(n.children)-1 {
			max = n.items[i].Range().Start
		}
		if !c.isKeyInRange(t, min, max) {
			return false
		}
	}
	return true
}

func (n *node) isSorted(t *testing.T) bool {
	for _, c := range n.children {
		if !c.isSorted(t) {
			return false
		}
	}
	if !sort.IsSorted(n.items) {
		return false
	}
	if !sort.IsSorted(n.children) {
		return false
	}
	return true
}

func (tree *btree) computeHeight() (h int) {
	h = -1
	for node := tree.root; ; {
		h++
		if len(node.children) == 0 {
			break
		}
		node = node.children[0]
	}
	return
}

func (n *node) isDepthEqualToHeight(t *testing.T, depth, height int) bool {
	if len(n.children) == 0 {
		return depth == height
	}
	for _, c := range n.children {
		if !c.isDepthEqualToHeight(t, depth+1, height) {
			return false
		}
	}
	return true
}

func (n *node) isCountAllowed(t *testing.T, minItems, maxItems int, root bool) bool {
	iLen := len(n.items)
	cLen := len(n.children)
	if !root {
		iAllowed := minItems <= iLen && iLen <= maxItems
		if !iAllowed {
			return false
		}
	}
	if cLen > 0 {
		cAllowed := cLen == iLen+1
		if !cAllowed {
			return false
		}
		for _, c := range n.children {
			allowed := c.isCountAllowed(t, minItems, maxItems, false)
			if !allowed {
				return false
			}
		}
	}
	return true
}

func (n *node) isIntervalInRange(t *testing.T) bool {
	for _, c := range n.children {
		if !c.isIntervalInRange(t) {
			return false
		}
	}
	r := n.bound()
	if !n.Range.Equal(r) {
		t.Errorf("%v expected range %v, got %v", n, r, n.Range)
		return false
	}
	return true
}

func (r *Range) combine(other Range) {
	if r.Start.Compare(other.Start) > 0 {
		r.Start = other.Start
	}
	if r.End.Compare(other.End) < 0 {
		r.End = other.End
	}
}

func (n *node) bound() Range {
	r := n.items[0].Range()
	ptr := &r
	for _, e := range n.items[1:] {
		ptr.combine(e.Range())
	}
	for _, c := range n.children {
		ptr.combine(c.Range)
	}
	return r
}

func checkWithLen(t *testing.T, tree *btree, l int) {
	if tree.Len() != l {
		t.Errorf("expected tree length %d, got %d", l, tree.Len())
	}
	check(t, tree)
}

func check(t *testing.T, tree *btree) {
	if !tree.isLeafSameDepth(t) {
		t.Error("Not all the leaves have the same depth as the tree height")
	}
	if !tree.isCountAllowed(t) {
		t.Error("Not all the nodes have allowed key count and child node count")
	}
	if !tree.isIntervalInRange(t) {
		t.Error("Not all the nodes bound all the intervals in its subtree with its Range field")
	}
	if !tree.isSorted(t) {
		t.Error("Not all the nodes have its items and children fields sorted")
	}
	if !tree.isKeyInRange(t) {
		t.Error("not all the nodes keep node keys (range.start) in range")
	}
}

func (tree *btree) isLeafSameDepth(t *testing.T) bool {
	if tree.isEmpty() {
		return true
	}
	h := tree.computeHeight()
	t.Logf("tree height: %d", h)
	return tree.root.isDepthEqualToHeight(t, 0, h)
}

func (tree *btree) isCountAllowed(t *testing.T) bool {
	if tree.isEmpty() {
		return true
	}
	return tree.root.isCountAllowed(t, tree.minItems(), tree.maxItems(), true)
}

// Does every node correctly annotate the range of its children.
func (tree *btree) isIntervalInRange(t *testing.T) bool {
	if tree.isEmpty() {
		return true
	}
	return tree.root.isIntervalInRange(t)
}

func (tree *btree) isSorted(t *testing.T) bool {
	if tree.isEmpty() {
		return true
	}
	return tree.root.isSorted(t)
}

func (tree *btree) isKeyInRange(t *testing.T) bool {
	if tree.isEmpty() {
		return true
	}
	return tree.root.isKeyInRange(t, nil, nil)
}

func checkEqualIntervals(t *testing.T, actual, expected items) {
	for i := 0; i < len(actual)-1; i++ {
		if actual[i].Range().Start.Compare(actual[i+1].Range().Start) > 0 {
			t.Fatalf("interval slice is not sorted: %v", actual)
			break
		}
	}
	itemsLen := len(expected)
	sortedExpected := make(items, itemsLen)
	copy(sortedExpected, expected)
	sort.Sort(sortedExpected)
	if !reflect.DeepEqual(actual, sortedExpected) {
		t.Errorf("expected intervals %v, got %v", expected, actual)
	}
}

func checkTraversal(t *testing.T, tree *btree, ivs items) {
	// Get, GetWithOverlapper
	r := Range{Comparable{0x0}, Comparable{0x1}}
	expectedIntervals := items{ivs[0], ivs[2], ivs[4]}
	checkEqualIntervals(t, tree.Get(r), expectedIntervals)
	checkEqualIntervals(t, tree.GetWithOverlapper(r, ExclusiveOverlapper), items{ivs[0]})

	// DoMatching
	var overlapped items
	tree.DoMatching(func(e Interface) bool {
		overlapped = append(overlapped, e)
		return false
	}, r)
	checkEqualIntervals(t, overlapped, expectedIntervals)

	// Do
	var all items
	tree.Do(func(e Interface) bool {
		all = append(all, e)
		return false
	})
	checkEqualIntervals(t, all, ivs)
}

func checkIterator(t *testing.T, tree *btree, ivs items) {
	var actual items
	it := tree.Iterator()
	for r, ok := it.Next(); ok; r, ok = it.Next() {
		actual = append(actual, r)
	}
	checkEqualIntervals(t, actual, ivs)
}

func checkFastDelete(t *testing.T, tree *btree, ivs items, deleteCount int) {
	for i, iv := range ivs[:deleteCount] {
		if err := tree.Delete(iv, true); err != nil {
			t.Fatalf("delete error: %s", err)
		}
		// Unlike fast insert, AdjustRanges must be called after each fast delete.
		// Otherwise, the following fast deletes may go wrong.
		tree.AdjustRanges()
		checkWithLen(t, tree, len(ivs)-i-1)
	}
}

func makeIntervals() items {
	ivs := items{
		&Interval{Range{Comparable{0}, Comparable{2}}, 0},
		&Interval{Range{Comparable{2}, Comparable{4}}, 0},
		&Interval{Range{Comparable{1}, Comparable{6}}, 0},
		&Interval{Range{Comparable{3}, Comparable{4}}, 0},
		&Interval{Range{Comparable{1}, Comparable{3}}, 0},
		&Interval{Range{Comparable{4}, Comparable{6}}, 0},
		&Interval{Range{Comparable{5}, Comparable{8}}, 0},
		&Interval{Range{Comparable{6}, Comparable{8}}, 0},
		&Interval{Range{Comparable{5}, Comparable{9}}, 0},
		&Interval{Range{Comparable{0x11}, Comparable{0x13}}, 0},
		&Interval{Range{Comparable{0x14}, Comparable{0x16}}, 0},
		&Interval{Range{Comparable{0x15}, Comparable{0x18}}, 0},
		&Interval{Range{Comparable{0x10}, Comparable{0x12}}, 0},
		&Interval{Range{Comparable{0x20}, Comparable{0x62}}, 0},
		&Interval{Range{Comparable{0x24}, Comparable{0xA0}}, 0},
		&Interval{Range{Comparable{0x31}, Comparable{0x63}}, 0},
		&Interval{Range{Comparable{0x44}, Comparable{0x56}}, 0},
		&Interval{Range{Comparable{0x45}, Comparable{0x68}}, 0},
		&Interval{Range{Comparable{0x30}, Comparable{0x72}}, 0},
		&Interval{Range{Comparable{0x30}, Comparable{0x52}}, 0},
		&Interval{Range{Comparable{0x44}, Comparable{0xB0}}, 0},
	}
	for i, iv := range ivs {
		iv.(*Interval).id = uintptr(i)
	}
	return ivs
}

// TestBTree is based on https://github.com/google/btree/blob/master/btree_test.go.
func TestBTree(t *testing.T) {
	tree := newBTreeWithDegree(InclusiveOverlapper, *btreeMinDegree)
	const treeSize = 10000
	for i := 0; i < 10; i++ {
		for _, iv := range perm(treeSize) {
			if x := tree.Insert(iv, false); x != nil {
				t.Fatalf("insert found interval %v", x)
			}
		}

		for _, iv := range perm(treeSize) {
			if x := tree.Insert(iv, false); x != nil {
				t.Fatalf("insert didn't find interval %v", x)
			}
		}

		var all items
		tree.DoMatching(func(e Interface) bool {
			all = append(all, e)
			return false
		}, Range{toBytes(0), toBytes(treeSize)})
		if expected := rang(0, treeSize-1); !reflect.DeepEqual(all, expected) {
			t.Fatalf("expected intervals %v, got %v", expected, all)
		}

		var slice items
		min := uint32(10)
		max := uint32(20)
		tree.DoMatching(func(e Interface) bool {
			slice = append(slice, e)
			return false
		}, Range{toBytes(min + 1), toBytes(max)})
		if expected := rang(min, max); !reflect.DeepEqual(slice, expected) {
			t.Fatalf("expected intervals %v, got %v", expected, slice)
		}

		var halfSlice items
		half := uint32(15)
		tree.DoMatching(func(e Interface) bool {
			if e.Range().Start.Compare(toBytes(half)) > 0 {
				return true
			}
			halfSlice = append(halfSlice, e)
			return false
		}, Range{toBytes(min + 1), toBytes(max)})
		if expected := rang(min, half); !reflect.DeepEqual(halfSlice, expected) {
			t.Fatalf("expected intervals %v, got %v", expected, halfSlice)
		}

		for _, item := range perm(treeSize) {
			if err := tree.Delete(item, false); err != nil {
				t.Fatalf("delete error: %s", err)
			}
		}

		if len := tree.Len(); len > 0 {
			t.Fatalf("expected 0 item, got %d items", len)
		}
	}
}

// TestDeleteAfterRootNodeMerge verifies that delete from a leaf node works
// correctly after a merge which involves the root node. During the delete of a
// Interface from a leaf node, if the root node has only one Interface and takes
// part of a merge, the root does have any Interface after the merge. The
// subsequent adjustment of node range should take this into account.
func TestDeleteAfterRootNodeMerge(t *testing.T) {
	tree := newBTreeWithDegree(InclusiveOverlapper, 2)
	ivs := items{
		&Interval{Range{Comparable{1}, Comparable{8}}, 0},
		&Interval{Range{Comparable{2}, Comparable{3}}, 1},
		&Interval{Range{Comparable{3}, Comparable{4}}, 2},
		&Interval{Range{Comparable{4}, Comparable{5}}, 3},
	}

	//
	//             +------+
	//             | id-1 |
	//             +------+
	//              /    \
	//             v      v
	//       +------+    +-----------+
	//       | id-0 |    | id-2 id-3 |
	//       +------+    +-----------+
	//
	for i := 0; i < len(ivs); i++ {
		if err := tree.Insert(ivs[i], false); err != nil {
			t.Fatalf("insert error: %s", err)
		}
	}

	//
	//             +------+
	//             | id-1 |
	//             +------+
	//              /    \
	//             v      v
	//       +------+    +------+
	//       | id-0 |    | id-3 |
	//       +------+    +------+
	//
	if err := tree.Delete(ivs[2], false); err != nil {
		t.Fatalf("delete error: %s", err)
	}

	// Delete id-0
	if err := tree.Delete(ivs[0], false); err != nil {
		t.Fatalf("delete error: %s", err)
	}
}

func TestSmallTree(t *testing.T) {
	tree := newBTreeWithDegree(InclusiveOverlapper, 2)
	ivs := makeIntervals()

	// Insert
	for i, iv := range ivs {
		if err := tree.Insert(iv, false); err != nil {
			t.Fatalf("insert error: %s", err)
		}
		checkWithLen(t, tree, i+1)
	}

	checkTraversal(t, tree, ivs)
	checkIterator(t, tree, ivs)

	// Delete
	l := tree.Len()
	for i, iv := range ivs {
		if err := tree.Delete(iv, false); err != nil {
			t.Fatalf("delete error: %s", err)
		}
		checkWithLen(t, tree, l-i-1)
	}
}

func TestSmallTreeWithFastOperations(t *testing.T) {
	tree := newBTreeWithDegree(InclusiveOverlapper, 2)
	ivs := makeIntervals()

	// Fast insert
	for _, iv := range ivs {
		if err := tree.Insert(iv, true); err != nil {
			t.Fatalf("insert error: %s", err)
		}
	}
	tree.AdjustRanges()
	checkWithLen(t, tree, len(ivs))

	checkTraversal(t, tree, ivs)
	checkIterator(t, tree, ivs)
	checkFastDelete(t, tree, ivs, tree.Len())
}

func TestLargeTree(t *testing.T) {
	var ivs items

	const treeSize = 40000
	for i := uint32(0); i < treeSize; i++ {
		iv := makeMultiByteInterval(i, i+1, i)
		ivs = append(ivs, iv)
	}

	tree := newBTreeWithDegree(ExclusiveOverlapper, *btreeMinDegree)
	for _, iv := range ivs {
		if err := tree.Insert(iv, true); err != nil {
			t.Fatalf("fast insert error: %s", err)
		}
	}
	tree.AdjustRanges()
	checkWithLen(t, tree, treeSize)
	checkFastDelete(t, tree, ivs, 10)
}

const cloneTestSize = 10000

func cloneTest(
	t *testing.T, b *btree, start int, p items, g *errgroup.Group, treeC chan *btree,
) error {
	t.Logf("Starting new clone at %v", start)
	treeC <- b
	for i := start; i < cloneTestSize; i++ {
		if err := b.Insert(p[i], false); err != nil {
			return err
		}
		if i%(cloneTestSize/5) == 0 {
			i := i
			c := b.cloneInternal()
			g.Go(func() error {
				return cloneTest(t, c, i+1, p, g, treeC)
			})
		}
	}
	return nil
}

func TestCloneConcurrentOperations(t *testing.T) {
	var trees []*btree
	treeC, treeDone := make(chan *btree), make(chan struct{})
	go func() {
		for b := range treeC {
			trees = append(trees, b)
		}
		close(treeDone)
	}()

	var g errgroup.Group
	b := newBTree(InclusiveOverlapper)
	p := perm(cloneTestSize)
	g.Go(func() error {
		return cloneTest(t, b, 0, p, &g, treeC)
	})
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	close(treeC)
	<-treeDone

	want := rang(0, cloneTestSize-1)
	t.Logf("Starting equality checks on %d trees", len(trees))
	for i, tree := range trees {
		if !reflect.DeepEqual(want, all(tree)) {
			t.Errorf("tree %v mismatch", i)
		}
	}

	t.Log("Removing half from first half")
	toRemove := want[cloneTestSize/2:]
	for i := 0; i < len(trees)/2; i++ {
		tree := trees[i]
		g.Go(func() error {
			for _, item := range toRemove {
				if err := tree.Delete(item, false); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	t.Log("Checking all values again")
	for i, tree := range trees {
		var wantpart items
		if i < len(trees)/2 {
			wantpart = want[:cloneTestSize/2]
		} else {
			wantpart = want
		}
		if got := all(tree); !reflect.DeepEqual(wantpart, got) {
			t.Errorf("tree %v mismatch, want %v got %v", i, len(want), len(got))
		}
	}
}

func TestIterator(t *testing.T) {
	var ivs items
	const treeSize = 400
	for i := uint32(0); i < treeSize; i++ {
		iv := makeMultiByteInterval(i, i+1, i)
		ivs = append(ivs, iv)
	}
	tree := newBTreeWithDegree(InclusiveOverlapper, 4)
	for _, iv := range ivs {
		if err := tree.Insert(iv, true); err != nil {
			t.Fatalf("fast insert error: %s", err)
		}
	}
	tree.AdjustRanges()
	checkIterator(t, tree, ivs)
}

func forBenchmarkSizes(b *testing.B, f func(b *testing.B, count int)) {
	for _, count := range []int{16, 128, 1024, 8192, 65536} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			f(b, count)
		})
	}
}

func BenchmarkBTreeInsert(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(uint32(count))
		b.ResetTimer()
		i := 0
		for i < b.N {
			tr := newBTree(InclusiveOverlapper)
			for _, item := range insertP {
				if err := tr.Insert(item, false); err != nil {
					b.Fatal(err)
				}
				i++
				if i >= b.N {
					return
				}
			}
		}
	})
}

func BenchmarkBTreeDelete(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP, removeP := perm(uint32(count)), perm(uint32(count))
		b.ResetTimer()
		i := 0
		for i < b.N {
			b.StopTimer()
			tr := newBTree(InclusiveOverlapper)
			for _, item := range insertP {
				if err := tr.Insert(item, false); err != nil {
					b.Fatal(err)
				}
			}
			b.StartTimer()
			for _, item := range removeP {
				if err := tr.Delete(item, false); err != nil {
					b.Fatal(err)
				}
				i++
				if i >= b.N {
					return
				}
			}
			if tr.Len() > 0 {
				panic(tr.Len())
			}
		}
	})
}

func BenchmarkBTreeDeleteInsert(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(uint32(count))
		tr := newBTree(InclusiveOverlapper)
		for _, item := range insertP {
			if err := tr.Insert(item, false); err != nil {
				b.Fatal(err)
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tr.Delete(insertP[i%count], false); err != nil {
				b.Fatal(err)
			}
			if err := tr.Insert(insertP[i%count], false); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBTreeDeleteInsertCloneOnce(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(uint32(count))
		tr := newBTree(InclusiveOverlapper)
		for _, item := range insertP {
			if err := tr.Insert(item, false); err != nil {
				b.Fatal(err)
			}
		}
		tr = tr.cloneInternal()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tr.Delete(insertP[i%count], false); err != nil {
				b.Fatal(err)
			}
			if err := tr.Insert(insertP[i%count], false); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBTreeDeleteInsertCloneEachTime(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(uint32(count))
		tr := newBTree(InclusiveOverlapper)
		for _, item := range insertP {
			if err := tr.Insert(item, false); err != nil {
				b.Fatal(err)
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tr = tr.cloneInternal()
			if err := tr.Delete(insertP[i%count], false); err != nil {
				b.Fatal(err)
			}
			if err := tr.Insert(insertP[i%count], false); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBTreeGet(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(uint32(count))
		removeP := perm(uint32(count))
		b.ResetTimer()
		i := 0
		for i < b.N {
			b.StopTimer()
			tr := newBTree(InclusiveOverlapper)
			for _, item := range insertP {
				if err := tr.Insert(item, false); err != nil {
					b.Fatal(err)
				}
			}
			b.StartTimer()
			for _, item := range removeP {
				tr.Get(item.Range())
				i++
				if i >= b.N {
					return
				}
			}
		}
	})
}

func BenchmarkBTreeGetCloneEachTime(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(uint32(count))
		removeP := perm(uint32(count))
		b.ResetTimer()
		i := 0
		for i < b.N {
			b.StopTimer()
			tr := newBTree(InclusiveOverlapper)
			for _, v := range insertP {
				if err := tr.Insert(v, false); err != nil {
					b.Fatal(err)
				}
			}
			b.StartTimer()
			for _, item := range removeP {
				tr = tr.cloneInternal()
				tr.Get(item.Range())
				i++
				if i >= b.N {
					return
				}
			}
		}
	})
}

func key(i int) Comparable {
	return []byte(fmt.Sprintf("%04d", i))
}

func rangeWithEnd(start, end int) Range {
	return Range{Start: key(start), End: key(end)}
}

func randomRange(rng *rand.Rand, n int) Range {
	start := rng.Intn(n)
	end := rng.Intn(n + 1)
	if end < start {
		start, end = end, start
	}
	return rangeWithEnd(start, end)
}

func BenchmarkBTreeOverlapScan(b *testing.B) {
	tr := newBTree(InclusiveOverlapper)
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	const count = 8 << 10
	const size = 2 * 31
	for i := 0; i < count; i++ {
		iv := &Interval{rangeWithEnd(i, i+size+1), uintptr(i)}
		if err := tr.Insert(iv, false); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := randomRange(rng, count)
		tr.DoMatching(func(e Interface) bool {
			return false
		}, cmd)
	}
}
