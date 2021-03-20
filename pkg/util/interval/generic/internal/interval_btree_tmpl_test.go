// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build ignore

package internal

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	// Load pkg/keys so that roachpb.Span.String() could be executed correctly.
	_ "github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func newItem(s roachpb.Span) T {
	i := nilT.New()
	i.SetKey(s.Key)
	i.SetEndKey(s.EndKey)
	return i
}

func spanFromItem(i T) roachpb.Span {
	return roachpb.Span{Key: i.Key(), EndKey: i.EndKey()}
}

//////////////////////////////////////////
//        Invariant verification        //
//////////////////////////////////////////

// Verify asserts that the tree's structural invariants all hold.
func (t *btree) Verify(tt *testing.T) {
	if t.length == 0 {
		require.Nil(tt, t.root)
		return
	}
	t.verifyLeafSameDepth(tt)
	t.verifyCountAllowed(tt)
	t.isSorted(tt)
	t.isUpperBoundCorrect(tt)
}

func (t *btree) verifyLeafSameDepth(tt *testing.T) {
	h := t.Height()
	t.root.verifyDepthEqualToHeight(tt, 1, h)
}

func (n *node) verifyDepthEqualToHeight(t *testing.T, depth, height int) {
	if n.leaf {
		require.Equal(t, height, depth, "all leaves should have the same depth as the tree height")
	}
	n.recurse(func(child *node, _ int16) {
		child.verifyDepthEqualToHeight(t, depth+1, height)
	})
}

func (t *btree) verifyCountAllowed(tt *testing.T) {
	t.root.verifyCountAllowed(tt, true)
}

func (n *node) verifyCountAllowed(t *testing.T, root bool) {
	if !root {
		require.GreaterOrEqual(t, n.count, int16(minItems), "latch count %d must be in range [%d,%d]", n.count, minItems, maxItems)
		require.LessOrEqual(t, n.count, int16(maxItems), "latch count %d must be in range [%d,%d]", n.count, minItems, maxItems)
	}
	for i, item := range n.items {
		if i < int(n.count) {
			require.NotNil(t, item, "latch below count")
		} else {
			require.Nil(t, item, "latch above count")
		}
	}
	if !n.leaf {
		for i, child := range n.children {
			if i <= int(n.count) {
				require.NotNil(t, child, "node below count")
			} else {
				require.Nil(t, child, "node above count")
			}
		}
	}
	n.recurse(func(child *node, _ int16) {
		child.verifyCountAllowed(t, false)
	})
}

func (t *btree) isSorted(tt *testing.T) {
	t.root.isSorted(tt)
}

func (n *node) isSorted(t *testing.T) {
	for i := int16(1); i < n.count; i++ {
		require.LessOrEqual(t, cmp(n.items[i-1], n.items[i]), 0)
	}
	if !n.leaf {
		for i := int16(0); i < n.count; i++ {
			prev := n.children[i]
			next := n.children[i+1]

			require.LessOrEqual(t, cmp(prev.items[prev.count-1], n.items[i]), 0)
			require.LessOrEqual(t, cmp(n.items[i], next.items[0]), 0)
		}
	}
	n.recurse(func(child *node, _ int16) {
		child.isSorted(t)
	})
}

func (t *btree) isUpperBoundCorrect(tt *testing.T) {
	t.root.isUpperBoundCorrect(tt)
}

func (n *node) isUpperBoundCorrect(t *testing.T) {
	require.Equal(t, 0, n.findUpperBound().compare(n.max))
	for i := int16(1); i < n.count; i++ {
		require.LessOrEqual(t, upperBound(n.items[i]).compare(n.max), 0)
	}
	if !n.leaf {
		for i := int16(0); i <= n.count; i++ {
			child := n.children[i]
			require.LessOrEqual(t, child.max.compare(n.max), 0)
		}
	}
	n.recurse(func(child *node, _ int16) {
		child.isUpperBoundCorrect(t)
	})
}

func (n *node) recurse(f func(child *node, pos int16)) {
	if !n.leaf {
		for i := int16(0); i <= n.count; i++ {
			f(n.children[i], i)
		}
	}
}

//////////////////////////////////////////
//              Unit Tests              //
//////////////////////////////////////////

func key(i int) roachpb.Key {
	if i < 0 || i > 99999 {
		panic("key out of bounds")
	}
	return []byte(fmt.Sprintf("%05d", i))
}

func span(i int) roachpb.Span {
	switch i % 10 {
	case 0:
		return roachpb.Span{Key: key(i)}
	case 1:
		return roachpb.Span{Key: key(i), EndKey: key(i).Next()}
	case 2:
		return roachpb.Span{Key: key(i), EndKey: key(i + 64)}
	default:
		return roachpb.Span{Key: key(i), EndKey: key(i + 4)}
	}
}

func spanWithEnd(start, end int) roachpb.Span {
	if start < end {
		return roachpb.Span{Key: key(start), EndKey: key(end)}
	} else if start == end {
		return roachpb.Span{Key: key(start)}
	} else {
		panic("illegal span")
	}
}

func spanWithMemo(i int, memo map[int]roachpb.Span) roachpb.Span {
	if s, ok := memo[i]; ok {
		return s
	}
	s := span(i)
	memo[i] = s
	return s
}

func randomSpan(rng *rand.Rand, n int) roachpb.Span {
	start := rng.Intn(n)
	end := rng.Intn(n + 1)
	if end < start {
		start, end = end, start
	}
	return spanWithEnd(start, end)
}

func checkIter(t *testing.T, it iterator, start, end int, spanMemo map[int]roachpb.Span) {
	i := start
	for it.First(); it.Valid(); it.Next() {
		item := it.Cur()
		expected := spanWithMemo(i, spanMemo)
		if !expected.Equal(spanFromItem(item)) {
			t.Fatalf("expected %s, but found %s", expected, spanFromItem(item))
		}
		i++
	}
	if i != end {
		t.Fatalf("expected %d, but at %d", end, i)
	}

	for it.Last(); it.Valid(); it.Prev() {
		i--
		item := it.Cur()
		expected := spanWithMemo(i, spanMemo)
		if !expected.Equal(spanFromItem(item)) {
			t.Fatalf("expected %s, but found %s", expected, spanFromItem(item))
		}
	}
	if i != start {
		t.Fatalf("expected %d, but at %d: %+v", start, i, it)
	}

	all := newItem(spanWithEnd(start, end))
	for it.FirstOverlap(all); it.Valid(); it.NextOverlap(all) {
		item := it.Cur()
		expected := spanWithMemo(i, spanMemo)
		if !expected.Equal(spanFromItem(item)) {
			t.Fatalf("expected %s, but found %s", expected, spanFromItem(item))
		}
		i++
	}
	if i != end {
		t.Fatalf("expected %d, but at %d", end, i)
	}
}

// TestBTree tests basic btree operations.
func TestBTree(t *testing.T) {
	var tr btree
	spanMemo := make(map[int]roachpb.Span)

	// With degree == 16 (max-items/node == 31) we need 513 items in order for
	// there to be 3 levels in the tree. The count here is comfortably above
	// that.
	const count = 768

	// Add keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Set(newItem(span(i)))
		tr.Verify(t)
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), 0, i+1, spanMemo)
	}

	// Delete keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(newItem(span(i)))
		tr.Verify(t)
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), i+1, count, spanMemo)
	}

	// Add keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Set(newItem(span(count - i)))
		tr.Verify(t)
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), count-i, count+1, spanMemo)
	}

	// Delete keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(newItem(span(count - i)))
		tr.Verify(t)
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), 1, count-i, spanMemo)
	}
}

// TestBTreeSeek tests basic btree iterator operations.
func TestBTreeSeek(t *testing.T) {
	const count = 513

	var tr btree
	for i := 0; i < count; i++ {
		tr.Set(newItem(span(i * 2)))
	}

	it := tr.MakeIter()
	for i := 0; i < 2*count-1; i++ {
		it.SeekGE(newItem(span(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		item := it.Cur()
		expected := span(2 * ((i + 1) / 2))
		if !expected.Equal(spanFromItem(item)) {
			t.Fatalf("%d: expected %s, but found %s", i, expected, spanFromItem(item))
		}
	}
	it.SeekGE(newItem(span(2*count - 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	for i := 1; i < 2*count; i++ {
		it.SeekLT(newItem(span(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		item := it.Cur()
		expected := span(2 * ((i - 1) / 2))
		if !expected.Equal(spanFromItem(item)) {
			t.Fatalf("%d: expected %s, but found %s", i, expected, spanFromItem(item))
		}
	}
	it.SeekLT(newItem(span(0)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}
}

// TestBTreeSeekOverlap tests btree iterator overlap operations.
func TestBTreeSeekOverlap(t *testing.T) {
	const count = 513
	const size = 2 * maxItems

	var tr btree
	for i := 0; i < count; i++ {
		tr.Set(newItem(spanWithEnd(i, i+size+1)))
	}

	// Iterate over overlaps with a point scan.
	it := tr.MakeIter()
	for i := 0; i < count+size; i++ {
		scanItem := newItem(spanWithEnd(i, i))
		it.FirstOverlap(scanItem)
		for j := 0; j < size+1; j++ {
			expStart := i - size + j
			if expStart < 0 {
				continue
			}
			if expStart >= count {
				continue
			}

			if !it.Valid() {
				t.Fatalf("%d/%d: expected valid iterator", i, j)
			}
			item := it.Cur()
			expected := spanWithEnd(expStart, expStart+size+1)
			if !expected.Equal(spanFromItem(item)) {
				t.Fatalf("%d: expected %s, but found %s", i, expected, spanFromItem(item))
			}

			it.NextOverlap(scanItem)
		}
		if it.Valid() {
			t.Fatalf("%d: expected invalid iterator %v", i, it.Cur())
		}
	}
	it.FirstOverlap(newItem(span(count + size + 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	// Iterate over overlaps with a range scan.
	it = tr.MakeIter()
	for i := 0; i < count+size; i++ {
		scanItem := newItem(spanWithEnd(i, i+size+1))
		it.FirstOverlap(scanItem)
		for j := 0; j < 2*size+1; j++ {
			expStart := i - size + j
			if expStart < 0 {
				continue
			}
			if expStart >= count {
				continue
			}

			if !it.Valid() {
				t.Fatalf("%d/%d: expected valid iterator", i, j)
			}
			item := it.Cur()
			expected := spanWithEnd(expStart, expStart+size+1)
			if !expected.Equal(spanFromItem(item)) {
				t.Fatalf("%d: expected %s, but found %s", i, expected, spanFromItem(item))
			}

			it.NextOverlap(scanItem)
		}
		if it.Valid() {
			t.Fatalf("%d: expected invalid iterator %v", i, it.Cur())
		}
	}
	it.FirstOverlap(newItem(span(count + size + 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}
}

// TestBTreeCmp tests the btree item comparison.
func TestBTreeCmp(t *testing.T) {
	// NB: go_generics doesn't do well with anonymous types, so name this type.
	// Avoid the slice literal syntax, which GofmtSimplify mandates the use of
	// anonymous constructors with.
	type testCase struct {
		spanA, spanB roachpb.Span
		idA, idB     uint64
		exp          int
	}
	var testCases []testCase
	testCases = append(testCases,
		testCase{
			spanA: roachpb.Span{Key: roachpb.Key("a")},
			spanB: roachpb.Span{Key: roachpb.Key("a")},
			idA:   1,
			idB:   1,
			exp:   0,
		},
		testCase{
			spanA: roachpb.Span{Key: roachpb.Key("a")},
			spanB: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			idA:   1,
			idB:   1,
			exp:   -1,
		},
		testCase{
			spanA: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			spanB: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			idA:   1,
			idB:   1,
			exp:   1,
		},
		testCase{
			spanA: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			spanB: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			idA:   1,
			idB:   1,
			exp:   0,
		},
		testCase{
			spanA: roachpb.Span{Key: roachpb.Key("a")},
			spanB: roachpb.Span{Key: roachpb.Key("a")},
			idA:   1,
			idB:   2,
			exp:   -1,
		},
		testCase{
			spanA: roachpb.Span{Key: roachpb.Key("a")},
			spanB: roachpb.Span{Key: roachpb.Key("a")},
			idA:   2,
			idB:   1,
			exp:   1,
		},
		testCase{
			spanA: roachpb.Span{Key: roachpb.Key("b")},
			spanB: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			idA:   1,
			idB:   1,
			exp:   1,
		},
		testCase{
			spanA: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("e")},
			spanB: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
			idA:   1,
			idB:   1,
			exp:   -1,
		},
	)
	for _, tc := range testCases {
		name := fmt.Sprintf("cmp(%s:%d,%s:%d)", tc.spanA, tc.idA, tc.spanB, tc.idB)
		t.Run(name, func(t *testing.T) {
			laA := newItem(tc.spanA)
			laA.SetID(tc.idA)
			laB := newItem(tc.spanB)
			laB.SetID(tc.idB)
			require.Equal(t, tc.exp, cmp(laA, laB))
		})
	}
}

// TestIterStack tests the interface of the iterStack type.
func TestIterStack(t *testing.T) {
	f := func(i int) iterFrame { return iterFrame{pos: int16(i)} }
	var is iterStack
	for i := 1; i <= 2*len(iterStackArr{}); i++ {
		var j int
		for j = 0; j < i; j++ {
			is.push(f(j))
		}
		require.Equal(t, j, is.len())
		for j--; j >= 0; j-- {
			require.Equal(t, f(j), is.pop())
		}
		is.reset()
	}
}

//////////////////////////////////////////
//        Randomized Unit Tests         //
//////////////////////////////////////////

// perm returns a random permutation of items with spans in the range [0, n).
func perm(n int) (out []T) {
	for _, i := range rand.Perm(n) {
		out = append(out, newItem(spanWithEnd(i, i+1)))
	}
	return out
}

// rang returns an ordered list of items with spans in the range [m, n].
func rang(m, n int) (out []T) {
	for i := m; i <= n; i++ {
		out = append(out, newItem(spanWithEnd(i, i+1)))
	}
	return out
}

// all extracts all items from a tree in order as a slice.
func all(tr *btree) (out []T) {
	it := tr.MakeIter()
	it.First()
	for it.Valid() {
		out = append(out, it.Cur())
		it.Next()
	}
	return out
}

func run(tb testing.TB, name string, f func(testing.TB)) {
	switch v := tb.(type) {
	case *testing.T:
		v.Run(name, func(t *testing.T) {
			f(t)
		})
	case *testing.B:
		v.Run(name, func(b *testing.B) {
			f(b)
		})
	default:
		tb.Fatalf("unknown %T", tb)
	}
}

func iters(tb testing.TB, count int) int {
	switch v := tb.(type) {
	case *testing.T:
		return count
	case *testing.B:
		return v.N
	default:
		tb.Fatalf("unknown %T", tb)
		return 0
	}
}

func verify(tb testing.TB, tr *btree) {
	if tt, ok := tb.(*testing.T); ok {
		tr.Verify(tt)
	}
}

func resetTimer(tb testing.TB) {
	if b, ok := tb.(*testing.B); ok {
		b.ResetTimer()
	}
}

func stopTimer(tb testing.TB) {
	if b, ok := tb.(*testing.B); ok {
		b.StopTimer()
	}
}

func startTimer(tb testing.TB) {
	if b, ok := tb.(*testing.B); ok {
		b.StartTimer()
	}
}

func runBTreeInsert(tb testing.TB, count int) {
	iters := iters(tb, count)
	insertP := perm(count)
	resetTimer(tb)
	for i := 0; i < iters; {
		var tr btree
		for _, item := range insertP {
			tr.Set(item)
			verify(tb, &tr)
			i++
			if i >= iters {
				return
			}
		}
	}
}

func runBTreeDelete(tb testing.TB, count int) {
	iters := iters(tb, count)
	insertP, removeP := perm(count), perm(count)
	resetTimer(tb)
	for i := 0; i < iters; {
		stopTimer(tb)
		var tr btree
		for _, item := range insertP {
			tr.Set(item)
			verify(tb, &tr)
		}
		startTimer(tb)
		for _, item := range removeP {
			tr.Delete(item)
			verify(tb, &tr)
			i++
			if i >= iters {
				return
			}
		}
		if tr.Len() > 0 {
			tb.Fatalf("tree not empty: %s", &tr)
		}
	}
}

func runBTreeDeleteInsert(tb testing.TB, count int) {
	iters := iters(tb, count)
	insertP := perm(count)
	var tr btree
	for _, item := range insertP {
		tr.Set(item)
		verify(tb, &tr)
	}
	resetTimer(tb)
	for i := 0; i < iters; i++ {
		item := insertP[i%count]
		tr.Delete(item)
		verify(tb, &tr)
		tr.Set(item)
		verify(tb, &tr)
	}
}

func runBTreeDeleteInsertCloneOnce(tb testing.TB, count int) {
	iters := iters(tb, count)
	insertP := perm(count)
	var tr btree
	for _, item := range insertP {
		tr.Set(item)
		verify(tb, &tr)
	}
	tr = tr.Clone()
	resetTimer(tb)
	for i := 0; i < iters; i++ {
		item := insertP[i%count]
		tr.Delete(item)
		verify(tb, &tr)
		tr.Set(item)
		verify(tb, &tr)
	}
}

func runBTreeDeleteInsertCloneEachTime(tb testing.TB, count int) {
	for _, reset := range []bool{false, true} {
		run(tb, fmt.Sprintf("reset=%t", reset), func(tb testing.TB) {
			iters := iters(tb, count)
			insertP := perm(count)
			var tr, trReset btree
			for _, item := range insertP {
				tr.Set(item)
				verify(tb, &tr)
			}
			resetTimer(tb)
			for i := 0; i < iters; i++ {
				item := insertP[i%count]
				if reset {
					trReset.Reset()
					trReset = tr
				}
				tr = tr.Clone()
				tr.Delete(item)
				verify(tb, &tr)
				tr.Set(item)
				verify(tb, &tr)
			}
		})
	}
}

// randN returns a random integer in the range [min, max).
func randN(min, max int) int { return rand.Intn(max-min) + min }
func randCount() int {
	if testing.Short() {
		return randN(1, 128)
	}
	return randN(1, 1024)
}

func TestBTreeInsert(t *testing.T) {
	count := randCount()
	runBTreeInsert(t, count)
}

func TestBTreeDelete(t *testing.T) {
	count := randCount()
	runBTreeDelete(t, count)
}

func TestBTreeDeleteInsert(t *testing.T) {
	count := randCount()
	runBTreeDeleteInsert(t, count)
}

func TestBTreeDeleteInsertCloneOnce(t *testing.T) {
	count := randCount()
	runBTreeDeleteInsertCloneOnce(t, count)
}

func TestBTreeDeleteInsertCloneEachTime(t *testing.T) {
	count := randCount()
	runBTreeDeleteInsertCloneEachTime(t, count)
}

// TestBTreeSeekOverlapRandom tests btree iterator overlap operations using
// randomized input.
func TestBTreeSeekOverlapRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	const trials = 10
	for i := 0; i < trials; i++ {
		var tr btree

		const count = 1000
		items := make([]T, count)
		itemSpans := make([]int, count)
		for j := 0; j < count; j++ {
			var item T
			end := rng.Intn(count + 10)
			if end <= j {
				end = j
				item = newItem(spanWithEnd(j, end))
			} else {
				item = newItem(spanWithEnd(j, end+1))
			}
			tr.Set(item)
			items[j] = item
			itemSpans[j] = end
		}

		const scanTrials = 100
		for j := 0; j < scanTrials; j++ {
			var scanItem T
			scanStart := rng.Intn(count)
			scanEnd := rng.Intn(count + 10)
			if scanEnd <= scanStart {
				scanEnd = scanStart
				scanItem = newItem(spanWithEnd(scanStart, scanEnd))
			} else {
				scanItem = newItem(spanWithEnd(scanStart, scanEnd+1))
			}

			var exp, found []T
			for startKey, endKey := range itemSpans {
				if startKey <= scanEnd && endKey >= scanStart {
					exp = append(exp, items[startKey])
				}
			}

			it := tr.MakeIter()
			it.FirstOverlap(scanItem)
			for it.Valid() {
				found = append(found, it.Cur())
				it.NextOverlap(scanItem)
			}

			require.Equal(t, len(exp), len(found), "search for %v", spanFromItem(scanItem))
		}
	}
}

// TestBTreeCloneConcurrentOperations tests that cloning a btree returns a new
// btree instance which is an exact logical copy of the original but that can be
// modified independently going forward.
func TestBTreeCloneConcurrentOperations(t *testing.T) {
	const cloneTestSize = 1000
	p := perm(cloneTestSize)

	var trees []*btree
	treeC, treeDone := make(chan *btree), make(chan struct{})
	go func() {
		for b := range treeC {
			trees = append(trees, b)
		}
		close(treeDone)
	}()

	var wg sync.WaitGroup
	var populate func(tr *btree, start int)
	populate = func(tr *btree, start int) {
		t.Logf("Starting new clone at %v", start)
		treeC <- tr
		for i := start; i < cloneTestSize; i++ {
			tr.Set(p[i])
			if i%(cloneTestSize/5) == 0 {
				wg.Add(1)
				c := tr.Clone()
				go populate(&c, i+1)
			}
		}
		wg.Done()
	}

	wg.Add(1)
	var tr btree
	go populate(&tr, 0)
	wg.Wait()
	close(treeC)
	<-treeDone

	t.Logf("Starting equality checks on %d trees", len(trees))
	want := rang(0, cloneTestSize-1)
	for i, tree := range trees {
		if !reflect.DeepEqual(want, all(tree)) {
			t.Errorf("tree %v mismatch", i)
		}
	}

	t.Log("Removing half of items from first half")
	toRemove := want[cloneTestSize/2:]
	for i := 0; i < len(trees)/2; i++ {
		tree := trees[i]
		wg.Add(1)
		go func() {
			for _, item := range toRemove {
				tree.Delete(item)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	t.Log("Checking all values again")
	for i, tree := range trees {
		var wantpart []T
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

//////////////////////////////////////////
//              Benchmarks              //
//////////////////////////////////////////

func forBenchmarkSizes(b *testing.B, f func(b *testing.B, count int)) {
	for _, count := range []int{16, 128, 1024, 8192, 65536} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			f(b, count)
		})
	}
}

// BenchmarkBTreeInsert measures btree insertion performance.
func BenchmarkBTreeInsert(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		runBTreeInsert(b, count)
	})
}

// BenchmarkBTreeDelete measures btree deletion performance.
func BenchmarkBTreeDelete(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		runBTreeDelete(b, count)
	})
}

// BenchmarkBTreeDeleteInsert measures btree deletion and insertion performance.
func BenchmarkBTreeDeleteInsert(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		runBTreeDeleteInsert(b, count)
	})
}

// BenchmarkBTreeDeleteInsertCloneOnce measures btree deletion and insertion
// performance after the tree has been copy-on-write cloned once.
func BenchmarkBTreeDeleteInsertCloneOnce(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		runBTreeDeleteInsertCloneOnce(b, count)
	})
}

// BenchmarkBTreeDeleteInsertCloneEachTime measures btree deletion and insertion
// performance while the tree is repeatedly copy-on-write cloned.
func BenchmarkBTreeDeleteInsertCloneEachTime(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		runBTreeDeleteInsertCloneEachTime(b, count)
	})
}

// BenchmarkBTreeMakeIter measures the cost of creating a btree iterator.
func BenchmarkBTreeMakeIter(b *testing.B) {
	var tr btree
	for i := 0; i < b.N; i++ {
		it := tr.MakeIter()
		it.First()
	}
}

// BenchmarkBTreeIterSeekGE measures the cost of seeking a btree iterator
// forward.
func BenchmarkBTreeIterSeekGE(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var spans []roachpb.Span
		var tr btree

		for i := 0; i < count; i++ {
			s := span(i)
			spans = append(spans, s)
			tr.Set(newItem(s))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s := spans[rng.Intn(len(spans))]
			it := tr.MakeIter()
			it.SeekGE(newItem(s))
			if testing.Verbose() {
				if !it.Valid() {
					b.Fatal("expected to find key")
				}
				if !s.Equal(spanFromItem(it.Cur())) {
					b.Fatalf("expected %s, but found %s", s, spanFromItem(it.Cur()))
				}
			}
		}
	})
}

// BenchmarkBTreeIterSeekLT measures the cost of seeking a btree iterator
// backward.
func BenchmarkBTreeIterSeekLT(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var spans []roachpb.Span
		var tr btree

		for i := 0; i < count; i++ {
			s := span(i)
			spans = append(spans, s)
			tr.Set(newItem(s))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := rng.Intn(len(spans))
			s := spans[j]
			it := tr.MakeIter()
			it.SeekLT(newItem(s))
			if testing.Verbose() {
				if j == 0 {
					if it.Valid() {
						b.Fatal("unexpected key")
					}
				} else {
					if !it.Valid() {
						b.Fatal("expected to find key")
					}
					s := spans[j-1]
					if !s.Equal(spanFromItem(it.Cur())) {
						b.Fatalf("expected %s, but found %s", s, spanFromItem(it.Cur()))
					}
				}
			}
		}
	})
}

// BenchmarkBTreeIterFirstOverlap measures the cost of finding a single
// overlapping item using a btree iterator.
func BenchmarkBTreeIterFirstOverlap(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var spans []roachpb.Span
		var tr btree

		for i := 0; i < count; i++ {
			s := spanWithEnd(i, i+1)
			spans = append(spans, s)
			tr.Set(newItem(s))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := rng.Intn(len(spans))
			s := spans[j]
			it := tr.MakeIter()
			it.FirstOverlap(newItem(s))
			if testing.Verbose() {
				if !it.Valid() {
					b.Fatal("expected to find key")
				}
				if !s.Equal(spanFromItem(it.Cur())) {
					b.Fatalf("expected %s, but found %s", s, spanFromItem(it.Cur()))
				}
			}
		}
	})
}

// BenchmarkBTreeIterNext measures the cost of seeking a btree iterator to the
// next item in the tree.
func BenchmarkBTreeIterNext(b *testing.B) {
	var tr btree

	const count = 8 << 10
	const size = 2 * maxItems
	for i := 0; i < count; i++ {
		item := newItem(spanWithEnd(i, i+size+1))
		tr.Set(item)
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

// BenchmarkBTreeIterPrev measures the cost of seeking a btree iterator to the
// previous item in the tree.
func BenchmarkBTreeIterPrev(b *testing.B) {
	var tr btree

	const count = 8 << 10
	const size = 2 * maxItems
	for i := 0; i < count; i++ {
		item := newItem(spanWithEnd(i, i+size+1))
		tr.Set(item)
	}

	it := tr.MakeIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.First()
		}
		it.Prev()
	}
}

// BenchmarkBTreeIterNextOverlap measures the cost of seeking a btree iterator
// to the next overlapping item in the tree.
func BenchmarkBTreeIterNextOverlap(b *testing.B) {
	var tr btree

	const count = 8 << 10
	const size = 2 * maxItems
	for i := 0; i < count; i++ {
		item := newItem(spanWithEnd(i, i+size+1))
		tr.Set(item)
	}

	allCmd := newItem(spanWithEnd(0, count+1))
	it := tr.MakeIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.FirstOverlap(allCmd)
		}
		it.NextOverlap(allCmd)
	}
}

// BenchmarkBTreeIterOverlapScan measures the cost of scanning over all
// overlapping items using a btree iterator.
func BenchmarkBTreeIterOverlapScan(b *testing.B) {
	var tr btree
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	const count = 8 << 10
	const size = 2 * maxItems
	for i := 0; i < count; i++ {
		tr.Set(newItem(spanWithEnd(i, i+size+1)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := newItem(randomSpan(rng, count))
		it := tr.MakeIter()
		it.FirstOverlap(item)
		for it.Valid() {
			it.NextOverlap(item)
		}
	}
}
