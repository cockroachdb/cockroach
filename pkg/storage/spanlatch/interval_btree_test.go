// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanlatch

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

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
		require.True(t, n.count >= minLatches, "latch count %d must be in range [%d,%d]", n.count, minLatches, maxLatches)
		require.True(t, n.count <= maxLatches, "latch count %d must be in range [%d,%d]", n.count, minLatches, maxLatches)
	}
	for i, la := range n.latches {
		if i < int(n.count) {
			require.NotNil(t, la, "latch below count")
		} else {
			require.Nil(t, la, "latch above count")
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
		require.True(t, cmp(n.latches[i-1], n.latches[i]) <= 0)
	}
	if !n.leaf {
		for i := int16(0); i < n.count; i++ {
			prev := n.children[i]
			next := n.children[i+1]

			require.True(t, cmp(prev.latches[prev.count-1], n.latches[i]) <= 0)
			require.True(t, cmp(n.latches[i], next.latches[0]) <= 0)
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
		require.True(t, upperBound(n.latches[i]).compare(n.max) <= 0)
	}
	if !n.leaf {
		for i := int16(0); i <= n.count; i++ {
			child := n.children[i]
			require.True(t, child.max.compare(n.max) <= 0)
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

func newLatch(s roachpb.Span) *latch {
	return &latch{span: s}
}

func checkIter(t *testing.T, it iterator, start, end int, spanMemo map[int]roachpb.Span) {
	i := start
	for it.First(); it.Valid(); it.Next() {
		la := it.Cur()
		expected := spanWithMemo(i, spanMemo)
		if !expected.Equal(la.span) {
			t.Fatalf("expected %s, but found %s", expected, la.span)
		}
		i++
	}
	if i != end {
		t.Fatalf("expected %d, but at %d", end, i)
	}

	for it.Last(); it.Valid(); it.Prev() {
		i--
		la := it.Cur()
		expected := spanWithMemo(i, spanMemo)
		if !expected.Equal(la.span) {
			t.Fatalf("expected %s, but found %s", expected, la.span)
		}
	}
	if i != start {
		t.Fatalf("expected %d, but at %d: %+v", start, i, it)
	}

	all := newLatch(spanWithEnd(start, end))
	for it.FirstOverlap(all); it.Valid(); it.NextOverlap() {
		la := it.Cur()
		expected := spanWithMemo(i, spanMemo)
		if !expected.Equal(la.span) {
			t.Fatalf("expected %s, but found %s", expected, la.span)
		}
		i++
	}
	if i != end {
		t.Fatalf("expected %d, but at %d", end, i)
	}
}

func TestBTree(t *testing.T) {
	var tr btree
	spanMemo := make(map[int]roachpb.Span)

	// With degree == 16 (max-items/node == 31) we need 513 items in order for
	// there to be 3 levels in the tree. The count here is comfortably above
	// that.
	const count = 768

	// Add keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Set(newLatch(span(i)))
		tr.Verify(t)
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), 0, i+1, spanMemo)
	}

	// Delete keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(newLatch(span(i)))
		tr.Verify(t)
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), i+1, count, spanMemo)
	}

	// Add keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Set(newLatch(span(count - i)))
		tr.Verify(t)
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), count-i, count+1, spanMemo)
	}

	// Delete keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(newLatch(span(count - i)))
		tr.Verify(t)
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), 1, count-i, spanMemo)
	}
}

func TestBTreeSeek(t *testing.T) {
	const count = 513

	var tr btree
	for i := 0; i < count; i++ {
		tr.Set(newLatch(span(i * 2)))
	}

	it := tr.MakeIter()
	for i := 0; i < 2*count-1; i++ {
		it.SeekGE(newLatch(span(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		la := it.Cur()
		expected := span(2 * ((i + 1) / 2))
		if !expected.Equal(la.span) {
			t.Fatalf("%d: expected %s, but found %s", i, expected, la.span)
		}
	}
	it.SeekGE(newLatch(span(2*count - 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	for i := 1; i < 2*count; i++ {
		it.SeekLT(newLatch(span(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		la := it.Cur()
		expected := span(2 * ((i - 1) / 2))
		if !expected.Equal(la.span) {
			t.Fatalf("%d: expected %s, but found %s", i, expected, la.span)
		}
	}
	it.SeekLT(newLatch(span(0)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}
}

func TestBTreeSeekOverlap(t *testing.T) {
	const count = 513
	const size = 2 * maxLatches

	var tr btree
	for i := 0; i < count; i++ {
		tr.Set(newLatch(spanWithEnd(i, i+size+1)))
	}

	// Iterate over overlaps with a point scan.
	it := tr.MakeIter()
	for i := 0; i < count+size; i++ {
		it.FirstOverlap(newLatch(spanWithEnd(i, i)))
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
			la := it.Cur()
			expected := spanWithEnd(expStart, expStart+size+1)
			if !expected.Equal(la.span) {
				t.Fatalf("%d: expected %s, but found %s", i, expected, la.span)
			}

			it.NextOverlap()
		}
		if it.Valid() {
			t.Fatalf("%d: expected invalid iterator %v", i, it.Cur())
		}
	}
	it.FirstOverlap(newLatch(span(count + size + 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	// Iterate over overlaps with a range scan.
	it = tr.MakeIter()
	for i := 0; i < count+size; i++ {
		it.FirstOverlap(newLatch(spanWithEnd(i, i+size+1)))
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
			la := it.Cur()
			expected := spanWithEnd(expStart, expStart+size+1)
			if !expected.Equal(la.span) {
				t.Fatalf("%d: expected %s, but found %s", i, expected, la.span)
			}

			it.NextOverlap()
		}
		if it.Valid() {
			t.Fatalf("%d: expected invalid iterator %v", i, it.Cur())
		}
	}
	it.FirstOverlap(newLatch(span(count + size + 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}
}

func TestBTreeSeekOverlapRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	const trials = 10
	for i := 0; i < trials; i++ {
		var tr btree

		const count = 1000
		latches := make([]*latch, count)
		latchSpans := make([]int, count)
		for j := 0; j < count; j++ {
			var la *latch
			end := rng.Intn(count + 10)
			if end <= j {
				end = j
				la = newLatch(spanWithEnd(j, end))
			} else {
				la = newLatch(spanWithEnd(j, end+1))
			}
			tr.Set(la)
			latches[j] = la
			latchSpans[j] = end
		}

		const scanTrials = 100
		for j := 0; j < scanTrials; j++ {
			var scanLa *latch
			scanStart := rng.Intn(count)
			scanEnd := rng.Intn(count + 10)
			if scanEnd <= scanStart {
				scanEnd = scanStart
				scanLa = newLatch(spanWithEnd(scanStart, scanEnd))
			} else {
				scanLa = newLatch(spanWithEnd(scanStart, scanEnd+1))
			}

			var exp, found []*latch
			for startKey, endKey := range latchSpans {
				if startKey <= scanEnd && endKey >= scanStart {
					exp = append(exp, latches[startKey])
				}
			}

			it := tr.MakeIter()
			it.FirstOverlap(scanLa)
			for it.Valid() {
				found = append(found, it.Cur())
				it.NextOverlap()
			}

			require.Equal(t, len(exp), len(found), "search for %v", scanLa.span)
		}
	}
}

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

	t.Log("Removing half of latches from first half")
	toRemove := want[cloneTestSize/2:]
	for i := 0; i < len(trees)/2; i++ {
		tree := trees[i]
		wg.Add(1)
		go func() {
			for _, la := range toRemove {
				tree.Delete(la)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	t.Log("Checking all values again")
	for i, tree := range trees {
		var wantpart []*latch
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

func TestBTreeCmp(t *testing.T) {
	testCases := []struct {
		spanA, spanB roachpb.Span
		idA, idB     uint64
		exp          int
	}{
		{
			spanA: roachpb.Span{Key: roachpb.Key("a")},
			spanB: roachpb.Span{Key: roachpb.Key("a")},
			idA:   1,
			idB:   1,
			exp:   0,
		},
		{
			spanA: roachpb.Span{Key: roachpb.Key("a")},
			spanB: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			idA:   1,
			idB:   1,
			exp:   -1,
		},
		{
			spanA: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			spanB: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			idA:   1,
			idB:   1,
			exp:   1,
		},
		{
			spanA: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			spanB: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			idA:   1,
			idB:   1,
			exp:   0,
		},
		{
			spanA: roachpb.Span{Key: roachpb.Key("a")},
			spanB: roachpb.Span{Key: roachpb.Key("a")},
			idA:   1,
			idB:   2,
			exp:   -1,
		},
		{
			spanA: roachpb.Span{Key: roachpb.Key("a")},
			spanB: roachpb.Span{Key: roachpb.Key("a")},
			idA:   2,
			idB:   1,
			exp:   1,
		},
		{
			spanA: roachpb.Span{Key: roachpb.Key("b")},
			spanB: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			idA:   1,
			idB:   1,
			exp:   1,
		},
		{
			spanA: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("e")},
			spanB: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
			idA:   1,
			idB:   1,
			exp:   -1,
		},
	}
	for _, tc := range testCases {
		name := fmt.Sprintf("cmp(%s:%d,%s:%d)", tc.spanA, tc.idA, tc.spanB, tc.idB)
		t.Run(name, func(t *testing.T) {
			laA := &latch{id: tc.idA, span: tc.spanA}
			laB := &latch{id: tc.idB, span: tc.spanB}
			require.Equal(t, tc.exp, cmp(laA, laB))
		})
	}
}

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
//              Benchmarks              //
//////////////////////////////////////////

// perm returns a random permutation of latches with spans in the range [0, n).
func perm(n int) (out []*latch) {
	for _, i := range rand.Perm(n) {
		out = append(out, newLatch(spanWithEnd(i, i+1)))
	}
	return out
}

// rang returns an ordered list of latches with spans in the range [m, n].
func rang(m, n int) (out []*latch) {
	for i := m; i <= n; i++ {
		out = append(out, newLatch(spanWithEnd(i, i+1)))
	}
	return out
}

// all extracts all latches from a tree in order as a slice.
func all(tr *btree) (out []*latch) {
	it := tr.MakeIter()
	it.First()
	for it.Valid() {
		out = append(out, it.Cur())
		it.Next()
	}
	return out
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
		insertP := perm(count)
		b.ResetTimer()
		for i := 0; i < b.N; {
			var tr btree
			for _, la := range insertP {
				tr.Set(la)
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
		insertP, removeP := perm(count), perm(count)
		b.ResetTimer()
		for i := 0; i < b.N; {
			b.StopTimer()
			var tr btree
			for _, la := range insertP {
				tr.Set(la)
			}
			b.StartTimer()
			for _, la := range removeP {
				tr.Delete(la)
				i++
				if i >= b.N {
					return
				}
			}
			if tr.Len() > 0 {
				b.Fatalf("tree not empty: %s", &tr)
			}
		}
	})
}

func BenchmarkBTreeDeleteInsert(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(count)
		var tr btree
		for _, la := range insertP {
			tr.Set(la)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			la := insertP[i%count]
			tr.Delete(la)
			tr.Set(la)
		}
	})
}

func BenchmarkBTreeDeleteInsertCloneOnce(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(count)
		var tr btree
		for _, la := range insertP {
			tr.Set(la)
		}
		tr = tr.Clone()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			la := insertP[i%count]
			tr.Delete(la)
			tr.Set(la)
		}
	})
}

func BenchmarkBTreeDeleteInsertCloneEachTime(b *testing.B) {
	for _, reset := range []bool{false, true} {
		b.Run(fmt.Sprintf("reset=%t", reset), func(b *testing.B) {
			forBenchmarkSizes(b, func(b *testing.B, count int) {
				insertP := perm(count)
				var tr, trReset btree
				for _, la := range insertP {
					tr.Set(la)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					la := insertP[i%count]
					if reset {
						trReset.Reset()
						trReset = tr
					}
					tr = tr.Clone()
					tr.Delete(la)
					tr.Set(la)
				}
			})
		})
	}
}

func BenchmarkBTreeMakeIter(b *testing.B) {
	var tr btree
	for i := 0; i < b.N; i++ {
		it := tr.MakeIter()
		it.First()
	}
}

func BenchmarkBTreeIterSeekGE(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var spans []roachpb.Span
		var tr btree

		for i := 0; i < count; i++ {
			s := span(i)
			spans = append(spans, s)
			tr.Set(newLatch(s))
		}

		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		it := tr.MakeIter()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s := spans[rng.Intn(len(spans))]
			it.SeekGE(newLatch(s))
			if testing.Verbose() {
				if !it.Valid() {
					b.Fatal("expected to find key")
				}
				if !s.Equal(it.Cur().span) {
					b.Fatalf("expected %s, but found %s", s, it.Cur().span)
				}
			}
		}
	})
}

func BenchmarkBTreeIterSeekLT(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var spans []roachpb.Span
		var tr btree

		for i := 0; i < count; i++ {
			s := span(i)
			spans = append(spans, s)
			tr.Set(newLatch(s))
		}

		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		it := tr.MakeIter()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := rng.Intn(len(spans))
			s := spans[j]
			it.SeekLT(newLatch(s))
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
					if !s.Equal(it.Cur().span) {
						b.Fatalf("expected %s, but found %s", s, it.Cur().span)
					}
				}
			}
		}
	})
}

func BenchmarkBTreeIterFirstOverlap(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var spans []roachpb.Span
		var latches []*latch
		var tr btree

		for i := 0; i < count; i++ {
			s := spanWithEnd(i, i+1)
			spans = append(spans, s)
			la := newLatch(s)
			latches = append(latches, la)
			tr.Set(la)
		}

		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		it := tr.MakeIter()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := rng.Intn(len(spans))
			s := spans[j]
			la := latches[j]
			it.FirstOverlap(la)
			if testing.Verbose() {
				if !it.Valid() {
					b.Fatal("expected to find key")
				}
				if !s.Equal(it.Cur().span) {
					b.Fatalf("expected %s, but found %s", s, it.Cur().span)
				}
			}
		}
	})
}

func BenchmarkBTreeIterNext(b *testing.B) {
	var tr btree

	const count = 8 << 10
	const size = 2 * maxLatches
	for i := 0; i < count; i++ {
		la := newLatch(spanWithEnd(i, i+size+1))
		tr.Set(la)
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

func BenchmarkBTreeIterPrev(b *testing.B) {
	var tr btree

	const count = 8 << 10
	const size = 2 * maxLatches
	for i := 0; i < count; i++ {
		la := newLatch(spanWithEnd(i, i+size+1))
		tr.Set(la)
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

func BenchmarkBTreeIterNextOverlap(b *testing.B) {
	var tr btree

	const count = 8 << 10
	const size = 2 * maxLatches
	for i := 0; i < count; i++ {
		la := newLatch(spanWithEnd(i, i+size+1))
		tr.Set(la)
	}

	allCmd := newLatch(spanWithEnd(0, count+1))
	it := tr.MakeIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.FirstOverlap(allCmd)
		}
		it.NextOverlap()
	}
}

func BenchmarkBTreeIterOverlapScan(b *testing.B) {
	var tr btree
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	const count = 8 << 10
	const size = 2 * maxLatches
	for i := 0; i < count; i++ {
		tr.Set(newLatch(spanWithEnd(i, i+size+1)))
	}

	la := new(latch)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		la.span = randomSpan(rng, count)
		it := tr.MakeIter()
		it.FirstOverlap(la)
		for it.Valid() {
			it.NextOverlap()
		}
	}
}
