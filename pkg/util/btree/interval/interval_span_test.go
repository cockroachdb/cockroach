// Copyright 2022 The Cockroach Authors.
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
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/btree/internal/aug"
	"github.com/cockroachdb/cockroach/pkg/util/btree/internal/augtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type bKey []byte

type Span struct {
	Key, EndKey bKey
}

func (k bKey) Next() bKey {
	return BytesNext(k)
}

// BytesNext returns the next possible byte slice, using the extra capacity
// of the provided slice if possible, and if not, appending an \x00.
func BytesNext(b []byte) []byte {
	if cap(b) > len(b) {
		bNext := b[:len(b)+1]
		if bNext[len(bNext)-1] == 0 {
			return bNext
		}
	}
	// TODO(spencer): Do we need to enforce KeyMaxLength here?
	// Switched to "make and copy" pattern in #4963 for performance.
	bn := make([]byte, len(b)+1)
	copy(bn, b)
	bn[len(bn)-1] = 0
	return bn
}

//////////////////////////////////////////
//        Invariant verification        //
//////////////////////////////////////////

func (m *Map[C, I, K, V]) Verify(t *testing.T) {
	augtestutils.Verify(t, m.m)
	if m.m.Root != nil {
		isUpperBoundCorrect(t, m.m.Root)
	}
}

func isUpperBoundCorrect[C Config[I, K], I, K, V any](
	t *testing.T, n *aug.Node[C, I, V, upperBound[K]],
) {
	{
		var u updater[C, I, K, V]
		b := u.findUpperBound(n)
		require.True(t, eq[C, I, K](b, n.Aug))
	}
	lte := func(a, b upperBound[K]) bool {
		return less[C, I](a, b) || eq[C, I](a, b)
	}
	for i := int16(1); i < n.Count; i++ {
		b := ub[C, I, K](n.Keys[i])
		require.True(t, lte(b, n.Aug))
	}
	if !n.IsLeaf() {
		for i := int16(0); i <= n.Count; i++ {
			require.True(t, lte(n.Children[i].Aug, n.Aug))
		}
	}
	augtestutils.Recurse(n, func(
		child *aug.Node[C, I, V, upperBound[K]], _ int16,
	) {
		isUpperBoundCorrect(t, child)
	})
}

//////////////////////////////////////////
//              Unit Tests              //
//////////////////////////////////////////

func newLatch(s Span) *latch {
	return &latch{span: s}
}

type latch struct {
	span Span
	id   uint64
}

func (l *latch) Key() bKey    { return l.span.Key }
func (l *latch) EndKey() bKey { return l.span.EndKey }
func (l *latch) ID() uint64   { return l.id }

type cfg = WithBytesSpanAndID[*latch, bKey, uint64]
type btree = Set[cfg, *latch, bKey]

func (sp Span) Equal(other Span) bool {
	if bytes.Compare(sp.Key, other.Key) == 0 {
		return bytes.Compare(sp.EndKey, other.EndKey) == 0
	}
	return false
}

type iterator = Iterator[cfg, *latch, bKey, struct{}]

func mkKey(i int) bKey {
	if i < 0 || i > 99999 {
		panic("key out of bounds")
	}
	return []byte(fmt.Sprintf("%05d", i))
}

func mkSpan(i int) Span {
	switch i % 10 {
	case 0:
		return Span{Key: mkKey(i)}
	case 1:
		return Span{Key: mkKey(i), EndKey: mkKey(i).Next()}
	case 2:
		return Span{Key: mkKey(i), EndKey: mkKey(i + 64)}
	default:
		return Span{Key: mkKey(i), EndKey: mkKey(i + 4)}
	}
}

func spanWithEnd(start, end int) Span {
	if start < end {
		return Span{Key: mkKey(start), EndKey: mkKey(end)}
	} else if start == end {
		return Span{Key: mkKey(start)}
	} else {
		panic("illegal span")
	}
}

func spanWithMemo(i int, memo map[int]Span) Span {
	if s, ok := memo[i]; ok {
		return s
	}
	s := mkSpan(i)
	memo[i] = s
	return s
}

func randomSpan(rng *rand.Rand, n int) Span {
	start := rng.Intn(n)
	end := rng.Intn(n + 1)
	if end < start {
		start, end = end, start
	}
	return spanWithEnd(start, end)
}

func checkIter(t *testing.T, it iterator, start, end int, spanMemo map[int]Span) {
	i := start
	for it.First(); it.Valid(); it.Next() {
		item := it.Cur()
		expected := spanWithMemo(i, spanMemo)
		if !expected.Equal(item.span) {
			t.Fatalf("expected %s, but found %s", expected, item.span)
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
		if !expected.Equal(item.span) {
			t.Fatalf("expected %s, but found %s", expected, item.span)
		}
	}
	if i != start {
		t.Fatalf("expected %d, but at %d: %+v", start, i, it)
	}

	all := newLatch(spanWithEnd(start, end))
	for it.FirstOverlap(all); it.Valid(); it.NextOverlap(all) {
		item := it.Cur()
		expected := spanWithMemo(i, spanMemo)
		if !expected.Equal(item.span) {
			t.Fatalf("expected %s, but found %s", expected, item.span)
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
	spanMemo := make(map[int]Span)

	// With degree == 16 (max-items/node == 31) we need 513 items in order for
	// there to be 3 levels in the tree. The count here is comfortably above
	// that.
	const count = 768

	// Add keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Upsert(newLatch(mkSpan(i)))
		tr.Verify(t)
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), 0, i+1, spanMemo)
	}

	// Delete keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(newLatch(mkSpan(i)))
		tr.Verify(t)
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), i+1, count, spanMemo)
	}

	// Add keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Upsert(newLatch(mkSpan(count - i)))
		tr.Verify(t)
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), count-i, count+1, spanMemo)
	}

	// Delete keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(newLatch(mkSpan(count - i)))
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
		tr.Upsert(newLatch(mkSpan(i * 2)))
	}

	it := tr.MakeIter()
	for i := 0; i < 2*count-1; i++ {
		it.SeekGE(newLatch(mkSpan(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		item := it.Cur()
		expected := mkSpan(2 * ((i + 1) / 2))
		if !expected.Equal(item.span) {
			t.Fatalf("%d: expected %s, but found %s", i, expected, item.span)
		}
	}
	it.SeekGE(newLatch(mkSpan(2*count - 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	for i := 1; i < 2*count; i++ {
		it.SeekLT(newLatch(mkSpan(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		item := it.Cur()
		expected := mkSpan(2 * ((i - 1) / 2))
		if !expected.Equal(item.span) {
			t.Fatalf("%d: expected %s, but found %s", i, expected, item.span)
		}
	}
	it.SeekLT(newLatch(mkSpan(0)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}
}

// TestBTreeSeekOverlap tests btree iterator overlap operations.
func TestBTreeSeekOverlap(t *testing.T) {
	const count = 513
	const size = 2 * aug.MaxEntries

	var tr btree
	for i := 0; i < count; i++ {
		tr.Upsert(newLatch(spanWithEnd(i, i+size+1)))
	}

	// Iterate over overlaps with a point scan.
	it := tr.MakeIter()
	for i := 0; i < count+size; i++ {
		scanItem := newLatch(spanWithEnd(i, i))
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
			if !expected.Equal(item.span) {
				t.Fatalf("%d: expected %s, but found %s", i, expected, item.span)
			}

			it.NextOverlap(scanItem)
		}
		if it.Valid() {
			t.Fatalf("%d: expected invalid iterator %v", i, it.Cur())
		}
	}
	it.FirstOverlap(newLatch(mkSpan(count + size + 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	// Iterate over overlaps with a range scan.
	it = tr.MakeIter()
	for i := 0; i < count+size; i++ {
		scanItem := newLatch(spanWithEnd(i, i+size+1))
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
			if !expected.Equal(item.span) {
				t.Fatalf("%d: expected %s, but found %s", i, expected, item.span)
			}

			it.NextOverlap(scanItem)
		}
		if it.Valid() {
			t.Fatalf("%d: expected invalid iterator %v", i, it.Cur())
		}
	}
	it.FirstOverlap(newLatch(mkSpan(count + size + 1)))
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
		spanA, spanB Span
		idA, idB     uint64
		exp          int
	}
	var testCases []testCase
	testCases = append(testCases,
		testCase{
			spanA: Span{Key: bKey("a")},
			spanB: Span{Key: bKey("a")},
			idA:   1,
			idB:   1,
			exp:   0,
		},
		testCase{
			spanA: Span{Key: bKey("a")},
			spanB: Span{Key: bKey("a"), EndKey: bKey("b")},
			idA:   1,
			idB:   1,
			exp:   -1,
		},
		testCase{
			spanA: Span{Key: bKey("a"), EndKey: bKey("c")},
			spanB: Span{Key: bKey("a"), EndKey: bKey("b")},
			idA:   1,
			idB:   1,
			exp:   1,
		},
		testCase{
			spanA: Span{Key: bKey("a"), EndKey: bKey("c")},
			spanB: Span{Key: bKey("a"), EndKey: bKey("c")},
			idA:   1,
			idB:   1,
			exp:   0,
		},
		testCase{
			spanA: Span{Key: bKey("a")},
			spanB: Span{Key: bKey("a")},
			idA:   1,
			idB:   2,
			exp:   -1,
		},
		testCase{
			spanA: Span{Key: bKey("a")},
			spanB: Span{Key: bKey("a")},
			idA:   2,
			idB:   1,
			exp:   1,
		},
		testCase{
			spanA: Span{Key: bKey("b")},
			spanB: Span{Key: bKey("a"), EndKey: bKey("c")},
			idA:   1,
			idB:   1,
			exp:   1,
		},
		testCase{
			spanA: Span{Key: bKey("b"), EndKey: bKey("e")},
			spanB: Span{Key: bKey("c"), EndKey: bKey("d")},
			idA:   1,
			idB:   1,
			exp:   -1,
		},
	)
	for _, tc := range testCases {
		name := fmt.Sprintf("cmp(%s:%d,%s:%d)", tc.spanA, tc.idA, tc.spanB, tc.idB)
		t.Run(name, func(t *testing.T) {
			laA := newLatch(tc.spanA)
			laA.id = tc.idA
			laB := newLatch(tc.spanB)
			laB.id = tc.idB
			require.Equal(t, tc.exp, cfg{}.Compare(laA, laB))
		})
	}
}

//////////////////////////////////////////
//        Randomized Unit Tests         //
//////////////////////////////////////////

// perm returns a random permutation of items with spans in the range [0, n).
func perm(n int) (out []*latch) {
	for _, i := range rand.Perm(n) {
		out = append(out, newLatch(spanWithEnd(i, i+1)))
	}
	return out
}

// rang returns an ordered list of items with spans in the range [m, n].
func rang(m, n int) (out []*latch) {
	for i := m; i <= n; i++ {
		out = append(out, newLatch(spanWithEnd(i, i+1)))
	}
	return out
}

// all extracts all items from a tree in order as a slice.
func all(tr *btree) (out []*latch) {
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

func verifyFunc(tb testing.TB) func(tb testing.TB, tr *btree) {
	if _, ok := tb.(*testing.T); ok {
		return func(tb testing.TB, tr *btree) { tr.Verify(tb.(*testing.T)) }
	}
	return func(tb testing.TB, tr *btree) {}
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
	verify := verifyFunc(tb)
	resetTimer(tb)
	for i := 0; i < iters; {
		var tr btree
		for _, item := range insertP {
			tr.Upsert(item)
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
	verify := verifyFunc(tb)
	resetTimer(tb)
	for i := 0; i < iters; {
		stopTimer(tb)
		var tr btree
		for _, item := range insertP {
			tr.Upsert(item)
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
	verify := verifyFunc(tb)
	for _, item := range insertP {
		tr.Upsert(item)
		verify(tb, &tr)
	}
	resetTimer(tb)
	for i := 0; i < iters; i++ {
		item := insertP[i%count]
		tr.Delete(item)
		verify(tb, &tr)
		tr.Upsert(item)
		verify(tb, &tr)
	}
}

func runBTreeDeleteInsertCloneOnce(tb testing.TB, count int) {
	iters := iters(tb, count)
	insertP := perm(count)
	var tr btree
	verify := verifyFunc(tb)
	for _, item := range insertP {
		tr.Upsert(item)
		verify(tb, &tr)
	}
	tr = tr.Clone()
	resetTimer(tb)
	for i := 0; i < iters; i++ {
		item := insertP[i%count]
		tr.Delete(item)
		verify(tb, &tr)
		tr.Upsert(item)
		verify(tb, &tr)
	}
}

func runBTreeDeleteInsertCloneEachTime(tb testing.TB, count int) {
	for _, reset := range []bool{false, true} {
		run(tb, fmt.Sprintf("reset=%t", reset), func(tb testing.TB) {
			verify := verifyFunc(tb)
			iters := iters(tb, count)
			insertP := perm(count)
			var tr, trReset btree
			for _, item := range insertP {
				tr.Upsert(item)
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
				tr.Upsert(item)
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
		items := make([]*latch, count)
		itemSpans := make([]int, count)
		for j := 0; j < count; j++ {
			var item *latch
			end := rng.Intn(count + 10)
			if end <= j {
				end = j
				item = newLatch(spanWithEnd(j, end))
			} else {
				item = newLatch(spanWithEnd(j, end+1))
			}
			tr.Upsert(item)
			items[j] = item
			itemSpans[j] = end
		}

		const scanTrials = 100
		for j := 0; j < scanTrials; j++ {
			var scanItem *latch
			scanStart := rng.Intn(count)
			scanEnd := rng.Intn(count + 10)
			if scanEnd <= scanStart {
				scanEnd = scanStart
				scanItem = newLatch(spanWithEnd(scanStart, scanEnd))
			} else {
				scanItem = newLatch(spanWithEnd(scanStart, scanEnd+1))
			}

			var exp, found []*latch
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

			require.Equal(t, len(exp), len(found), "search for %v", scanItem.span)
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
			tr.Upsert(p[i])
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
		var spans []Span
		var tr btree

		for i := 0; i < count; i++ {
			s := mkSpan(i)
			spans = append(spans, s)
			tr.Upsert(newLatch(s))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s := spans[rng.Intn(len(spans))]
			it := tr.MakeIter()
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

// BenchmarkBTreeIterSeekLT measures the cost of seeking a btree iterator
// backward.
func BenchmarkBTreeIterSeekLT(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var spans []Span
		var tr btree

		for i := 0; i < count; i++ {
			s := mkSpan(i)
			spans = append(spans, s)
			tr.Upsert(newLatch(s))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := rng.Intn(len(spans))
			s := spans[j]
			it := tr.MakeIter()
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

// BenchmarkBTreeIterFirstOverlap measures the cost of finding a single
// overlapping item using a btree iterator.
func BenchmarkBTreeIterFirstOverlap(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var spans []Span
		var tr btree

		for i := 0; i < count; i++ {
			s := spanWithEnd(i, i+1)
			spans = append(spans, s)
			tr.Upsert(newLatch(s))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := rng.Intn(len(spans))
			s := spans[j]
			it := tr.MakeIter()
			it.FirstOverlap(newLatch(s))
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

// BenchmarkBTreeIterNext measures the cost of seeking a btree iterator to the
// next item in the tree.
func BenchmarkBTreeIterNext(b *testing.B) {
	var tr btree

	const count = 8 << 10
	const size = 2 * aug.MaxEntries
	for i := 0; i < count; i++ {
		item := newLatch(spanWithEnd(i, i+size+1))
		tr.Upsert(item)
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
	const size = 2 * aug.MaxEntries
	for i := 0; i < count; i++ {
		item := newLatch(spanWithEnd(i, i+size+1))
		tr.Upsert(item)
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
	const size = 2 * aug.MaxEntries
	for i := 0; i < count; i++ {
		item := newLatch(spanWithEnd(i, i+size+1))
		tr.Upsert(item)
	}

	allCmd := newLatch(spanWithEnd(0, count+1))
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
	const size = 2 * aug.MaxEntries
	for i := 0; i < count; i++ {
		tr.Upsert(newLatch(spanWithEnd(i, i+size+1)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := newLatch(randomSpan(rng, count))
		it := tr.MakeIter()
		it.FirstOverlap(item)
		for it.Valid() {
			it.NextOverlap(item)
		}
	}
}
