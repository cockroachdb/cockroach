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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

//////////////////////////////////////////
//        Invariant verification        //
//////////////////////////////////////////

// Verify asserts that the tree's structural invariants all hold.
func (t *btree) Verify(tt *testing.T) {
	if t.root == nil {
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
		require.True(t, n.count >= minCmds, "cmd count %d must be in range [%d,%d]", n.count, minCmds, maxCmds)
		require.True(t, n.count <= maxCmds, "cmd count %d must be in range [%d,%d]", n.count, minCmds, maxCmds)
	}
	for i, cmd := range n.cmds {
		if i < int(n.count) {
			require.NotNil(t, cmd, "cmd below count")
		} else {
			require.Nil(t, cmd, "cmd above count")
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
		require.True(t, cmp(n.cmds[i-1], n.cmds[i]) <= 0)
	}
	if !n.leaf {
		for i := int16(0); i < n.count; i++ {
			prev := n.children[i]
			next := n.children[i+1]

			require.True(t, cmp(prev.cmds[prev.count-1], n.cmds[i]) <= 0)
			require.True(t, cmp(n.cmds[i], next.cmds[0]) <= 0)
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
		require.True(t, upperBound(n.cmds[i]).compare(n.max) <= 0)
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

func randomSpan(rng *rand.Rand, n int) roachpb.Span {
	start := rng.Intn(n)
	end := rng.Intn(n + 1)
	if end < start {
		start, end = end, start
	}
	return spanWithEnd(start, end)
}

func newCmd(s roachpb.Span) *cmd {
	return &cmd{span: s}
}

func checkIter(t *testing.T, it iterator, start, end int) {
	i := start
	for it.First(); it.Valid(); it.Next() {
		cmd := it.Cmd()
		expected := span(i)
		if !expected.Equal(cmd.span) {
			t.Fatalf("expected %s, but found %s", expected, cmd.span)
		}
		i++
	}
	if i != end {
		t.Fatalf("expected %d, but at %d", end, i)
	}

	for it.Last(); it.Valid(); it.Prev() {
		i--
		cmd := it.Cmd()
		expected := span(i)
		if !expected.Equal(cmd.span) {
			t.Fatalf("expected %s, but found %s", expected, cmd.span)
		}
	}
	if i != start {
		t.Fatalf("expected %d, but at %d: %+v", start, i, it)
	}

	all := newCmd(spanWithEnd(start, end))
	for it.FirstOverlap(all); it.Valid(); it.NextOverlap() {
		cmd := it.Cmd()
		expected := span(i)
		if !expected.Equal(cmd.span) {
			t.Fatalf("expected %s, but found %s", expected, cmd.span)
		}
		i++
	}
	if i != end {
		t.Fatalf("expected %d, but at %d", end, i)
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
		tr.Set(newCmd(span(i)))
		tr.Verify(t)
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), 0, i+1)
	}

	// Delete keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(newCmd(span(i)))
		tr.Verify(t)
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), i+1, count)
	}

	// Add keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Set(newCmd(span(count - i)))
		tr.Verify(t)
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), count-i, count+1)
	}

	// Delete keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(newCmd(span(count - i)))
		tr.Verify(t)
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
		tr.Set(newCmd(span(i * 2)))
	}

	it := tr.MakeIter()
	for i := 0; i < 2*count-1; i++ {
		it.SeekGE(newCmd(span(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		cmd := it.Cmd()
		expected := span(2 * ((i + 1) / 2))
		if !expected.Equal(cmd.span) {
			t.Fatalf("%d: expected %s, but found %s", i, expected, cmd.span)
		}
	}
	it.SeekGE(newCmd(span(2*count - 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	for i := 1; i < 2*count; i++ {
		it.SeekLT(newCmd(span(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		cmd := it.Cmd()
		expected := span(2 * ((i - 1) / 2))
		if !expected.Equal(cmd.span) {
			t.Fatalf("%d: expected %s, but found %s", i, expected, cmd.span)
		}
	}
	it.SeekLT(newCmd(span(0)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}
}

func TestBTreeSeekOverlap(t *testing.T) {
	const count = 513
	const size = 2 * maxCmds

	var tr btree
	for i := 0; i < count; i++ {
		tr.Set(newCmd(spanWithEnd(i, i+size+1)))
	}

	// Iterate over overlaps with a point scan.
	it := tr.MakeIter()
	for i := 0; i < count+size; i++ {
		it.FirstOverlap(newCmd(spanWithEnd(i, i)))
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
			cmd := it.Cmd()
			expected := spanWithEnd(expStart, expStart+size+1)
			if !expected.Equal(cmd.span) {
				t.Fatalf("%d: expected %s, but found %s", i, expected, cmd.span)
			}

			it.NextOverlap()
		}
		if it.Valid() {
			t.Fatalf("%d: expected invalid iterator %v", i, it.Cmd())
		}
	}
	it.FirstOverlap(newCmd(span(count + size + 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	// Iterate over overlaps with a range scan.
	it = tr.MakeIter()
	for i := 0; i < count+size; i++ {
		it.FirstOverlap(newCmd(spanWithEnd(i, i+size+1)))
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
			cmd := it.Cmd()
			expected := spanWithEnd(expStart, expStart+size+1)
			if !expected.Equal(cmd.span) {
				t.Fatalf("%d: expected %s, but found %s", i, expected, cmd.span)
			}

			it.NextOverlap()
		}
		if it.Valid() {
			t.Fatalf("%d: expected invalid iterator %v", i, it.Cmd())
		}
	}
	it.FirstOverlap(newCmd(span(count + size + 1)))
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
		cmds := make([]*cmd, count)
		cmdSpans := make([]int, count)
		for j := 0; j < count; j++ {
			var cmd *cmd
			end := rng.Intn(count + 10)
			if end <= j {
				end = j
				cmd = newCmd(spanWithEnd(j, end))
			} else {
				cmd = newCmd(spanWithEnd(j, end+1))
			}
			tr.Set(cmd)
			cmds[j] = cmd
			cmdSpans[j] = end
		}

		const scanTrials = 100
		for j := 0; j < scanTrials; j++ {
			var scanCmd *cmd
			scanStart := rng.Intn(count)
			scanEnd := rng.Intn(count + 10)
			if scanEnd <= scanStart {
				scanEnd = scanStart
				scanCmd = newCmd(spanWithEnd(scanStart, scanEnd))
			} else {
				scanCmd = newCmd(spanWithEnd(scanStart, scanEnd+1))
			}

			var exp, found []*cmd
			for startKey, endKey := range cmdSpans {
				if startKey <= scanEnd && endKey >= scanStart {
					exp = append(exp, cmds[startKey])
				}
			}

			it := tr.MakeIter()
			it.FirstOverlap(scanCmd)
			for it.Valid() {
				found = append(found, it.Cmd())
				it.NextOverlap()
			}

			require.Equal(t, len(exp), len(found), "search for %v", scanCmd.span)
		}
	}
}

func TestBTreeCmp(t *testing.T) {
	testCases := []struct {
		spanA, spanB roachpb.Span
		idA, idB     int64
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
			cmdA := &cmd{id: tc.idA, span: tc.spanA}
			cmdB := &cmd{id: tc.idB, span: tc.spanB}
			require.Equal(t, tc.exp, cmp(cmdA, cmdB))
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

// perm returns a random permutation of cmds with spans in the range [0, n).
func perm(n int) (out []*cmd) {
	for _, i := range rand.Perm(n) {
		out = append(out, newCmd(spanWithEnd(i, i+1)))
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
			for _, cmd := range insertP {
				tr.Set(cmd)
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
			for _, cmd := range insertP {
				tr.Set(cmd)
			}
			b.StartTimer()
			for _, cmd := range removeP {
				tr.Delete(cmd)
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
		for _, cmd := range insertP {
			tr.Set(cmd)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cmd := insertP[i%count]
			tr.Delete(cmd)
			tr.Set(cmd)
		}
	})
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
			tr.Set(newCmd(s))
		}

		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		it := tr.MakeIter()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s := spans[rng.Intn(len(spans))]
			it.SeekGE(newCmd(s))
			if testing.Verbose() {
				if !it.Valid() {
					b.Fatal("expected to find key")
				}
				if !s.Equal(it.Cmd().span) {
					b.Fatalf("expected %s, but found %s", s, it.Cmd().span)
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
			tr.Set(newCmd(s))
		}

		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		it := tr.MakeIter()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := rng.Intn(len(spans))
			s := spans[j]
			it.SeekLT(newCmd(s))
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
					if !s.Equal(it.Cmd().span) {
						b.Fatalf("expected %s, but found %s", s, it.Cmd().span)
					}
				}
			}
		}
	})
}

func BenchmarkBTreeIterFirstOverlap(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var spans []roachpb.Span
		var cmds []*cmd
		var tr btree

		for i := 0; i < count; i++ {
			s := spanWithEnd(i, i+1)
			spans = append(spans, s)
			cmd := newCmd(s)
			cmds = append(cmds, cmd)
			tr.Set(cmd)
		}

		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		it := tr.MakeIter()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := rng.Intn(len(spans))
			s := spans[j]
			cmd := cmds[j]
			it.FirstOverlap(cmd)
			if testing.Verbose() {
				if !it.Valid() {
					b.Fatal("expected to find key")
				}
				if !s.Equal(it.Cmd().span) {
					b.Fatalf("expected %s, but found %s", s, it.Cmd().span)
				}
			}
		}
	})
}

func BenchmarkBTreeIterNext(b *testing.B) {
	var tr btree

	const count = 8 << 10
	const size = 2 * maxCmds
	for i := 0; i < count; i++ {
		cmd := newCmd(spanWithEnd(i, i+size+1))
		tr.Set(cmd)
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
	const size = 2 * maxCmds
	for i := 0; i < count; i++ {
		cmd := newCmd(spanWithEnd(i, i+size+1))
		tr.Set(cmd)
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
	const size = 2 * maxCmds
	for i := 0; i < count; i++ {
		cmd := newCmd(spanWithEnd(i, i+size+1))
		tr.Set(cmd)
	}

	allCmd := newCmd(spanWithEnd(0, count+1))
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
	const size = 2 * maxCmds
	for i := 0; i < count; i++ {
		tr.Set(newCmd(spanWithEnd(i, i+size+1)))
	}

	cmd := new(cmd)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd.span = randomSpan(rng, count)
		it := tr.MakeIter()
		it.FirstOverlap(cmd)
		for it.Valid() {
			it.NextOverlap()
		}
	}
}
