// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package span

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func (f *Frontier) entriesStr() string {
	var buf strings.Builder
	f.Entries(func(sp roachpb.Span, ts hlc.Timestamp) OpResult {
		if buf.Len() != 0 {
			buf.WriteString(` `)
		}
		fmt.Fprintf(&buf, `%s@%d`, sp, ts.WallTime)
		return ContinueMatch
	})
	return buf.String()
}

type frontierForwarder struct {
	t        *testing.T
	f        *Frontier
	advanced bool
}

func (f frontierForwarder) expectedAdvanced(expected bool) frontierForwarder {
	require.Equal(f.t, expected, f.advanced)
	return f
}
func (f frontierForwarder) expectFrontier(wall int64) frontierForwarder {
	require.Equal(f.t, hlc.Timestamp{WallTime: wall}, f.f.Frontier())
	return f
}
func (f frontierForwarder) expectEntries(expected string) frontierForwarder {
	require.Equal(f.t, expected, f.f.entriesStr())
	return f
}

func makeFrontierForwarded(
	t *testing.T, f *Frontier,
) func(s roachpb.Span, wall int64) frontierForwarder {
	t.Helper()
	return func(s roachpb.Span, wall int64) frontierForwarder {
		advanced, err := f.Forward(s, hlc.Timestamp{WallTime: wall})
		require.NoError(t, err)
		return frontierForwarder{
			t:        t,
			f:        f,
			advanced: advanced,
		}
	}
}
func TestSpanFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	spAB := roachpb.Span{Key: keyA, EndKey: keyB}
	spAC := roachpb.Span{Key: keyA, EndKey: keyC}
	spAD := roachpb.Span{Key: keyA, EndKey: keyD}
	spBC := roachpb.Span{Key: keyB, EndKey: keyC}
	spBD := roachpb.Span{Key: keyB, EndKey: keyD}
	spCD := roachpb.Span{Key: keyC, EndKey: keyD}

	f, err := MakeFrontier(spAD)
	require.NoError(t, err)
	require.Equal(t, hlc.Timestamp{}, f.Frontier())
	require.Equal(t, `{a-d}@0`, f.entriesStr())

	forwardFrontier := makeFrontierForwarded(t, f)

	// Untracked spans are ignored
	forwardFrontier(roachpb.Span{Key: []byte("d"), EndKey: []byte("e")}, 1).
		expectedAdvanced(false).
		expectFrontier(0).
		expectEntries(`{a-d}@0`)

	// Forward the entire tracked spanspace.
	forwardFrontier(spAD, 1).
		expectedAdvanced(true).
		expectFrontier(1).
		expectEntries(`{a-d}@1`)

	// Forward it again.
	forwardFrontier(spAD, 2).
		expectedAdvanced(true).
		expectFrontier(2).
		expectEntries(`{a-d}@2`)

	// Forward to the previous frontier.
	forwardFrontier(spAD, 2).
		expectedAdvanced(false).
		expectFrontier(2).
		expectEntries(`{a-d}@2`)

	// Forward into the past is ignored.
	forwardFrontier(spAD, 1).
		expectedAdvanced(false).
		expectFrontier(2).
		expectEntries(`{a-d}@2`)

	// Forward a subset.
	forwardFrontier(spBC, 3).
		expectedAdvanced(false).
		expectFrontier(2).
		expectEntries(`{a-b}@2 {b-c}@3 {c-d}@2`)

	// Forward it more.
	forwardFrontier(spBC, 4).
		expectedAdvanced(false).
		expectFrontier(2).
		expectEntries(`{a-b}@2 {b-c}@4 {c-d}@2`)

	// Forward all tracked spans to timestamp before BC (currently at 4).
	// Advances to the min of tracked spans. Note that this requires the
	// forwarded span to be split into two spans, one on each side of BC.
	forwardFrontier(spAD, 3).
		expectedAdvanced(true).
		expectFrontier(3).
		expectEntries(`{a-b}@3 {b-c}@4 {c-d}@3`)

	// Forward everything but BC, advances to the min of tracked spans.
	forwardFrontier(spAB, 5).
		expectedAdvanced(false).
		expectFrontier(3)

	forwardFrontier(spCD, 5).
		expectedAdvanced(true).
		expectFrontier(4).
		expectEntries(`{a-b}@5 {b-c}@4 {c-d}@5`)

	// Catch BC up: spans collapse.
	forwardFrontier(spBC, 5).
		expectedAdvanced(true).
		expectFrontier(5).
		expectEntries(`{a-d}@5`)

	// Forward them all at once.
	forwardFrontier(spAD, 6).
		expectedAdvanced(true).
		expectFrontier(6).
		expectEntries(`{a-d}@6`)

	// Split AC with BD.
	forwardFrontier(spCD, 7).
		expectedAdvanced(false).
		expectFrontier(6).
		expectEntries(`{a-c}@6 {c-d}@7`)

	forwardFrontier(spBD, 8).
		expectedAdvanced(false).
		expectFrontier(6).
		expectEntries(`{a-b}@6 {b-d}@8`)

	forwardFrontier(spAB, 8).
		expectedAdvanced(true).
		expectFrontier(8).
		expectEntries(`{a-d}@8`)

	// Split BD with AC.
	forwardFrontier(spAC, 9).
		expectedAdvanced(false).
		expectFrontier(8).
		expectEntries(`{a-c}@9 {c-d}@8`)

	forwardFrontier(spCD, 9).
		expectedAdvanced(true).
		expectFrontier(9).
		expectEntries(`{a-d}@9`)
}

func TestSpanFrontierDisjointSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	keyD, keyE, keyF := roachpb.Key("d"), roachpb.Key("e"), roachpb.Key("f")
	spAB := roachpb.Span{Key: keyA, EndKey: keyB}
	spAD := roachpb.Span{Key: keyA, EndKey: keyD}
	spCE := roachpb.Span{Key: keyC, EndKey: keyE}
	spDF := roachpb.Span{Key: keyD, EndKey: keyF}

	f, err := MakeFrontier(spAB, spCE)
	require.NoError(t, err)
	require.Equal(t, hlc.Timestamp{}, f.Frontier())
	require.Equal(t, `{a-b}@0 {c-e}@0`, f.entriesStr())

	forwardFrontier := makeFrontierForwarded(t, f)

	// Advance just the tracked spans
	forwardFrontier(spCE, 1).
		expectedAdvanced(false).
		expectFrontier(0).
		expectEntries(`{a-b}@0 {c-e}@1`)

	forwardFrontier(spAB, 1).
		expectedAdvanced(true).
		expectFrontier(1).
		expectEntries(`{a-b}@1 {c-e}@1`)

	// Advance a span that partially overlaps the tracked spans
	forwardFrontier(spDF, 2).
		expectedAdvanced(false).
		expectFrontier(1).
		expectEntries(`{a-b}@1 {c-d}@1 {d-e}@2`)

	// Advance one span that covers two tracked spans and so needs two entries.
	forwardFrontier(spAD, 3).
		expectedAdvanced(true).
		expectFrontier(2).
		expectEntries(`{a-b}@3 {c-d}@3 {d-e}@2`)

	// Advance span that overlaps all the spans tracked by this frontier.
	// {c-d} and {d-e} should collapse.
	forwardFrontier(roachpb.Span{Key: roachpb.Key(`0`), EndKey: roachpb.Key(`q`)}, 4).
		expectedAdvanced(true).
		expectFrontier(4).
		expectEntries(`{a-b}@4 {c-e}@4`)
}

func TestSpanFrontierHeap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	spAB := roachpb.Span{Key: keyA, EndKey: keyB}
	spBC := roachpb.Span{Key: keyB, EndKey: keyC}

	var fh frontierHeap

	eAB1 := &frontierEntry{span: spAB, ts: hlc.Timestamp{WallTime: 1}}
	eBC1 := &frontierEntry{span: spBC, ts: hlc.Timestamp{WallTime: 1}}
	eAB2 := &frontierEntry{span: spAB, ts: hlc.Timestamp{WallTime: 2}}

	// Push one
	heap.Push(&fh, eAB1)
	require.Equal(t, eAB1, heap.Pop(&fh))

	// Push different spans and times
	heap.Push(&fh, eAB1)
	heap.Push(&fh, eBC1)
	heap.Push(&fh, eAB2)
	require.Equal(t, eAB1, heap.Pop(&fh))
	require.Equal(t, eBC1, heap.Pop(&fh))
	require.Equal(t, eAB2, heap.Pop(&fh))

	// Push in a different span order
	heap.Push(&fh, eBC1)
	heap.Push(&fh, eAB1)
	heap.Push(&fh, eAB2)
	require.Equal(t, eAB1, heap.Pop(&fh))
	require.Equal(t, eBC1, heap.Pop(&fh))
	require.Equal(t, eAB2, heap.Pop(&fh))

	// Push in a different time order
	heap.Push(&fh, eAB2)
	heap.Push(&fh, eAB1)
	heap.Push(&fh, eBC1)
	require.Equal(t, eAB1, heap.Pop(&fh))
	require.Equal(t, eBC1, heap.Pop(&fh))
	require.Equal(t, eAB2, heap.Pop(&fh))
}

func TestSequentialSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var abc = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	startKey, endKey := []byte{abc[0]}, []byte{abc[len(abc)-1]}
	mkspan := func() roachpb.Span {
		return roachpb.Span{Key: startKey, EndKey: endKey}
	}

	f, err := MakeFrontier(mkspan())
	require.NoError(t, err)

	var expectedRanges []string
	for i := 0; i < len(abc)-1; i++ {
		startKey[0] = abc[i]
		endKey[0] = abc[i+1]
		span := mkspan()
		_, err := f.Forward(span, hlc.Timestamp{WallTime: int64(i) + 1})
		require.NoError(t, err)
		expectedRanges = append(expectedRanges, fmt.Sprintf("%s@%d", span, i+1))
	}
	require.Equal(t, strings.Join(expectedRanges, " "), f.entriesStr())
}

func TestSpanEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	key := func(c byte) roachpb.Key {
		return roachpb.Key{c}
	}
	mkspan := func(start, end byte) roachpb.Span {
		return roachpb.Span{Key: key(start), EndKey: key(end)}
	}

	spAZ := mkspan('A', 'Z')
	f, err := MakeFrontier(spAZ)
	require.NoError(t, err)

	advance := func(s roachpb.Span, wall int64) {
		_, err := f.Forward(s, hlc.Timestamp{WallTime: wall})
		require.NoError(t, err)
	}

	spanEntries := func(sp roachpb.Span) string {
		var buf strings.Builder
		f.SpanEntries(sp, func(s roachpb.Span, ts hlc.Timestamp) OpResult {
			if buf.Len() != 0 {
				buf.WriteString(` `)
			}
			fmt.Fprintf(&buf, `%s@%d`, s, ts.WallTime)
			return ContinueMatch
		})
		return buf.String()
	}

	// Nothing overlaps span fully to the left of frontier.
	require.Equal(t, ``, spanEntries(mkspan('0', '9')))
	// Nothing overlaps span fully to the right of the frontier.
	require.Equal(t, ``, spanEntries(mkspan('a', 'z')))

	// Span overlaps entire frontier.
	require.Equal(t, `{A-Z}@0`, spanEntries(spAZ))
	advance(spAZ, 1)
	require.Equal(t, `{A-Z}@1`, spanEntries(spAZ))

	// Span overlaps part of the frontier, with left part outside frontier.
	require.Equal(t, `{A-C}@1`, spanEntries(mkspan('0', 'C')))

	// Span overlaps part of the frontier, with right part outside frontier.
	require.Equal(t, `{Q-Z}@1`, spanEntries(mkspan('Q', 'c')))

	// Span fully inside frontier.
	require.Equal(t, `{P-W}@1`, spanEntries(mkspan('P', 'W')))

	// Advance part of the frontier.
	advance(mkspan('C', 'E'), 2)
	advance(mkspan('H', 'M'), 5)
	advance(mkspan('N', 'Q'), 3)

	// Span overlaps various parts of the frontier.
	require.Equal(t,
		`{A-C}@1 {C-E}@2 {E-H}@1 {H-M}@5 {M-N}@1 {N-P}@3`,
		spanEntries(mkspan('3', 'P')))
}

func TestUpdatedEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	key := func(c byte) roachpb.Key {
		return roachpb.Key{c}
	}
	mkspan := func(start, end byte) roachpb.Span {
		return roachpb.Span{Key: key(start), EndKey: key(end)}
	}

	spAZ := mkspan('A', 'Z')
	f, err := MakeFrontier(spAZ)
	require.NoError(t, err)

	var wall int64 = 0
	advance := func(s roachpb.Span, newWall int64) {
		wall = newWall
		_, err := f.Forward(s, hlc.Timestamp{WallTime: wall})
		require.NoError(t, err)
	}

	updatedEntries := func(cutoff int64) string {
		var buf strings.Builder
		f.UpdatedEntries(hlc.Timestamp{WallTime: cutoff}, func(s roachpb.Span, ts hlc.Timestamp) OpResult {
			if buf.Len() != 0 {
				buf.WriteString(` `)
			}
			fmt.Fprintf(&buf, `%s@%d`, s, ts.WallTime)
			return ContinueMatch
		})
		return buf.String()
	}

	// If we haven't configured frontier to keep track of updates, we expect to see
	// all spans as updated.
	require.Equal(t, ``, updatedEntries(0))
	require.Equal(t, ``, updatedEntries(1))
	advance(mkspan('C', 'E'), 2)
	require.Equal(t, ``, updatedEntries(1))

	f.TrackUpdateTimestamp(func() hlc.Timestamp { return hlc.Timestamp{WallTime: wall} })

	advance(mkspan('C', 'E'), 3)
	require.Equal(t, `{C-E}@3`, updatedEntries(0))
	require.Equal(t, `{C-E}@3`, updatedEntries(2))
	advance(mkspan('D', 'E'), 4)
	require.Equal(t, `{C-D}@3 {D-E}@4`, updatedEntries(3))

	// Nothing was updated after t=4
	require.Equal(t, ``, updatedEntries(4))

	advance(mkspan('C', 'E'), 5)
	require.Equal(t, `{C-E}@5`, updatedEntries(4))

	advance(spAZ, 5)
	require.Equal(t, `{A-Z}@5`, updatedEntries(4))
	require.Equal(t, ``, updatedEntries(5))
}

// symbols that can make up spans.
var spanSymbols = []byte("@$0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type spanMaker struct {
	rnd        *rand.Rand
	numSymbols int
	starts     []interval.Comparable
}

func newSpanMaker(numSymbols int, rnd *rand.Rand) (*spanMaker, roachpb.Span) {
	m := &spanMaker{
		rnd:        rnd,
		numSymbols: numSymbols,
	}
	span := roachpb.Span{
		Key:    roachpb.Key{'A'},
		EndKey: roachpb.Key{'z'},
	}
	return m, span
}

func (m *spanMaker) rndKey() interval.Comparable {
	var key []byte
	for n := 1 + m.rnd.Intn(m.numSymbols); n > 0; n-- {
		key = append(key, spanSymbols[m.rnd.Intn(len(spanSymbols))])
	}
	return key
}

func (m *spanMaker) rndSpan() roachpb.Span {
	var startKey interval.Comparable

	if len(m.starts) > 0 && m.rnd.Int()%17 == 0 {
		// With some probability use previous starting point.
		startKey = m.starts[m.rnd.Intn(len(m.starts))]
		// Just for fun, nudge start a bit forward or back.
		if dice := m.rnd.Intn(3) - 1; dice != 0 {
			startKey[len(startKey)-1] += byte(dice)
		}
	} else {
		// Generate a new start.
		startKey = m.rndKey()
		m.starts = append(m.starts, startKey)
	}

	endKey := m.rndKey()

	if startKey.Equal(endKey) {
		endKey = append(endKey, spanSymbols[m.rnd.Intn(len(spanSymbols))])
	}

	if endKey.Compare(startKey) < 0 {
		startKey, endKey = endKey, startKey
	}
	if endKey.Equal(startKey) {
		panic(roachpb.Span{Key: roachpb.Key(startKey), EndKey: roachpb.Key(endKey)}.String())
	}
	return roachpb.Span{Key: roachpb.Key(startKey), EndKey: roachpb.Key(endKey)}
}

func BenchmarkFrontier(b *testing.B) {
	var rndSeed = randutil.NewPseudoSeed()

	rnd := rand.New(rand.NewSource(rndSeed))
	spanMaker, span := newSpanMaker(4, rnd)

	f, err := MakeFrontier(span)
	require.NoError(b, err)
	log.Infof(context.Background(), "N=%d TestSpan: %s (seed: %d) Entries: %s", b.N, span, rndSeed, f.entriesStr())
	b.ReportAllocs()
	var wall int64 = 10

	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			wall += rnd.Int63n(10)
		}

		delta := rnd.Int63n(10) - rnd.Int63n(3)
		rndSpan := spanMaker.rndSpan()
		_, err := f.Forward(rndSpan, hlc.Timestamp{WallTime: wall + delta})
		require.NoError(b, err)
	}
	log.Infof(context.Background(), "%d entries: %s", f.tree.Len(), f.entriesStr())
}
