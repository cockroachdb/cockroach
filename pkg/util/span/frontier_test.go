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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func (f *Frontier) entriesStr() string {
	var buf strings.Builder
	f.Entries(func(sp roachpb.Span, ts hlc.Timestamp) {
		if buf.Len() != 0 {
			buf.WriteString(` `)
		}
		fmt.Fprintf(&buf, `%s@%d`, sp, ts.WallTime)
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

	// Catch BC up.
	forwardFrontier(spBC, 5).
		expectedAdvanced(true).
		expectFrontier(5).
		expectEntries(`{a-b}@5 {b-c}@5 {c-d}@5`)

	// Forward them all at once (spans don't collapse for now, this is a TODO).
	forwardFrontier(spAD, 6).
		expectedAdvanced(true).
		expectFrontier(6).
		expectEntries(`{a-b}@6 {b-c}@6 {c-d}@6`)

	// Split AC with BD.
	forwardFrontier(spCD, 7).
		expectedAdvanced(false).
		expectFrontier(6).
		expectEntries(`{a-b}@6 {b-c}@6 {c-d}@7`)

	forwardFrontier(spBD, 8).
		expectedAdvanced(false).
		expectFrontier(6).
		expectEntries(`{a-b}@6 {b-c}@8 {c-d}@8`)

	forwardFrontier(spAB, 8).
		expectedAdvanced(true).
		expectFrontier(8).
		expectEntries(`{a-b}@8 {b-c}@8 {c-d}@8`)

	// Split BD with AC.
	forwardFrontier(spAC, 9).
		expectedAdvanced(false).
		expectFrontier(8).
		expectEntries(`{a-b}@9 {b-c}@9 {c-d}@8`)

	forwardFrontier(spCD, 9).
		expectedAdvanced(true).
		expectFrontier(9).
		expectEntries(`{a-b}@9 {b-c}@9 {c-d}@9`)
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
