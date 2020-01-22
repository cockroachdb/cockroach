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

	f := MakeFrontier(spAD)
	require.Equal(t, hlc.Timestamp{}, f.Frontier())
	require.Equal(t, `{a-d}@0`, f.entriesStr())

	// Untracked spans are ignored
	adv := f.Forward(
		roachpb.Span{Key: []byte("d"), EndKey: []byte("e")},
		hlc.Timestamp{WallTime: 1},
	)
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{}, f.Frontier())
	require.Equal(t, `{a-d}@0`, f.entriesStr())

	// Forward the entire tracked spanspace.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 1})
	require.Equal(t, true, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 1}, f.Frontier())
	require.Equal(t, `{a-d}@1`, f.entriesStr())

	// Forward it again.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 2})
	require.Equal(t, true, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())
	require.Equal(t, `{a-d}@2`, f.entriesStr())

	// Forward to the previous frontier.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 2})
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())
	require.Equal(t, `{a-d}@2`, f.entriesStr())

	// Forward into the past is ignored.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 1})
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())
	require.Equal(t, `{a-d}@2`, f.entriesStr())

	// Forward a subset.
	adv = f.Forward(spBC, hlc.Timestamp{WallTime: 3})
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())
	require.Equal(t, `{a-b}@2 {b-c}@3 {c-d}@2`, f.entriesStr())

	// Forward it more.
	adv = f.Forward(spBC, hlc.Timestamp{WallTime: 4})
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())
	require.Equal(t, `{a-b}@2 {b-c}@4 {c-d}@2`, f.entriesStr())

	// Forward all tracked spans to timestamp before BC (currently at 4).
	// Advances to the min of tracked spans. Note that this requires the
	// forwarded span to be split into two spans, one on each side of BC.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 3})
	require.Equal(t, true, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 3}, f.Frontier())
	require.Equal(t, `{a-b}@3 {b-c}@4 {c-d}@3`, f.entriesStr())

	// Forward everything but BC, advances to the min of tracked spans.
	adv = f.Forward(spAB, hlc.Timestamp{WallTime: 5})
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 3}, f.Frontier())
	adv = f.Forward(spCD, hlc.Timestamp{WallTime: 5})
	require.Equal(t, true, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 4}, f.Frontier())
	require.Equal(t, `{a-b}@5 {b-c}@4 {c-d}@5`, f.entriesStr())

	// Catch BC up.
	adv = f.Forward(spBC, hlc.Timestamp{WallTime: 5})
	require.Equal(t, true, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 5}, f.Frontier())
	require.Equal(t, `{a-b}@5 {b-c}@5 {c-d}@5`, f.entriesStr())

	// Forward them all at once (spans don't collapse for now, this is a TODO).
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 6})
	require.Equal(t, true, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 6}, f.Frontier())
	require.Equal(t, `{a-b}@6 {b-c}@6 {c-d}@6`, f.entriesStr())

	// Split AC with BD.
	adv = f.Forward(spCD, hlc.Timestamp{WallTime: 7})
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 6}, f.Frontier())
	require.Equal(t, `{a-b}@6 {b-c}@6 {c-d}@7`, f.entriesStr())
	adv = f.Forward(spBD, hlc.Timestamp{WallTime: 8})
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 6}, f.Frontier())
	require.Equal(t, `{a-b}@6 {b-c}@8 {c-d}@8`, f.entriesStr())
	adv = f.Forward(spAB, hlc.Timestamp{WallTime: 8})
	require.Equal(t, true, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 8}, f.Frontier())
	require.Equal(t, `{a-b}@8 {b-c}@8 {c-d}@8`, f.entriesStr())

	// Split BD with AC.
	adv = f.Forward(spAC, hlc.Timestamp{WallTime: 9})
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 8}, f.Frontier())
	require.Equal(t, `{a-b}@9 {b-c}@9 {c-d}@8`, f.entriesStr())
	adv = f.Forward(spCD, hlc.Timestamp{WallTime: 9})
	require.Equal(t, true, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 9}, f.Frontier())
	require.Equal(t, `{a-b}@9 {b-c}@9 {c-d}@9`, f.entriesStr())
}

func TestSpanFrontierDisjointSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	keyD, keyE, keyF := roachpb.Key("d"), roachpb.Key("e"), roachpb.Key("f")
	spAB := roachpb.Span{Key: keyA, EndKey: keyB}
	spAD := roachpb.Span{Key: keyA, EndKey: keyD}
	spCE := roachpb.Span{Key: keyC, EndKey: keyE}
	spDF := roachpb.Span{Key: keyD, EndKey: keyF}

	f := MakeFrontier(spAB, spCE)
	require.Equal(t, hlc.Timestamp{}, f.Frontier())
	require.Equal(t, `{a-b}@0 {c-e}@0`, f.entriesStr())

	// Advance just the tracked spans
	adv := f.Forward(spCE, hlc.Timestamp{WallTime: 1})
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{}, f.Frontier())
	require.Equal(t, `{a-b}@0 {c-e}@1`, f.entriesStr())
	adv = f.Forward(spAB, hlc.Timestamp{WallTime: 1})
	require.Equal(t, true, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 1}, f.Frontier())
	require.Equal(t, `{a-b}@1 {c-e}@1`, f.entriesStr())

	// Advance a span that partially overlaps the tracked spans
	adv = f.Forward(spDF, hlc.Timestamp{WallTime: 2})
	require.Equal(t, false, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 1}, f.Frontier())
	require.Equal(t, `{a-b}@1 {c-d}@1 {d-e}@2`, f.entriesStr())

	// Advance one span that covers two tracked spans and so needs two entries.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 3})
	require.Equal(t, true, adv)
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())
	require.Equal(t, `{a-b}@3 {c-d}@3 {d-e}@2`, f.entriesStr())
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
