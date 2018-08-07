// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

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

	f := makeSpanFrontier()
	require.Equal(t, hlc.Timestamp{}, f.Frontier())

	f.ChangeTrackedSpans(spAD)
	require.Equal(t, hlc.Timestamp{}, f.Frontier())

	// Forward the entire watched spanspace.
	adv := f.Forward(spAD, hlc.Timestamp{WallTime: 1})
	require.Equal(t, true, adv)
	require.Equal(t, 1, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 1}, f.Frontier())

	// Forward it again.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 2})
	require.Equal(t, true, adv)
	require.Equal(t, 1, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())

	// Forward to the previous frontier.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 2})
	require.Equal(t, false, adv)
	require.Equal(t, 1, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())

	// Forward into the past is ignored.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 1})
	require.Equal(t, false, adv)
	require.Equal(t, 1, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())

	// Forward a subset.
	adv = f.Forward(spBC, hlc.Timestamp{WallTime: 3})
	require.Equal(t, false, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())

	// Forward it more.
	adv = f.Forward(spBC, hlc.Timestamp{WallTime: 4})
	require.Equal(t, false, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())

	// Forward all watched spans to timestamp before BC (currently at 4).
	// Advances to the min of watched spans. Note that this requires AD to be
	// split into two spans, one on each side of BC.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 3})
	require.Equal(t, true, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 3}, f.Frontier())

	// Forward everything but BC, advances to the min of watched spans.
	adv = f.Forward(spAB, hlc.Timestamp{WallTime: 5})
	require.Equal(t, false, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 3}, f.Frontier())
	adv = f.Forward(spCD, hlc.Timestamp{WallTime: 5})
	require.Equal(t, true, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 4}, f.Frontier())

	// Catch BC up.
	adv = f.Forward(spBC, hlc.Timestamp{WallTime: 5})
	require.Equal(t, true, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 5}, f.Frontier())

	// Forward them all at once (spans don't collapse for now, note the Len).
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 6})
	require.Equal(t, true, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 6}, f.Frontier())

	// Split AC with BD.
	adv = f.Forward(spCD, hlc.Timestamp{WallTime: 7})
	require.Equal(t, false, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 6}, f.Frontier())
	adv = f.Forward(spBD, hlc.Timestamp{WallTime: 8})
	require.Equal(t, false, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 6}, f.Frontier())
	adv = f.Forward(spAB, hlc.Timestamp{WallTime: 8})
	require.Equal(t, true, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 8}, f.Frontier())

	// Split BD with AC.
	adv = f.Forward(spAC, hlc.Timestamp{WallTime: 9})
	require.Equal(t, false, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 8}, f.Frontier())
	adv = f.Forward(spCD, hlc.Timestamp{WallTime: 9})
	require.Equal(t, true, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 9}, f.Frontier())
}

func TestSpanFrontierChangeSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	keyD, keyE := roachpb.Key("d"), roachpb.Key("e")

	spAB := roachpb.Span{Key: keyA, EndKey: keyB}
	spAC := roachpb.Span{Key: keyA, EndKey: keyC}
	spAD := roachpb.Span{Key: keyA, EndKey: keyD}
	spBC := roachpb.Span{Key: keyB, EndKey: keyC}
	spBD := roachpb.Span{Key: keyB, EndKey: keyD}
	spCE := roachpb.Span{Key: keyC, EndKey: keyE}
	spDE := roachpb.Span{Key: keyD, EndKey: keyE}

	f := makeSpanFrontier()
	require.Equal(t, hlc.Timestamp{}, f.Frontier())

	// All spans are ignored when not watching anything.
	adv := f.Forward(spBC, hlc.Timestamp{WallTime: 1})
	require.Equal(t, false, adv)
	require.Equal(t, 0, f.Len())
	require.Equal(t, hlc.Timestamp{}, f.Frontier())

	// Span bounds are clipped. AB doesn't advance the frontier after being
	// clipped out from AC on the first call.
	f.ChangeTrackedSpans(spAB)
	adv = f.Forward(spAC, hlc.Timestamp{WallTime: 2})
	require.Equal(t, true, adv)
	require.Equal(t, 1, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())
	adv = f.Forward(spAB, hlc.Timestamp{WallTime: 2})
	require.Equal(t, false, adv)
	require.Equal(t, 1, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())

	// Enlarging the watched spans keeps AB @ 2 but the frontier resets to zero
	// because we've never seen any Forwards for CD.
	f.ChangeTrackedSpans(spAD)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{}, f.Frontier())
	adv = f.Forward(spBD, hlc.Timestamp{WallTime: 1})
	require.Equal(t, true, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 1}, f.Frontier())

	// Disjoint watched spans work, as does splitting up the previously watched
	// spans.
	f.ChangeTrackedSpans(spAB, spCE)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 0}, f.Frontier())
	adv = f.Forward(spCE, hlc.Timestamp{WallTime: 3})
	require.Equal(t, true, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 2}, f.Frontier())
	adv = f.Forward(spAB, hlc.Timestamp{WallTime: 3})
	require.Equal(t, true, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 3}, f.Frontier())

	// Advance one span that covers two watched spans and so needs two entries.
	adv = f.Forward(spAD, hlc.Timestamp{WallTime: 4})
	require.Equal(t, false, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 3}, f.Frontier())
	adv = f.Forward(spDE, hlc.Timestamp{WallTime: 4})
	require.Equal(t, true, adv)
	require.Equal(t, 3, f.Len())
	require.Equal(t, hlc.Timestamp{WallTime: 4}, f.Frontier())
}
