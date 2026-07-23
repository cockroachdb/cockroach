// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lockspanset

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Test that spans are properly classified according to the lock strength they
// are added with.
func TestGetSpansStrength(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var lss LockSpanSet
	spA := roachpb.Span{Key: roachpb.Key("a")}
	spBC := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}
	spD := roachpb.Span{Key: roachpb.Key("d")}
	spE := roachpb.Span{Key: roachpb.Key("e")}
	spF := roachpb.Span{Key: roachpb.Key("f")}
	spGH := roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}

	lss.Add(lock.None, spA)
	lss.Add(lock.Shared, spBC)
	lss.Add(lock.Update, spD)

	lss.Add(lock.Exclusive, spE)
	lss.Add(lock.Exclusive, spGH)

	lss.Add(lock.Intent, spF)

	spans := lss.GetSpans(lock.None)
	require.Equal(t, []roachpb.Span{spA}, spans)

	spans = lss.GetSpans(lock.Shared)
	require.Equal(t, []roachpb.Span{spBC}, spans)

	spans = lss.GetSpans(lock.Update)
	require.Equal(t, []roachpb.Span{spD}, spans)

	spans = lss.GetSpans(lock.Exclusive)
	require.Equal(t, []roachpb.Span{spE, spGH}, spans)

	spans = lss.GetSpans(lock.Intent)
	require.Equal(t, []roachpb.Span{spF}, spans)
}

// TestLockSpanSetSortAndDeDup ensures that spans in a lock span set are sorted
// and de-duplicated correctly. Spans should be sorted and de-duplicated within
// a particular lock strength but not amongst different lock strengths.
func TestLockSpanSetSortAndDeDup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	makeSpan := func(start, end string) roachpb.Span {
		var endKey roachpb.Key
		if end != "" {
			endKey = roachpb.Key(end)
		}
		return roachpb.Span{Key: roachpb.Key(start), EndKey: endKey}
	}

	spA := makeSpan("a", "")
	spB := makeSpan("b", "")
	spCF := makeSpan("c", "f")
	spEH := makeSpan("e", "h")
	spGJ := makeSpan("g", "j")
	spIL := makeSpan("i", "l")
	spXZ := makeSpan("x", "z")

	var lss LockSpanSet
	lss.Add(lock.None, spA)
	lss.Add(lock.None, spA)  // duplicate
	lss.Add(lock.None, spA)  // duplicate
	lss.Add(lock.None, spA)  // duplicate
	lss.Add(lock.None, spCF) // overlapping
	lss.Add(lock.None, spEH) // overlapping
	lss.Add(lock.None, spGJ) // overlapping
	lss.Add(lock.None, spB)  // out of order

	// Shared.
	lss.Add(lock.Shared, spXZ) // out of order
	lss.Add(lock.Shared, spA)  // should not be considered a duplicate
	lss.Add(lock.Shared, spIL) // should not overlap

	lss.SortAndDeDup()

	spans := lss.GetSpans(lock.None)
	require.Len(t, spans, 3)
	require.Equal(t, spans, []roachpb.Span{spA, spB, makeSpan("c", "j")})

	spans = lss.GetSpans(lock.Shared)
	require.Equal(t, spans, []roachpb.Span{spA, spIL, spXZ})

	require.Len(t, lss.GetSpans(lock.Update), 0)
	require.Len(t, lss.GetSpans(lock.Exclusive), 0)
	require.Len(t, lss.GetSpans(lock.Intent), 0)
}

// TestLockSpanSetCopy tests copying of lock span sets.
func TestLockSpanSetCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	lss := New()
	lss.Add(lock.None, roachpb.Span{Key: roachpb.Key("abc")})
	lss.Add(lock.Update, roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")})

	c := lss.Copy()
	require.Equal(t, lss, c)

	// modifying element in lss should not modify copy
	lss.Add(lock.None, roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")})
	require.NotEqual(t, lss, c)
}
