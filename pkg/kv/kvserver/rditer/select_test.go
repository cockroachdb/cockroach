// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rditer

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))
	for _, tt := range []struct {
		name string
		span roachpb.RSpan
	}{{
		name: "r1",
		span: roachpb.RSpan{
			// r1 is special - see https://github.com/cockroachdb/cockroach/issues/95055.
			Key:    roachpb.RKeyMin,
			EndKey: roachpb.RKey("c"),
		},
	}, {
		name: "r2",
		span: roachpb.RSpan{
			Key:    roachpb.RKey("a"),
			EndKey: roachpb.RKey("c"),
		},
	}, {
		name: "r3",
		span: roachpb.RSpan{
			Key:    roachpb.RKey("a"),
			EndKey: roachpb.RKeyMax,
		},
	}} {
		t.Run(tt.name, w.Run(t, tt.name, func(t *testing.T) string {
			const rangeID = roachpb.RangeID(123)
			return testSelect(t, rangeID, tt.span)
		}))
	}
}

func testSelect(t *testing.T, rangeID roachpb.RangeID, span roachpb.RSpan) string {
	t.Helper()
	// Currently, there are 5 key types that can be owned by a range, and returned
	// by Select. This test exhaustively checks that Select returns the right set
	// of spans for each subset of these types. The order of the types corresponds
	// to the key order.
	spaces := []string{
		"ReplicatedByRangeID",
		"UnreplicatedByRangeID",
		"SystemKeys",
		"LockTable",
		"UserKeys",
	}
	// runSelect generates and executes Select based on the bitmask, with each bit
	// encoding one of the key spaces.
	runSelect := func(rangeID roachpb.RangeID, span roachpb.RSpan, mask int) []roachpb.Span {
		so := SelectOpts{
			ReplicatedByRangeID:   mask&1 != 0,
			UnreplicatedByRangeID: mask&2 != 0,
			Ranged: SelectRangedOptions{
				SystemKeys: mask&4 != 0,
				LockTable:  mask&8 != 0,
				UserKeys:   mask&16 != 0,
			},
		}
		// Set the RSpan only if any system/lock/user key spaces is requested.
		if mask&0b11100 != 0 {
			so.Ranged.RSpan = span
		}
		return Select(rangeID, so)
	}

	// Print each key space separately to the datadriven output for eyeballing.
	var buf []byte
	for i, space := range spaces {
		buf = fmt.Appendf(buf, "%s:\n", space)
		for _, s := range runSelect(rangeID, span, 1<<i) {
			buf = fmt.Appendf(buf, "  %v\n", s)
		}
	}
	// Print the result of "select all" as well, for convenience.
	buf = fmt.Appendf(buf, "All:\n")
	for _, s := range runSelect(rangeID, span, (1<<len(spaces))-1) {
		buf = fmt.Appendf(buf, "  %v\n", s)
	}

	// Verify the following two properties, by exhaustively checking all subsets:
	//	1. Select(A|B) == Select(A) | Select(B)
	//	2. Select(A) returns non-overlapping spans, ordered by key
	//
	// NB: we don't need to print these combinations to the datadriven output
	// because this does not increase the power of this test.
	for mask, end := 0, 1<<len(spaces); mask < end; mask++ {
		// Decompose the Select into single-type selects. These are already printed
		// to the datadriven output above, so we can assume them correct.
		var spans []roachpb.Span
		for i := range len(spaces) {
			if (mask>>i)&1 == 1 {
				spans = append(spans, runSelect(rangeID, span, 1<<i)...)
			}
		}
		// The combined Select must return the union of the individual selects.
		require.Equal(t, spans, runSelect(rangeID, span, mask))
		// All spans must be non-overlapping and key-ordered.
		assert.True(t, slices.IsSortedFunc(spans, func(a, b roachpb.Span) int {
			return bytes.Compare(a.EndKey, b.Key)
		}))
	}

	return string(buf)
}
