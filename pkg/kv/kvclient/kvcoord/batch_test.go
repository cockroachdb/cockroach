// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestBatchPrevNext tests prev() and next()
func TestBatchPrevNext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	loc := func(s string) string {
		return string(keys.RangeDescriptorKey(roachpb.RKey(s)))
	}
	span := func(strs ...string) []roachpb.Span {
		var r []roachpb.Span
		for i, str := range strs {
			if i%2 == 0 {
				r = append(r, roachpb.Span{Key: roachpb.Key(str)})
			} else {
				r[len(r)-1].EndKey = roachpb.Key(str)
			}
		}
		return r
	}
	max, min := string(roachpb.RKeyMax), string(roachpb.RKeyMin)
	abc := span("a", "", "b", "", "c", "")
	testCases := []struct {
		spans             []roachpb.Span
		key, expFW, expBW string
	}{
		{spans: span(), key: "whatevs",
			// Sanity check for empty batch.
			expFW: max,
			expBW: min,
		},
		{spans: span("a", "b", "c", "d"), key: "c",
			// Done with `key < c`, so `c <= key` next.
			expFW: "c",
			// This is the interesting case in this test. See
			// https://github.com/cockroachdb/cockroach/issues/18174
			//
			// Done with `key >= c`, and nothing's in `[b, c)`, so all that's
			// left is `key < b`. Before fixing #18174 we would end up at `c`
			// which could create empty batches.
			expBW: "b",
		},
		{spans: span("a", "c", "b", ""), key: "b",
			// Done with `key < b`, so `b <= key` is next.
			expFW: "b",
			// Done with `key >= b`, but we have work to do for `b > key`.
			expBW: "b",
		},
		{spans: span("a", "c", "b", ""), key: "a",
			// Same as last one.
			expFW: "a",
			// Same as the first test case, except we drop all the way to `min`.
			expBW: min,
		},
		{spans: span("a", "c", "d", ""), key: "c",
			// Having dealt with `key < c` there's a gap which leaves us at `d <= key`.
			expFW: "d",
			// Nothing exciting: [a, c) is outstanding.
			expBW: "c",
		},
		{spans: span("a", "c\x00", "d", ""), key: "c",
			// First request overlaps `c` in both directions,
			// so that's where we stay.
			expFW: "c",
			expBW: "c",
		},
		{spans: abc, key: "b",
			// We've seen `key < b` so we still need to hit `b`.
			expFW: "b",
			// We've seen `key >= b`, so hop over the gap to `a`. Similar to the
			// first test case.
			expBW: "a",
		},
		{spans: abc, key: "b\x00",
			// No surprises.
			expFW: "c",
			expBW: "b",
		},
		{spans: abc, key: "bb",
			// Ditto.
			expFW: "c",
			expBW: "b",
		},

		// Multiple candidates. No surprises, just a sanity check.
		{spans: span("a", "b", "c", "d"), key: "e",
			expFW: max,
			expBW: "d",
		},
		{spans: span("a", "b", "c", "d"), key: "0",
			expFW: "a",
			expBW: min,
		},

		// Local keys are tricky. See keys.AddrUpperBound for a comment. The basic
		// intuition should be that /Local/a/b lives between a and a\x00 (for any b)
		// and so the smallest address-resolved range that covers /Local/a/b is [a, a\x00].
		{spans: span(loc("a"), loc("c")), key: "c",
			// We're done with any key that addresses to `< c`, and `loc(c)` isn't covered
			// by that. `loc(c)` lives between `c` and `c\x00` and so `c` is where we have
			// to start in forward mode.
			expFW: "c",
			// We're done with any key that addresses to `>=c`, `loc(c)` is the exclusive
			// end key here, so we next handle `key < c`.
			expBW: "c",
		},
		{spans: span(loc("a"), loc("c")), key: "c\x00",
			// We've dealt with everything addressing to `< c\x00`, and in particular
			// `addr(loc(c)) < c\x00`, so that span is handled and we see `max`.
			expFW: max,
			// Having dealt with `>= c\x00` we have to restart at `c\x00` itself (and not `c`!)
			// because otherwise we'd not see the local keys `/Local/c/x` which are not in `key < c`
			// but are in `key < c\x00`.
			expBW: "c\x00",
		},
		// Explanations below are an exercise for the reader, but it's very similar to above.
		{spans: span(loc("b"), ""), key: "a",
			expFW: "b",
			expBW: min,
		},
		{spans: span(loc("b"), ""), key: "b",
			expFW: "b",
			expBW: min,
		},
		{spans: span(loc("b"), ""), key: "b\x00",
			expFW: max,
			// Handled `key >= b\x00`, so next we'll have to chip away at `[KeyMin, b\x00)`. Note
			// how this doesn't return `b` which would be incorrect as `[KeyMin, b)` does not
			// contain `loc(b)`.
			expBW: "b\x00",
		},

		// Multiple candidates. No surprises, just a sanity check.
		{spans: span(loc("a"), loc("b"), loc("c"), loc("d")), key: "e",
			expFW: max,
			expBW: "d\x00",
		},
		{spans: span(loc("a"), loc("b"), loc("c"), loc("d")), key: "0",
			expFW: "a",
			expBW: min,
		},
	}

	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			var ba roachpb.BatchRequest
			for _, span := range test.spans {
				args := &roachpb.ScanRequest{}
				args.Key, args.EndKey = span.Key, span.EndKey
				ba.Add(args)
			}
			if next, err := next(ba, roachpb.RKey(test.key)); err != nil {
				t.Error(err)
			} else if !bytes.Equal(next, roachpb.Key(test.expFW)) {
				t.Errorf("next: expected %q, got %q", test.expFW, next)
			}
			if prev, err := prev(ba, roachpb.RKey(test.key)); err != nil {
				t.Error(err)
			} else if !bytes.Equal(prev, roachpb.Key(test.expBW)) {
				t.Errorf("prev: expected %q, got %q", test.expBW, prev)
			}
		})
	}
}
