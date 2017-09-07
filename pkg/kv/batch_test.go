// Copyright 2015 The Cockroach Authors.
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

package kv

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestBatchPrevNext tests prev() and next()
func TestBatchPrevNext(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		// FIXINPR(tschottdorf): explain these test cases too. Add more test cases for
		// local keys and explain what's going on (requires understanding it first).
		{spans: abc, key: "b\x00", expFW: "c", expBW: "b"},
		{spans: abc, key: "bb", expFW: "c", expBW: "b"},
		{spans: span(), key: "whatevs", expFW: max, expBW: min},
		{spans: span(loc("a"), loc("c")), key: "c", expFW: "c", expBW: "c"},
		{spans: span(loc("a"), loc("c")), key: "c\x00", expFW: max, expBW: "c\x00"},
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

func TestBatchPrevNextWithNoop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	leftKey := roachpb.Key("a")
	middleKey := roachpb.RKey("b")
	rightKey := roachpb.Key("c")
	var ba roachpb.BatchRequest
	ba.Add(&roachpb.GetRequest{Span: roachpb.Span{Key: leftKey}})
	ba.Add(&roachpb.NoopRequest{})
	ba.Add(&roachpb.GetRequest{Span: roachpb.Span{Key: rightKey}})

	t.Run("prev", func(t *testing.T) {
		rk, err := prev(ba, middleKey)
		if err != nil {
			t.Fatal(err)
		}
		if !rk.Equal(leftKey) {
			t.Errorf("got %s, expected %s", rk, leftKey)
		}
	})
	t.Run("next", func(t *testing.T) {
		rk, err := next(ba, middleKey)
		if err != nil {
			t.Fatal(err)
		}
		if !rk.Equal(rightKey) {
			t.Errorf("got %s, expected %s", rk, rightKey)
		}
	})
}
