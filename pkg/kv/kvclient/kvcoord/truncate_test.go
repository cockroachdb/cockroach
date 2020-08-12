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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	loc := func(s string) string {
		return string(keys.RangeDescriptorKey(roachpb.RKey(s)))
	}
	locPrefix := func(s string) string {
		return string(keys.MakeRangeKeyPrefix(roachpb.RKey(s)))
	}
	testCases := []struct {
		keys     [][2]string
		expKeys  [][2]string
		from, to string
		desc     [2]string // optional, defaults to {from,to}
		err      string
	}{
		{
			// Keys inside of active range.
			keys:    [][2]string{{"a", "q"}, {"c"}, {"b, e"}, {"q"}},
			expKeys: [][2]string{{"a", "q"}, {"c"}, {"b, e"}, {"q"}},
			from:    "a", to: "q\x00",
		},
		{
			// Keys outside of active range.
			keys:    [][2]string{{"a"}, {"a", "b"}, {"q"}, {"q", "z"}},
			expKeys: [][2]string{{}, {}, {}, {}},
			from:    "b", to: "q",
		},
		{
			// Range-local keys inside of active range.
			keys:    [][2]string{{loc("b")}, {loc("c")}},
			expKeys: [][2]string{{loc("b")}, {loc("c")}},
			from:    "b", to: "e",
		},
		{
			// Range-local key outside of active range.
			keys:    [][2]string{{loc("a")}},
			expKeys: [][2]string{{}},
			from:    "b", to: "e",
		},
		{
			// Range-local range contained in active range.
			keys:    [][2]string{{loc("b"), loc("e") + "\x00"}},
			expKeys: [][2]string{{loc("b"), loc("e") + "\x00"}},
			from:    "b", to: "e\x00",
		},
		{
			// Range-local range not contained in active range.
			keys:    [][2]string{{loc("a"), loc("b")}},
			expKeys: [][2]string{{}},
			from:    "c", to: "e",
		},
		{
			// Range-local range not contained in active range.
			keys:    [][2]string{{loc("a"), locPrefix("b")}, {loc("e"), loc("f")}},
			expKeys: [][2]string{{}, {}},
			from:    "b", to: "e",
		},
		{
			// Range-local range partially contained in active range.
			keys:    [][2]string{{loc("a"), loc("b")}},
			expKeys: [][2]string{{loc("a"), locPrefix("b")}},
			from:    "a", to: "b",
		},
		{
			// Range-local range partially contained in active range.
			keys:    [][2]string{{loc("a"), loc("b")}},
			expKeys: [][2]string{{locPrefix("b"), loc("b")}},
			from:    "b", to: "e",
		},
		{
			// Range-local range contained in active range.
			keys:    [][2]string{{locPrefix("b"), loc("b")}},
			expKeys: [][2]string{{locPrefix("b"), loc("b")}},
			from:    "b", to: "c",
		},
		{
			// Mixed range-local vs global key range.
			keys: [][2]string{{loc("c"), "d\x00"}},
			from: "b", to: "e",
			err: "local key mixed with global key",
		},
		{
			// Key range touching and intersecting active range.
			keys:    [][2]string{{"a", "b"}, {"a", "c"}, {"p", "q"}, {"p", "r"}, {"a", "z"}},
			expKeys: [][2]string{{"b", "c"}, {"p", "q"}, {"p", "q"}, {"b", "q"}},
			from:    "b", to: "q",
		},
		// Active key range is intersection of descriptor and [from,to).
		{
			keys:    [][2]string{{"c", "q"}},
			expKeys: [][2]string{{"d", "p"}},
			from:    "a", to: "z",
			desc: [2]string{"d", "p"},
		},
		{
			keys:    [][2]string{{"c", "q"}},
			expKeys: [][2]string{{"d", "p"}},
			from:    "d", to: "p",
			desc: [2]string{"a", "z"},
		},
	}

	for i, test := range testCases {
		goldenOriginal := roachpb.BatchRequest{}
		for _, ks := range test.keys {
			if len(ks[1]) > 0 {
				goldenOriginal.Add(&roachpb.ResolveIntentRangeRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: roachpb.Key(ks[0]), EndKey: roachpb.Key(ks[1]),
					},
					IntentTxn: enginepb.TxnMeta{ID: uuid.MakeV4()},
				})
			} else {
				goldenOriginal.Add(&roachpb.GetRequest{
					RequestHeader: roachpb.RequestHeader{Key: roachpb.Key(ks[0])},
				})
			}
		}

		original := roachpb.BatchRequest{Requests: make([]roachpb.RequestUnion, len(goldenOriginal.Requests))}
		for i, request := range goldenOriginal.Requests {
			original.Requests[i].MustSetInner(request.GetInner().ShallowCopy())
		}

		desc := &roachpb.RangeDescriptor{
			StartKey: roachpb.RKey(test.desc[0]), EndKey: roachpb.RKey(test.desc[1]),
		}
		if len(desc.StartKey) == 0 {
			desc.StartKey = roachpb.RKey(test.from)
		}
		if len(desc.EndKey) == 0 {
			desc.EndKey = roachpb.RKey(test.to)
		}
		rs := roachpb.RSpan{Key: roachpb.RKey(test.from), EndKey: roachpb.RKey(test.to)}
		rs, err := rs.Intersect(desc)
		if err != nil {
			t.Errorf("%d: intersection failure: %v", i, err)
			continue
		}
		ba, pos, err := truncate(original, rs)
		if err != nil || test.err != "" {
			if !testutils.IsError(err, test.err) {
				t.Errorf("%d: %v (expected: %q)", i, err, test.err)
			}
			continue
		}
		var reqs int
		for j, arg := range ba.Requests {
			req := arg.GetInner()
			if h := req.Header(); !bytes.Equal(h.Key, roachpb.Key(test.expKeys[j][0])) || !bytes.Equal(h.EndKey, roachpb.Key(test.expKeys[j][1])) {
				t.Errorf("%d.%d: range mismatch: actual [%q,%q), wanted [%q,%q)", i, j,
					h.Key, h.EndKey, test.expKeys[j][0], test.expKeys[j][1])
			} else if len(h.Key) != 0 {
				reqs++
			}
		}
		if num := len(pos); reqs != num {
			t.Errorf("%d: counted %d requests, but truncation indicated %d", i, reqs, num)
		}
		if !reflect.DeepEqual(original, goldenOriginal) {
			t.Errorf("%d: truncation mutated original:\nexpected: %s\nactual: %s",
				i, goldenOriginal, original)
		}
	}
}
