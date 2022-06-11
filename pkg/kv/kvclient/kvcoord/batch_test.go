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
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestBatchPrevNext tests BatchTruncationHelper.next() and
// BatchTruncationHelper.prev().
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
		{
			spans: span("a"), key: "a",
			// Done with `key < a`, so `a <= key` is next.
			expFW: "a",
			// Done with `key >= a`, and there's nothing in `[min, a]`, so we return
			// min here.
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
			// We've seen `key >=b`, so hop over the gap to `a`, similar to the first
			// test case. [`a`] is represented as [`a`,`a.Next()`), so we expect prev
			// to return a.Next() here..
			expBW: "a\x00",
		},
		{spans: abc, key: "b\x00",
			// No surprises.
			expFW: "c",
			// Similar to the explanation above, we expect [`b`] = [`b`, `b.Next()`)
			// here.
			expBW: "b\x00",
		},
		{spans: abc, key: "bb",
			// Ditto.
			expFW: "c",
			// Similar to the explanation above, we expect [`b`] = [`b`, `b.Next()`)
			// here.
			expBW: "b\x00",
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
		{spans: span(loc("b"), loc("c")), key: "a",
			expFW: "b",
			expBW: min,
		},
		{spans: span(loc("b"), loc("c")), key: "b",
			expFW: "b",
			expBW: min,
		},
		// Note that this test case results in an invalid ScanRequest where
		// Key == EndKey, but that's ok.
		{spans: span(loc("b"), loc("b")), key: "b\x00",
			expFW: max,
			// Handled `key >= b\x00`, so next we'll have to chip away at `[KeyMin, b\x00)`. Note
			// how this doesn't return `b` which would be incorrect as `[KeyMin, b)` does not
			// contain `loc(b)`.
			expBW: "b\x00",
		},
		// Note that this test case results in invalid ScanRequests where
		// Key == EndKey, but that's ok.
		{
			spans: span(loc("a"), loc("a"), loc("b"), loc("b")), key: "b",
			// We've dealt with any key that addresses to `< b`, and `loc(b)` is not
			// covered by it. `loc(b)` lives between `b` and `b\x00`, so we start at
			// `b`.
			expFW: "b",
			// We've dealt with any key that addresses to `>= b`, which includes
			// `loc(b)`. The next thing cover is `loc(a)`, which lives between `a`
			// and `a\x00`, so we return `a\x00` here.
			expBW: "a\x00",
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
			var ascHelper, descHelper BatchTruncationHelper
			require.NoError(t, ascHelper.Init(Ascending, ba.Requests))
			require.NoError(t, descHelper.Init(Descending, ba.Requests))
			if _, _, next, err := ascHelper.Truncate(
				roachpb.RSpan{
					Key:    roachpb.RKeyMin,
					EndKey: roachpb.RKey(test.key)},
			); err != nil {
				t.Error(err)
			} else if !bytes.Equal(next, roachpb.Key(test.expFW)) {
				t.Errorf("next: expected %q, got %q", test.expFW, next)
			}
			if _, _, prev, err := descHelper.Truncate(
				roachpb.RSpan{
					Key:    roachpb.RKey(test.key),
					EndKey: roachpb.RKeyMax},
			); err != nil {
				t.Error(err)
			} else if !bytes.Equal(prev, roachpb.Key(test.expBW)) {
				t.Errorf("prev: expected %q, got %q", test.expBW, prev)
			}
		})
	}
}

// TestTruncate verifies the truncation logic over a single range.
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

		var truncationHelper BatchTruncationHelper
		require.NoError(t, truncationHelper.Init(Ascending, original.Requests))
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
		reqs, pos, _, err := truncationHelper.Truncate(rs)
		if err != nil || test.err != "" {
			if !testutils.IsError(err, test.err) {
				t.Errorf("%d: %v (expected: %q)", i, err, test.err)
			}
			continue
		}
		var numReqs int
		for j, arg := range reqs {
			req := arg.GetInner()
			if h := req.Header(); !bytes.Equal(h.Key, roachpb.Key(test.expKeys[j][0])) || !bytes.Equal(h.EndKey, roachpb.Key(test.expKeys[j][1])) {
				t.Errorf("%d.%d: range mismatch: actual [%q,%q), wanted [%q,%q)", i, j,
					h.Key, h.EndKey, test.expKeys[j][0], test.expKeys[j][1])
			} else if len(h.Key) != 0 {
				numReqs++
			}
		}
		if num := len(pos); numReqs != num {
			t.Errorf("%d: counted %d requests, but truncation indicated %d", i, reqs, num)
		}
		if !reflect.DeepEqual(original, goldenOriginal) {
			t.Errorf("%d: truncation mutated original:\nexpected: %s\nactual: %s",
				i, goldenOriginal, original)
		}
	}
}

type requestsWithPositions struct {
	reqs      []roachpb.RequestUnion
	positions []int
}

var _ sort.Interface = &requestsWithPositions{}

func (r *requestsWithPositions) Len() int {
	return len(r.positions)
}

func (r *requestsWithPositions) Less(i, j int) bool {
	return r.positions[i] < r.positions[j]
}

func (r *requestsWithPositions) Swap(i, j int) {
	r.reqs[i], r.reqs[j] = r.reqs[j], r.reqs[i]
	r.positions[i], r.positions[j] = r.positions[j], r.positions[i]
}

// TestTruncateLoop verifies that using the BatchTruncationHelper in a loop over
// multiple ranges works as expected.
func TestTruncateLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	makeRangeDesc := func(boundaries [2]string) roachpb.RangeDescriptor {
		return roachpb.RangeDescriptor{
			StartKey: roachpb.RKey(boundaries[0]), EndKey: roachpb.RKey(boundaries[1]),
		}
	}
	makeGetRequest := func(key string) roachpb.RequestUnion {
		var req roachpb.RequestUnion
		req.MustSetInner(&roachpb.GetRequest{
			RequestHeader: roachpb.RequestHeader{Key: roachpb.Key(key)},
		})
		return req
	}
	makeScanRequest := func(start, end string) roachpb.RequestUnion {
		var req roachpb.RequestUnion
		req.MustSetInner(&roachpb.ScanRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: roachpb.Key(start), EndKey: roachpb.Key(end),
			},
		})
		return req
	}

	type iterationCase struct {
		rangeBoundaries [2]string
		// When both strings are non-empty, then it represents a ScanRequest, if
		// the second string is empty, then it is a GetRequest.
		requests    [][2]string
		positions   []int
		ascSeekKey  roachpb.RKey
		descSeekKey roachpb.RKey
	}
	getRequests := func(reqs [][2]string) []roachpb.RequestUnion {
		var requests []roachpb.RequestUnion
		for _, req := range reqs {
			if req[1] == "" {
				requests = append(requests, makeGetRequest(req[0]))
			} else {
				requests = append(requests, makeScanRequest(req[0], req[1]))
			}
		}
		return requests
	}
	assertExpected := func(tc iterationCase, reqs []roachpb.RequestUnion, positions []int) {
		// Since requests can be truncated in an arbitrary order, we will sort
		// them according to their positions.
		scratch := requestsWithPositions{reqs: reqs, positions: positions}
		sort.Sort(&scratch)
		require.True(t, reflect.DeepEqual(getRequests(tc.requests), reqs))
		require.True(t, reflect.DeepEqual(tc.positions, positions))
	}

	for _, testCase := range []struct {
		// When both strings are non-empty, then it represents a ScanRequest, if
		// the second string is empty, then it is a GetRequest.
		requests  [][2]string
		iteration []iterationCase
	}{
		// A test case with three ranges and requests touching each of those
		// ranges.
		{
			requests: [][2]string{
				{"i", "l"}, {"d"}, {"h", "k"}, {"g", "i"}, {"i"}, {"d", "f"}, {"b", "j"},
			},
			iteration: []iterationCase{
				{
					rangeBoundaries: [2]string{"a", "e"},
					requests:        [][2]string{{"d"}, {"d", "e"}, {"b", "e"}},
					positions:       []int{1, 5, 6},
					ascSeekKey:      roachpb.RKey("e"),
					descSeekKey:     roachpb.RKeyMin,
				},
				{
					rangeBoundaries: [2]string{"e", "i"},
					requests:        [][2]string{{"h", "i"}, {"g", "i"}, {"e", "f"}, {"e", "i"}},
					positions:       []int{2, 3, 5, 6},
					ascSeekKey:      roachpb.RKey("i"),
					descSeekKey:     roachpb.RKey("e"),
				},
				{
					rangeBoundaries: [2]string{"i", "m"},
					requests:        [][2]string{{"i", "l"}, {"i", "k"}, {"i"}, {"i", "j"}},
					positions:       []int{0, 2, 4, 6},
					ascSeekKey:      roachpb.RKeyMax,
					descSeekKey:     roachpb.RKey("i"),
				},
			},
		},
		// A test case where each request fits within a single range, and there
		// is a "gap" range that doesn't touch any of the requests.
		{
			requests: [][2]string{
				{"k", "l"}, {"d"}, {"j", "k"}, {"c", "d"}, {"k"}, {"a", "b"},
			},
			iteration: []iterationCase{
				{
					rangeBoundaries: [2]string{"a", "e"},
					requests:        [][2]string{{"d"}, {"c", "d"}, {"a", "b"}},
					positions:       []int{1, 3, 5},
					ascSeekKey:      roachpb.RKey("j"),
					descSeekKey:     roachpb.RKeyMin,
				},
				{
					rangeBoundaries: [2]string{"e", "i"},
					requests:        nil,
					positions:       nil,
					ascSeekKey:      roachpb.RKey("j"),
					descSeekKey:     roachpb.RKey("d").Next(),
				},
				{
					rangeBoundaries: [2]string{"i", "m"},
					requests:        [][2]string{{"k", "l"}, {"j", "k"}, {"k"}},
					positions:       []int{0, 2, 4},
					ascSeekKey:      roachpb.RKeyMax,
					descSeekKey:     roachpb.RKey("d").Next(),
				},
			},
		},
	} {
		requests := getRequests(testCase.requests)
		rs, err := keys.Range(requests)
		require.NoError(t, err)

		for _, scanDir := range []ScanDirection{Ascending, Descending} {
			var helper BatchTruncationHelper
			require.NoError(t, helper.Init(scanDir, requests))
			for i := 0; i < len(testCase.iteration); i++ {
				tc := testCase.iteration[i]
				if scanDir == Descending {
					// Test cases are written assuming the Ascending scan
					// direction, so we have to iterate in reverse here.
					tc = testCase.iteration[len(testCase.iteration)-1-i]
				}
				desc := makeRangeDesc(tc.rangeBoundaries)
				curRangeRS, err := rs.Intersect(&desc)
				require.NoError(t, err)
				reqs, positions, seekKey, err := helper.Truncate(curRangeRS)
				require.NoError(t, err)
				assertExpected(tc, reqs, positions)
				expectedSeekKey := tc.ascSeekKey
				if scanDir == Descending {
					expectedSeekKey = tc.descSeekKey
				}
				require.Equal(t, expectedSeekKey, seekKey)
			}
		}
	}
}

func BenchmarkTruncate(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	rng, _ := randutil.NewTestRand()
	randomKey := func() []byte {
		const keyLength = 8
		res := make([]byte, keyLength)
		for i := range res {
			// Plus two is needed to skip local key space.
			res[i] = byte(2 + rng.Intn(253))
		}
		return res
	}
	for _, numRequests := range []int{128, 16384} {
		for _, numRanges := range []int{4, 64} {
			splitPoints := make([][]byte, numRanges-1)
			for i := range splitPoints {
				splitPoints[i] = randomKey()
			}
			for i := range splitPoints {
				for j := i + 1; j < len(splitPoints); j++ {
					if bytes.Compare(splitPoints[i], splitPoints[j]) > 0 {
						splitPoints[i], splitPoints[j] = splitPoints[j], splitPoints[i]
					}
				}
			}
			rangeSpans := make([]roachpb.RSpan, numRanges)
			rangeSpans[0].Key = roachpb.RKeyMin
			rangeSpans[numRanges-1].EndKey = roachpb.RKeyMax
			for i := range splitPoints {
				rangeSpans[i].EndKey = splitPoints[i]
				rangeSpans[i+1].Key = splitPoints[i]
			}
			for _, requestType := range []string{"get", "scan"} {
				b.Run(fmt.Sprintf(
					"reqs=%d/ranges=%d/type=%s", numRequests, numRanges, requestType,
				), func(b *testing.B) {
					reqs := make([]roachpb.RequestUnion, numRequests)
					switch requestType {
					case "get":
						for i := 0; i < numRequests; i++ {
							var get roachpb.GetRequest
							get.Key = randomKey()
							reqs[i].MustSetInner(&get)
						}
					case "scan":
						for i := 0; i < numRequests; i++ {
							var scan roachpb.ScanRequest
							startKey := randomKey()
							endKey := randomKey()
							for bytes.Equal(startKey, endKey) {
								endKey = randomKey()
							}
							if bytes.Compare(startKey, endKey) > 0 {
								startKey, endKey = endKey, startKey
							}
							scan.Key = startKey
							scan.EndKey = endKey
							reqs[i].MustSetInner(&scan)
						}
					}
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var h BatchTruncationHelper
						require.NoError(b, h.Init(Ascending, reqs))
						for _, rs := range rangeSpans {
							_, _, _, err := h.Truncate(rs)
							require.NoError(b, err)
						}
					}
				})
			}
		}
	}
}
