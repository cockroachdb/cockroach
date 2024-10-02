// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestBatchPrevNext tests the seeking behavior of the
// BatchTruncationHelper.Truncate.
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
		// Note that such a span results in an invalid ScanRequest, but it's ok
		// for the purposes of this test.
		{spans: span(loc("b"), loc("b")), key: "b\x00",
			expFW: max,
			// Handled `key >= b\x00`, so next we'll have to chip away at `[KeyMin, b\x00)`. Note
			// how this doesn't return `b` which would be incorrect as `[KeyMin, b)` does not
			// contain `loc(b)`.
			expBW: "b\x00",
		},
		// Note that such a span results in an invalid ScanRequest, but it's ok
		// for the purposes of this test.
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
			var ba kvpb.BatchRequest
			for _, span := range test.spans {
				args := &kvpb.ScanRequest{}
				args.Key, args.EndKey = span.Key, span.EndKey
				ba.Add(args)
			}
			const mustPreserveOrder = false
			const canReorderRequestsSlice = false
			ascHelper, err := NewBatchTruncationHelper(Ascending, ba.Requests, mustPreserveOrder, canReorderRequestsSlice)
			require.NoError(t, err)
			descHelper, err := NewBatchTruncationHelper(Descending, ba.Requests, mustPreserveOrder, canReorderRequestsSlice)
			require.NoError(t, err)
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

type requestsWithPositions struct {
	reqs      []kvpb.RequestUnion
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

// TestTruncate verifies the truncation logic of the BatchTruncationHelper over
// a single range as well as truncateLegacy() function directly.
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
	for _, isLegacy := range []bool{false, true} {
		for _, mustPreserveOrder := range []bool{false, true} {
			if isLegacy && mustPreserveOrder {
				// This config is meaningless because truncateLegacy() always
				// preserves the ordering.
				continue
			}
			for _, canReorderRequestsSlice := range []bool{false, true} {
				if isLegacy && canReorderRequestsSlice {
					// This config is meaningless because truncateLegacy()
					// doesn't reorder the original requests slice.
					continue
				}

				for i, test := range testCases {
					goldenOriginal := &kvpb.BatchRequest{}
					for _, ks := range test.keys {
						if len(ks[1]) > 0 {
							goldenOriginal.Add(&kvpb.ResolveIntentRangeRequest{
								RequestHeader: kvpb.RequestHeader{
									Key: roachpb.Key(ks[0]), EndKey: roachpb.Key(ks[1]),
								},
								IntentTxn: enginepb.TxnMeta{ID: uuid.MakeV4()},
							})
						} else {
							goldenOriginal.Add(&kvpb.GetRequest{
								RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(ks[0])},
							})
						}
					}

					original := &kvpb.BatchRequest{Requests: make([]kvpb.RequestUnion, len(goldenOriginal.Requests))}
					for i, request := range goldenOriginal.Requests {
						original.Requests[i].MustSetInner(request.GetInner().ShallowCopy())
					}

					var truncationHelper *BatchTruncationHelper
					if !isLegacy {
						var err error
						truncationHelper, err = NewBatchTruncationHelper(
							Ascending, original.Requests, mustPreserveOrder, canReorderRequestsSlice,
						)
						if err != nil {
							t.Errorf("%d: Init failure: %v", i, err)
							continue
						}
						// We need to truncate all requests up to the start of
						// the test range since this is assumed by Truncate().
						truncateKey := roachpb.RKey(test.from)
						if truncateKey.Less(roachpb.RKey(test.desc[0])) {
							truncateKey = roachpb.RKey(test.desc[0])
						}
						_, _, _, err = truncationHelper.Truncate(
							roachpb.RSpan{Key: roachpb.RKeyMin, EndKey: truncateKey},
						)
						if err != nil || test.err != "" {
							if !testutils.IsError(err, test.err) {
								t.Errorf("%d: %v (expected: %q)", i, err, test.err)
							}
							continue
						}
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
					rs, err := rs.Intersect(desc.RSpan())
					if err != nil {
						t.Errorf("%d: intersection failure: %v", i, err)
						continue
					}
					reqs, pos, err := truncateLegacy(original.Requests, rs)
					if isLegacy {
						if err != nil || test.err != "" {
							if !testutils.IsError(err, test.err) {
								t.Errorf("%d: %v (expected: %q)", i, err, test.err)
							}
							continue
						}
					} else {
						reqs, pos, _, err = truncationHelper.Truncate(rs)
					}
					if err != nil {
						t.Errorf("%d: truncation failure: %v", i, err)
						continue
					}
					if !isLegacy && !mustPreserveOrder {
						// Truncate can return results in an arbitrary order, so
						// we need to restore the order according to positions.
						scratch := &requestsWithPositions{reqs: reqs, positions: pos}
						sort.Sort(scratch)
					}
					var numReqs int
					for j, arg := range reqs {
						req := arg.GetInner()
						if h := req.Header(); !bytes.Equal(h.Key, roachpb.Key(test.expKeys[j][0])) || !bytes.Equal(h.EndKey, roachpb.Key(test.expKeys[j][1])) {
							t.Errorf("%d.%d: range mismatch: actual [%q,%q), wanted [%q,%q)", i, j,
								h.Key, h.EndKey, roachpb.RKey(test.expKeys[j][0]), roachpb.RKey(test.expKeys[j][1]))
						} else if len(h.Key) != 0 {
							numReqs++
						}
					}
					if num := len(pos); numReqs != num {
						t.Errorf("%d: counted %d requests, but truncation indicated %d", i, numReqs, num)
					}
					if isLegacy || !canReorderRequestsSlice {
						if !reflect.DeepEqual(original, goldenOriginal) {
							t.Errorf("%d: truncation mutated original:\nexpected: %s\nactual: %s",
								i, goldenOriginal, original)
						}
					} else {
						// Modifying the order of requests in a BatchRequest is
						// ok, but we want to make sure that each request hasn't
						// been modified "deeply", so we try different
						// permutations of the original requests.
						matched := make([]bool, len(original.Requests))
						var matchedCount int
						for _, goldenReq := range goldenOriginal.Requests {
							for j, origReq := range original.Requests {
								if matched[j] {
									continue
								}
								if reflect.DeepEqual(goldenReq, origReq) {
									matched[j] = true
									matchedCount++
									break
								}
							}
						}
						if matchedCount != len(matched) {
							t.Errorf("%d: truncation mutated original:\nexpected: %s\nactual: %s",
								i, goldenOriginal, original)
						}
					}
				}
			}
		}
	}
}

// TestTruncateLoop verifies that using the BatchTruncationHelper in a loop over
// multiple ranges works as expected.
func TestTruncateLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	makeGetRequest := func(key string) kvpb.RequestUnion {
		var req kvpb.RequestUnion
		req.MustSetInner(&kvpb.GetRequest{
			RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(key)},
		})
		return req
	}
	makeScanRequest := func(start, end string) kvpb.RequestUnion {
		var req kvpb.RequestUnion
		req.MustSetInner(&kvpb.ScanRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: roachpb.Key(start), EndKey: roachpb.Key(end),
			},
		})
		return req
	}
	rng, _ := randutil.NewTestRand()
	const maxKeyLength = 8
	randomKey := func() string {
		keyLength := rng.Intn(maxKeyLength) + 1
		return string(randomKey(rng, keyLength))
	}

	makeRandomTestCase := func() ([]kvpb.RequestUnion, []roachpb.RSpan) {
		var requests []kvpb.RequestUnion
		numRequests := rng.Intn(20) + 1
		for i := 0; i < numRequests; i++ {
			if rng.Float64() < 0.5 {
				// With 50% chance, use a Get request; otherwise, a Scan
				// request.
				requests = append(requests, makeGetRequest(randomKey()))
			} else {
				req := [2]string{randomKey(), randomKey()}
				for strings.Compare(req[0], req[1]) >= 0 {
					req[1] = randomKey()
				}
				requests = append(requests, makeScanRequest(req[0], req[1]))
			}
		}

		numRanges := rng.Intn(5) + 1
		return requests, makeRanges(rng, numRanges, maxKeyLength)
	}

	for numRuns := 0; numRuns < 100; numRuns++ {
		requests, ranges := makeRandomTestCase()
		for _, scanDir := range []ScanDirection{Ascending, Descending} {
			for _, mustPreserveOrder := range []bool{false, true} {
				t.Run(fmt.Sprintf("run=%d/%s/order=%t", numRuns, scanDir, mustPreserveOrder), func(t *testing.T) {
					const canReorderRequestsSlice = false
					helper, err := NewBatchTruncationHelper(
						scanDir, requests, mustPreserveOrder, canReorderRequestsSlice,
					)
					require.NoError(t, err)
					for i := 0; i < len(ranges); i++ {
						curRangeRS := ranges[i]
						if scanDir == Descending {
							// Test cases are written assuming the Ascending
							// scan direction, so we have to iterate in reverse
							// here.
							curRangeRS = ranges[len(ranges)-1-i]
						}
						actualReqs, actualPositions, actualSeekKey, err := helper.Truncate(curRangeRS)
						require.NoError(t, err)
						expectedReqs, expectedPositions, err := truncateLegacy(requests, curRangeRS)
						require.NoError(t, err)
						if !mustPreserveOrder {
							// Since requests can be truncated in an arbitrary
							// order, we will sort them according to their
							// positions.
							scratch := requestsWithPositions{reqs: actualReqs, positions: actualPositions}
							sort.Sort(&scratch)
						}
						require.True(t, reflect.DeepEqual(expectedReqs, actualReqs))
						require.True(t, reflect.DeepEqual(expectedPositions, actualPositions))
						var expectedSeekKey roachpb.RKey
						if scanDir == Ascending {
							expectedSeekKey, err = nextLegacy(requests, curRangeRS.EndKey)
						} else {
							expectedSeekKey, err = prevLegacy(requests, curRangeRS.Key)
						}
						require.NoError(t, err)
						require.Equal(t, expectedSeekKey, actualSeekKey)
					}
				})
			}
		}
	}
}

func randomKeyByte(rng *rand.Rand) byte {
	// Plus two is needed to skip local key space.
	return byte(2 + rng.Intn(253))
}

func randomKey(rng *rand.Rand, keyLength int) []byte {
	res := make([]byte, keyLength)
	for i := range res {
		res[i] = randomKeyByte(rng)
	}
	return res
}

// makeRanges returns a set of spans that describes the specified number of
// ranges spanning the whole key space. The returned spans are ordered by the
// start key and are non-overlapping.
func makeRanges(rng *rand.Rand, numRanges int, keyLength int) []roachpb.RSpan {
	splitPoints := make([][]byte, numRanges-1)
	for i := range splitPoints {
		splitPoints[i] = randomKey(rng, keyLength)
	}
	// Sort all the split points so that we can treat them as boundaries of
	// consecutive ranges.
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
	return rangeSpans
}

func BenchmarkTruncateLoop(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	rng, _ := randutil.NewTestRand()
	const keyLength = 8
	for _, scanDir := range []ScanDirection{Ascending, Descending} {
		for _, mustPreserveOrder := range []bool{false, true} {
			for _, numRequests := range []int{128, 16384} {
				for _, numRanges := range []int{4, 64} {
					// We'll split the whole key space into numRanges ranges
					// using random numRanges-1 split points.
					rangeSpans := makeRanges(rng, numRanges, keyLength)
					if scanDir == Descending {
						// Reverse all the range spans for the Descending scan
						// direction.
						for i, j := 0, numRanges-1; i < j; i, j = i+1, j-1 {
							rangeSpans[i], rangeSpans[j] = rangeSpans[j], rangeSpans[i]
						}
					}
					var orderStr string
					if mustPreserveOrder {
						orderStr = "/preserveOrder"
					}
					for _, requestType := range []string{"get", "scan"} {
						b.Run(fmt.Sprintf(
							"%s%s/reqs=%d/ranges=%d/type=%s",
							scanDir, orderStr, numRequests, numRanges, requestType,
						), func(b *testing.B) {
							reqs := make([]kvpb.RequestUnion, numRequests)
							switch requestType {
							case "get":
								for i := 0; i < numRequests; i++ {
									var get kvpb.GetRequest
									get.Key = randomKey(rng, keyLength)
									reqs[i].MustSetInner(&get)
								}
							case "scan":
								for i := 0; i < numRequests; i++ {
									var scan kvpb.ScanRequest
									startKey := randomKey(rng, keyLength)
									endKey := randomKey(rng, keyLength)
									for bytes.Equal(startKey, endKey) {
										endKey = randomKey(rng, keyLength)
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
								const canReorderRequestsSlice = false
								h, err := NewBatchTruncationHelper(
									scanDir, reqs, mustPreserveOrder, canReorderRequestsSlice,
								)
								require.NoError(b, err)
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
	}
}

func BenchmarkTruncateLegacy(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	rng, _ := randutil.NewTestRand()
	// For simplicity, we'll work with single-byte keys, so we divide up the
	// single-byte key space into three parts, and we'll be truncating the
	// requests according to the middle part.
	rs := roachpb.RSpan{Key: roachpb.RKey([]byte{85}), EndKey: roachpb.RKey([]byte{170})}
	for _, numRequests := range []int{1 << 5, 1 << 10, 1 << 15} {
		for _, requestType := range []string{"get", "scan"} {
			b.Run(fmt.Sprintf("reqs=%d/type=%s", numRequests, requestType), func(b *testing.B) {
				reqs := make([]kvpb.RequestUnion, numRequests)
				switch requestType {
				case "get":
					for i := 0; i < numRequests; i++ {
						var get kvpb.GetRequest
						get.Key = []byte{randomKeyByte(rng)}
						reqs[i].MustSetInner(&get)
					}
				case "scan":
					for i := 0; i < numRequests; i++ {
						var scan kvpb.ScanRequest
						startKey := randomKeyByte(rng)
						endKey := randomKeyByte(rng)
						if endKey < startKey {
							startKey, endKey = endKey, startKey
						}
						scan.Key = []byte{startKey}
						scan.EndKey = []byte{endKey}
						reqs[i].MustSetInner(&scan)
					}
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _, err := truncateLegacy(reqs, rs)
					require.NoError(b, err)
				}
			})
		}
	}
}
