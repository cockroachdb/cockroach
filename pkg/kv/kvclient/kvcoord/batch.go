// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"sort"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// BatchTruncationHelper is a utility struct that helps with truncating requests
// to range boundaries as well as figuring out the next key to seek to for the
// range iterator.
//
// The caller should not use the helper if all requests fit within a single
// range since the helper has non-trivial setup cost.
//
// It is designed to be used roughly as follows:
//
//	rs := keys.Range(requests)
//	ri.Seek(scanDir, rs.Key)
//	if !ri.NeedAnother(rs) {
//	  // All requests fit within a single range, don't use the helper.
//	  ...
//	}
//	helper := NewBatchTruncationHelper(scanDir, requests)
//	for ri.Valid() {
//	  curRangeRS := rs.Intersect(ri.Token().Desc())
//	  curRangeReqs, positions, seekKey := helper.Truncate(curRangeRS)
//	  // Process curRangeReqs that touch a single range and then use positions
//	  // to reassemble the result.
//	  ...
//	  ri.Seek(scanDir, seekKey)
//	}
//
// The helper utilizes two different strategies depending on whether the
// requests use local keys or not:
//
//   - a "legacy" strategy is used when requests use local keys. This strategy
//     utilizes "legacy" methods that operate on the original requests without
//     keeping any additional bookkeeping. In particular, it leads to truncating
//     already processed requests as well as to iterating over the fully processed
//     requests when searching for the next seek key.
//
//   - an "optimized" strategy is used when requests only use global keys.
//     Although this strategy has the same worst-case complexity of O(N * R) as
//     the "legacy" strategy (where N is the number of requests, R is the number
//     of ranges that all requests fit int, the worst-case is achieved when all
//     requests are range-spanning and each request spans all R ranges), in
//     practice it is much faster. See the comments on truncateAsc() and
//     truncateDesc() for the details.
//
// The gist of the optimized strategy is sorting all of the requests according
// to the keys upfront and then, on each truncation iteration, examining only a
// subset of requests that might overlap with the current range boundaries. The
// strategy is careful to update the internal state so that the ordering of
// unprocessed requests is maintained.
//
// The key insight is best shown by an example. Imagine that we have two
// requests Scan(a, c) and Scan(b, d) (which are ordered according to start keys
// with the Ascending scan direction), and on the first iteration we're
// truncating to range [a, b). Only the first request overlaps with the range,
// so we include Scan(a, b) into the truncation response, and, crucially, we can
// update the header of the first request to be [b, c) to track the remaining
// part of the first request. We can update the header in-place without breaking
// the ordering property.
type BatchTruncationHelper struct {
	scanDir ScanDirection
	// requests are the original requests this helper needs to process (possibly
	// in non-original order).
	requests []kvpb.RequestUnion
	// ownRequestsSlice indicates whether a separate slice was allocated for
	// requests. It is used for the purposes of the memory accounting.
	//
	// It is the same as !canReorderRequestsSlice in most cases, except for when
	// the local keys are present. In such a scenario, even if
	// canReorderRequestsSlice is false, ownRequestsSlice might remain false.
	ownRequestsSlice bool
	// mustPreserveOrder indicates whether the requests must be returned by
	// Truncate() in the original order.
	mustPreserveOrder bool
	// canReorderRequestsSlice indicates whether the helper will hold on to the
	// given slice of requests and might reorder the requests within it
	// (although each request will not be modified "deeply" - i.e. its header
	// won't be updated or anything like that).
	canReorderRequestsSlice bool
	// foundLocalKey, if true, indicates whether some of the requests reference
	// the local keys. When true, the helper falls back to the legacy methods.
	foundLocalKey bool

	// Fields below are only used if the optimized strategy is used.

	// headers contains the parts of the corresponding requests that have not
	// been processed yet. For range-spanning requests:
	// - with the Ascending direction, we're advancing the start key of the
	// header once a prefix of the request is returned by Truncate();
	// - with the Descending direction, we're moving the end key of the header
	// backwards once a suffix of the request is returned by Truncate(). We also
	// ensure that all requests have an end key (meaning that for point requests
	// as well as range-spanning requests we populate the end key as
	// startKey.Next()).
	//
	// All keys in the headers are global.
	headers []kvpb.RequestHeader
	// positions stores the corresponding indices of requests in the original
	// requests slice. Once request is fully processed, it's position value
	// becomes negative.
	positions []int
	// isRange indicates whether the corresponding request is a range-spanning
	// one.
	isRange []bool
	// startIdx tracks the "smallest" request (according to the order of the
	// original start keys) that might not have been fully processed. In other
	// words, all requests in range [0, startIdx) have negative positions
	// values.
	startIdx int
	// helper is only initialized and used if mustPreserveOrder is true.
	helper orderRestorationHelper
}

// Len implements the sort.Interface interface.
func (h *BatchTruncationHelper) Len() int {
	return len(h.requests)
}

// Swap implements the sort.Interface interface.
func (h *BatchTruncationHelper) Swap(i, j int) {
	h.requests[i], h.requests[j] = h.requests[j], h.requests[i]
	h.headers[i], h.headers[j] = h.headers[j], h.headers[i]
	h.positions[i], h.positions[j] = h.positions[j], h.positions[i]
	h.isRange[i], h.isRange[j] = h.isRange[j], h.isRange[i]
}

// ascBatchTruncationHelper is used for the Ascending scan direction in order to
// sort the requests in the ascending order of the start keys.
type ascBatchTruncationHelper struct {
	*BatchTruncationHelper
}

var _ sort.Interface = ascBatchTruncationHelper{}

func (h ascBatchTruncationHelper) Less(i, j int) bool {
	return h.headers[i].Key.Compare(h.headers[j].Key) < 0
}

// descBatchTruncationHelper is used for the Descending scan direction in order
// to sort the requests in the descending order of the end keys.
type descBatchTruncationHelper struct {
	*BatchTruncationHelper
}

var _ sort.Interface = descBatchTruncationHelper{}

func (h descBatchTruncationHelper) Less(i, j int) bool {
	return h.headers[i].EndKey.Compare(h.headers[j].EndKey) > 0
}

// NewBatchTruncationHelper returns a new BatchTruncationHelper for the given
// requests. The helper can be reused later for a different set of requests via
// a separate Init() call.
//
// mustPreserveOrder, if true, indicates that the caller requires that requests
// are returned by Truncate() in the original order (i.e. with strictly
// increasing positions values).
//
// If canReorderRequestsSlice is true, then the helper will hold on to the given
// slice and might reorder the requests within it (although each request will
// not be modified "deeply" - i.e. its header won't be updated or anything like
// that). Set it to false when the caller cares about the slice not being
// mutated in any way.
func NewBatchTruncationHelper(
	scanDir ScanDirection,
	requests []kvpb.RequestUnion,
	mustPreserveOrder bool,
	canReorderRequestsSlice bool,
) (*BatchTruncationHelper, error) {
	ret := &BatchTruncationHelper{
		scanDir:                 scanDir,
		mustPreserveOrder:       mustPreserveOrder,
		canReorderRequestsSlice: canReorderRequestsSlice,
	}
	return ret, ret.Init(requests)
}

// Init sets up the helper for the provided requests. It can be called multiple
// times, and it will reuse as much internal allocations as possible.
func (h *BatchTruncationHelper) Init(requests []kvpb.RequestUnion) error {
	// Determine whether we can use the optimized strategy before making any
	// allocations.
	h.foundLocalKey = false
	for i := range requests {
		header := requests[i].GetInner().Header()
		if keys.IsLocal(header.Key) {
			h.requests = requests
			h.foundLocalKey = true
			return nil
		}
	}
	// We can use the optimized strategy, so set up all of the internal state.
	h.startIdx = 0
	if h.canReorderRequestsSlice {
		h.requests = requests
	} else {
		// If we can't reorder the original requests slice, we must make a copy.
		if cap(h.requests) < len(requests) {
			h.requests = make([]kvpb.RequestUnion, len(requests))
			h.ownRequestsSlice = true
		} else {
			if len(requests) < len(h.requests) {
				// Ensure that we lose references to the old requests that will
				// not be overwritten by copy.
				//
				// Note that we only need to go up to the number of old requests
				// and not the capacity of the slice since we assume that
				// everything past the length is already nil-ed out.
				oldRequests := h.requests[len(requests):len(h.requests)]
				for i := range oldRequests {
					oldRequests[i] = kvpb.RequestUnion{}
				}
			}
			h.requests = h.requests[:len(requests)]
		}
		copy(h.requests, requests)
	}
	if cap(h.headers) < len(requests) {
		h.headers = make([]kvpb.RequestHeader, len(requests))
	} else {
		if len(requests) < len(h.headers) {
			// Ensure that we lose references to the old header that will
			// not be overwritten in the loop below.
			//
			// Note that we only need to go up to the number of old headers and
			// not the capacity of the slice since we assume that everything
			// past the length is already nil-ed out.
			oldHeaders := h.headers[len(requests):len(h.headers)]
			for i := range oldHeaders {
				oldHeaders[i] = kvpb.RequestHeader{}
			}
		}
		h.headers = h.headers[:len(requests)]
	}
	if cap(h.positions) < len(requests) {
		h.positions = make([]int, len(requests))
	} else {
		h.positions = h.positions[:len(requests)]
	}
	if cap(h.isRange) < len(requests) {
		h.isRange = make([]bool, len(requests))
	} else {
		h.isRange = h.isRange[:len(requests)]
	}
	// Populate the internal state as well as perform some sanity checks on the
	// requests.
	for i := range requests {
		req := requests[i].GetInner()
		h.headers[i] = req.Header()
		h.positions[i] = i
		h.isRange[i] = kvpb.IsRange(req)
		if h.isRange[i] {
			// We're dealing with a range-spanning request.
			if l, r := keys.IsLocal(h.headers[i].Key), keys.IsLocal(h.headers[i].EndKey); (l && !r) || (!l && r) {
				return errors.AssertionFailedf("local key mixed with global key in range")
			}
		} else if len(h.headers[i].EndKey) > 0 {
			return errors.AssertionFailedf("%T is not a range command, but EndKey is set", req)
		}
	}
	if h.scanDir == Ascending {
		sort.Sort(ascBatchTruncationHelper{BatchTruncationHelper: h})
	} else {
		// With the Descending scan direction, we have to convert all point
		// requests into range-spanning requests that include only a single
		// point.
		for i := range h.headers {
			if len(h.headers[i].EndKey) == 0 {
				h.headers[i].EndKey = h.headers[i].Key.Next()
			}
		}
		sort.Sort(descBatchTruncationHelper{BatchTruncationHelper: h})
	}
	if h.mustPreserveOrder {
		h.helper.init(len(requests))
	}
	return nil
}

const (
	requestUnionOverhead  = int64(unsafe.Sizeof(kvpb.RequestUnion{}))
	requestHeaderOverhead = int64(unsafe.Sizeof(kvpb.RequestHeader{}))
	intOverhead           = int64(unsafe.Sizeof(int(0)))
	boolOverhead          = int64(unsafe.Sizeof(false))
)

// MemUsage returns the memory usage of the internal state of the helper.
func (h *BatchTruncationHelper) MemUsage() int64 {
	var memUsage int64
	if h.ownRequestsSlice {
		// Only account for the requests slice if we own it.
		memUsage += int64(cap(h.requests)) * requestUnionOverhead
	}
	memUsage += int64(cap(h.headers)) * requestHeaderOverhead
	memUsage += int64(cap(h.positions)) * intOverhead
	memUsage += int64(cap(h.isRange)) * boolOverhead
	memUsage += h.helper.memUsage()
	return memUsage
}

// Truncate restricts all requests to the given key range and returns new,
// truncated, requests. All returned requests are "truncated" to the given span,
// and requests which are found to not overlap the given span at all are
// removed. A mapping of response index to request index is returned. It also
// returns the next seek key for the range iterator. With Ascending scan
// direction, the next seek key is such that requests in [RKeyMin, seekKey)
// range have been processed, with Descending scan direction, it is such that
// requests in [seekKey, RKeyMax) range have been processed.
//
// For example, if
//
//	reqs = Put[a], Put[c], Put[b],
//	rs = [a,bb],
//	BatchTruncationHelper.Init(Ascending, reqs)
//
// then BatchTruncationHelper.Truncate(rs) returns (Put[a], Put[b]), positions
// [0,2] as well as seekKey 'c'.
//
// Truncate returns the requests in an arbitrary order (meaning that positions
// return values might not be ascending), unless mustPreserveOrder was true in
// Init().
//
// NOTE: it is assumed that
//  1. Truncate has been called on the previous ranges that intersect with
//     keys.Range(reqs);
//  2. rs is intersected with the current range boundaries.
func (h *BatchTruncationHelper) Truncate(
	rs roachpb.RSpan,
) ([]kvpb.RequestUnion, []int, roachpb.RKey, error) {
	var truncReqs []kvpb.RequestUnion
	var positions []int
	var err error
	if !h.foundLocalKey {
		if h.scanDir == Ascending {
			truncReqs, positions, err = h.truncateAsc(rs)
		} else {
			truncReqs, positions, err = h.truncateDesc(rs)
		}
		if err != nil {
			return nil, nil, nil, err
		}
		if h.mustPreserveOrder {
			truncReqs, positions = h.helper.restoreOrder(truncReqs, positions)
		}
	} else {
		truncReqs, positions, err = truncateLegacy(h.requests, rs)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	var seekKey roachpb.RKey
	if h.scanDir == Ascending {
		// In next iteration, query next range.
		// It's important that we use the EndKey of the current descriptor
		// as opposed to the StartKey of the next one: if the former is stale,
		// it's possible that the next range has since merged the subsequent
		// one, and unless both descriptors are stale, the next descriptor's
		// StartKey would move us to the beginning of the current range,
		// resulting in a duplicate scan.
		seekKey, err = h.next(rs.EndKey)
	} else {
		// In next iteration, query previous range.
		// We use the StartKey of the current descriptor as opposed to the
		// EndKey of the previous one since that doesn't have bugs when
		// stale descriptors come into play.
		seekKey, err = h.prev(rs.Key)
	}
	return truncReqs, positions, seekKey, err
}

// truncateAsc is the optimized strategy for Truncate() with the Ascending scan
// direction when requests only use global keys.
//
// The first step of this strategy is to reorder all requests according to their
// start keys (this is done in Init()). Then, on every call to truncateAsc(), we
// only look at a subset of original requests that might overlap with rs and
// ignore already processed requests entirely.
//
// Let's go through an example. Say we have seven original requests:
//
//	requests : Scan(i, l), Get(d), Scan(h, k), Scan(g, i), Get(i), Scan(d, f), Scan(b, h)
//	positions:      0          1        2           3          4        5           6
//
// as well three ranges to iterate over:
//
//	ranges: range[a, e), range[e, i), range[i, m).
//
// In Init(), we have reordered the requests according to their start keys:
//
//	requests : Scan(b, h), Get(d), Scan(d, f), Scan(g, i), Scan(h, k), Get(i), Scan(i, l)
//	positions:      6          1        5           3           2          4        0
//	headers  :     [b, h)     [d)      [d, f)      [g, i)      [h, k)     [i)      [i, l)
//
// On the first call to Truncate(), we're using the range [a, e). We only need
// to look at the first four requests since the fourth request starts after the
// end of the current range, and, due to the ordering, all following requests
// too. We truncate first three requests to the range boundaries, update the
// headers to refer to the unprocessed parts of the corresponding requests, and
// mark the 2nd request Get(d) as fully processed.
//
// The first call prepares
//
//	truncReqs = [Scan(b, e), Get(d), Scan(d, e)], positions = [6, 1, 5]
//
// and the internal state is now
//
//	requests : Scan(b, h), Get(d), Scan(d, f), Scan(g, i), Scan(h, k), Get(i), Scan(i, l)
//	positions:      6         -1        5           3           2          4        0
//	headers  :     [e, h)    <nil>     [e, f)      [g, i)      [h, k)     [i)      [i, l)
//
// Then the optimized next() function determines the seekKey as 'e' and keeps
// the startIdx at 0.
//
// On the second call to Truncate(), we're using the range [e, i). We only need
// to look at the first six requests since the sixth request starts after the
// end of the current range, and, due to the ordering, all following requests
// too. We truncate first five requests to the range boundaries (skipping the
// second that has been fully processed already), update the headers to refer to
// the unprocessed parts of the corresponding requests, and mark the 1st, the
// 3rd, and the 4th requests as fully processed.
//
// The second call prepares
//
//	truncReqs = [Scan(e, h), Scan(e, f), Scan(g, i), Scan(h, i)], positions = [6, 5, 3, 2]
//
// and the internal state is now
//
//	requests : Scan(b, h), Get(d), Scan(d, f), Scan(g, i), Scan(h, k), Get(i), Scan(i, l)
//	positions:     -1         -1       -1          -1           2          4        0
//	headers  :    <nil>      <nil>    <nil>       <nil>        [i, k)     [i)      [i, l)
//
// Then the optimized next() function determines the seekKey as 'i' and sets
// the startIdx at 4 (meaning that all first four requests have been fully
// processed).
//
// On the third call to Truncate(), we're using the range [i, m). We only look
// at the 5th, 6th, and 7th requests (because of the value of startIdx). All
// requests are contained within the range, so we include them into the return
// value and mark all of them as processed.
//
// The third call prepares
//
//	truncReqs = [Scan(i, k), Get(i), Scan(i, l)], positions = [2, 4, 0]
//
// and the internal state is now
//
//	requests : Scan(b, h), Get(d), Scan(d, f), Scan(g, i), Scan(h, k), Get(i), Scan(i, l)
//	positions:     -1         -1       -1          -1          -1         -1       -1
//	headers  :    <nil>      <nil>    <nil>       <nil>       <nil>      <nil>    <nil>
//
// Then the optimized next() function determines the seekKey as KeyMax and sets
// the startIdx at 7 (meaning that all requests have been fully processed), and
// we're done.
//
// NOTE: for all requests, headers always keep track of the unprocessed part of
// the request and is such that the ordering of the keys in the headers is
// preserved when the requests are truncated.
//
// Note that this function is very similar to truncateDesc(), and we could
// extract out the differences into an interface; however, this leads to
// non-trivial slowdown and increase in allocations, so we choose to duplicate
// the code for performance.
func (h *BatchTruncationHelper) truncateAsc(rs roachpb.RSpan) ([]kvpb.RequestUnion, []int, error) {
	var truncReqs []kvpb.RequestUnion
	var positions []int
	for i := h.startIdx; i < len(h.positions); i++ {
		pos := h.positions[i]
		if pos < 0 {
			// This request has already been fully processed, so there is no
			// need to look at it.
			continue
		}
		header := h.headers[i]
		// rs.EndKey can't be local because it contains range split points,
		// which are never local.
		ek := rs.EndKey.AsRawKey()
		if ek.Compare(header.Key) <= 0 {
			// All of the remaining requests start after this range, so we're
			// done.
			break
		}
		if !h.isRange[i] {
			// This is a point request, and the key is contained within this
			// range, so we include the request as is and mark it as "fully
			// processed".
			truncReqs = append(truncReqs, h.requests[i])
			positions = append(positions, pos)
			h.headers[i] = kvpb.RequestHeader{}
			h.positions[i] = -1
			continue
		}
		// We're dealing with a range-spanning request.
		if buildutil.CrdbTestBuild {
			// rs.Key can't be local because it contains range split points,
			// which are never local.
			if header.Key.Compare(rs.Key.AsRawKey()) < 0 {
				return nil, nil, errors.AssertionFailedf(
					"unexpectedly header.Key %s is less than rs %s", header.Key, rs,
				)
			}
		}
		inner := h.requests[i].GetInner()
		if header.EndKey.Compare(ek) <= 0 {
			// This is the last part of this request since it is fully contained
			// within this range, so we mark the request as "fully processed".
			h.headers[i] = kvpb.RequestHeader{}
			h.positions[i] = -1
			if origStartKey := inner.Header().Key; origStartKey.Equal(header.Key) {
				// This range-spanning request fits within a single range, so we
				// can just use the original request.
				truncReqs = append(truncReqs, h.requests[i])
				positions = append(positions, pos)
				continue
			}
		} else {
			header.EndKey = ek
			// Adjust the start key of the header so that it contained only the
			// unprocessed suffix of the request.
			h.headers[i].Key = header.EndKey
		}
		shallowCopy := inner.ShallowCopy()
		shallowCopy.SetHeader(header)
		truncReqs = append(truncReqs, kvpb.RequestUnion{})
		truncReqs[len(truncReqs)-1].MustSetInner(shallowCopy)
		positions = append(positions, pos)
	}
	return truncReqs, positions, nil
}

// truncateDesc is the optimized strategy for Truncate() with the Descending
// scan direction when requests only use global keys.
//
// The first step of this strategy is to reorder all requests according to their
// end keys with the descending direction (this is done in Init()). Then, on
// every call to truncateDesc(), we only look at a subset of original requests
// that might overlap with rs and ignore already processed requests entirely.
//
// Let's go through an example. Say we have seven original requests:
//
//	requests : Scan(i, l), Get(d), Scan(h, k), Scan(g, i), Get(i), Scan(d, f), Scan(b, h)
//	positions:      0          1        2           3          4        5           6
//
// as well three ranges to iterate over:
//
//	ranges: range[i, m), range[e, i), range[a, e).
//
// In Init(), we have reordered the requests according to their end keys with
// the descending direction (below, i' denotes Key("i").Next()):
//
//	requests : Scan(i, l), Scan(h, k), Get(i), Scan(g, i), Scan(b, h), Scan(d, f), Get(d)
//	positions:      0           2          4        3           6          5           1
//	headers  :     [i, l)      [h, k)   [i, i')    [g, i)      [b, h)     [d, f)    [d, d')
//
// On the first call to Truncate(), we're using the range [i, m). We only need
// to look at the first four requests since the fourth request ends before the
// start of the current range, and, due to the ordering, all following requests
// too. We truncate first three requests to the range boundaries, update the
// headers to refer to the unprocessed parts of the corresponding requests, and
// mark the 1st and the 3rd requests as fully processed.
//
// The first call prepares
//
//	truncReqs = [Scan(i, l), Scan(i, k), Get(i)], positions = [0, 2, 4]
//
// and the internal state is now
//
//	requests : Scan(i, l), Scan(h, k), Get(i), Scan(g, i), Scan(b, h), Scan(d, f), Get(d)
//	positions:     -1           2         -1        3           6          5           1
//	headers  :    <nil>        [h, i)    <nil>     [g, i)      [b, h)     [d, f)    [d, d')
//
// Then the optimized prev() function determines the seekKey as 'i' and moves
// the startIdx to 1.
//
// On the second call to Truncate(), we're using the range [e, i). We skip
// looking at the first request entirely (due to value of startIdx) and only
// need to look at all remaining requests (skipping the third one since it's fully
// processed). We truncate the requests to the range boundaries, update the
// headers to refer to the unprocessed parts of the corresponding requests, and
// mark the 2nd and the 4th requests as fully processed.
//
// The second call prepares
//
//	truncReqs = [Scan(h, i), Scan(g, i), Scan(e, h), Scan(e, f)], positions = [2, 3, 6, 5]
//
// and the internal state is now
//
//	requests : Scan(i, l), Scan(h, k), Get(i), Scan(g, i), Scan(b, h), Scan(d, f), Get(d)
//	positions:     -1          -1         -1       -1           6          5           1
//	headers  :    <nil>       <nil>      <nil>    <nil>        [b, e)     [d, e)    [d, d')
//
// Then the optimized prev() function determines the seekKey as 'e' and sets
// the startIdx at 4 (meaning that all first four requests have been fully
// processed).
//
// On the third call to Truncate(), we're using the range [a, e). We only look
// at the 5th, 6th, and 7th requests (because of the value of startIdx). All
// requests are contained within the range, so we include them into the return
// value and mark all of them as processed.
//
// The third call prepares
//
//	truncReqs = [Scan(b, e), Scan(d, e), Get(d)], positions = [6, 5, 1]
//
// and the internal state is now
//
//	requests : Scan(i, l), Scan(h, k), Get(i), Scan(g, i), Scan(b, h), Scan(d, f), Get(d)
//	positions:     -1          -1         -1       -1          -1          -1          -1
//	headers  :    <nil>       <nil>      <nil>    <nil>       <nil>       <nil>       <nil>
//
// Then the optimized prev() function determines the seekKey as KeyMin and sets
// the startIdx at 7 (meaning that all requests have been fully processed), and
// we're done.
//
// NOTE: for all requests, headers always keep track of the unprocessed part of
// the request and is such that the ordering of the end keys in the headers is
// preserved when the requests are truncated.
//
// Note that this function is very similar to truncateAsc(), and we could
// extract out the differences into an interface; however, this leads to
// non-trivial slowdown and increase in allocations, so we choose to duplicate
// the code for performance.
func (h *BatchTruncationHelper) truncateDesc(rs roachpb.RSpan) ([]kvpb.RequestUnion, []int, error) {
	var truncReqs []kvpb.RequestUnion
	var positions []int
	for i := h.startIdx; i < len(h.positions); i++ {
		pos := h.positions[i]
		if pos < 0 {
			// This request has already been fully processed, so there is no
			// need to look at it.
			continue
		}
		header := h.headers[i]
		// rs.Key can't be local because it contains range split points, which
		// are never local.
		sk := rs.Key.AsRawKey()
		if sk.Compare(header.EndKey) >= 0 {
			// All of the remaining requests end before this range, so we're
			// done.
			break
		}
		if !h.isRange[i] {
			// This is a point request, and the key is contained within this
			// range, so we include the request as is and mark it as "fully
			// processed".
			truncReqs = append(truncReqs, h.requests[i])
			positions = append(positions, pos)
			h.headers[i] = kvpb.RequestHeader{}
			h.positions[i] = -1
			continue
		}
		// We're dealing with a range-spanning request.
		if buildutil.CrdbTestBuild {
			// rs.EndKey can't be local because it contains range split points,
			// which are never local.
			if header.EndKey.Compare(rs.EndKey.AsRawKey()) > 0 {
				return nil, nil, errors.AssertionFailedf(
					"unexpectedly header.EndKey %s is greater than rs %s", header.Key, rs,
				)
			}
		}
		inner := h.requests[i].GetInner()
		if header.Key.Compare(sk) >= 0 {
			// This is the last part of this request since it is fully contained
			// within this range, so we mark the request as "fully processed".
			h.headers[i] = kvpb.RequestHeader{}
			h.positions[i] = -1
			if origEndKey := inner.Header().EndKey; len(origEndKey) == 0 || origEndKey.Equal(header.EndKey) {
				// This range-spanning request fits within a single range, so we
				// can just use the original request.
				truncReqs = append(truncReqs, h.requests[i])
				positions = append(positions, pos)
				continue
			}
		} else {
			header.Key = sk
			// Adjust the end key of the header so that it contained only the
			// unprocessed prefix of the request.
			h.headers[i].EndKey = header.Key
		}
		shallowCopy := inner.ShallowCopy()
		shallowCopy.SetHeader(header)
		truncReqs = append(truncReqs, kvpb.RequestUnion{})
		truncReqs[len(truncReqs)-1].MustSetInner(shallowCopy)
		positions = append(positions, pos)
	}
	return truncReqs, positions, nil
}

var emptyHeader = kvpb.RequestHeader{}

// truncateLegacy restricts all requests to the given key range and returns new,
// truncated, requests. All returned requests are "truncated" to the given span,
// and requests which are found to not overlap the given span at all are
// removed. A mapping of response index to request index is returned. For
// example, if
//
// reqs = Put[a], Put[c], Put[b],
// rs = [a,bb],
//
// then truncateLegacy(reqs,rs) returns (Put[a], Put[b]) and positions [0,2].
func truncateLegacy(
	reqs []kvpb.RequestUnion, rs roachpb.RSpan,
) ([]kvpb.RequestUnion, []int, error) {
	truncateOne := func(args kvpb.Request) (hasRequest bool, changed bool, _ kvpb.RequestHeader, _ error) {
		header := args.Header()
		if !kvpb.IsRange(args) {
			// This is a point request.
			if len(header.EndKey) > 0 {
				return false, false, emptyHeader, errors.AssertionFailedf("%T is not a range command, but EndKey is set", args)
			}
			keyAddr, err := keys.Addr(header.Key)
			if err != nil {
				return false, false, emptyHeader, err
			}
			if !rs.ContainsKey(keyAddr) {
				return false, false, emptyHeader, nil
			}
			return true, false, header, nil
		}
		// We're dealing with a range-spanning request.
		local := false
		keyAddr, err := keys.Addr(header.Key)
		if err != nil {
			return false, false, emptyHeader, err
		}
		endKeyAddr, err := keys.Addr(header.EndKey)
		if err != nil {
			return false, false, emptyHeader, err
		}
		if l, r := keys.IsLocal(header.Key), keys.IsLocal(header.EndKey); l || r {
			if !l || !r {
				return false, false, emptyHeader, errors.AssertionFailedf("local key mixed with global key in range")
			}
			local = true
		}
		if keyAddr.Less(rs.Key) {
			// rs.Key can't be local because it contains range split points,
			// which are never local.
			changed = true
			if !local {
				header.Key = rs.Key.AsRawKey()
			} else {
				// The local start key should be truncated to the boundary of
				// local keys which address to rs.Key.
				header.Key = keys.MakeRangeKeyPrefix(rs.Key)
			}
		}
		if !endKeyAddr.Less(rs.EndKey) {
			// rs.EndKey can't be local because it contains range split points,
			// which are never local.
			changed = true
			if !local {
				header.EndKey = rs.EndKey.AsRawKey()
			} else {
				// The local end key should be truncated to the boundary of
				// local keys which address to rs.EndKey.
				header.EndKey = keys.MakeRangeKeyPrefix(rs.EndKey)
			}
		}
		// Check whether the truncation has left any keys in the range. If not,
		// we need to cut it out of the request.
		if changed && header.Key.Compare(header.EndKey) >= 0 {
			return false, false, emptyHeader, nil
		}
		return true, changed, header, nil
	}

	// TODO(tschottdorf): optimize so that we don't always make a new request
	// slice, only when something changed (copy-on-write).

	var positions []int
	var truncReqs []kvpb.RequestUnion
	for pos, arg := range reqs {
		inner := arg.GetInner()
		hasRequest, changed, newHeader, err := truncateOne(inner)
		if hasRequest {
			// Keep the old one. If we must adjust the header, must copy.
			if changed {
				shallowCopy := inner.ShallowCopy()
				shallowCopy.SetHeader(newHeader)
				truncReqs = append(truncReqs, kvpb.RequestUnion{})
				truncReqs[len(truncReqs)-1].MustSetInner(shallowCopy)
			} else {
				truncReqs = append(truncReqs, arg)
			}
			positions = append(positions, pos)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return truncReqs, positions, nil
}

// prev returns the next seek key for the range iterator with the Descending
// scan direction.
//
// Informally, a call `prev(k)` means: we've already executed the parts of
// `reqs` that intersect `[k, KeyMax)`; please tell me how far to the left the
// next relevant request begins.
func (h *BatchTruncationHelper) prev(k roachpb.RKey) (roachpb.RKey, error) {
	if h.foundLocalKey {
		return prevLegacy(h.requests, k)
	}
	// Skip over first startIdx-1 requests since they have been fully processed.
	for i, pos := range h.positions[h.startIdx:] {
		if pos < 0 {
			continue
		}
		// This is the first request that hasn't been fully processed, so we can
		// bump the startIdx to this request's index and use the end key of the
		// unprocessed part for the next seek key.
		//
		// By construction, all requests after this one will have their end key
		// greater or equal to this request's end key, thus, there is no need to
		// iterate any further. See the comment on truncateDesc() for more
		// details.
		h.startIdx += i
		return keys.Addr(h.headers[h.startIdx].EndKey)
	}
	// If we got to this point, then all requests have been fully processed.
	h.startIdx = len(h.requests)
	return roachpb.RKeyMin, nil
}

// prevLegacy gives the right boundary of the union of all requests which don't
// affect keys larger than the given key. Note that a right boundary is
// exclusive, that is, the returned RKey is to be used as the exclusive right
// endpoint in finding the next range to query.
//
// Informally, a call `prevLegacy(reqs, k)` means: we've already executed the
// parts of `reqs` that intersect `[k, KeyMax)`; please tell me how far to the
// left the next relevant request begins.
func prevLegacy(reqs []kvpb.RequestUnion, k roachpb.RKey) (roachpb.RKey, error) {
	candidate := roachpb.RKeyMin
	for _, union := range reqs {
		inner := union.GetInner()
		h := inner.Header()
		addr, err := keys.Addr(h.Key)
		if err != nil {
			return nil, err
		}
		endKey := h.EndKey
		if len(endKey) == 0 {
			// If we have a point request for `x < k` then that request has not
			// been satisfied (since the batch has only been executed for keys
			// `>=k`). We treat `x` as `[x, x.Next())` which does the right
			// thing below. This also works when `x > k` or `x=k` as the logic
			// below will skip `x`.
			//
			// Note that if the key is /Local/x/something, then instead of using
			// /Local/x/something.Next() as the end key, we rely on
			// AddrUpperBound to handle local keys. In particular,
			// AddrUpperBound will turn it into `x\x00`, so we're looking at the
			// key-range `[x, x.Next())`. This is exactly what we want as the
			// local key is contained in that range.
			//
			// See TestBatchPrevNext for test cases with commentary.
			endKey = h.Key.Next()
		}
		eAddr, err := keys.AddrUpperBound(endKey)
		if err != nil {
			return nil, err
		}
		if !eAddr.Less(k) {
			// EndKey is k or higher.
			//           [x-------y)    !x.Less(k) -> skip
			//         [x-------y)      !x.Less(k) -> skip
			//      [x-------y)          x.Less(k) -> return k
			//  [x------y)               x.Less(k) -> return k
			// [x------y)                not in this branch
			//          k
			if addr.Less(k) {
				// Range contains k, so won't be able to go lower.
				// Note that in the special case in which the interval
				// touches k, we don't take this branch. This reflects
				// the fact that `prev(k)` means that all keys >= k have
				// been handled, so a request `[k, x)` should simply be
				// skipped.
				return k, nil
			}
			// Range is disjoint from [KeyMin,k).
			continue
		}
		// Current candidate interval is strictly to the left of `k`.
		// We want the largest surviving candidate.
		if candidate.Less(eAddr) {
			candidate = eAddr
		}
	}
	return candidate, nil
}

// next returns the next seek key for the range iterator.
//
// Informally, a call `next(k)` means: we've already executed the parts of
// `reqs` that intersect `[KeyMin, k)`; please tell me how far to the right the
// next relevant request begins.
func (h *BatchTruncationHelper) next(k roachpb.RKey) (roachpb.RKey, error) {
	if h.foundLocalKey {
		return nextLegacy(h.requests, k)
	}
	// Skip over first startIdx-1 requests since they have been fully processed.
	for i, pos := range h.positions[h.startIdx:] {
		if pos < 0 {
			continue
		}
		// This is the first request that hasn't been fully processed, so we can
		// bump the startIdx to this request's index and use the start key of
		// the unprocessed part for the next seek key.
		//
		// By construction, all requests after this one will have their start
		// key greater or equal to this request's start key, thus, there is no
		// need to iterate any further. See the comment on truncateAsc() for
		// more details.
		h.startIdx += i
		return keys.Addr(h.headers[h.startIdx].Key)
	}
	// If we got to this point, then all requests have been fully processed.
	h.startIdx = len(h.requests)
	return roachpb.RKeyMax, nil
}

// nextLegacy gives the left boundary of the union of all requests which don't
// affect keys less than the given key. Note that the left boundary is
// inclusive, that is, the returned RKey is the inclusive left endpoint of the
// keys the request should operate on next.
//
// Informally, a call `nextLegacy(reqs, k)` means: we've already executed the
// parts of `reqs` that intersect `[KeyMin, k)`; please tell me how far to the
// right the next relevant request begins.
func nextLegacy(reqs []kvpb.RequestUnion, k roachpb.RKey) (roachpb.RKey, error) {
	candidate := roachpb.RKeyMax
	for _, union := range reqs {
		inner := union.GetInner()
		h := inner.Header()
		addr, err := keys.Addr(h.Key)
		if err != nil {
			return nil, err
		}
		if addr.Less(k) {
			if len(h.EndKey) == 0 {
				// `h` affects only `[KeyMin,k)`, all of which is less than `k`.
				continue
			}
			eAddr, err := keys.AddrUpperBound(h.EndKey)
			if err != nil {
				return nil, err
			}
			if k.Less(eAddr) {
				// Starts below k, but continues beyond. Need to stay at k.
				return k, nil
			}
			// `h` affects only `[KeyMin,k)`, all of which is less than `k`.
			continue
		}
		// We want the smallest of the surviving candidates.
		if addr.Less(candidate) {
			candidate = addr
		}
	}
	return candidate, nil
}

// orderRestorationHelper is a utility struct that allows to restore the order
// of requests according to the positions values in O(N) time where N is the
// number of the original requests (i.e. before truncation). Benchmarks have
// shown that it is faster that a solution with explicitly sorting the requests
// in O(T log T) time where T is the number of truncated requests returned by
// a single Truncate() call.
type orderRestorationHelper struct {
	// scratch is reused on the next call to restoreOrder() if it has enough
	// capacity.
	scratch []kvpb.RequestUnion
	// found is used as a map indicating whether a request for a particular
	// positions value is included into the truncated requests and at what
	// index, -1 if the corresponding request is not found in the truncated
	// requests.
	//
	// found has the length of N where N is the number of the original requests.
	// It is reused on all restoreOrder() calls.
	found []int
}

func (h *orderRestorationHelper) init(numOriginalRequests int) {
	if cap(h.found) < numOriginalRequests {
		h.found = make([]int, numOriginalRequests)
	} else {
		h.found = h.found[:numOriginalRequests]
	}
	for i := range h.found {
		h.found[i] = -1
	}
}

func (h *orderRestorationHelper) memUsage() int64 {
	return int64(cap(h.scratch))*requestUnionOverhead + int64(cap(h.found))*intOverhead
}

// restoreOrder reorders truncReqs in the ascending order of the corresponding
// positions values.
//
// Let's go through a quick example. Say we have five original requests and the
// following setup:
//
//	truncReqs = [Scan(a, c), Get(b), Scan(c, d)], positions = [3, 0, 4]
//
// We first populate the found map:
//
//	found = [1, -1, -1, 0, 2]
//
// meaning that requests at positions 0, 3, 4 are present in truncReqs. Then we
// iterate over the found map, and for all non-negative found values, we include
// the corresponding request:
// 1. found[0] = 1  -> toReturn = [Get(b)]                          positions = [0]
// 2. found[1] = -1 -> skip
// 3. found[2] = -1 -> skip
// 4. found[3] = 0  -> toReturn = [Get(b), Scan(a, c)]              positions = [0, 3]
// 5. found[4] = 2  -> toReturn = [Get(b), Scan(a, c), Scan(c, d)]  positions = [0, 3, 4]
func (h *orderRestorationHelper) restoreOrder(
	truncReqs []kvpb.RequestUnion, positions []int,
) ([]kvpb.RequestUnion, []int) {
	if len(truncReqs) == 0 {
		return truncReqs, positions
	}
	for i, pos := range positions {
		h.found[pos] = i
	}
	var toReturn []kvpb.RequestUnion
	if cap(h.scratch) >= len(positions) {
		toReturn = h.scratch[:0]
	} else {
		toReturn = make([]kvpb.RequestUnion, 0, len(positions))
	}
	positions = positions[:0]
	for pos, found := range h.found {
		if found < 0 {
			// The request with positions value 'pos' is not included in
			// truncReqs.
			continue
		}
		toReturn = append(toReturn, truncReqs[found])
		positions = append(positions, pos)
		// Lose the reference to the request so that we can keep truncReqs as
		// the scratch space for the next call.
		truncReqs[found] = kvpb.RequestUnion{}
		// Make sure that the found map is set up for the next call.
		h.found[pos] = -1
	}
	h.scratch = truncReqs
	return toReturn, positions
}
