// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// getKVBatchSize returns the number of keys we request at a time.
// On a single node, 1000 was enough to avoid any performance degradation. On
// multi-node clusters, we want bigger chunks to make up for the higher latency.
//
// If forceProductionKVBatchSize is true, then the "production" value will be
// returned regardless of whether the build is metamorphic or not. This should
// only be used by tests the output of which differs if defaultKVBatchSize is
// randomized.
// TODO(radu): parameters like this should be configurable
func getKVBatchSize(forceProductionKVBatchSize bool) rowinfra.KeyLimit {
	if forceProductionKVBatchSize {
		return rowinfra.ProductionKVBatchSize
	}
	return defaultKVBatchSize
}

var defaultKVBatchSize = rowinfra.KeyLimit(util.ConstantWithMetamorphicTestValue(
	"kv-batch-size",
	int(rowinfra.ProductionKVBatchSize), /* defaultValue */
	1,                                   /* metamorphicValue */
))

// sendFunc is the function used to execute a KV batch; normally
// wraps (*client.Txn).Send.
type sendFunc func(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error)

// txnKVFetcher handles retrieval of key/values.
type txnKVFetcher struct {
	// "Constant" fields, provided by the caller.
	sendFn sendFunc
	// spans is the list of Spans that will be read by this KV Fetcher. If an
	// individual Span has only a start key, it will be interpreted as a
	// single-key fetch and may use a GetRequest under the hood.
	spans roachpb.Spans
	// spansScratch is the initial state of spans (given to the fetcher when
	// starting the scan) that we need to hold on to since we're slicing off
	// spans in nextBatch(). Any resume spans are copied into this slice when
	// processing each response.
	spansScratch roachpb.Spans
	// newFetchSpansIdx tracks the number of resume spans we have copied into
	// spansScratch after the last fetch.
	newFetchSpansIdx int

	// If firstBatchKeyLimit is set, the first batch is limited in number of keys
	// to this value and subsequent batches are larger (up to a limit, see
	// getKVBatchSize()). If not set, batches do not have a key limit (they might
	// still have a bytes limit as per batchBytesLimit).
	firstBatchKeyLimit rowinfra.KeyLimit
	// If batchBytesLimit is set, the batches are limited in response size. This
	// protects from OOMs, but comes at the cost of inhibiting DistSender-level
	// parallelism within a batch.
	//
	// If batchBytesLimit is not set, the assumption is that SQL *knows* that
	// there is only a "small" amount of data to be read (i.e. scanning `spans`
	// doesn't result in too much data), and wants to preserve concurrency for
	// this scans inside of DistSender.
	batchBytesLimit rowinfra.BytesLimit

	reverse bool
	// lockStrength represents the locking mode to use when fetching KVs.
	lockStrength lock.Strength
	// lockWaitPolicy represents the policy to be used for handling conflicting
	// locks held by other active transactions.
	lockWaitPolicy lock.WaitPolicy
	// lockTimeout specifies the maximum amount of time that the fetcher will
	// wait while attempting to acquire a lock on a key or while blocking on an
	// existing lock in order to perform a non-locking read on a key.
	lockTimeout time.Duration

	// alreadyFetched indicates whether fetch() has already been executed at
	// least once.
	alreadyFetched bool
	batchIdx       int

	responses        []roachpb.ResponseUnion
	remainingBatches [][]byte

	acc *mon.BoundAccount
	// spansAccountedFor and batchResponseAccountedFor track the number of bytes
	// that we've already registered with acc in regards to spans and the batch
	// response, respectively.
	spansAccountedFor         int64
	batchResponseAccountedFor int64

	// If set, we will use the production value for kvBatchSize.
	forceProductionKVBatchSize bool

	// For request and response admission control.
	requestAdmissionHeader roachpb.AdmissionHeader
	responseAdmissionQ     *admission.WorkQueue
}

var _ KVBatchFetcher = &txnKVFetcher{}

// getBatchKeyLimit returns the max size of the next batch. The size is
// expressed in number of result keys (i.e. this size will be used for
// MaxSpanRequestKeys).
func (f *txnKVFetcher) getBatchKeyLimit() rowinfra.KeyLimit {
	return f.getBatchKeyLimitForIdx(f.batchIdx)
}

func (f *txnKVFetcher) getBatchKeyLimitForIdx(batchIdx int) rowinfra.KeyLimit {
	if f.firstBatchKeyLimit == 0 {
		return 0
	}

	kvBatchSize := getKVBatchSize(f.forceProductionKVBatchSize)
	if f.firstBatchKeyLimit >= kvBatchSize {
		return kvBatchSize
	}

	// We grab the first batch according to the limit. If it turns out that we
	// need another batch, we grab a bigger batch. If that's still not enough,
	// we revert to the default batch size.
	switch batchIdx {
	case 0:
		return f.firstBatchKeyLimit

	case 1:
		// Make the second batch 10 times larger (but at most the default batch
		// size and at least 1/10 of the default batch size). Sample
		// progressions of batch sizes:
		//
		//  First batch |  Second batch  | Subsequent batches
		//  -----------------------------------------------
		//         1    |     10,000     |     100,000
		//       100    |     10,000     |     100,000
		//      5000    |     50,000     |     100,000
		//     10000    |    100,000     |     100,000
		secondBatch := f.firstBatchKeyLimit * 10
		switch {
		case secondBatch < kvBatchSize/10:
			return kvBatchSize / 10
		case secondBatch > kvBatchSize:
			return kvBatchSize
		default:
			return secondBatch
		}

	default:
		return kvBatchSize
	}
}

func makeKVBatchFetcherDefaultSendFunc(txn *kv.Txn) sendFunc {
	return func(
		ctx context.Context,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		res, err := txn.Send(ctx, ba)
		if err != nil {
			return nil, err.GoError()
		}
		return res, nil
	}
}

// makeKVBatchFetcher initializes a KVBatchFetcher for the given spans. If
// useBatchLimit is true, the number of result keys per batch is limited; the
// limit grows between subsequent batches, starting at firstBatchKeyLimit (if not
// 0) to ProductionKVBatchSize.
//
// The fetcher takes ownership of the spans slice - it can modify the slice and
// will perform the memory accounting accordingly (if acc is non-nil). The
// caller can only reuse the spans slice after the fetcher has been closed, and
// if the caller does, it becomes responsible for the memory accounting.
//
// Batch limits can only be used if the spans are ordered.
//
// The passed-in memory account is owned by the fetcher throughout its lifetime
// but is **not** closed - it is the caller's responsibility to close acc if it
// is non-nil.
func makeKVBatchFetcher(
	ctx context.Context,
	sendFn sendFunc,
	spans roachpb.Spans,
	reverse bool,
	batchBytesLimit rowinfra.BytesLimit,
	firstBatchKeyLimit rowinfra.KeyLimit,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	lockTimeout time.Duration,
	acc *mon.BoundAccount,
	forceProductionKVBatchSize bool,
	requestAdmissionHeader roachpb.AdmissionHeader,
	responseAdmissionQ *admission.WorkQueue,
) (txnKVFetcher, error) {
	if firstBatchKeyLimit < 0 || (batchBytesLimit == 0 && firstBatchKeyLimit != 0) {
		// Passing firstBatchKeyLimit without batchBytesLimit doesn't make sense - the
		// only reason to not set batchBytesLimit is in order to get DistSender-level
		// parallelism, and setting firstBatchKeyLimit inhibits that.
		return txnKVFetcher{}, errors.Errorf("invalid batch limit %d (batchBytesLimit: %d)",
			firstBatchKeyLimit, batchBytesLimit)
	}

	if batchBytesLimit != 0 {
		// Verify the spans are ordered if a batch limit is used.
		for i := 1; i < len(spans); i++ {
			prevKey := spans[i-1].EndKey
			if prevKey == nil {
				// This is the case of a GetRequest.
				prevKey = spans[i-1].Key
			}
			if spans[i].Key.Compare(prevKey) < 0 {
				return txnKVFetcher{}, errors.Errorf("unordered spans (%s %s)", spans[i-1], spans[i])
			}
		}
	} else if util.RaceEnabled {
		// Otherwise, just verify the spans don't contain consecutive overlapping
		// spans.
		for i := 1; i < len(spans); i++ {
			prevEndKey := spans[i-1].EndKey
			if prevEndKey == nil {
				prevEndKey = spans[i-1].Key
			}
			curEndKey := spans[i].EndKey
			if curEndKey == nil {
				curEndKey = spans[i].Key
			}
			if spans[i].Key.Compare(prevEndKey) >= 0 {
				// Current span's start key is greater than or equal to the last span's
				// end key - we're good.
				continue
			} else if curEndKey.Compare(spans[i-1].Key) <= 0 {
				// Current span's end key is less than or equal to the last span's start
				// key - also good.
				continue
			}
			// Otherwise, the two spans overlap, which isn't allowed - it leaves us at
			// risk of incorrect results, since the row fetcher can't distinguish
			// between identical rows in two different batches.
			return txnKVFetcher{}, errors.Errorf("overlapping neighbor spans (%s %s)", spans[i-1], spans[i])
		}
	}

	f := txnKVFetcher{
		sendFn:                     sendFn,
		reverse:                    reverse,
		batchBytesLimit:            batchBytesLimit,
		firstBatchKeyLimit:         firstBatchKeyLimit,
		lockStrength:               getKeyLockingStrength(lockStrength),
		lockWaitPolicy:             GetWaitPolicy(lockWaitPolicy),
		lockTimeout:                lockTimeout,
		acc:                        acc,
		forceProductionKVBatchSize: forceProductionKVBatchSize,
		requestAdmissionHeader:     requestAdmissionHeader,
		responseAdmissionQ:         responseAdmissionQ,
	}

	// Account for the memory of the spans that we're taking the ownership of.
	if f.acc != nil {
		f.spansAccountedFor = spans.MemUsage()
		if err := f.acc.Grow(ctx, f.spansAccountedFor); err != nil {
			return txnKVFetcher{}, err
		}
	}

	// Since the fetcher takes ownership of the spans slice, we don't need to
	// perform the deep copy. Notably, the spans might be modified (when the
	// fetcher receives the resume spans), but the fetcher will always keep the
	// memory accounting up to date.
	f.spans = spans
	if reverse {
		// Reverse scans receive the spans in decreasing order. Note that we
		// need to be this tricky since we're updating the spans slice in place.
		i, j := 0, len(spans)-1
		for i < j {
			f.spans[i], f.spans[j] = f.spans[j], f.spans[i]
			i++
			j--
		}
	}
	// Keep the reference to the full spans slice. We will never need larger
	// slice for the resume spans.
	f.spansScratch = f.spans

	return f, nil
}

// fetch retrieves spans from the kv layer.
func (f *txnKVFetcher) fetch(ctx context.Context) error {
	var ba roachpb.BatchRequest
	ba.Header.WaitPolicy = f.lockWaitPolicy
	ba.Header.LockTimeout = f.lockTimeout
	ba.Header.TargetBytes = int64(f.batchBytesLimit)
	ba.Header.MaxSpanRequestKeys = int64(f.getBatchKeyLimit())
	ba.AdmissionHeader = f.requestAdmissionHeader
	ba.Requests = spansToRequests(f.spans, f.reverse, f.lockStrength)

	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "Scan %s", f.spans)
	}

	monitoring := f.acc != nil

	const tokenFetchAllocation = 1 << 10
	if !monitoring || f.batchResponseAccountedFor < tokenFetchAllocation {
		// In case part of this batch ends up being evaluated locally, we want
		// that local evaluation to do memory accounting since we have reserved
		// negligible bytes. Ideally, we would split the memory reserved across
		// the various servers that DistSender will split this batch into, but we
		// do not yet have that capability.
		ba.AdmissionHeader.NoMemoryReservedAtSource = true
	}
	if monitoring && f.batchResponseAccountedFor < tokenFetchAllocation {
		// Pre-reserve a token fraction of the maximum amount of memory this scan
		// could return. Most of the time, scans won't use this amount of memory,
		// so it's unnecessary to reserve it all. We reserve something rather than
		// nothing at all to preserve some accounting.
		f.batchResponseAccountedFor = tokenFetchAllocation
		if err := f.acc.Grow(ctx, f.batchResponseAccountedFor); err != nil {
			return err
		}
	}

	br, err := f.sendFn(ctx, ba)
	if err != nil {
		return err
	}
	if br != nil {
		f.responses = br.Responses
	} else {
		f.responses = nil
	}
	returnedBytes := int64(br.Size())
	if monitoring && (returnedBytes > int64(f.batchBytesLimit) || returnedBytes > f.batchResponseAccountedFor) {
		// Resize up to the actual amount of bytes we got back from the fetch,
		// but don't ratchet down below f.batchBytesLimit if we ever exceed it.
		// We would much prefer to over-account than under-account, especially when
		// we are in a situation where we have large batches caused by parallel
		// unlimited scans (index joins and lookup joins where cols are key).
		//
		// The reason we don't want to precisely account here is to hopefully
		// protect ourselves from "slop" in our memory handling. In general, we
		// expect that all SQL operators that buffer data for longer than a single
		// call to Next do their own accounting, so theoretically, by the time
		// this fetch method is called again, all memory will either be released
		// from the system or accounted for elsewhere. In reality, though, Go's
		// garbage collector has some lag between when the memory is no longer
		// referenced and when it is freed. Also, we're not perfect with
		// accounting by any means. When we start doing large fetches, it's more
		// likely that we'll expose ourselves to OOM conditions, so that's the
		// reasoning for why we never ratchet this account down past the maximum
		// fetch size once it's exceeded.
		used := f.acc.Used()
		delta := returnedBytes - f.batchResponseAccountedFor
		if err := f.acc.Resize(ctx, used, used+delta); err != nil {
			return err
		}
		f.batchResponseAccountedFor = returnedBytes
	}
	// Do admission control after we've accounted for the response bytes.
	if br != nil && f.responseAdmissionQ != nil {
		responseAdmission := admission.WorkInfo{
			TenantID:   roachpb.SystemTenantID,
			Priority:   admission.WorkPriority(f.requestAdmissionHeader.Priority),
			CreateTime: f.requestAdmissionHeader.CreateTime,
		}
		if _, err := f.responseAdmissionQ.Admit(ctx, responseAdmission); err != nil {
			return err
		}
	}

	f.batchIdx++
	f.newFetchSpansIdx = 0
	f.alreadyFetched = true

	// TODO(radu): We should fetch the next chunk in the background instead of waiting for the next
	// call to fetch(). We can use a pool of workers to issue the KV ops which will also limit the
	// total number of fetches that happen in parallel (and thus the amount of resources we use).
	return nil
}

// popBatch returns the 0th byte slice in a slice of byte slices, as well as
// the rest of the slice of the byte slices. It nils the pointer to the 0th
// element before reslicing the outer slice.
func popBatch(batches [][]byte) (batch []byte, remainingBatches [][]byte) {
	batch, remainingBatches = batches[0], batches[1:]
	batches[0] = nil
	return batch, remainingBatches
}

// nextBatch returns the next batch of key/value pairs. If there are none
// available, a fetch is initiated. When there are no more keys, ok is false.
// atBatchBoundary indicates whether the next call to nextBatch will request
// another fetch from the KV system.
func (f *txnKVFetcher) nextBatch(
	ctx context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResp []byte, err error) {
	// The purpose of this loop is to unpack the two-level batch structure that is
	// returned from the KV layer.
	//
	// A particular BatchRequest from fetch will populate the f.responses field
	// with one response per request in the input BatchRequest. Each response
	// in the responses field itself can also have a list of "BatchResponses",
	// each of which is a byte slice containing result data from KV. Since this
	// function, by contract, returns just a single byte slice at a time, we store
	// the inner list as state for the next invocation to pop from.
	if len(f.remainingBatches) > 0 {
		// Are there remaining data batches? If so, just pop one off from the
		// list and return it.
		batchResp, f.remainingBatches = popBatch(f.remainingBatches)
		return true, nil, batchResp, nil
	}
	// There are no remaining data batches. Find the first non-empty ResponseUnion
	// in the list of unprocessed responses from the last BatchResponse we sent,
	// and process it.
	for len(f.responses) > 0 {
		reply := f.responses[0].GetInner()
		f.responses[0] = roachpb.ResponseUnion{}
		f.responses = f.responses[1:]
		// Get the original span right away since we might overwrite it with the
		// resume span below.
		origSpan := f.spans[0]
		f.spans[0] = roachpb.Span{}
		f.spans = f.spans[1:]

		// Check whether we need to resume scanning this span.
		header := reply.Header()
		if header.NumKeys > 0 && f.newFetchSpansIdx > 0 {
			return false, nil, nil, errors.Errorf(
				"span with results after resume span; it shouldn't happen given that "+
					"we're only scanning non-overlapping spans. New spans: %s",
				catalogkeys.PrettySpans(nil, f.spans, 0 /* skip */))
		}

		// Any requests that were not fully completed will have the ResumeSpan set.
		// Here we accumulate all of them.
		if resumeSpan := header.ResumeSpan; resumeSpan != nil {
			f.spansScratch[f.newFetchSpansIdx] = *resumeSpan
			f.newFetchSpansIdx++
		}

		switch t := reply.(type) {
		case *roachpb.ScanResponse:
			if len(t.BatchResponses) > 0 {
				batchResp, f.remainingBatches = popBatch(t.BatchResponses)
			}
			if len(t.Rows) > 0 {
				return false, nil, nil, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse using KEY_VALUES response format",
				)
			}
			if len(t.IntentRows) > 0 {
				return false, nil, nil, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse with non-nil IntentRows",
				)
			}
			// Note that batchResp might be nil when the ScanResponse is empty,
			// and the caller (the KVFetcher) will skip over it.
			return true, nil, batchResp, nil
		case *roachpb.ReverseScanResponse:
			if len(t.BatchResponses) > 0 {
				batchResp, f.remainingBatches = popBatch(t.BatchResponses)
			}
			if len(t.Rows) > 0 {
				return false, nil, nil, errors.AssertionFailedf(
					"unexpectedly got a ReverseScanResponse using KEY_VALUES response format",
				)
			}
			if len(t.IntentRows) > 0 {
				return false, nil, nil, errors.AssertionFailedf(
					"unexpectedly got a ReverseScanResponse with non-nil IntentRows",
				)
			}
			// Note that batchResp might be nil when the ReverseScanResponse is
			// empty, and the caller (the KVFetcher) will skip over it.
			return true, nil, batchResp, nil
		case *roachpb.GetResponse:
			if t.IntentValue != nil {
				return false, nil, nil, errors.AssertionFailedf("unexpectedly got an IntentValue back from a SQL GetRequest %v", *t.IntentValue)
			}
			if t.Value == nil {
				// Nothing found in this particular response, let's continue to the next
				// one.
				continue
			}
			return true, []roachpb.KeyValue{{Key: origSpan.Key, Value: *t.Value}}, nil, nil
		}
	}
	// No more responses from the last BatchRequest.
	if f.alreadyFetched {
		if f.newFetchSpansIdx == 0 {
			// If we are out of keys, we can return and we're finished with the
			// fetch.
			return false, nil, nil, nil
		}
		// We have some resume spans.
		f.spans = f.spansScratch[:f.newFetchSpansIdx]
		if f.acc != nil {
			used := f.acc.Used()
			delta := f.spans.MemUsage() - f.spansAccountedFor
			if err := f.acc.Resize(ctx, used, used+delta); err != nil {
				return false, nil, nil, err
			}
			f.spansAccountedFor += delta
		}
	}
	// We have more work to do. Ask the KV layer to continue where it left off.
	if err := f.fetch(ctx); err != nil {
		return false, nil, nil, err
	}
	// We've got more data to process, recurse and process it.
	return f.nextBatch(ctx)
}

// close releases the resources of this txnKVFetcher.
func (f *txnKVFetcher) close(ctx context.Context) {
	f.responses = nil
	f.remainingBatches = nil
	f.spans = nil
	f.spansScratch = nil
	f.acc.Clear(ctx)
}

// spansToRequests converts the provided spans to the corresponding requests. If
// a span doesn't have the EndKey set, then a Get request is used for it;
// otherwise, a Scan (or ReverseScan if reverse is true) request is used with
// BATCH_RESPONSE format.
func spansToRequests(
	spans roachpb.Spans, reverse bool, keyLocking lock.Strength,
) []roachpb.RequestUnion {
	reqs := make([]roachpb.RequestUnion, len(spans))
	// Detect the number of gets vs scans, so we can batch allocate all of the
	// requests precisely.
	nGets := 0
	for i := range spans {
		if spans[i].EndKey == nil {
			nGets++
		}
	}
	gets := make([]struct {
		req   roachpb.GetRequest
		union roachpb.RequestUnion_Get
	}, nGets)

	// curGet is incremented each time we fill in a GetRequest.
	curGet := 0
	if reverse {
		scans := make([]struct {
			req   roachpb.ReverseScanRequest
			union roachpb.RequestUnion_ReverseScan
		}, len(spans)-nGets)
		for i := range spans {
			if spans[i].EndKey == nil {
				// A span without an EndKey indicates that the caller is requesting a
				// single key fetch, which can be served using a GetRequest.
				gets[curGet].req.Key = spans[i].Key
				gets[curGet].req.KeyLocking = keyLocking
				gets[curGet].union.Get = &gets[curGet].req
				reqs[i].Value = &gets[curGet].union
				curGet++
				continue
			}
			curScan := i - curGet
			scans[curScan].req.SetSpan(spans[i])
			scans[curScan].req.ScanFormat = roachpb.BATCH_RESPONSE
			scans[curScan].req.KeyLocking = keyLocking
			scans[curScan].union.ReverseScan = &scans[curScan].req
			reqs[i].Value = &scans[curScan].union
		}
	} else {
		scans := make([]struct {
			req   roachpb.ScanRequest
			union roachpb.RequestUnion_Scan
		}, len(spans)-nGets)
		for i := range spans {
			if spans[i].EndKey == nil {
				// A span without an EndKey indicates that the caller is requesting a
				// single key fetch, which can be served using a GetRequest.
				gets[curGet].req.Key = spans[i].Key
				gets[curGet].req.KeyLocking = keyLocking
				gets[curGet].union.Get = &gets[curGet].req
				reqs[i].Value = &gets[curGet].union
				curGet++
				continue
			}
			curScan := i - curGet
			scans[curScan].req.SetSpan(spans[i])
			scans[curScan].req.ScanFormat = roachpb.BATCH_RESPONSE
			scans[curScan].req.KeyLocking = keyLocking
			scans[curScan].union.Scan = &scans[curScan].req
			reqs[i].Value = &scans[curScan].union
		}
	}
	return reqs
}
