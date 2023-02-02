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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error)

// identifiableSpans is a helper for keeping track of the roachpb.Spans with the
// corresponding spanIDs (when necessary).
type identifiableSpans struct {
	roachpb.Spans
	spanIDs []int
}

// swap swaps two spans as well as their span IDs.
func (s *identifiableSpans) swap(i, j int) {
	s.Spans.Swap(i, j)
	if s.spanIDs != nil {
		s.spanIDs[i], s.spanIDs[j] = s.spanIDs[j], s.spanIDs[i]
	}
}

// pop removes the first span as well as its span ID from s and returns them.
func (s *identifiableSpans) pop() (roachpb.Span, int) {
	origSpan := s.Spans[0]
	s.Spans[0] = roachpb.Span{}
	s.Spans = s.Spans[1:]
	var spanID int
	if s.spanIDs != nil {
		spanID = s.spanIDs[0]
		s.spanIDs = s.spanIDs[1:]
	}
	return origSpan, spanID
}

// push appends the given span as well as its span ID to s.
func (s *identifiableSpans) push(span roachpb.Span, spanID int) {
	s.Spans = append(s.Spans, span)
	if s.spanIDs != nil {
		s.spanIDs = append(s.spanIDs, spanID)
	}
}

// reset sets up s for reuse - the underlying slices will be overwritten with
// future calls to push().
func (s *identifiableSpans) reset() {
	s.Spans = s.Spans[:0]
	s.spanIDs = s.spanIDs[:0]
}

// txnKVFetcher handles retrieval of key/values.
type txnKVFetcher struct {
	kvBatchFetcherHelper
	// "Constant" fields, provided by the caller.
	sendFn sendFunc
	// spans is the list of Spans that will be read by this KV Fetcher. If an
	// individual Span has only a start key, it will be interpreted as a
	// single-key fetch and may use a GetRequest under the hood.
	spans identifiableSpans
	// spansScratch is the initial state of spans (given to the fetcher when
	// starting the scan) that we need to hold on to since we're poping off
	// spans in nextBatch(). Any resume spans are copied into this struct when
	// processing each response.
	//
	// scratchSpans.Len() is the number of resume spans we have accumulated
	// during the last fetch.
	scratchSpans identifiableSpans

	curSpanID int

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

	// scanFormat indicates the scan format that should be used for Scans and
	// ReverseScans. With COL_BATCH_RESPONSE scan format, indexFetchSpec must be
	// set.
	scanFormat     roachpb.ScanFormat
	indexFetchSpec *fetchpb.IndexFetchSpec

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
	reqsScratch    []roachpb.RequestUnion

	responses        []roachpb.ResponseUnion
	remainingBatches [][]byte

	// getResponseScratch is reused to return the result of Get requests.
	getResponseScratch [1]roachpb.KeyValue

	acc *mon.BoundAccount
	// spansAccountedFor, batchResponseAccountedFor, and reqsScratchAccountedFor
	// track the number of bytes that we've already registered with acc in
	// regards to spans, the batch response, and reqsScratch, respectively.
	spansAccountedFor         int64
	batchResponseAccountedFor int64
	reqsScratchAccountedFor   int64

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

func makeTxnKVFetcherDefaultSendFunc(txn *kv.Txn, batchRequestsIssued *int64) sendFunc {
	return func(
		ctx context.Context,
		ba *roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		res, err := txn.Send(ctx, ba)
		if err != nil {
			return nil, err.GoError()
		}
		*batchRequestsIssued++
		return res, nil
	}
}

type newTxnKVFetcherArgs struct {
	sendFn                     sendFunc
	reverse                    bool
	lockStrength               descpb.ScanLockingStrength
	lockWaitPolicy             descpb.ScanLockingWaitPolicy
	lockTimeout                time.Duration
	acc                        *mon.BoundAccount
	forceProductionKVBatchSize bool
	batchRequestsIssued        *int64
	requestAdmissionHeader     roachpb.AdmissionHeader
	responseAdmissionQ         *admission.WorkQueue
}

// newTxnKVFetcherInternal initializes a txnKVFetcher.
//
// The passed-in memory account is owned by the fetcher throughout its lifetime
// but is **not** closed - it is the caller's responsibility to close acc if it
// is non-nil.
func newTxnKVFetcherInternal(args newTxnKVFetcherArgs) *txnKVFetcher {
	f := &txnKVFetcher{
		sendFn: args.sendFn,
		// Default to BATCH_RESPONSE. The caller will override if needed.
		scanFormat:                 roachpb.BATCH_RESPONSE,
		reverse:                    args.reverse,
		lockStrength:               GetKeyLockingStrength(args.lockStrength),
		lockWaitPolicy:             getWaitPolicy(args.lockWaitPolicy),
		lockTimeout:                args.lockTimeout,
		acc:                        args.acc,
		forceProductionKVBatchSize: args.forceProductionKVBatchSize,
		requestAdmissionHeader:     args.requestAdmissionHeader,
		responseAdmissionQ:         args.responseAdmissionQ,
	}
	f.kvBatchFetcherHelper.init(f.nextBatch, args.batchRequestsIssued)
	return f
}

// setTxnAndSendFn updates the txnKVFetcher with the new txn and sendFn. txn and
// sendFn are assumed to be non-nil.
func (f *txnKVFetcher) setTxnAndSendFn(txn *kv.Txn, sendFn sendFunc) {
	f.sendFn = sendFn
	f.requestAdmissionHeader = txn.AdmissionHeader()
	f.responseAdmissionQ = txn.DB().SQLKVResponseAdmissionQ
}

// SetupNextFetch sets up the Fetcher for the next set of spans.
//
// The fetcher takes ownership of the spans slice - it can modify the slice and
// will perform the memory accounting accordingly (if acc is non-nil). The
// caller can only reuse the spans slice after the fetcher has been closed, and
// if the caller does, it becomes responsible for the memory accounting.
//
// The fetcher also takes ownership of the spanIDs slice - it can modify the
// slice, but it will **not** perform the memory accounting. It is the caller's
// responsibility to track the memory under the spanIDs slice, and the slice
// can only be reused once the fetcher has been closed. Notably, the capacity of
// the slice will not be increased by the fetcher.
//
// If spanIDs is non-nil, then it must be of the same length as spans.
//
// Batch limits can only be used if the spans are ordered.
func (f *txnKVFetcher) SetupNextFetch(
	ctx context.Context,
	spans roachpb.Spans,
	spanIDs []int,
	batchBytesLimit rowinfra.BytesLimit,
	firstBatchKeyLimit rowinfra.KeyLimit,
) error {
	f.reset(ctx)

	if firstBatchKeyLimit < 0 || (batchBytesLimit == 0 && firstBatchKeyLimit != 0) {
		// Passing firstBatchKeyLimit without batchBytesLimit doesn't make sense
		// - the only reason to not set batchBytesLimit is in order to get
		// DistSender-level parallelism, and setting firstBatchKeyLimit inhibits
		// that.
		return errors.Errorf("invalid batch limit %d (batchBytesLimit: %d)", firstBatchKeyLimit, batchBytesLimit)
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
				return errors.Errorf("unordered spans (%s %s)", spans[i-1], spans[i])
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
			return errors.Errorf("overlapping neighbor spans (%s %s)", spans[i-1], spans[i])
		}
	}

	f.batchBytesLimit = batchBytesLimit
	f.firstBatchKeyLimit = firstBatchKeyLimit

	// Account for the memory of the spans that we're taking the ownership of.
	if f.acc != nil {
		newSpansAccountedFor := spans.MemUsage()
		if err := f.acc.Grow(ctx, newSpansAccountedFor); err != nil {
			return err
		}
		f.spansAccountedFor = newSpansAccountedFor
	}

	if spanIDs != nil && len(spans) != len(spanIDs) {
		return errors.AssertionFailedf("unexpectedly non-nil spanIDs slice has a different length than spans")
	}

	// Since the fetcher takes ownership of the spans slice, we don't need to
	// perform the deep copy. Notably, the spans might be modified (when the
	// fetcher receives the resume spans), but the fetcher will always keep the
	// memory accounting up to date.
	f.spans = identifiableSpans{
		Spans:   spans,
		spanIDs: spanIDs,
	}
	if f.reverse {
		// Reverse scans receive the spans in decreasing order. Note that we
		// need to be this tricky since we're updating the spans in place.
		i, j := 0, f.spans.Len()-1
		for i < j {
			f.spans.swap(i, j)
			i++
			j--
		}
	}
	// Keep the reference to the full identifiable spans. We will never need
	// larger slices when processing the resume spans.
	f.scratchSpans = f.spans

	return nil
}

// fetch retrieves spans from the kv layer.
func (f *txnKVFetcher) fetch(ctx context.Context) error {
	ba := &roachpb.BatchRequest{}
	ba.Header.WaitPolicy = f.lockWaitPolicy
	ba.Header.LockTimeout = f.lockTimeout
	ba.Header.TargetBytes = int64(f.batchBytesLimit)
	ba.Header.MaxSpanRequestKeys = int64(f.getBatchKeyLimit())
	if buildutil.CrdbTestBuild {
		if f.scanFormat == roachpb.COL_BATCH_RESPONSE && f.indexFetchSpec == nil {
			return errors.AssertionFailedf("IndexFetchSpec not provided with COL_BATCH_RESPONSE scan format")
		}
	}
	if f.indexFetchSpec != nil {
		ba.IndexFetchSpec = f.indexFetchSpec
		// SQL operators assume that rows are always complete in
		// coldata.Batch'es, so we must use the WholeRowsOfSize option in order
		// to tell the KV layer to never split SQL rows across the
		// BatchResponses.
		ba.Header.WholeRowsOfSize = int32(f.indexFetchSpec.MaxKeysPerRow)
	}
	ba.AdmissionHeader = f.requestAdmissionHeader
	ba.Requests = spansToRequests(f.spans.Spans, f.scanFormat, f.reverse, f.lockStrength, f.reqsScratch)

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
		if err := f.acc.Resize(ctx, f.batchResponseAccountedFor, tokenFetchAllocation); err != nil {
			return err
		}
		f.batchResponseAccountedFor = tokenFetchAllocation
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
		if err := f.acc.Resize(ctx, f.batchResponseAccountedFor, returnedBytes); err != nil {
			return err
		}
		f.batchResponseAccountedFor = returnedBytes
	}
	// Do admission control after we've accounted for the response bytes.
	if br != nil && f.responseAdmissionQ != nil {
		responseAdmission := admission.WorkInfo{
			TenantID:   roachpb.SystemTenantID,
			Priority:   admissionpb.WorkPriority(f.requestAdmissionHeader.Priority),
			CreateTime: f.requestAdmissionHeader.CreateTime,
		}
		if _, err := f.responseAdmissionQ.Admit(ctx, responseAdmission); err != nil {
			return err
		}
	}

	f.batchIdx++
	f.scratchSpans.reset()
	f.alreadyFetched = true
	// Keep the reference to the requests slice in order to reuse in the future
	// after making sure to nil out the requests in order to lose references to
	// the underlying Get and Scan requests which could keep large byte slices
	// alive.
	f.reqsScratch = ba.Requests
	for i := range f.reqsScratch {
		f.reqsScratch[i] = roachpb.RequestUnion{}
	}
	if monitoring {
		reqsScratchMemUsage := requestUnionOverhead * int64(cap(f.reqsScratch))
		if err := f.acc.Resize(ctx, f.reqsScratchAccountedFor, reqsScratchMemUsage); err != nil {
			return err
		}
		f.reqsScratchAccountedFor = reqsScratchMemUsage
	}

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

func (f *txnKVFetcher) nextBatch(ctx context.Context) (resp KVBatchFetcherResponse, err error) {
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
		var batchResp []byte
		batchResp, f.remainingBatches = popBatch(f.remainingBatches)
		return KVBatchFetcherResponse{
			MoreKVs:       true,
			BatchResponse: batchResp,
			spanID:        f.curSpanID,
		}, nil
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
		var origSpan roachpb.Span
		origSpan, f.curSpanID = f.spans.pop()

		// Check whether we need to resume scanning this span.
		header := reply.Header()
		if header.NumKeys > 0 && f.scratchSpans.Len() > 0 {
			return KVBatchFetcherResponse{}, errors.Errorf(
				"span with results after resume span; it shouldn't happen given that "+
					"we're only scanning non-overlapping spans. New spans: %s",
				catalogkeys.PrettySpans(nil, f.spans.Spans, 0 /* skip */))
		}

		// Any requests that were not fully completed will have the ResumeSpan set.
		// Here we accumulate all of them.
		if resumeSpan := header.ResumeSpan; resumeSpan != nil {
			f.scratchSpans.push(*resumeSpan, f.curSpanID)
		}

		ret := KVBatchFetcherResponse{
			MoreKVs: true,
			spanID:  f.curSpanID,
		}

		switch t := reply.(type) {
		case *roachpb.ScanResponse:
			if len(t.BatchResponses) > 0 {
				ret.BatchResponse, f.remainingBatches = popBatch(t.BatchResponses)
			}
			if len(t.Rows) > 0 {
				return KVBatchFetcherResponse{}, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse using KEY_VALUES response format",
				)
			}
			if len(t.IntentRows) > 0 {
				return KVBatchFetcherResponse{}, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse with non-nil IntentRows",
				)
			}
			// Note that ret.BatchResponse might be nil when the ScanResponse is
			// empty, and the caller (the KVFetcher) will skip over it.
			return ret, nil
		case *roachpb.ReverseScanResponse:
			if len(t.BatchResponses) > 0 {
				ret.BatchResponse, f.remainingBatches = popBatch(t.BatchResponses)
			}
			if len(t.Rows) > 0 {
				return KVBatchFetcherResponse{}, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse using KEY_VALUES response format",
				)
			}
			if len(t.IntentRows) > 0 {
				return KVBatchFetcherResponse{}, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse with non-nil IntentRows",
				)
			}
			// Note that ret.BatchResponse might be nil when the ScanResponse is
			// empty, and the caller (the KVFetcher) will skip over it.
			return ret, nil
		case *roachpb.GetResponse:
			if t.IntentValue != nil {
				return KVBatchFetcherResponse{}, errors.AssertionFailedf("unexpectedly got an IntentValue back from a SQL GetRequest %v", *t.IntentValue)
			}
			if t.Value == nil {
				// Nothing found in this particular response, let's continue to the next
				// one.
				continue
			}
			f.getResponseScratch[0] = roachpb.KeyValue{Key: origSpan.Key, Value: *t.Value}
			ret.KVs = f.getResponseScratch[:]
			return ret, nil
		}
	}
	// No more responses from the last BatchRequest.
	if f.alreadyFetched {
		if f.scratchSpans.Len() == 0 {
			// If we are out of keys, we can return and we're finished with the
			// fetch.
			return KVBatchFetcherResponse{MoreKVs: false}, nil
		}
		// We have some resume spans.
		f.spans = f.scratchSpans
		if f.acc != nil {
			newSpansMemUsage := f.spans.MemUsage()
			if err := f.acc.Resize(ctx, f.spansAccountedFor, newSpansMemUsage); err != nil {
				return KVBatchFetcherResponse{}, err
			}
			f.spansAccountedFor = newSpansMemUsage
		}
	}
	// We have more work to do. Ask the KV layer to continue where it left off.
	if err := f.fetch(ctx); err != nil {
		return KVBatchFetcherResponse{}, err
	}
	// We've got more data to process, recurse and process it.
	return f.nextBatch(ctx)
}

func (f *txnKVFetcher) reset(ctx context.Context) {
	f.alreadyFetched = false
	f.batchIdx = 0
	f.responses = nil
	f.remainingBatches = nil
	f.spans = identifiableSpans{}
	f.scratchSpans = identifiableSpans{}
	// Release only the allocations made by this fetcher. Note that we're still
	// keeping the reference to reqsScratch, so we don't release the allocation
	// for it.
	f.acc.Shrink(ctx, f.batchResponseAccountedFor+f.spansAccountedFor)
	f.batchResponseAccountedFor, f.spansAccountedFor = 0, 0
}

// Close releases the resources of this txnKVFetcher.
func (f *txnKVFetcher) Close(ctx context.Context) {
	f.reset(ctx)
}

const requestUnionOverhead = int64(unsafe.Sizeof(roachpb.RequestUnion{}))

// spansToRequests converts the provided spans to the corresponding requests. If
// a span doesn't have the EndKey set, then a Get request is used for it;
// otherwise, a Scan (or ReverseScan if reverse is true) request is used with
// the provided scan format.
//
// The provided reqsScratch is reused if it has enough capacity for all spans,
// if not, a new slice is allocated.
func spansToRequests(
	spans roachpb.Spans,
	scanFormat roachpb.ScanFormat,
	reverse bool,
	keyLocking lock.Strength,
	reqsScratch []roachpb.RequestUnion,
) []roachpb.RequestUnion {
	var reqs []roachpb.RequestUnion
	if cap(reqsScratch) >= len(spans) {
		reqs = reqsScratch[:len(spans)]
	} else {
		reqs = make([]roachpb.RequestUnion, len(spans))
	}
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
			scans[curScan].req.ScanFormat = scanFormat
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
			scans[curScan].req.ScanFormat = scanFormat
			scans[curScan].req.KeyLocking = keyLocking
			scans[curScan].union.Scan = &scans[curScan].req
			reqs[i].Value = &scans[curScan].union
		}
	}
	return reqs
}

// kvBatchFetcherHelper is a small helper that extracts common logic for
// implementing some methods of the KVBatchFetcher interface related to
// observability.
type kvBatchFetcherHelper struct {
	nextBatch func(context.Context) (KVBatchFetcherResponse, error)
	atomics   struct {
		bytesRead           int64
		batchRequestsIssued *int64
	}
}

func (h *kvBatchFetcherHelper) init(
	nextBatch func(context.Context) (KVBatchFetcherResponse, error), batchRequestsIssued *int64,
) {
	h.nextBatch = nextBatch
	h.atomics.batchRequestsIssued = batchRequestsIssued
}

// NextBatch implements the KVBatchFetcher interface.
func (h *kvBatchFetcherHelper) NextBatch(ctx context.Context) (KVBatchFetcherResponse, error) {
	resp, err := h.nextBatch(ctx)
	if !resp.MoreKVs || err != nil {
		return resp, err
	}
	nBytes := len(resp.BatchResponse)
	for i := range resp.KVs {
		nBytes += len(resp.KVs[i].Key)
		nBytes += len(resp.KVs[i].Value.RawBytes)
	}
	atomic.AddInt64(&h.atomics.bytesRead, int64(nBytes))
	return resp, nil
}

// GetBytesRead implements the KVBatchFetcher interface.
func (h *kvBatchFetcherHelper) GetBytesRead() int64 {
	if h == nil {
		return 0
	}
	return atomic.LoadInt64(&h.atomics.bytesRead)
}

// GetBatchRequestsIssued implements the KVBatchFetcher interface.
func (h *kvBatchFetcherHelper) GetBatchRequestsIssued() int64 {
	if h == nil || h.atomics.batchRequestsIssued == nil {
		return 0
	}
	return atomic.LoadInt64(h.atomics.batchRequestsIssued)
}
