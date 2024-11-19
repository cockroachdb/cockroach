// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvpb

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

//go:generate go run gen/main.go --filename batch_generated.go *.pb.go

// WriteTimestamp returns the timestamps at which this request is writing. For
// non-transactional requests, this is the same as the read timestamp. For
// transactional requests, the write timestamp can be higher until commit time.
//
// This should only be called after SetActiveTimestamp().
func (h Header) WriteTimestamp() hlc.Timestamp {
	ts := h.Timestamp
	if h.Txn != nil {
		ts.Forward(h.Txn.WriteTimestamp)
	}
	return ts
}

// RequiredFrontier returns the largest timestamp at which the request may read
// values when performing a read-only operation. For non-transactional requests,
// this is the batch timestamp. For transactional requests, this is the maximum
// of the transaction's read timestamp, its write timestamp, and its global
// uncertainty limit.
func (h Header) RequiredFrontier() hlc.Timestamp {
	if h.Txn != nil {
		return h.Txn.RequiredFrontier()
	}
	return h.Timestamp
}

// ShallowCopy returns a shallow copy of the receiver.
func (ba *BatchRequest) ShallowCopy() *BatchRequest {
	shallowCopy := *ba
	return &shallowCopy
}

// SetActiveTimestamp sets the correct timestamp at which the request is to be
// carried out. For transactional requests, ba.Timestamp must be zero initially
// and it will be set to txn.ReadTimestamp (note though this mostly impacts
// reads; writes use txn.WriteTimestamp). For non-transactional requests, if no
// timestamp is specified, clock is used to create and set one.
func (ba *BatchRequest) SetActiveTimestamp(clock *hlc.Clock) error {
	if txn := ba.Txn; txn != nil {
		if !ba.Timestamp.IsEmpty() {
			return errors.New("transactional request must not set batch timestamp")
		}

		// The batch timestamp is the timestamp at which reads are performed. We set
		// this to the txn's read timestamp, even if the txn's provisional
		// commit timestamp has been forwarded, so that all reads within a txn
		// observe the same snapshot of the database regardless of how the
		// provisional commit timestamp evolves.
		//
		// Note that writes will be performed at the provisional commit timestamp,
		// txn.WriteTimestamp, regardless of the batch timestamp.
		ba.Timestamp = txn.ReadTimestamp
	} else {
		// When not transactional, allow empty timestamp and use clock instead.
		if ba.Timestamp.IsEmpty() {
			now := clock.NowAsClockTimestamp()
			ba.Timestamp = now.ToTimestamp() // copies value, not aliasing reference
			ba.TimestampFromServerClock = &now
		}
	}
	return nil
}

// EarliestActiveTimestamp returns the earliest timestamp at which the batch
// would operate, which is nominally ba.Timestamp but could be earlier if a
// request in the batch operates on a time span such as ExportRequest or
// RevertRangeRequest, which both specify the start of that span in their
// arguments while using ba.Timestamp to indicate the upper bound of that span.
func (ba *BatchRequest) EarliestActiveTimestamp() hlc.Timestamp {
	ts := ba.Timestamp
	for _, ru := range ba.Requests {
		switch t := ru.GetInner().(type) {
		case *ExportRequest:
			if !t.StartTime.IsEmpty() {
				// NB: StartTime.Next() because StartTime is exclusive.
				ts.Backward(t.StartTime.Next())
			}
		case *RevertRangeRequest:
			// This method is only used to check GC Threshold so Revert requests that
			// opt-out of checking the target vs threshold should skip this.
			if !t.IgnoreGcThreshold {
				ts.Backward(t.TargetTime)
			}
		case *RefreshRequest:
			// A Refresh request needs to observe all MVCC versions between its
			// exclusive RefreshFrom time and its inclusive RefreshTo time. If it were
			// to permit MVCC GC between these times then it could miss conflicts that
			// should cause the refresh to fail. This could in turn lead to violations
			// of serializability. For example:
			//
			//  txn1 reads value k1@10
			//  txn2 deletes (tombstones) k1@15
			//  mvcc gc @ 20 clears versions k1@10 and k1@15
			//  txn1 refreshes @ 25, sees no value between (10, 25], refresh successful
			//
			// In the example, the refresh erroneously succeeds because the request is
			// permitted to evaluate after part of the MVCC history it needs to read
			// has been GCed. By considering the RefreshFrom time to be the earliest
			// active timestamp of the request, we avoid this hazard. Instead of being
			// allowed to evaluate, the refresh request in the example would have hit
			// a BatchTimestampBeforeGCError.
			//
			// NB: RefreshFrom.Next() because RefreshFrom is exclusive.
			ts.Backward(t.RefreshFrom.Next())
		case *RefreshRangeRequest:
			// The same requirement applies to RefreshRange request.
			ts.Backward(t.RefreshFrom.Next())
		}
	}
	return ts
}

// UpdateTxn updates the batch transaction from the supplied one in
// a copy-on-write fashion, i.e. without mutating an existing
// Transaction struct.
func (ba *BatchRequest) UpdateTxn(o *roachpb.Transaction) {
	if o == nil {
		return
	}
	o.AssertInitialized(context.TODO())
	if ba.Txn == nil {
		ba.Txn = o
		return
	}
	clonedTxn := ba.Txn.Clone()
	clonedTxn.Update(o)
	ba.Txn = clonedTxn
}

// AppliesTimestampCache returns whether the command is a write that applies the
// timestamp cache (and closed timestamp), possibly pushing its write timestamp
// into the future to avoid re-writing history.
func (ba *BatchRequest) AppliesTimestampCache() bool {
	return ba.hasFlag(appliesTSCache)
}

// IsAdmin returns true iff the BatchRequest contains an admin request.
func (ba *BatchRequest) IsAdmin() bool {
	return ba.hasFlag(isAdmin)
}

// IsWrite returns true iff the BatchRequest contains a write.
func (ba *BatchRequest) IsWrite() bool {
	return ba.hasFlag(isWrite)
}

// IsReadOnly returns true if all requests within are read-only.
func (ba *BatchRequest) IsReadOnly() bool {
	return len(ba.Requests) > 0 && !ba.hasFlag(isWrite|isAdmin)
}

// IsReverse returns true iff the BatchRequest contains a reverse request.
func (ba *BatchRequest) IsReverse() bool {
	return ba.hasFlag(isReverse)
}

// IsTransactional returns true iff the BatchRequest contains requests that can
// be part of a transaction.
func (ba *BatchRequest) IsTransactional() bool {
	return ba.hasFlag(isTxn)
}

// IsAllTransactional returns true iff the BatchRequest contains only requests
// that can be part of a transaction.
func (ba *BatchRequest) IsAllTransactional() bool {
	return ba.hasFlagForAll(isTxn)
}

// IsLocking returns true iff the BatchRequest intends to acquire locks.
func (ba *BatchRequest) IsLocking() bool {
	return ba.hasFlag(isLocking)
}

// IsIntentWrite returns true iff the BatchRequest contains an intent write.
func (ba *BatchRequest) IsIntentWrite() bool {
	return ba.hasFlag(isIntentWrite)
}

// IsUnsplittable returns true iff the BatchRequest an un-splittable request.
func (ba *BatchRequest) IsUnsplittable() bool {
	return ba.hasFlag(isUnsplittable)
}

// IsSingleRequest returns true iff the BatchRequest contains a single request.
func (ba *BatchRequest) IsSingleRequest() bool {
	return len(ba.Requests) == 1
}

// IsSingleBarrierRequest returns true iff the batch contains a single request,
// and that request is a Barrier.
func (ba *BatchRequest) IsSingleBarrierRequest() bool {
	return ba.isSingleRequestWithMethod(Barrier)
}

// IsSingleSkipsLeaseCheckRequest returns true iff the batch contains a single
// request, and that request has the skipsLeaseCheck flag set.
func (ba *BatchRequest) IsSingleSkipsLeaseCheckRequest() bool {
	return ba.IsSingleRequest() && ba.hasFlag(skipsLeaseCheck)
}

func (ba *BatchRequest) isSingleRequestWithMethod(m Method) bool {
	return ba.IsSingleRequest() && ba.Requests[0].GetInner().Method() == m
}

// IsSingleRequestLeaseRequest returns true iff the batch contains a single
// request, and that request is a RequestLease. Note that TransferLease requests
// return false.
// RequestLease requests are special because they're the only type of requests a
// non-lease-holder can propose.
func (ba *BatchRequest) IsSingleRequestLeaseRequest() bool {
	return ba.isSingleRequestWithMethod(RequestLease)
}

// IsSingleTransferLeaseRequest returns true iff the batch contains a single
// request, and that request is a TransferLease.
func (ba *BatchRequest) IsSingleTransferLeaseRequest() bool {
	return ba.isSingleRequestWithMethod(TransferLease)
}

// IsSingleLeaseInfoRequest returns true iff the batch contains a single
// request, and that request is a LeaseInfoRequest.
func (ba *BatchRequest) IsSingleLeaseInfoRequest() bool {
	return ba.isSingleRequestWithMethod(LeaseInfo)
}

// IsSingleProbeRequest returns true iff the batch is a single
// Probe request.
func (ba *BatchRequest) IsSingleProbeRequest() bool {
	return ba.isSingleRequestWithMethod(Probe)
}

// IsSinglePushTxnRequest returns true iff the batch contains a single
// request, and that request is a PushTxn.
func (ba *BatchRequest) IsSinglePushTxnRequest() bool {
	return ba.isSingleRequestWithMethod(PushTxn)
}

// IsSingleRecoverTxnRequest returns true iff the batch contains a single request,
// and that request is a RecoverTxnRequest.
func (ba *BatchRequest) IsSingleRecoverTxnRequest() bool {
	return ba.isSingleRequestWithMethod(RecoverTxn)
}

// IsSingleHeartbeatTxnRequest returns true iff the batch contains a single
// request, and that request is a HeartbeatTxn.
func (ba *BatchRequest) IsSingleHeartbeatTxnRequest() bool {
	return ba.isSingleRequestWithMethod(HeartbeatTxn)
}

// IsSingleEndTxnRequest returns true iff the batch contains a single request,
// and that request is an EndTxnRequest.
func (ba *BatchRequest) IsSingleEndTxnRequest() bool {
	return ba.isSingleRequestWithMethod(EndTxn)
}

// Require1PC returns true if the batch contains an EndTxn with the Require1PC
// flag set.
func (ba *BatchRequest) Require1PC() bool {
	arg, ok := ba.GetArg(EndTxn)
	if !ok {
		return false
	}
	etArg := arg.(*EndTxnRequest)
	return etArg.Require1PC
}

// RequiresClosedTSOlderThanStorageSnapshot returns true if the batch contains a
// request that needs to read a replica's closed timestamp that is older than
// the state of the storage snapshot the request is evaluating over.
//
// NB: This is only used by QueryResolvedTimestampRequest at the moment.
func (ba *BatchRequest) RequiresClosedTSOlderThanStorageSnapshot() bool {
	return ba.hasFlag(requiresClosedTSOlderThanStorageSnapshot)
}

// IsSingleAbortTxnRequest returns true iff the batch contains a single request,
// and that request is an EndTxnRequest(commit=false).
func (ba *BatchRequest) IsSingleAbortTxnRequest() bool {
	if ba.isSingleRequestWithMethod(EndTxn) {
		return !ba.Requests[0].GetInner().(*EndTxnRequest).Commit
	}
	return false
}

// IsSingleCommitRequest returns true iff the batch contains a single request,
// and that request is an EndTxnRequest(commit=true).
func (ba *BatchRequest) IsSingleCommitRequest() bool {
	if ba.isSingleRequestWithMethod(EndTxn) {
		return ba.Requests[0].GetInner().(*EndTxnRequest).Commit
	}
	return false
}

// IsSingleRefreshRequest returns true iff the batch contains a single request,
// and that request is a RefreshRequest.
func (ba *BatchRequest) IsSingleRefreshRequest() bool {
	return ba.isSingleRequestWithMethod(Refresh)
}

// IsSingleSubsumeRequest returns true iff the batch contains a single request,
// and that request is an SubsumeRequest.
func (ba *BatchRequest) IsSingleSubsumeRequest() bool {
	return ba.isSingleRequestWithMethod(Subsume)
}

// IsSingleComputeChecksumRequest returns true iff the batch contains a single
// request, and that request is a ComputeChecksumRequest.
func (ba *BatchRequest) IsSingleComputeChecksumRequest() bool {
	return ba.isSingleRequestWithMethod(ComputeChecksum)
}

// IsSingleCheckConsistencyRequest returns true iff the batch contains a single
// request, and that request is a CheckConsistencyRequest.
func (ba *BatchRequest) IsSingleCheckConsistencyRequest() bool {
	return ba.isSingleRequestWithMethod(CheckConsistency)
}

// IsSingleExportRequest returns true iff the batch contains a single
// request, and that request is an ExportRequest.
func (ba *BatchRequest) IsSingleExportRequest() bool {
	return ba.isSingleRequestWithMethod(Export)
}

// IsSingleAddSSTableRequest returns true iff the batch contains a single
// request, and that request is an ExportRequest.
func (ba *BatchRequest) IsSingleAddSSTableRequest() bool {
	return ba.isSingleRequestWithMethod(AddSSTable)
}

// RequiresConsensus returns true iff the batch contains a request that should
// always force replication and proposal through raft, even if evaluation is
// a no-op. The Barrier request requires consensus even though its evaluation
// is a no-op.
func (ba *BatchRequest) RequiresConsensus() bool {
	return ba.IsSingleBarrierRequest() || ba.IsSingleProbeRequest()
}

// IsCompleteTransaction determines whether a batch contains every write in a
// transactions.
func (ba *BatchRequest) IsCompleteTransaction() bool {
	et, hasET := ba.GetArg(EndTxn)
	if !hasET || !et.(*EndTxnRequest).Commit {
		return false
	}
	maxSeq := et.Header().Sequence
	switch maxSeq {
	case 0:
		// If the batch isn't using sequence numbers,
		// assume that it is not a complete transaction.
		return false
	case 1:
		// The transaction performed no writes.
		return true
	}
	if int(maxSeq) > len(ba.Requests) {
		// Fast-path.
		return false
	}
	// Check whether any sequence numbers were skipped between 1 and the
	// EndTxn's sequence number. A Batch is only a complete transaction
	// if it contains every write that the transaction performed.
	// Sequence numbers are assigned to requests in txnSeqNumAllocator.
	nextSeq := enginepb.TxnSeq(1)
	for _, args := range ba.Requests {
		req := args.GetInner()
		seq := req.Header().Sequence
		if seq > nextSeq {
			return false
		}
		if seq == nextSeq {
			if !IsIntentWrite(req) {
				return false
			}
			nextSeq++
			if nextSeq == maxSeq {
				return true
			}
		}
	}
	// Unreachable.
	panic(fmt.Sprintf("unreachable. Batch requests: %s", TruncatedRequestsString(ba.Requests, 1024)))
}

// hasFlag returns true iff at least one of the requests within the batch
// contains at least one of the specified flags.
func (ba *BatchRequest) hasFlag(flags flag) bool {
	for _, union := range ba.Requests {
		if (union.GetInner().flags() & flags) != 0 {
			return true
		}
	}
	return false
}

// hasFlagForAll returns true iff all of the requests within the batch contain
// all of the specified flags.
func (ba *BatchRequest) hasFlagForAll(flags flag) bool {
	if len(ba.Requests) == 0 {
		return false
	}
	for _, union := range ba.Requests {
		if (union.GetInner().flags() & flags) != flags {
			return false
		}
	}
	return true
}

// GetArg returns a request of the given type if one is contained in the
// Batch. The request returned is the first of its kind, with the exception
// of EndTxn, where it examines the very last request only.
func (ba *BatchRequest) GetArg(method Method) (Request, bool) {
	// when looking for EndTxn, just look at the last entry.
	if method == EndTxn {
		if length := len(ba.Requests); length > 0 {
			if req := ba.Requests[length-1].GetInner(); req.Method() == EndTxn {
				return req, true
			}
		}
		return nil, false
	}

	for _, arg := range ba.Requests {
		if req := arg.GetInner(); req.Method() == method {
			return req, true
		}
	}
	return nil, false
}

func (br *BatchResponse) String() string {
	var str []string
	str = append(str, fmt.Sprintf("(err: %v)", br.Error))
	for count, union := range br.Responses {
		// Limit the strings to provide just a summary. Without this limit a log
		// message with a BatchResponse can be very long.
		if count >= 20 && count < len(br.Responses)-5 {
			if count == 20 {
				str = append(str, fmt.Sprintf("... %d skipped ...", len(br.Responses)-25))
			}
			continue
		}
		str = append(str, fmt.Sprintf("%T", union.GetInner()))
	}
	return strings.Join(str, ", ")
}

// LockSpanIterate calls LockSpanIterate for each request in the batch.
func (ba *BatchRequest) LockSpanIterate(
	br *BatchResponse, fn func(roachpb.Span, lock.Durability),
) error {
	for i, arg := range ba.Requests {
		req := arg.GetInner()
		var resp Response
		if br != nil {
			resp = br.Responses[i].GetInner()
		}
		if err := LockSpanIterate(req, resp, fn); err != nil {
			return err
		}
	}
	return nil
}

// LockSpanIterate calls the passed function with the keys or key spans of the
// transactional locks acquired by the request. Usually the full key span that
// is addressed by the request is used. However, a more minimal span of keys can
// be provided in the following cases:
//   - the request operates over a range of keys but the response includes the
//     specific set of keys that were locked. In these cases, the provided
//     function is called with each individual locked key.
//   - the request operates over a range of keys but the response contains a
//     ResumeSpan signifying the key spans not reached by the request. In these
//     cases, the ResumeSpan is subtracted from the request span.
func LockSpanIterate(req Request, resp Response, fn func(roachpb.Span, lock.Durability)) error {
	if !IsLocking(req) {
		return nil
	}
	dur := LockingDurability(req)
	if canIterateResponseKeys(req, resp) {
		return ResponseKeyIterate(req, resp, func(key roachpb.Key) {
			fn(roachpb.Span{Key: key}, dur)
		})
	}
	if span, ok := actualSpan(req, resp); ok {
		fn(span, dur)
	}
	return nil
}

// RefreshSpanIterate calls the passed function with the key spans of
// requests in the batch which need to be refreshed. These requests
// must be checked via Refresh/RefreshRange to avoid having to restart
// a SERIALIZABLE transaction. Usually the key spans contained in the
// requests are used, but when a response contains a ResumeSpan the
// ResumeSpan is subtracted from the request span to provide a more
// minimal span of keys affected by the request. The supplied function
// is called with each span.
func (ba *BatchRequest) RefreshSpanIterate(br *BatchResponse, fn func(roachpb.Span)) error {
	for i, arg := range ba.Requests {
		req := arg.GetInner()
		if !NeedsRefresh(req) {
			continue
		}
		var resp Response
		if br != nil {
			resp = br.Responses[i].GetInner()
		}
		if ba.WaitPolicy == lock.WaitPolicy_SkipLocked && CanSkipLocked(req) && resp != nil {
			if ba.IndexFetchSpec != nil {
				return errors.AssertionFailedf("unexpectedly IndexFetchSpec is set with SKIP LOCKED wait policy")
			}
			// If the request is using a SkipLocked wait policy, it behaves as if run
			// at a lower isolation level for any keys that it skips over. For this
			// reason, the request only adds point reads for the individual keys
			// returned to the timestamp cache, as opposed to adding an entry across
			// the entire read span. See Replica.updateTimestampCache.
			//
			// For the same reason, the request only records refresh spans for the
			// individual keys returned, instead of recording a refresh span across
			// the entire read span. Because the issuing transaction is not intending
			// to enforce serializable isolation across keys that were skipped by this
			// request, it does not need to validate that they have not changed if the
			// transaction ever needs to refresh.
			if err := ResponseKeyIterate(req, resp, func(k roachpb.Key) {
				fn(roachpb.Span{Key: k})
			}); err != nil {
				return err
			}
		} else {
			// Otherwise, call the function with the span which was operated on.
			if span, ok := actualSpan(req, resp); ok {
				fn(span)
			}
		}
	}
	return nil
}

// actualSpan returns the actual request span which was operated on,
// according to the existence of a resume span in the response. If
// nothing was operated on, returns false.
func actualSpan(req Request, resp Response) (roachpb.Span, bool) {
	h := req.Header()
	if resp != nil {
		resumeSpan := resp.Header().ResumeSpan
		// If a resume span exists we need to cull the span.
		if resumeSpan != nil {
			// Handle the reverse case first.
			if bytes.Equal(resumeSpan.Key, h.Key) {
				if bytes.Equal(resumeSpan.EndKey, h.EndKey) {
					return roachpb.Span{}, false
				}
				return roachpb.Span{Key: resumeSpan.EndKey, EndKey: h.EndKey}, true
			}
			// The forward case.
			return roachpb.Span{Key: h.Key, EndKey: resumeSpan.Key}, true
		}
	}
	return h.Span(), true
}

// canIterateResponseKeys returns whether the response to the given request
// contains keys that can be iterated over using ResponseKeyIterate.
func canIterateResponseKeys(req Request, resp Response) bool {
	if resp == nil {
		return false
	}
	switch v := req.(type) {
	case *GetRequest:
		return true
	case *ScanRequest:
		return v.ScanFormat != COL_BATCH_RESPONSE
	case *ReverseScanRequest:
		return v.ScanFormat != COL_BATCH_RESPONSE
	case *DeleteRangeRequest:
		return v.ReturnKeys
	}
	return false
}

// ResponseKeyIterate calls the passed function with the keys returned
// in the provided request's response. If no keys are being returned,
// the function will not be called.
// NOTE: it is assumed that req (if it is a Scan or a ReverseScan) didn't use
// COL_BATCH_RESPONSE scan format.
func ResponseKeyIterate(req Request, resp Response, fn func(roachpb.Key)) error {
	if resp == nil {
		return errors.Errorf("cannot iterate over response keys of %s request with nil response", req.Method())
	}
	switch v := resp.(type) {
	case *GetResponse:
		if v.Value != nil {
			fn(req.Header().Key)
		}
	case *ScanResponse:
		// If ScanFormat == KEY_VALUES.
		for _, kv := range v.Rows {
			fn(kv.Key)
		}
		// If ScanFormat == BATCH_RESPONSE.
		if err := enginepb.ScanDecodeKeyValues(v.BatchResponses, func(key []byte, _ hlc.Timestamp, _ []byte) error {
			// Clone the key to avoid handing out slices that directly point into the
			// BatchResponses buffer, which contains the response's keys and values.
			// This guards against callers which store the response keys in a data
			// structure accidentally retaining long-lasting references to this
			// response's underlying result buffer and preventing it from being GCed.
			//
			// This is not a concern for other scan formats because keys and values
			// are already separate heap allocations.
			fn(roachpb.Key(key).Clone())
			return nil
		}); err != nil {
			return err
		}
		if buildutil.CrdbTestBuild {
			// COL_BATCH_RESPONSE scan format is not supported. The caller is
			// responsible for ensuring this, so this assertion is hidden behind
			// the test-build flag.
			if req.(*ScanRequest).ScanFormat == COL_BATCH_RESPONSE {
				return errors.AssertionFailedf(
					"unexpectedly called ResponseKeyIterate on a " +
						"ScanRequest with COL_BATCH_RESPONSE scan format",
				)
			}
			if len(v.ColBatches.ColBatches) > 0 {
				return errors.AssertionFailedf("unexpectedly non-empty ColBatches")
			}
		}
	case *ReverseScanResponse:
		// If ScanFormat == KEY_VALUES.
		for _, kv := range v.Rows {
			fn(kv.Key)
		}
		// If ScanFormat == BATCH_RESPONSE.
		if err := enginepb.ScanDecodeKeyValues(v.BatchResponses, func(key []byte, _ hlc.Timestamp, _ []byte) error {
			// Same explanation as above.
			fn(roachpb.Key(key).Clone())
			return nil
		}); err != nil {
			return err
		}
		if buildutil.CrdbTestBuild {
			// COL_BATCH_RESPONSE scan format is not supported. The caller is
			// responsible for ensuring this, so this assertion is hidden behind
			// the test-build flag.
			if req.(*ReverseScanRequest).ScanFormat == COL_BATCH_RESPONSE {
				return errors.AssertionFailedf(
					"unexpectedly called ResponseKeyIterate on a " +
						"ReverseScanRequest with COL_BATCH_RESPONSE scan format",
				)
			}
			if len(v.ColBatches.ColBatches) > 0 {
				return errors.AssertionFailedf("unexpectedly non-empty ColBatches")
			}
		}
	case *DeleteRangeResponse:
		if !req.(*DeleteRangeRequest).ReturnKeys {
			return errors.AssertionFailedf("cannot iterate over response keys of DeleteRange " +
				"request when ReturnKeys=false")
		}
		for _, k := range v.Keys {
			fn(k)
		}
	default:
		return errors.Errorf("cannot iterate over response keys of %s request", req.Method())
	}
	return nil
}

// Combine combines each slot of the given request into the corresponding slot
// of the base response. The number of slots must be equal and the respective
// slots must be combinable.
// On error, the receiver BatchResponse is in an invalid state. In either case,
// the supplied BatchResponse must not be used any more.
// It is an error to call Combine on responses with errors in them. The
// DistSender strips the errors from any responses that it combines.
func (br *BatchResponse) Combine(
	ctx context.Context, otherBatch *BatchResponse, positions []int, ba *BatchRequest,
) error {
	if err := br.BatchResponse_Header.combine(otherBatch.BatchResponse_Header); err != nil {
		return err
	}
	for i := range otherBatch.Responses {
		pos := positions[i]
		if br.Responses[pos] == (ResponseUnion{}) {
			br.Responses[pos] = otherBatch.Responses[i]
			continue
		}
		valLeft := br.Responses[pos].GetInner()
		valRight := otherBatch.Responses[i].GetInner()
		if err := CombineResponses(ctx, valLeft, valRight, ba); err != nil {
			return err
		}
	}
	return nil
}

// Add adds a request to the batch request. It's a convenience method;
// requests may also be added directly into the slice.
func (ba *BatchRequest) Add(requests ...Request) {
	for _, args := range requests {
		ba.Requests = append(ba.Requests, RequestUnion{})
		ba.Requests[len(ba.Requests)-1].MustSetInner(args)
	}
}

// Add adds a response to the batch response. It's a convenience method;
// responses may also be added directly.
func (br *BatchResponse) Add(reply Response) {
	br.Responses = append(br.Responses, ResponseUnion{})
	br.Responses[len(br.Responses)-1].MustSetInner(reply)
}

// Methods returns a slice of the contained methods.
func (ba *BatchRequest) Methods() []Method {
	var res []Method
	for _, arg := range ba.Requests {
		res = append(res, arg.GetInner().Method())
	}
	return res
}

// Split separates the requests contained in a batch so that each subset of
// requests can be executed by a Store (without changing order). In particular,
// Admin requests are always singled out and mutating requests separated from
// reads. The boolean parameter indicates whether EndTxn should be
// special-cased: If false, an EndTxn request will never be split into a new
// chunk (otherwise, it is treated according to its flags). This allows sending
// a whole transaction in a single Batch when addressing a single range.
//
// NOTE: One reason for splitting reads from writes is that write-only batches
// can sometimes have their read timestamp bumped on the server, which doesn't
// work for read requests due to how the timestamp-aware latching works (i.e. a
// read that acquired a latch @ ts10 can't simply be bumped to ts 20 because
// there might have been overlapping writes in the 10..20 window).
func (ba *BatchRequest) Split(canSplitET bool) [][]RequestUnion {
	compatible := func(exFlags, newFlags flag) bool {
		// isAlone requests are never compatible.
		if (exFlags&isAlone) != 0 || (newFlags&isAlone) != 0 {
			return false
		}
		// If the current or new flags are empty and neither include isAlone,
		// everything goes.
		if exFlags == 0 || newFlags == 0 {
			return true
		}
		// Otherwise, the flags below must remain the same with the new
		// request added.
		//
		// Note that we're not checking isRead: The invariants we're
		// enforcing are that a batch can't mix non-writes with writes.
		// Checking isRead would cause ConditionalPut and Put to conflict,
		// which is not what we want.
		mask := isWrite | isAdmin
		if (exFlags&isRange) != 0 && (newFlags&isRange) != 0 {
			// The directions of requests in a batch need to be the same because
			// the DistSender (in divideAndSendBatchToRanges) will perform a
			// single traversal of all requests while advancing the
			// RangeIterator. If we were to have requests with different
			// directions in a single batch, the DistSender wouldn't know how to
			// route the requests (without incurring a noticeable performance
			// hit).
			//
			// At the same time, non-ranged operations are not directional, so
			// we need to require the same value for isReverse flag iff the
			// existing and the new requests are ranged. For example, this
			// allows us to have Gets and ReverseScans in a single batch.
			mask |= isReverse
		}
		return (mask & exFlags) == (mask & newFlags)
	}
	reqs := ba.Requests
	var parts [][]RequestUnion
	for len(reqs) > 0 {
		part := reqs
		var gFlags, hFlags flag = -1, -1
		for i, union := range reqs {
			args := union.GetInner()
			flags := args.flags()
			method := args.Method()
			if (flags & isPrefix) != 0 {
				// Requests with the isPrefix flag want to be grouped with the
				// next non-header request in a batch. Scan forward and find
				// first non-header request. Naively, this would result in
				// quadratic behavior for repeat isPrefix requests. We avoid
				// this by caching first non-header request's flags in hFlags.
				if hFlags == -1 {
					for _, nUnion := range reqs[i+1:] {
						nArgs := nUnion.GetInner()
						nFlags := nArgs.flags()
						nMethod := nArgs.Method()
						if !canSplitET && nMethod == EndTxn {
							nFlags = 0 // always compatible
						}
						if (nFlags & isPrefix) == 0 {
							hFlags = nFlags
							break
						}
					}
				}
				if hFlags != -1 && (hFlags&isAlone) == 0 {
					flags = hFlags
				}
			} else {
				hFlags = -1 // reset
			}
			cmpFlags := flags
			if !canSplitET && method == EndTxn {
				cmpFlags = 0 // always compatible
			}
			if gFlags == -1 {
				// If no flags are set so far, everything goes.
				gFlags = flags
			} else {
				if !compatible(gFlags, cmpFlags) {
					part = reqs[:i]
					break
				}
				gFlags |= flags
			}
		}
		parts = append(parts, part)
		reqs = reqs[len(part):]
	}
	return parts
}

// SafeFormat implements redact.SafeFormatter.
// It gives a brief summary of the contained requests and keys in the batch.
func (ba *BatchRequest) SafeFormat(s redact.SafePrinter, verb rune) {
	for count, arg := range ba.Requests {
		// Limit the strings to provide just a summary. Without this limit
		// a log message with a BatchRequest can be very long.
		if count >= 20 && count < len(ba.Requests)-5 {
			if count == 20 {
				s.Printf(",... %d skipped ...", len(ba.Requests)-25)
			}
			continue
		}
		if count > 0 {
			s.Print(redact.SafeString(", "))
		}

		req := arg.GetInner()
		if safeFormatterReq, ok := req.(SafeFormatterRequest); ok {
			safeFormatterReq.SafeFormat(s, verb)
		} else {
			s.Print(req.Method())
		}
		h := req.Header()
		if len(h.EndKey) > 0 {
			s.Printf(" [%s,%s)", h.Key, h.EndKey)
		} else {
			s.Printf(" [%s]", h.Key)
		}
	}
	{
		if ba.Txn != nil {
			s.Printf(", [txn: %s]", ba.Txn.Short())
		}
	}
	if ba.WaitPolicy != lock.WaitPolicy_Block {
		s.Printf(", [wait-policy: %s]", ba.WaitPolicy)
	}
	if ba.LockTimeout != 0 {
		s.Printf(", [lock-timeout: %s]", ba.LockTimeout)
	}
	if ba.DeadlockTimeout != 0 {
		s.Printf(", [deadlock-timeout: %s]", ba.DeadlockTimeout)
	}
	if ba.AmbiguousReplayProtection {
		s.Printf(", [protect-ambiguous-replay]")
	}
	if ba.CanForwardReadTimestamp {
		s.Printf(", [can-forward-ts]")
	}
	if cfg := ba.BoundedStaleness; cfg != nil {
		s.Printf(", [bounded-staleness, min_ts_bound: %s", cfg.MinTimestampBound)
		if cfg.MinTimestampBoundStrict {
			s.Printf(", min_ts_bound_strict")
		}
		if !cfg.MaxTimestampBound.IsEmpty() {
			s.Printf(", max_ts_bound: %s", cfg.MaxTimestampBound)
		}
		s.Printf("]")
	}
	if ba.MaxSpanRequestKeys != 0 || ba.TargetBytes != 0 {
		s.Printf(", [max_span_request_keys: %d], [target_bytes: %d]", ba.MaxSpanRequestKeys, ba.TargetBytes)
	}
	if ba.AllowEmpty {
		s.Print(", [allow-empty]")
	}
}

func (ba *BatchRequest) String() string {
	return redact.StringWithoutMarkers(ba)
}

// ValidateForEvaluation performs sanity checks on the batch when it's received
// by the "server" for evaluation.
func (ba *BatchRequest) ValidateForEvaluation() error {
	if ba.RangeID == 0 {
		return errors.AssertionFailedf("batch request missing range ID")
	} else if ba.Replica.StoreID == 0 {
		return errors.AssertionFailedf("batch request missing store ID")
	}
	if _, ok := ba.GetArg(EndTxn); ok && ba.Txn == nil {
		return errors.AssertionFailedf("EndTxn request without transaction")
	}
	if ba.Txn != nil {
		if ba.Txn.WriteTooOld && ba.Txn.ReadTimestamp == ba.Txn.WriteTimestamp {
			return errors.AssertionFailedf("WriteTooOld set but no offset in timestamps. txn: %s", ba.Txn)
		}
	}
	return nil
}

func (cb ColBatches) Size() int {
	var size int
	for _, b := range cb.ColBatches {
		size += int(coldata.GetBatchMemSize(b))
	}
	return size
}

func (ColBatches) MarshalToSizedBuffer([]byte) (int, error) { return 0, nil }

func (ColBatches) Unmarshal(b []byte) error {
	if buildutil.CrdbTestBuild {
		if len(b) > 0 {
			return errors.AssertionFailedf("unexpectedly unmarshaling a non-empty ColBatches")
		}
	}
	return nil
}

func (cb ColBatches) String() string {
	var sb strings.Builder
	for i, b := range cb.ColBatches {
		sb.WriteString(b.String())
		if i > 0 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}
