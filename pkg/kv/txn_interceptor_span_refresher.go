// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

const (
	// maxTxnRefreshAttempts defines the maximum number of times a single
	// transactional batch can trigger a refresh spans attempt. A batch
	// may need multiple refresh attempts if it runs into progressively
	// larger timestamps as more and more of its component requests are
	// executed.
	maxTxnRefreshAttempts = 5
)

// MaxTxnRefreshSpansBytes is a threshold in bytes for refresh spans stored
// on the coordinator during the lifetime of a transaction. Refresh spans
// are used for SERIALIZABLE transactions to avoid client restarts.
var MaxTxnRefreshSpansBytes = settings.RegisterIntSetting(
	"kv.transaction.max_refresh_spans_bytes",
	"maximum number of bytes used to track refresh spans in serializable transactions",
	256*1000,
)

// txnSpanRefresher is a txnInterceptor that collects the read spans of a
// serializable transaction in the event it gets a serializable retry error. It
// can then use the set of read spans to avoid retrying the transaction if all
// the spans can be updated to the current transaction timestamp.
//
// Serializable isolation mandates that transactions appear to have occurred in
// some total order, where none of their component sub-operations appear to have
// interleaved with sub-operations from other transactions. CockroachDB enforces
// this isolation level by ensuring that all of a transaction's reads and writes
// are performed at the same HLC timestamp. This timestamp is referred to as the
// transaction's commit timestamp.
//
// As a transaction in CockroachDB executes at a certain provisional commit
// timestamp, it lays down intents at this timestamp for any write operations
// and ratchets various timestamp cache entries to this timestamp for any read
// operations. If a transaction performs all of its reads and writes and is able
// to commit at its original provisional commit timestamp then it may go ahead
// and do so. However, for a number of reasons including conflicting reads and
// writes, a transaction may discover that its provisional commit timestamp is
// too low and that it needs to move this timestamp forward to commit.
//
// This poses a problem for operations that the transaction has already
// completed at lower timestamps. Are the effects of these operations still
// valid? The transaction is always free to perform a full restart at a higher
// epoch, but this often requires iterating in a client-side retry loop and
// performing all of the transaction's operations again. Intents are maintained
// across retries to improve the chance that later epochs succeed, but it is
// vastly preferable to avoid re-issuing these operations. Instead, it would be
// ideal if the transaction could "move" each of its operations to its new
// provisional commit timestamp without redoing them entirely.
//
// Only a single write intent can exist on a key and no reads are allowed above
// the intent's timestamp until the intent is resolved, so a transaction is free
// to move any of its intent to a higher timestamp. In fact, a synchronous
// rewrite of these intents isn't even necessary because intent resolution will
// already rewrite the intents at higher timestamp if necessary. So, moving
// write intents to a higher timestamp can be performed implicitly by committing
// their transaction at a higher timestamp. However, unlike intents created by
// writes, timestamp cache entries created by reads only prevent writes on
// overlapping keys from being written at or below their timestamp; they do
// nothing to prevent writes on overlapping keys from being written above their
// timestamp. This means that a transaction is not free to blindly move its
// reads to a higher timestamp because writes from other transaction may have
// already invalidated them. In effect, this means that transactions acquire
// pessimistic write locks and optimistic read locks.
//
// The txnSpanRefresher is in charge of detecting when a transaction may want to
// move its provisional commit timestamp forward and determining whether doing
// so is safe given the reads that it has performed (i.e. its "optimistic read
// locks"). When the interceptor decides to attempt to move a transaction's
// timestamp forward, it first "refreshes" each of its reads. This refreshing
// step revisits all of the key spans that the transaction has read and checks
// whether any writes have occurred between the original time that these span
// were read and the timestamp that the transaction now wants to commit at that
// change the result of these reads. If any read would produce a different
// result at the newer commit timestamp, the refresh fails and the transaction
// is forced to fall back to a full transaction restart. However, if all of the
// reads would produce exactly the same result at the newer commit timestamp,
// the timestamp cache entries for these reads are updated and the transaction
// is free to update its provisional commit timestamp without needing to
// restart.
type txnSpanRefresher struct {
	st      *cluster.Settings
	knobs   *ClientTestingKnobs
	wrapped lockedSender

	// See TxnCoordMeta.RefreshReads and TxnCoordMeta.RefreshWrites.
	refreshReads  []roachpb.Span
	refreshWrites []roachpb.Span
	// See TxnCoordMeta.RefreshInvalid.
	refreshInvalid bool
	// refreshSpansBytes is the total size in bytes of the spans
	// encountered during this transaction that need to be refreshed
	// to avoid serializable restart.
	refreshSpansBytes int64
	// refreshedTimestamp keeps track of the largest timestamp that a
	// transaction was able to refresh all of its refreshable spans at.
	// It is updated under lock and used to ensure that concurrent requests
	// don't cause the refresh spans to get out of sync.
	// See appendRefreshSpans.
	refreshedTimestamp hlc.Timestamp

	// canAutoRetry is set if the txnSpanRefresher is allowed to auto-retry.
	canAutoRetry bool
	// autoRetryCounter counts the number of auto retries which avoid
	// client-side restarts.
	autoRetryCounter *metric.Counter
}

// SendLocked implements the lockedSender interface.
func (sr *txnSpanRefresher) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if rArgs, hasET := ba.GetArg(roachpb.EndTransaction); hasET {
		et := rArgs.(*roachpb.EndTransactionRequest)
		if !sr.refreshInvalid && len(sr.refreshReads) == 0 && len(sr.refreshWrites) == 0 {
			et.NoRefreshSpans = true
		}
	}

	maxAttempts := maxTxnRefreshAttempts
	if knob := sr.knobs.MaxTxnRefreshAttempts; knob != 0 {
		if knob == -1 {
			maxAttempts = 0
		} else {
			maxAttempts = knob
		}
	}

	// Send through wrapped lockedSender. Unlocks while sending then re-locks.
	br, pErr, largestRefreshTS := sr.sendLockedWithRefreshAttempts(ctx, ba, maxAttempts)
	if pErr != nil {
		return nil, pErr
	}

	// If the transaction is no longer pending, just return without
	// attempting to record its refresh spans.
	if br.Txn.Status != roachpb.PENDING {
		return br, nil
	}

	// Iterate over and aggregate refresh spans in the requests,
	// qualified by possible resume spans in the responses, if we
	// haven't yet exceeded the max read key bytes.
	if !sr.refreshInvalid {
		ba.Txn.RefreshedTimestamp.Forward(largestRefreshTS)
		if !sr.appendRefreshSpans(ctx, ba, br) {
			// The refresh spans are out of date, return a generic client-side retry error.
			return nil, roachpb.NewErrorWithTxn(
				roachpb.NewTransactionRetryError(
					roachpb.RETRY_SERIALIZABLE, "refresh spans are out of date",
				), br.Txn,
			)
		}
	}
	// Verify and enforce the size in bytes of all read-only spans
	// doesn't exceed the max threshold.
	if sr.refreshSpansBytes > MaxTxnRefreshSpansBytes.Get(&sr.st.SV) {
		log.VEventf(ctx, 2, "refresh spans max size exceeded; clearing")
		sr.refreshReads = nil
		sr.refreshWrites = nil
		sr.refreshInvalid = true
		sr.refreshSpansBytes = 0
	}
	return br, nil
}

// sendLockedWithRefreshAttempts sends the batch through the wrapped sender. It
// catches serializable errors and attempts to avoid them by refreshing the txn
// at a larger timestamp. It returns the response, an error, and the largest
// timestamp that the request was able to refresh at.
func (sr *txnSpanRefresher) sendLockedWithRefreshAttempts(
	ctx context.Context, ba roachpb.BatchRequest, maxRefreshAttempts int,
) (_ *roachpb.BatchResponse, _ *roachpb.Error, largestRefreshTS hlc.Timestamp) {
	br, pErr := sr.wrapped.SendLocked(ctx, ba)
	if pErr != nil && maxRefreshAttempts > 0 {
		br, pErr, largestRefreshTS = sr.maybeRetrySend(ctx, ba, br, pErr, maxRefreshAttempts)
	}
	return br, pErr, largestRefreshTS
}

// maybeRetrySend attempts to catch serializable errors and avoid them by
// refreshing the txn at a larger timestamp. If it succeeds at refreshing the
// txn timestamp, it recurses into sendLockedWithRefreshAttempts and retries the
// suffix of the original batch that has not yet completed successfully.
func (sr *txnSpanRefresher) maybeRetrySend(
	ctx context.Context,
	ba roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	pErr *roachpb.Error,
	maxRefreshAttempts int,
) (*roachpb.BatchResponse, *roachpb.Error, hlc.Timestamp) {
	// Check for an error which can be retried after updating spans.
	canRetryTxn, retryTxn := roachpb.CanTransactionRetryAtRefreshedTimestamp(ctx, pErr)
	if !canRetryTxn || !sr.canAutoRetry {
		return nil, pErr, hlc.Timestamp{}
	}

	// If a prefix of the batch was executed, collect refresh spans for
	// that executed portion, and retry the remainder. The canonical
	// case is a batch split between everything up to but not including
	// the EndTransaction. Requests up to the EndTransaction succeed,
	// but the EndTransaction fails with a retryable error. We want to
	// retry only the EndTransaction.
	ba.UpdateTxn(retryTxn)
	retryBa := ba
	if br != nil {
		doneBa := ba
		doneBa.Requests = ba.Requests[:len(br.Responses)]
		log.VEventf(ctx, 2, "collecting refresh spans after partial batch execution of %s", doneBa)
		if !sr.appendRefreshSpans(ctx, doneBa, br) {
			return nil, pErr, hlc.Timestamp{}
		}
		retryBa.Requests = ba.Requests[len(br.Responses):]
	}

	log.VEventf(ctx, 2, "retrying %s at refreshed timestamp %s because of %s",
		retryBa, retryTxn.RefreshedTimestamp, pErr)

	// Try updating the txn spans so we can retry.
	if ok := sr.tryUpdatingTxnSpans(ctx, retryTxn); !ok {
		return nil, pErr, hlc.Timestamp{}
	}

	// We've refreshed all of the read spans successfully and set
	// newBa.Txn.RefreshedTimestamp to the current timestamp. Submit the
	// batch again.
	retryBr, retryErr, retryLargestRefreshTS := sr.sendLockedWithRefreshAttempts(
		ctx, retryBa, maxRefreshAttempts-1,
	)
	if retryErr != nil {
		log.VEventf(ctx, 2, "retry failed with %s", retryErr)
		return nil, retryErr, hlc.Timestamp{}
	}

	log.VEventf(ctx, 2, "retry successful @%s", retryBa.Txn.Timestamp)
	sr.autoRetryCounter.Inc(1)
	retryTxn.RefreshedTimestamp.Forward(retryLargestRefreshTS)

	// On success, combine responses if applicable and set error to nil.
	if br != nil {
		br.Responses = append(br.Responses, retryBr.Responses...)
		retryBr.CollectedSpans = append(br.CollectedSpans, retryBr.CollectedSpans...)
		br.BatchResponse_Header = retryBr.BatchResponse_Header
	} else {
		br = retryBr
	}
	return br, nil, retryTxn.RefreshedTimestamp
}

// tryUpdatingTxnSpans sends Refresh and RefreshRange commands to all spans read
// during the transaction to ensure that no writes were written more recently
// than the original transaction timestamp. All implicated timestamp caches are
// updated with the final transaction timestamp. Returns whether the refresh was
// successful or not.
func (sr *txnSpanRefresher) tryUpdatingTxnSpans(
	ctx context.Context, refreshTxn *roachpb.Transaction,
) bool {
	// Forward the refreshed timestamp under lock. This in conjunction with a
	// check in appendRefreshSpans prevents a race where a concurrent request
	// may add new refresh spans only "verified" up to its batch timestamp after
	// we've refreshed past that timestamp.
	sr.refreshedTimestamp.Forward(refreshTxn.RefreshedTimestamp)

	if sr.refreshInvalid {
		log.VEvent(ctx, 2, "can't refresh txn spans; not valid")
		return false
	} else if len(sr.refreshReads) == 0 && len(sr.refreshWrites) == 0 {
		log.VEvent(ctx, 2, "there are no txn spans to refresh")
		return true
	}

	// Refresh all spans (merge first).
	// TODO(nvanbenschoten): actually merge spans.
	refreshSpanBa := roachpb.BatchRequest{}
	refreshSpanBa.Txn = refreshTxn
	addRefreshes := func(refreshes []roachpb.Span, write bool) {
		for _, u := range refreshes {
			var req roachpb.Request
			if len(u.EndKey) == 0 {
				req = &roachpb.RefreshRequest{
					RequestHeader: roachpb.RequestHeaderFromSpan(u),
					Write:         write,
				}
			} else {
				req = &roachpb.RefreshRangeRequest{
					RequestHeader: roachpb.RequestHeaderFromSpan(u),
					Write:         write,
				}
			}
			refreshSpanBa.Add(req)
			log.VEventf(ctx, 2, "updating span %s @%s - @%s to avoid serializable restart",
				req.Header().Span(), refreshTxn.OrigTimestamp, refreshTxn.Timestamp)
		}
	}
	addRefreshes(sr.refreshReads, false)
	addRefreshes(sr.refreshWrites, true)

	// Send through wrapped lockedSender. Unlocks while sending then re-locks.
	if _, batchErr := sr.wrapped.SendLocked(ctx, refreshSpanBa); batchErr != nil {
		log.VEventf(ctx, 2, "failed to refresh txn spans (%s); propagating original retry error", batchErr)
		return false
	}

	return true
}

// appendRefreshSpans appends refresh spans from the supplied batch request,
// qualified by the batch response where appropriate.
//
// Returns whether the batch transaction's refreshed timestamp is greater or
// equal to the max refreshed timestamp used so far with this sender. In other
// words, returns false if this batch ran straddled a refresh request, in which
// case the refresh was invalid - to be valid, it should have included the this
// request's spans. In that case the caller should return an error for
// client-side retry.
// Note that batches that don't produce any refresh spans are allowed to run
// concurrently with refreshes. This is useful for the async rollbacks performed
// by the heartbeater.
func (sr *txnSpanRefresher) appendRefreshSpans(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
) bool {
	origTS := ba.Txn.OrigTimestamp
	origTS.Forward(ba.Txn.RefreshedTimestamp)
	var concurrentRefresh bool
	if origTS.Less(sr.refreshedTimestamp) {
		// We'll return an error if any of the requests generate a refresh span.
		concurrentRefresh = true
	}
	if !ba.RefreshSpanIterate(br, func(span roachpb.Span, write bool) bool {
		if concurrentRefresh {
			return false
		}
		if log.V(3) {
			log.Infof(ctx, "refresh: %s write=%t", span, write)
		}
		if write {
			sr.refreshWrites = append(sr.refreshWrites, span)
		} else {
			sr.refreshReads = append(sr.refreshReads, span)
		}
		sr.refreshSpansBytes += int64(len(span.Key) + len(span.EndKey))
		return true
	}) {
		log.VEventf(ctx, 2, "txn orig timestamp %s < sender refreshed timestamp %s",
			origTS, sr.refreshedTimestamp)
		return false
	}
	return true
}

// setWrapped implements the txnInterceptor interface.
func (sr *txnSpanRefresher) setWrapped(wrapped lockedSender) { sr.wrapped = wrapped }

// populateMetaLocked implements the txnInterceptor interface.
func (sr *txnSpanRefresher) populateMetaLocked(meta *roachpb.TxnCoordMeta) {
	meta.RefreshInvalid = sr.refreshInvalid
	if !sr.refreshInvalid {
		// Copy mutable state so access is safe for the caller.
		meta.RefreshReads = append([]roachpb.Span(nil), sr.refreshReads...)
		meta.RefreshWrites = append([]roachpb.Span(nil), sr.refreshWrites...)
	}
}

// augmentMetaLocked implements the txnInterceptor interface.
func (sr *txnSpanRefresher) augmentMetaLocked(meta roachpb.TxnCoordMeta) {
	// Do not modify existing span slices when copying.
	if meta.RefreshInvalid {
		sr.refreshInvalid = true
		sr.refreshReads = nil
		sr.refreshWrites = nil
	} else if !sr.refreshInvalid {
		if meta.RefreshReads != nil {
			sr.refreshReads, _ = roachpb.MergeSpans(
				append(append([]roachpb.Span(nil), sr.refreshReads...), meta.RefreshReads...),
			)
		}
		if meta.RefreshWrites != nil {
			sr.refreshWrites, _ = roachpb.MergeSpans(
				append(append([]roachpb.Span(nil), sr.refreshWrites...), meta.RefreshWrites...),
			)
		}
	}
	// Recompute the size of the refreshes.
	sr.refreshSpansBytes = 0
	for _, u := range sr.refreshReads {
		sr.refreshSpansBytes += int64(len(u.Key) + len(u.EndKey))
	}
	for _, u := range sr.refreshWrites {
		sr.refreshSpansBytes += int64(len(u.Key) + len(u.EndKey))
	}
}

// epochBumpedLocked implements the txnInterceptor interface.
func (sr *txnSpanRefresher) epochBumpedLocked() {
	sr.refreshReads = nil
	sr.refreshWrites = nil
	sr.refreshInvalid = false
	sr.refreshSpansBytes = 0
}

// closeLocked implements the txnInterceptor interface.
func (*txnSpanRefresher) closeLocked() {}
