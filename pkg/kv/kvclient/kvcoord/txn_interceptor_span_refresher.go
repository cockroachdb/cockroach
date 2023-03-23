// Copyright 2018 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	settings.TenantWritable,
	"kv.transaction.max_refresh_spans_bytes",
	"maximum number of bytes used to track refresh spans in serializable transactions",
	1<<22, /* 4 MB */
).WithPublic()

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
	riGen   rangeIteratorFactory
	wrapped lockedSender

	// refreshFootprint contains key spans which were read during the
	// transaction. In case the transaction's timestamp needs to be pushed, we can
	// avoid a retriable error by "refreshing" these spans: verifying that there
	// have been no changes to their data in between the timestamp at which they
	// were read and the higher timestamp we want to move to.
	refreshFootprint condensableSpanSet
	// refreshInvalid is set if refresh spans have not been collected (because the
	// memory budget was exceeded). When set, refreshFootprint is empty. This is
	// set when we've failed to condense the refresh spans below the target memory
	// limit.
	refreshInvalid bool
	// refreshedTimestamp keeps track of the largest timestamp that a transaction
	// was able to refresh all of its refreshable spans to. It is updated under
	// lock and used to ensure that concurrent requests don't cause the refresh
	// spans to get out of sync. See assertRefreshSpansAtInvalidTimestamp.
	refreshedTimestamp hlc.Timestamp

	// canAutoRetry is set if the txnSpanRefresher is allowed to auto-retry.
	canAutoRetry bool

	refreshSuccess                *metric.Counter
	refreshFail                   *metric.Counter
	refreshFailWithCondensedSpans *metric.Counter
	refreshMemoryLimitExceeded    *metric.Counter
	refreshAutoRetries            *metric.Counter
}

// SendLocked implements the lockedSender interface.
func (sr *txnSpanRefresher) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	// Set the batch's CanForwardReadTimestamp flag.
	ba.CanForwardReadTimestamp = sr.canForwardReadTimestampWithoutRefresh(ba.Txn)

	// Attempt a refresh before sending the batch.
	ba, pErr := sr.maybeRefreshPreemptivelyLocked(ctx, ba, false)
	if pErr != nil {
		return nil, pErr
	}

	// Send through wrapped lockedSender. Unlocks while sending then re-locks.
	br, pErr := sr.sendLockedWithRefreshAttempts(ctx, ba, sr.maxRefreshAttempts())
	if pErr != nil {
		return nil, pErr
	}

	// If the transaction is no longer pending, just return without
	// attempting to record its refresh spans.
	if br.Txn.Status != roachpb.PENDING {
		return br, nil
	}

	// Iterate over and aggregate refresh spans in the requests, qualified by
	// possible resume spans in the responses.
	if err := sr.assertRefreshSpansAtInvalidTimestamp(br.Txn.ReadTimestamp); err != nil {
		return nil, kvpb.NewError(err)
	}
	if !sr.refreshInvalid {
		if err := sr.appendRefreshSpans(ctx, ba, br); err != nil {
			return nil, kvpb.NewError(err)
		}
		// Check whether we should condense the refresh spans.
		sr.maybeCondenseRefreshSpans(ctx, br.Txn)
	}
	return br, nil
}

// maybeCondenseRefreshSpans checks whether the refresh footprint exceeds the
// maximum size (as determined by the MaxTxnRefreshSpansBytes cluster setting)
// and attempts to condense it if so. If condensing is insufficient, then the
// refresh is invalidated. It is assumed that the refresh is valid when this
// method is called.
func (sr *txnSpanRefresher) maybeCondenseRefreshSpans(
	ctx context.Context, txn *roachpb.Transaction,
) {
	maxBytes := MaxTxnRefreshSpansBytes.Get(&sr.st.SV)
	if sr.refreshFootprint.bytes >= maxBytes {
		condensedBefore := sr.refreshFootprint.condensed
		var condensedSufficient bool
		if sr.knobs.CondenseRefreshSpansFilter == nil || sr.knobs.CondenseRefreshSpansFilter() {
			sr.refreshFootprint.maybeCondense(ctx, sr.riGen, maxBytes)
			condensedSufficient = sr.refreshFootprint.bytes < maxBytes
		}
		if condensedSufficient {
			log.VEventf(ctx, 2, "condensed refresh spans for txn %s to %d bytes",
				txn, sr.refreshFootprint.bytes)
		} else {
			// Condensing was not enough. Giving up on tracking reads. Refreshes
			// will not be possible.
			log.VEventf(ctx, 2, "condensed refresh spans didn't save enough memory. txn %s. "+
				"refresh spans after condense: %d bytes",
				txn, sr.refreshFootprint.bytes)
			sr.refreshInvalid = true
			sr.refreshFootprint.clear()
		}
		if sr.refreshFootprint.condensed && !condensedBefore {
			sr.refreshMemoryLimitExceeded.Inc(1)
		}
	}
}

// sendLockedWithRefreshAttempts sends the batch through the wrapped sender. It
// catches serializable errors and attempts to avoid them by refreshing the txn
// at a larger timestamp.
func (sr *txnSpanRefresher) sendLockedWithRefreshAttempts(
	ctx context.Context, ba *kvpb.BatchRequest, maxRefreshAttempts int,
) (*kvpb.BatchResponse, *kvpb.Error) {
	if ba.Txn.WriteTooOld {
		// The WriteTooOld flag is not supposed to be set on requests. It's only set
		// by the server and it's terminated by this interceptor on the client.
		log.Fatalf(ctx, "unexpected WriteTooOld request. ba: %s (txn: %s)",
			ba.String(), ba.Txn.String())
	}
	br, pErr := sr.wrapped.SendLocked(ctx, ba)

	// We might receive errors with the WriteTooOld flag set. This interceptor
	// wants to always terminate that flag. In the case of an error, we can just
	// ignore it.
	if pErr != nil && pErr.GetTxn() != nil {
		pErr.GetTxn().WriteTooOld = false
	}

	if pErr == nil && br.Txn.WriteTooOld {
		// If we got a response with the WriteTooOld flag set, then we pretend that
		// we got a WriteTooOldError, which will cause us to attempt to refresh and
		// propagate the error if we failed. When it can, the server prefers to
		// return the WriteTooOld flag, rather than a WriteTooOldError because, in
		// the former case, it can leave intents behind. We like refreshing eagerly
		// when the WriteTooOld flag is set because it's likely that the refresh
		// will fail (if we previously read the key that's now causing a WTO, then
		// the refresh will surely fail).
		// TODO(andrei): Implement a more discerning policy based on whether we've
		// read that key before.
		//
		// If the refresh fails, we could continue running the transaction even
		// though it will not be able to commit, in order for it to lay down more
		// intents. Not doing so, though, gives the SQL a chance to auto-retry.
		// TODO(andrei): Implement a more discerning policy based on whether
		// auto-retries are still possible.
		//
		// For the refresh, we have two options: either refresh everything read
		// *before* this batch, and then retry this batch, or refresh the current
		// batch's reads too and then, if successful, there'd be nothing to retry.
		// We take the former option by setting br = nil below to minimized the
		// chances that the refresh fails.
		bumpedTxn := br.Txn.Clone()
		bumpedTxn.WriteTooOld = false
		pErr = kvpb.NewErrorWithTxn(
			kvpb.NewTransactionRetryError(kvpb.RETRY_WRITE_TOO_OLD,
				"WriteTooOld flag converted to WriteTooOldError"),
			bumpedTxn)
		br = nil
	}
	if pErr != nil {
		if maxRefreshAttempts > 0 {
			br, pErr = sr.maybeRefreshAndRetrySend(ctx, ba, pErr, maxRefreshAttempts)
		} else {
			log.VEventf(ctx, 2, "not checking error for refresh; refresh attempts exhausted")
		}
	}
	if err := sr.forwardRefreshTimestampOnResponse(ba, br, pErr); err != nil {
		return nil, kvpb.NewError(err)
	}
	return br, pErr
}

// maybeRefreshAndRetrySend attempts to catch serializable errors and avoid them
// by refreshing the txn at a larger timestamp. If it succeeds at refreshing the
// txn timestamp, it recurses into sendLockedWithRefreshAttempts and retries the
// batch. If the refresh fails, the input pErr is returned.
func (sr *txnSpanRefresher) maybeRefreshAndRetrySend(
	ctx context.Context, ba *kvpb.BatchRequest, pErr *kvpb.Error, maxRefreshAttempts int,
) (*kvpb.BatchResponse, *kvpb.Error) {
	txn := pErr.GetTxn()
	if txn == nil || !sr.canForwardReadTimestamp(txn) {
		return nil, pErr
	}
	// Check for an error which can be refreshed away to avoid a client-side
	// transaction restart.
	ok, refreshTS := kvpb.TransactionRefreshTimestamp(pErr)
	if !ok {
		return nil, pErr
	}
	refreshFrom := txn.ReadTimestamp
	refreshToTxn := txn.Clone()
	refreshToTxn.Refresh(refreshTS)
	log.VEventf(ctx, 2, "trying to refresh to %s because of %s",
		refreshToTxn.ReadTimestamp, pErr)

	// Try refreshing the txn spans so we can retry.
	if refreshErr := sr.tryRefreshTxnSpans(ctx, refreshFrom, refreshToTxn); refreshErr != nil {
		log.Eventf(ctx, "refresh failed; propagating original retry error")
		// TODO(lidor): we should add refreshErr info to the returned error. See issue #41057.
		return nil, pErr
	}

	// We've refreshed all of the read spans successfully and bumped
	// ba.Txn's timestamps. Attempt the request again.
	log.Eventf(ctx, "refresh succeeded; retrying original request")
	ba = ba.ShallowCopy()
	ba.UpdateTxn(refreshToTxn)
	sr.refreshAutoRetries.Inc(1)

	// To prevent starvation of batches that are trying to commit, split off the
	// EndTxn request into its own batch on auto-retries. This avoids starvation
	// in two ways. First, it helps ensure that we lay down intents if any of
	// the other requests in the batch are writes. Second, it ensures that if
	// any writes are getting pushed due to contention with reads or due to the
	// closed timestamp, they will still succeed and allow the batch to make
	// forward progress. Without this, each retry attempt may get pushed because
	// of writes in the batch and then rejected wholesale when the EndTxn tries
	// to evaluate the pushed batch. When split, the writes will be pushed but
	// succeed, the transaction will be refreshed, and the EndTxn will succeed.
	args, hasET := ba.GetArg(kvpb.EndTxn)
	if len(ba.Requests) > 1 && hasET && !args.(*kvpb.EndTxnRequest).Require1PC {
		log.Eventf(ctx, "sending EndTxn separately from rest of batch on retry")
		return sr.splitEndTxnAndRetrySend(ctx, ba)
	}

	retryBr, retryErr := sr.sendLockedWithRefreshAttempts(ctx, ba, maxRefreshAttempts-1)
	if retryErr != nil {
		log.VEventf(ctx, 2, "retry failed with %s", retryErr)
		return nil, retryErr
	}

	log.VEventf(ctx, 2, "retry successful @%s", retryBr.Txn.ReadTimestamp)
	return retryBr, nil
}

// splitEndTxnAndRetrySend splits the batch in two, with a prefix containing all
// requests up to but not including the EndTxn request and a suffix containing
// only the EndTxn request. It then issues the two partial batches in order,
// stitching their results back together at the end.
func (sr *txnSpanRefresher) splitEndTxnAndRetrySend(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	// NOTE: call back into SendLocked with each partial batch, not into
	// sendLockedWithRefreshAttempts. This ensures that we properly set
	// CanForwardReadTimestamp on each partial batch and that we provide
	// the EndTxn batch with a chance to perform a preemptive refresh.

	// Issue a batch up to but not including the EndTxn request.
	etIdx := len(ba.Requests) - 1
	baPrefix := ba.ShallowCopy()
	baPrefix.Requests = ba.Requests[:etIdx]
	brPrefix, pErr := sr.SendLocked(ctx, baPrefix)
	if pErr != nil {
		return nil, pErr
	}

	// Issue a batch containing only the EndTxn request.
	baSuffix := ba.ShallowCopy()
	baSuffix.Requests = ba.Requests[etIdx:]
	baSuffix.UpdateTxn(brPrefix.Txn)
	brSuffix, pErr := sr.SendLocked(ctx, baSuffix)
	if pErr != nil {
		return nil, pErr
	}

	// Combine the responses.
	br := brPrefix
	br.Responses = append(br.Responses, kvpb.ResponseUnion{})
	if err := br.Combine(ctx, brSuffix, []int{etIdx}, ba); err != nil {
		return nil, kvpb.NewError(err)
	}
	return br, nil
}

// maybeRefreshPreemptivelyLocked attempts to refresh a transaction's read timestamp
// eagerly. Doing so can take advantage of opportunities where the refresh is
// free or can avoid wasting work issuing a batch containing an EndTxn that will
// necessarily throw a serializable error. The method returns a batch with an
// updated transaction if the refresh is successful, or a retry error if not.
// If the force flag is true, the refresh will be attempted even if a refresh
// is not inevitable.
func (sr *txnSpanRefresher) maybeRefreshPreemptivelyLocked(
	ctx context.Context, ba *kvpb.BatchRequest, force bool,
) (*kvpb.BatchRequest, *kvpb.Error) {
	// If we know that the transaction will need a refresh at some point because
	// its write timestamp has diverged from its read timestamp, consider doing
	// so preemptively. We perform a preemptive refresh if either a) doing so
	// would be free because we have not yet accumulated any refresh spans, or
	// b) the batch contains a committing EndTxn request that we know will be
	// rejected if issued.
	//
	// The first case is straightforward. If the transaction has yet to perform
	// any reads but has had its write timestamp bumped, refreshing is a trivial
	// no-op. In this case, refreshing eagerly prevents the transaction for
	// performing any future reads at its current read timestamp. Not doing so
	// preemptively guarantees that we will need to perform a real refresh in
	// the future if the transaction ever performs a read. At best, this would
	// be wasted work. At worst, this could result in the future refresh
	// failing. So we might as well refresh preemptively while doing so is free.
	//
	// Note that this first case here does NOT obviate the need for server-side
	// refreshes. Notably, a transaction's write timestamp might be bumped in
	// the same batch in which it performs its first read. In such cases, a
	// preemptive refresh would not be needed but a reactive refresh would not
	// be a trivial no-op. These situations are common for one-phase commit
	// transactions.
	//
	// The second case is more complex. If the batch contains a committing
	// EndTxn request that we know will need a refresh, we don't want to bother
	// issuing it just for it to be rejected. Instead, preemptively refresh
	// before issuing the EndTxn batch. If we view reads as acquiring a form of
	// optimistic read locks under an optimistic concurrency control scheme (as
	// is discussed in the comment on txnSpanRefresher) then this preemptive
	// refresh immediately before the EndTxn is synonymous with the "validation"
	// phase of a standard OCC transaction model. However, as an optimization
	// compared to standard OCC, the validation phase is only performed when
	// necessary in CockroachDB (i.e. if the transaction's writes have been
	// pushed to higher timestamps).
	//
	// TODO(andrei): whether or not we can still auto-retry at the SQL level
	// should also play a role in deciding whether we want to refresh eagerly or
	// not.

	// If the transaction has yet to be pushed, no refresh is necessary.
	if ba.Txn.ReadTimestamp == ba.Txn.WriteTimestamp {
		return ba, nil
	}

	// If true, tryRefreshTxnSpans will trivially succeed.
	refreshFree := ba.CanForwardReadTimestamp

	// If true, this batch is guaranteed to fail without a refresh.
	args, hasET := ba.GetArg(kvpb.EndTxn)
	refreshInevitable := hasET && args.(*kvpb.EndTxnRequest).Commit

	// If neither condition is true, defer the refresh.
	if !refreshFree && !refreshInevitable && !force {
		return ba, nil
	}

	// If the transaction cannot change its read timestamp, no refresh is
	// possible.
	if !sr.canForwardReadTimestamp(ba.Txn) {
		return nil, newRetryErrorOnFailedPreemptiveRefresh(ba.Txn, nil)
	}

	refreshFrom := ba.Txn.ReadTimestamp
	refreshToTxn := ba.Txn.Clone()
	refreshToTxn.Refresh(ba.Txn.WriteTimestamp)
	log.VEventf(ctx, 2, "preemptively refreshing to timestamp %s before issuing %s",
		refreshToTxn.ReadTimestamp, ba)

	// Try refreshing the txn spans at a timestamp that will allow us to commit.
	if refreshErr := sr.tryRefreshTxnSpans(ctx, refreshFrom, refreshToTxn); refreshErr != nil {
		log.Eventf(ctx, "preemptive refresh failed; propagating retry error")
		return nil, newRetryErrorOnFailedPreemptiveRefresh(ba.Txn, refreshErr)
	}

	log.Eventf(ctx, "preemptive refresh succeeded")
	ba = ba.ShallowCopy()
	ba.UpdateTxn(refreshToTxn)
	return ba, nil
}

func newRetryErrorOnFailedPreemptiveRefresh(
	txn *roachpb.Transaction, refreshErr *kvpb.Error,
) *kvpb.Error {
	reason := kvpb.RETRY_SERIALIZABLE
	if txn.WriteTooOld {
		reason = kvpb.RETRY_WRITE_TOO_OLD
	}
	msg := redact.StringBuilder{}
	msg.SafeString("failed preemptive refresh")
	if refreshErr != nil {
		if refreshErr, ok := refreshErr.GetDetail().(*kvpb.RefreshFailedError); ok {
			msg.Printf(" due to a conflict: %s on key %s", refreshErr.FailureReason(), refreshErr.Key)
		} else {
			msg.Printf(" - unknown error: %s", refreshErr)
		}
	}
	retryErr := kvpb.NewTransactionRetryError(reason, msg.RedactableString())
	return kvpb.NewErrorWithTxn(retryErr, txn)
}

// tryRefreshTxnSpans sends Refresh and RefreshRange commands to all spans read
// during the transaction to ensure that no writes were written more recently
// than refreshFrom. All implicated timestamp caches are updated with the final
// transaction timestamp. Returns whether the refresh was successful or not.
//
// The provided transaction should be a Clone() of the original transaction with
// its ReadTimestamp adjusted by the Refresh() method.
func (sr *txnSpanRefresher) tryRefreshTxnSpans(
	ctx context.Context, refreshFrom hlc.Timestamp, refreshToTxn *roachpb.Transaction,
) (err *kvpb.Error) {
	// Track the result of the refresh in metrics.
	defer func() {
		if err == nil {
			sr.refreshSuccess.Inc(1)
		} else {
			sr.refreshFail.Inc(1)
			if sr.refreshFootprint.condensed {
				sr.refreshFailWithCondensedSpans.Inc(1)
			}
		}
	}()

	if sr.refreshInvalid {
		log.VEvent(ctx, 2, "can't refresh txn spans; not valid")
		return kvpb.NewError(errors.AssertionFailedf("can't refresh txn spans; not valid"))
	} else if sr.refreshFootprint.empty() {
		log.VEvent(ctx, 2, "there are no txn spans to refresh")
		sr.forwardRefreshTimestampOnRefresh(refreshToTxn)
		return nil
	}

	// Refresh all spans (merge first).
	// TODO(nvanbenschoten): actually merge spans.
	refreshSpanBa := &kvpb.BatchRequest{}
	refreshSpanBa.Txn = refreshToTxn
	addRefreshes := func(refreshes *condensableSpanSet) {
		// We're going to check writes between the previous refreshed timestamp, if
		// any, and the timestamp we want to bump the transaction to. Note that if
		// we've already refreshed the transaction before, we don't need to check
		// the (key ranges x timestamp range) that we've already checked - there's
		// no values there for sure.
		// More importantly, reads that have happened since we've previously
		// refreshed don't need to be checked below below the timestamp at which
		// they've been read (which is the timestamp to which we've previously
		// refreshed). Checking below that timestamp (like we would, for example, if
		// we simply used txn.OrigTimestamp here), could cause false-positives that
		// would fail the refresh.
		for _, u := range refreshes.asSlice() {
			var req kvpb.Request
			if len(u.EndKey) == 0 {
				req = &kvpb.RefreshRequest{
					RequestHeader: kvpb.RequestHeaderFromSpan(u),
					RefreshFrom:   refreshFrom,
				}
			} else {
				req = &kvpb.RefreshRangeRequest{
					RequestHeader: kvpb.RequestHeaderFromSpan(u),
					RefreshFrom:   refreshFrom,
				}
			}
			refreshSpanBa.Add(req)
			log.VEventf(ctx, 2, "updating span %s @%s - @%s to avoid serializable restart",
				req.Header().Span(), refreshFrom, refreshToTxn.WriteTimestamp)
		}
	}
	addRefreshes(&sr.refreshFootprint)

	// Send through wrapped lockedSender. Unlocks while sending then re-locks.
	if _, batchErr := sr.wrapped.SendLocked(ctx, refreshSpanBa); batchErr != nil {
		log.VEventf(ctx, 2, "failed to refresh txn spans (%s)", batchErr)
		return batchErr
	}

	sr.forwardRefreshTimestampOnRefresh(refreshToTxn)
	return nil
}

// appendRefreshSpans appends refresh spans from the supplied batch request,
// qualified by the batch response where appropriate.
func (sr *txnSpanRefresher) appendRefreshSpans(
	ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse,
) error {
	expLogEnabled := log.ExpensiveLogEnabled(ctx, 3)
	return ba.RefreshSpanIterate(br, func(span roachpb.Span) {
		if expLogEnabled {
			log.VEventf(ctx, 3, "recording span to refresh: %s", span.String())
		}
		sr.refreshFootprint.insert(span)
	})
}

// canForwardReadTimestampWithoutRefresh returns whether the transaction can
// forward its read timestamp after refreshing all the reads that has performed
// to this point. This requires that the transaction's timestamp has not leaked.
// It also requires that the txnSpanRefresher has been configured to allow
// auto-retries.
func (sr *txnSpanRefresher) canForwardReadTimestamp(txn *roachpb.Transaction) bool {
	return sr.canAutoRetry && !txn.CommitTimestampFixed
}

// canForwardReadTimestampWithoutRefresh returns whether the transaction can
// forward its read timestamp without refreshing any read spans. This allows for
// the "server-side refresh" optimization, where batches are re-evaluated at a
// higher read-timestamp without returning to transaction coordinator.
//
// This requires that the transaction has encountered no spans which require
// refreshing at the forwarded timestamp and that the transaction's timestamp
// has not leaked. If either of those conditions are true, a client-side refresh
// is required.
//
// Note that when deciding whether a transaction can be bumped to a particular
// timestamp, the transaction's deadline must also be taken into account.
func (sr *txnSpanRefresher) canForwardReadTimestampWithoutRefresh(txn *roachpb.Transaction) bool {
	return sr.canForwardReadTimestamp(txn) && !sr.refreshInvalid && sr.refreshFootprint.empty()
}

// forwardRefreshTimestampOnRefresh updates the refresher's tracked
// refreshedTimestamp under lock after a successful refresh. This in conjunction
// with a check in assertRefreshSpansAtInvalidTimestamp prevents a race where a
// concurrent request may add new refresh spans only "verified" up to its batch
// timestamp after we've refreshed past that timestamp.
func (sr *txnSpanRefresher) forwardRefreshTimestampOnRefresh(refreshToTxn *roachpb.Transaction) {
	sr.refreshedTimestamp.Forward(refreshToTxn.ReadTimestamp)
}

// forwardRefreshTimestampOnResponse updates the refresher's tracked
// refreshedTimestamp to stay in sync with "server-side refreshes", where the
// transaction's read timestamp is updated during the evaluation of a batch.
func (sr *txnSpanRefresher) forwardRefreshTimestampOnResponse(
	ba *kvpb.BatchRequest, br *kvpb.BatchResponse, pErr *kvpb.Error,
) error {
	baTxn := ba.Txn
	var brTxn *roachpb.Transaction
	if pErr != nil {
		brTxn = pErr.GetTxn()
	} else {
		brTxn = br.Txn
	}
	if baTxn == nil || brTxn == nil {
		return nil
	}
	if baTxn.ReadTimestamp.Less(brTxn.ReadTimestamp) {
		sr.refreshedTimestamp.Forward(brTxn.ReadTimestamp)
	} else if brTxn.ReadTimestamp.Less(baTxn.ReadTimestamp) {
		return errors.AssertionFailedf("received transaction in response with "+
			"earlier read timestamp than in the request. ba.Txn: %s, br.Txn: %s", baTxn, brTxn)
	}
	return nil
}

// assertRefreshSpansAtInvalidTimestamp returns an error if the timestamp at
// which a set of reads was performed is below the largest timestamp that this
// transaction has already refreshed to.
func (sr *txnSpanRefresher) assertRefreshSpansAtInvalidTimestamp(
	readTimestamp hlc.Timestamp,
) error {
	if readTimestamp.Less(sr.refreshedTimestamp) {
		// This can happen with (illegal) concurrent txn use, but that's supposed to
		// be detected by the gatekeeper interceptor.
		return errors.AssertionFailedf("attempting to append refresh spans after the tracked"+
			" timestamp has moved forward. batchTimestamp: %s refreshedTimestamp: %s",
			errors.Safe(readTimestamp), errors.Safe(sr.refreshedTimestamp))
	}
	return nil
}

// maxRefreshAttempts returns the configured number of times that a transaction
// should attempt to refresh its spans for a single batch.
func (sr *txnSpanRefresher) maxRefreshAttempts() int {
	if knob := sr.knobs.MaxTxnRefreshAttempts; knob != 0 {
		if knob == -1 {
			return 0
		}
		return knob
	}
	return maxTxnRefreshAttempts
}

// setWrapped implements the txnInterceptor interface.
func (sr *txnSpanRefresher) setWrapped(wrapped lockedSender) { sr.wrapped = wrapped }

// populateLeafInputState is part of the txnInterceptor interface.
func (sr *txnSpanRefresher) populateLeafInputState(tis *roachpb.LeafTxnInputState) {
	tis.RefreshInvalid = sr.refreshInvalid
}

// populateLeafFinalState is part of the txnInterceptor interface.
func (sr *txnSpanRefresher) populateLeafFinalState(tfs *roachpb.LeafTxnFinalState) {
	tfs.RefreshInvalid = sr.refreshInvalid
	if !sr.refreshInvalid {
		// Copy mutable state so access is safe for the caller.
		tfs.RefreshSpans = append([]roachpb.Span(nil), sr.refreshFootprint.asSlice()...)
	}
}

// importLeafFinalState is part of the txnInterceptor interface.
func (sr *txnSpanRefresher) importLeafFinalState(
	ctx context.Context, tfs *roachpb.LeafTxnFinalState,
) error {
	if err := sr.assertRefreshSpansAtInvalidTimestamp(tfs.Txn.ReadTimestamp); err != nil {
		return err
	}
	if tfs.RefreshInvalid {
		sr.refreshInvalid = true
		sr.refreshFootprint.clear()
	} else if !sr.refreshInvalid {
		sr.refreshFootprint.insert(tfs.RefreshSpans...)
		// Check whether we should condense the refresh spans.
		sr.maybeCondenseRefreshSpans(ctx, &tfs.Txn)
	}
	return nil
}

// epochBumpedLocked implements the txnInterceptor interface.
func (sr *txnSpanRefresher) epochBumpedLocked() {
	sr.refreshFootprint.clear()
	sr.refreshInvalid = false
	sr.refreshedTimestamp.Reset()
}

// createSavepointLocked is part of the txnInterceptor interface.
func (sr *txnSpanRefresher) createSavepointLocked(ctx context.Context, s *savepoint) {
	s.refreshSpans = make([]roachpb.Span, len(sr.refreshFootprint.asSlice()))
	copy(s.refreshSpans, sr.refreshFootprint.asSlice())
	s.refreshInvalid = sr.refreshInvalid
}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (sr *txnSpanRefresher) rollbackToSavepointLocked(ctx context.Context, s savepoint) {
	sr.refreshFootprint.clear()
	sr.refreshFootprint.insert(s.refreshSpans...)
	sr.refreshInvalid = s.refreshInvalid
}

// closeLocked implements the txnInterceptor interface.
func (*txnSpanRefresher) closeLocked() {}
