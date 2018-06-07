// Copyright 2018 The Cockroach Authors.
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
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// maxTxnRefreshSpansBytes is a threshold in bytes for refresh spans stored
// on the coordinator during the lifetime of a transaction. Refresh spans
// are used for SERIALIZABLE transactions to avoid client restarts.
var maxTxnRefreshSpansBytes = settings.RegisterIntSetting(
	"kv.transaction.max_refresh_spans_bytes",
	"maximum number of bytes used to track refresh spans in serializable transactions",
	256*1000,
)

// txnSpanRefresher is an implementation of txnReqInterceptor. It collects the read
// spans of a serializable transaction in the event we get a serializable retry
// error. We can use the set of read spans to avoid retrying the transaction if
// all the spans can be updated to the current transaction timestamp.
type txnSpanRefresher struct {
	st *cluster.Settings

	// mu locks the TxnCoordMeta.
	mu sync.Locker
	// meta contains all refresh spans.
	meta *roachpb.TxnCoordMeta
	// refreshSpansBytes is the total size in bytes of the spans
	// encountered during this transaction that need to be refreshed to
	// avoid serializable restart.
	refreshSpansBytes int64

	// canAutoRetry is set if the txnSpanRefresher is allowed to auto-retry.
	canAutoRetry bool
	// wrapped is a Sender that the interceptor can use to perform retries.
	wrapped client.Sender
	// autoRetryCounter counts the number of auto retries which avoid
	// client-side restarts.
	autoRetryCounter *metric.CounterWithRates
}

var _ txnReqInterceptor = &txnSpanRefresher{}

func (sr *txnSpanRefresher) beforeSendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (roachpb.BatchRequest, *roachpb.Error) {
	rArgs, hasET := ba.GetArg(roachpb.EndTransaction)
	if !hasET {
		// No-op.
		return ba, nil
	}

	et := rArgs.(*roachpb.EndTransactionRequest)
	if sr.meta.Txn.IsSerializable() && sr.meta.RefreshValid &&
		len(sr.meta.RefreshReads) == 0 && len(sr.meta.RefreshWrites) == 0 {
		et.NoRefreshSpans = true
	}
	return ba, nil
}

func (sr *txnSpanRefresher) maybeRetrySend(
	ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if pErr == nil {
		// No-op.
		return br, nil
	}

	// Check for an error which can be retried after updating spans.
	canRetryTxn, retryTxn := roachpb.CanTransactionRetryAtRefreshedTimestamp(ctx, pErr)
	if !canRetryTxn || !sr.canAutoRetry || !sr.st.Version.IsActive(cluster.VersionTxnSpanRefresh) {
		return nil, pErr
	}

	// If a prefix of the batch was executed, collect refresh spans for
	// that executed portion, and retry the remainder. The canonical
	// case is a batch split between everything up to but not including
	// the EndTransaction. Requests up to the EndTransaction succeed,
	// but the EndTransaction fails with a retryable error. We want to
	// retry only the EndTransaction.
	ba.UpdateTxn(retryTxn)
	retryBa := *ba
	if br != nil {
		doneBa := *ba
		doneBa.Requests = ba.Requests[:len(br.Responses)]
		log.VEventf(ctx, 2, "collecting refresh spans after partial batch execution of %s", doneBa)
		sr.mu.Lock()
		if !sr.appendRefreshSpansLocked(ctx, doneBa, br) {
			sr.mu.Unlock()
			return nil, pErr
		}
		sr.meta.Txn.RefreshedTimestamp.Forward(retryTxn.RefreshedTimestamp)
		sr.mu.Unlock()
		retryBa.Requests = ba.Requests[len(br.Responses):]
	}

	log.VEventf(ctx, 2, "retrying %s at refreshed timestamp %s because of %s",
		retryBa, retryTxn.RefreshedTimestamp, pErr)

	// Try updating the txn spans so we can retry.
	if ok := sr.tryUpdatingTxnSpans(ctx, retryTxn); !ok {
		return nil, pErr
	}

	// We've refreshed all of the read spans successfully and set
	// newBa.Txn.RefreshedTimestamp to the current timestamp. Submit the
	// batch again.
	retryBr, retryErr := sr.wrapped.Send(ctx, retryBa)
	if retryErr != nil {
		log.VEventf(ctx, 2, "retry failed with %s", retryErr)
		return nil, retryErr
	}

	log.VEventf(ctx, 2, "retry successful @%s", retryBa.Txn.Timestamp)
	sr.autoRetryCounter.Inc(1)

	// On success, combine responses if applicable and set error to nil.
	if br != nil {
		br.Responses = append(br.Responses, retryBr.Responses...)
		retryBr.CollectedSpans = append(br.CollectedSpans, retryBr.CollectedSpans...)
		br.BatchResponse_Header = retryBr.BatchResponse_Header
	} else {
		br = retryBr
	}
	return br, nil
}

// tryUpdatingTxnSpans sends Refresh and RefreshRange commands to all
// spans read during the transaction to ensure that no writes were
// written more recently than the original transaction timestamp. All
// implicated timestamp caches are updated with the final transaction
// timestamp. On success, returns true and an updated BatchRequest
// containing a transaction whose original timestamp and timestamp
// have been set to the same value.
func (sr *txnSpanRefresher) tryUpdatingTxnSpans(
	ctx context.Context, refreshTxn *roachpb.Transaction,
) bool {
	sr.mu.Lock()
	refreshReads := sr.meta.RefreshReads
	refreshWrites := sr.meta.RefreshWrites
	refreshValid := sr.meta.RefreshValid
	sr.mu.Unlock()

	if !refreshValid {
		log.VEvent(ctx, 2, "can't refresh txn spans; not valid")
		return false
	} else if len(refreshReads) == 0 && len(refreshWrites) == 0 {
		log.VEvent(ctx, 2, "there are no txn spans to refresh")
		return true
	}

	// Refresh all spans (merge first).
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
	addRefreshes(refreshReads, false)
	addRefreshes(refreshWrites, true)
	if _, batchErr := sr.wrapped.Send(ctx, refreshSpanBa); batchErr != nil {
		log.VEventf(ctx, 2, "failed to refresh txn spans (%s); propagating original retry error", batchErr)
		return false
	}

	return true
}

func (sr *txnSpanRefresher) afterSendLocked(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Iterate over and aggregate refresh spans in the requests,
	// qualified by possible resume spans in the responses, if the txn
	// has serializable isolation and we haven't yet exceeded the max
	// read key bytes.
	if pErr == nil && ba.Txn.IsSerializable() {
		if sr.meta.RefreshValid {
			if !sr.appendRefreshSpansLocked(ctx, ba, br) {
				// The refresh spans are out of date, return a generic client-side retry error.
				pErr = roachpb.NewErrorWithTxn(roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE), br.Txn)
			}
		}
		// Verify and enforce the size in bytes of all read-only spans
		// doesn't exceed the max threshold.
		if sr.refreshSpansBytes > maxTxnRefreshSpansBytes.Get(&sr.st.SV) {
			log.VEventf(ctx, 2, "refresh spans max size exceeded; clearing")
			sr.meta.RefreshReads = nil
			sr.meta.RefreshWrites = nil
			sr.meta.RefreshValid = false
			sr.refreshSpansBytes = 0
		}
	}
	// If the transaction will retry and the refresh spans are
	// exhausted, return a non-retryable error indicating that the
	// transaction is too large and should potentially be split.
	// We do this to avoid endlessly retrying a txn likely refail.
	if pErr == nil && !sr.meta.RefreshValid &&
		(br.Txn.WriteTooOld || br.Txn.OrigTimestamp != br.Txn.Timestamp) {
		pErr = roachpb.NewErrorWithTxn(
			errors.New("transaction is too large to complete; try splitting into pieces"), br.Txn,
		)
	}
	return br, pErr
}

func (sr *txnSpanRefresher) refreshMetaLocked() {
	sr.refreshSpansBytes = 0
	for _, u := range sr.meta.RefreshReads {
		sr.refreshSpansBytes += int64(len(u.Key) + len(u.EndKey))
	}
	for _, u := range sr.meta.RefreshWrites {
		sr.refreshSpansBytes += int64(len(u.Key) + len(u.EndKey))
	}
}

// appendRefreshSpansLocked appends refresh spans from the supplied batch
// request, qualified by the batch response where appropriate. Returns
// whether the batch transaction's refreshed timestamp is greater or equal
// to the max refreshed timestamp used so far with this sender.
//
// The batch refreshed timestamp and the max refreshed timestamp for
// the sender can get out of step because the txn coord sender can be
// used concurrently (i.e. when using the "RETURNING NOTHING"
// syntax). What we don't want is to append refreshes which are
// already too old compared to the max refreshed timestamp that's already
// in use with this sender. In that case the caller should return an
// error for client-side retry.
func (sr *txnSpanRefresher) appendRefreshSpansLocked(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
) bool {
	origTS := ba.Txn.OrigTimestamp
	origTS.Forward(ba.Txn.RefreshedTimestamp)
	if origTS.Less(sr.meta.Txn.RefreshedTimestamp) {
		log.VEventf(ctx, 2, "txn orig timestamp %s < sender refreshed timestamp %s",
			origTS, sr.meta.Txn.RefreshedTimestamp)
		return false
	}
	ba.RefreshSpanIterate(br, func(span roachpb.Span, write bool) {
		if log.V(3) {
			log.Infof(ctx, "refresh: %s write=%t", span, write)
		}
		if write {
			sr.meta.RefreshWrites = append(sr.meta.RefreshWrites, span)
		} else {
			sr.meta.RefreshReads = append(sr.meta.RefreshReads, span)
		}
		sr.refreshSpansBytes += int64(len(span.Key) + len(span.EndKey))
	})
	return true
}
