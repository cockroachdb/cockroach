// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnrecovery

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/errors"
)

// Manager organizes the recovery of transactions whose states require global
// (as opposed to local) coordination to transition away from.
type Manager interface {
	// ResolveIndeterminateCommit attempts to resolve the status of transactions
	// that have been abandoned while in the STAGING state, attempting to commit.
	// Unlike most transitions in the transaction state machine, moving from the
	// STAGING state to any other state requires global coordination instead of
	// localized coordination. This method performs this coordination with the
	// goal of finalizing the transaction as either COMMITTED or ABORTED.
	//
	// The method may also return a transaction in any other state if it is
	// discovered to still be live and undergoing state transitions.
	ResolveIndeterminateCommit(
		context.Context, *roachpb.IndeterminateCommitError,
	) (*roachpb.Transaction, error)

	// Metrics returns the Manager's metrics struct.
	Metrics() Metrics
}

const (
	// defaultTaskLimit is the maximum number of recovery processes that may be
	// run concurrently. Once this limit is reached, future attempts to resolve
	// indeterminate transaction commits will wait until other attempts complete.
	defaultTaskLimit = 1024

	// defaultBatchSize is the maximum number of intents that will be queried in
	// a single batch. Batches that span many ranges will be split into many
	// batches by the DistSender.
	defaultBatchSize = 128
)

// manager implements the Manager interface.
type manager struct {
	log.AmbientContext

	clock   *hlc.Clock
	db      *kv.DB
	stopper *stop.Stopper
	metrics Metrics
	txns    singleflight.Group
	sem     chan struct{}
}

// NewManager returns an implementation of a transaction recovery Manager.
func NewManager(ac log.AmbientContext, clock *hlc.Clock, db *kv.DB, stopper *stop.Stopper) Manager {
	ac.AddLogTag("txn-recovery", nil)
	return &manager{
		AmbientContext: ac,
		clock:          clock,
		db:             db,
		stopper:        stopper,
		metrics:        makeMetrics(),
		sem:            make(chan struct{}, defaultTaskLimit),
	}
}

// ResolveIndeterminateCommit implements the Manager interface.
func (m *manager) ResolveIndeterminateCommit(
	ctx context.Context, ice *roachpb.IndeterminateCommitError,
) (*roachpb.Transaction, error) {
	txn := &ice.StagingTxn
	if txn.Status != roachpb.STAGING {
		return nil, errors.Errorf("IndeterminateCommitError with non-STAGING transaction: %v", txn)
	}

	// Launch a single-flight task to recover the transaction. This may be
	// coalesced with other recovery attempts for the same transaction.
	log.VEventf(ctx, 2, "recovering txn %s from indeterminate commit", txn.ID.Short())
	resC, _ := m.txns.DoChan(txn.ID.String(), func() (interface{}, error) {
		return m.resolveIndeterminateCommitForTxn(txn)
	})

	// Wait for the inflight request.
	select {
	case res := <-resC:
		if res.Err != nil {
			log.VEventf(ctx, 2, "recovery error: %v", res.Err)
			return nil, res.Err
		}
		txn := res.Val.(*roachpb.Transaction)
		log.VEventf(ctx, 2, "recovered txn %s with status: %s", txn.ID.Short(), txn.Status)
		return txn, nil
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "abandoned indeterminate commit recovery")
	}
}

// resolveIndeterminateCommitForTxn attempts to to resolve the status of
// transactions that have been abandoned while in the STAGING state, attempting
// to commit. It does so by first querying each of the transaction's in-flight
// writes to determine whether any of them failed, trying to prevent at least
// one of them. While doing so, it also monitors the state of the transaction
// and returns early if it ever changes. Once the result of all in-flight writes
// is determined, the method issues a RecoverTxn request with a summary of their
// outcome.
func (m *manager) resolveIndeterminateCommitForTxn(
	txn *roachpb.Transaction,
) (resTxn *roachpb.Transaction, resErr error) {
	// Record the recovery attempt in the Manager's metrics.
	onComplete := m.updateMetrics()
	defer func() { onComplete(resTxn, resErr) }()

	// TODO(nvanbenschoten): Set up tracing.
	ctx := m.AnnotateCtx(context.Background())

	// Launch the recovery task.
	resErr = m.stopper.RunTaskWithErr(ctx,
		"recovery.manager: resolving indeterminate commit",
		func(ctx context.Context) error {
			// Grab semaphore with defaultTaskLimit.
			select {
			case m.sem <- struct{}{}:
				defer func() { <-m.sem }()
			case <-m.stopper.ShouldQuiesce():
				return stop.ErrUnavailable
			}

			// We probe to determine whether the transaction is implicitly
			// committed or not. If not, we prevent it from ever becoming
			// implicitly committed at this (epoch, timestamp) pair.
			preventedIntent, changedTxn, err := m.resolveIndeterminateCommitForTxnProbe(ctx, txn)
			if err != nil {
				return err
			}
			if changedTxn != nil {
				resTxn = changedTxn
				return nil
			}

			// Now that we know whether the transaction was implicitly committed
			// or not (implicitly committed = !preventedIntent), we attempt to
			// recover it. If this succeeds, it will either move the transaction
			// record to a COMMITTED or ABORTED status.
			resTxn, err = m.resolveIndeterminateCommitForTxnRecover(ctx, txn, preventedIntent)
			return err
		},
	)
	return resTxn, resErr
}

// resolveIndeterminateCommitForTxnProbe performs the "probing phase" of the
// indeterminate commit resolution process. This phase queries each of the
// transaction's in-flight writes to determine whether any of them failed,
// trying to prevent at least one of them. While doing so, it also monitors the
// state of the transaction and returns early if it ever changes.
func (m *manager) resolveIndeterminateCommitForTxnProbe(
	ctx context.Context, txn *roachpb.Transaction,
) (preventedIntent bool, changedTxn *roachpb.Transaction, err error) {
	// Create a QueryTxnRequest that we will periodically send to the
	// transaction's record during recovery processing.
	queryTxnReq := roachpb.QueryTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Txn:           txn.TxnMeta,
		WaitForUpdate: false,
	}

	// Create a QueryIntentRequest for each of the transaction's in-flight
	// writes. We will attempt to prove that all have succeeded using these
	// requests. There are two possible outcomes from this probing:
	// 1. we find that all of the transaction's in-flight writes at the time that
	//    it was staged to commit have succeeded in being written. This is all the
	//    evidence that we need in order to declare the transaction "implicitly
	//    committed", at which point we can mark it as "explicitly committed" by
	//    moving the transaction's record from the STAGING state to the COMMITTED
	//    state.
	// 2. we find that one or more of the transaction's in-flight writes at the
	//    time that it was staged to commit have not yet succeeded. In this case,
	//    the QueryIntent that found the missing in-flight write atomically ensures
	//    that the intent write will never succeed in the future (NOTE: this is a
	//    side-effect of any QueryIntent request that finds a missing intent). This
	//    guarantees that if we determine that the transaction cannot be committed,
	//    the write we're searching for can never occur after we observe it to be
	//    missing (for instance, if it was delayed) and cause others to determine
	//    that the transaction can be committed. After it has done so, we have all
	//    the evidence that we need in order to declare the transaction commit a
	//    failure and move the transaction's record from the STAGING state to the
	//    ABORTED state. Moving the transaction's record to the ABORTED state will
	//    succeed if the transaction hasn't made any updates to its transaction
	//    record (e.g. if the record has been abandoned). However, it can fail if
	//    the transaction has already refreshed at a higher timestamp in the
	//    current epoch or restarted at a higher epoch.
	queryIntentReqs := make([]roachpb.QueryIntentRequest, 0, len(txn.InFlightWrites))
	for _, w := range txn.InFlightWrites {
		meta := txn.TxnMeta
		meta.Sequence = w.Sequence
		queryIntentReqs = append(queryIntentReqs, roachpb.QueryIntentRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: w.Key,
			},
			Txn: meta,
		})
	}

	// Sort the query intent requests to maximize batching by range.
	sort.Slice(queryIntentReqs, func(i, j int) bool {
		return queryIntentReqs[i].Header().Key.Compare(queryIntentReqs[j].Header().Key) < 0
	})

	// Query all of the intents in batches of size defaultBatchSize. The maximum
	// timeout is defaultTimeout, and this is applied to each batch to ensure
	// forward progress is made. A large set of intents might require more time
	// than a single timeout allows.
	//
	// We begin each batch with a query of the transaction's record as well,
	// which will be issued in parallel with the query intent requests. This
	// allows us to break out of recovery processing early if recovery is
	// completed by some other actor before us, or if the transaction begins
	// changes, indicating activity.
	//
	// Loop until either the transaction is observed to change, an in-flight
	// write is prevented, or we run out of in-flight writes to query.
	for len(queryIntentReqs) > 0 {
		var b kv.Batch
		b.Header.Timestamp = m.batchTimestamp(txn)
		b.AddRawRequest(&queryTxnReq)
		for i := 0; i < defaultBatchSize && len(queryIntentReqs) > 0; i++ {
			b.AddRawRequest(&queryIntentReqs[0])
			queryIntentReqs = queryIntentReqs[1:]
		}

		if err := m.db.Run(ctx, &b); err != nil {
			// Bail out on the first error.
			return false, nil, err
		}

		// First, check the QueryTxnResponse to determine whether the
		// state of the transaction record has changed since we began
		// the recovery process.
		resps := b.RawResponse().Responses
		queryTxnResp := resps[0].GetInner().(*roachpb.QueryTxnResponse)
		queriedTxn := &queryTxnResp.QueriedTxn
		if queriedTxn.Status.IsFinalized() ||
			txn.Epoch < queriedTxn.Epoch ||
			txn.WriteTimestamp.Less(queriedTxn.WriteTimestamp) {
			// The transaction was already found to have changed.
			// No need to issue a RecoverTxnRequest, just return
			// the transaction as is.
			return false, queriedTxn, nil
		}

		// Next, look through the QueryIntentResponses to check whether
		// any of the in-flight writes failed.
		for _, ru := range resps[1:] {
			queryIntentResp := ru.GetInner().(*roachpb.QueryIntentResponse)
			if !queryIntentResp.FoundIntent {
				return true /* preventedIntent */, nil, nil
			}
		}
	}
	return false /* preventedIntent */, nil, nil
}

// resolveIndeterminateCommitForTxnRecover performs the "recovery phase" of the
// indeterminate commit resolution process. Using the result of the probing
// phase, recovery issues a RecoverTxn request to resolve the state of the
// transaction.
//
// The method will return a finalized transaction if the RecoverTxn request
// succeeds, but it may also return a transaction in any other state if it is
// discovered to still be live and undergoing state transitions. The only
// guarantee is that the returned transaction will not be in an identical state
// to that of the transaction provided.
func (m *manager) resolveIndeterminateCommitForTxnRecover(
	ctx context.Context, txn *roachpb.Transaction, preventedIntent bool,
) (*roachpb.Transaction, error) {
	var b kv.Batch
	b.Header.Timestamp = m.batchTimestamp(txn)
	b.AddRawRequest(&roachpb.RecoverTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Txn:                 txn.TxnMeta,
		ImplicitlyCommitted: !preventedIntent,
	})

	if err := m.db.Run(ctx, &b); err != nil {
		return nil, err
	}

	resps := b.RawResponse().Responses
	recTxnResp := resps[0].GetInner().(*roachpb.RecoverTxnResponse)
	return &recTxnResp.RecoveredTxn, nil
}

// batchTimestamp returns the timestamp that should be used for operations while
// recovering the provided transaction. The timestamp is at least as high as the
// local clock, but is also forwarded to the transaction's write timestamp to
// satisfy the requirement that QueryIntent requests operate at or above the
// time that they are querying their intent at.
func (m *manager) batchTimestamp(txn *roachpb.Transaction) hlc.Timestamp {
	now := m.clock.Now()
	now.Forward(txn.WriteTimestamp)
	return now
}

// Metrics implements the Manager interface.
func (m *manager) Metrics() Metrics {
	return m.metrics
}

// updateMetrics updates the Manager's metrics to account for a new
// transaction recovery attempt. It returns a function that should
// be called when the recovery attempt completes.
func (m *manager) updateMetrics() func(*roachpb.Transaction, error) {
	m.metrics.AttemptsPending.Inc(1)
	m.metrics.Attempts.Inc(1)
	return func(txn *roachpb.Transaction, err error) {
		m.metrics.AttemptsPending.Dec(1)
		if err != nil {
			m.metrics.Failures.Inc(1)
		} else {
			switch txn.Status {
			case roachpb.COMMITTED:
				m.metrics.SuccessesAsCommitted.Inc(1)
			case roachpb.ABORTED:
				m.metrics.SuccessesAsAborted.Inc(1)
			case roachpb.PENDING, roachpb.STAGING:
				m.metrics.SuccessesAsPending.Inc(1)
			default:
				panic("unexpected")
			}
		}
	}
}
