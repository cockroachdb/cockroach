// Copyright 2014 The Cockroach Authors.
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
	"context"
	"fmt"
	"runtime/debug"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"go.opentelemetry.io/otel/attribute"
)

const (
	// OpTxnCoordSender represents a txn coordinator send operation.
	OpTxnCoordSender = "txn coordinator send"
)

// DisableCommitSanityCheck allows opting out of a fatal assertion error that was observed in the wild
// and for which a root cause is not yet available.
//
// See: https://github.com/cockroachdb/cockroach/pull/73512.
var DisableCommitSanityCheck = envutil.EnvOrDefaultBool("COCKROACH_DISABLE_COMMIT_SANITY_CHECK", false)

// txnState represents states relating to whether an EndTxn request needs
// to be sent.
//go:generate stringer -type=txnState
type txnState int

const (
	// txnPending is the normal state for ongoing transactions.
	txnPending txnState = iota

	// txnRetryableError means that the transaction encountered a
	// TransactionRetryWithProtoRefreshError, and calls to Send() fail in this
	// state. It is possible to move back to txnPending by calling
	// ClearTxnRetryableErr().
	txnRetryableError

	// txnError means that a batch encountered a non-retriable error. Further
	// batches except EndTxn(commit=false) will be rejected.
	txnError

	// txnFinalized means that an EndTxn(commit=true) has been executed
	// successfully, or an EndTxn(commit=false) was sent - regardless of
	// whether it executed successfully or not. Further batches except
	// EndTxn(commit=false) will be rejected; a second rollback is allowed
	// in case the first one fails.
	// TODO(andrei): we'd probably benefit from splitting this state into at least
	// two - transaction definitely cleaned up, and transaction potentially
	// cleaned up.
	txnFinalized
)

// A TxnCoordSender is the production implementation of client.TxnSender. It is
// a Sender which wraps a lower-level Sender (a DistSender) to which it sends
// commands. It works on behalf of the client to keep a transaction's state
// (e.g. intents) and to perform periodic heartbeating of the transaction
// required when necessary.  Unlike other senders, TxnCoordSender is not a
// singleton - an instance is created for every transaction by the
// TxnCoordSenderFactory.
//
// Among the functions it performs are:
// - Heartbeating of the transaction record. Note that heartbeating is done only
// from the root transaction coordinator, in the event that multiple
// coordinators are active (i.e. in a distributed SQL flow).
// - Accumulating lock spans.
// - Attaching lock spans to EndTxn requests, for cleanup.
// - Handles retriable errors by either bumping the transaction's epoch or, in
// case of TransactionAbortedErrors, cleaning up the transaction (in this case,
// the client.Txn is expected to create a new TxnCoordSender instance
// transparently for the higher-level client).
//
// Since it is stateful, the TxnCoordSender needs to understand when a
// transaction is "finished" and the state can be destroyed. As such there's a
// contract that the client.Txn needs obey. Read-only transactions don't matter
// - they're stateless. For the others, once an intent write is sent by the
// client, the TxnCoordSender considers the transactions completed in the
// following situations:
// - A batch containing an EndTxns (commit or rollback) succeeds.
// - A batch containing an EndTxn(commit=false) succeeds or fails. Only
// more rollback attempts can follow a rollback attempt.
// - A batch returns a TransactionAbortedError. As mentioned above, the client
// is expected to create a new TxnCoordSender for the next transaction attempt.
//
// Note that "1PC" batches (i.e. batches containing both a Begin and an
// EndTxn) are no exception from the contract - if the batch fails, the
// client is expected to send a rollback (or perform another transaction attempt
// in case of retriable errors).
type TxnCoordSender struct {
	mu struct {
		syncutil.Mutex

		txnState txnState

		// storedRetryableErr is set when txnState == txnRetryableError. This
		// storedRetryableErr is returned to clients on Send().
		storedRetryableErr *roachpb.TransactionRetryWithProtoRefreshError

		// storedErr is set when txnState == txnError. This storedErr is returned to
		// clients on Send().
		storedErr *roachpb.Error

		// active is set whenever the transaction has sent any requests. Rolling
		// back to a savepoint taken before the TxnCoordSender became active resets
		// the field to false.
		active bool

		// closed is set once this transaction has either committed or rolled back
		// (including when the heartbeat loop cleans it up asynchronously). If the
		// client sends anything other than a rollback, it will get an error
		// (a retryable TransactionAbortedError in case of the async abort).
		closed bool

		// txn is the Transaction proto attached to all the requests and updated on
		// all the responses.
		txn roachpb.Transaction

		// userPriority is the txn's priority. Used when restarting the transaction.
		// This field is only populated on rootTxns.
		userPriority roachpb.UserPriority

		// commitWaitDeferred is set to true when the transaction commit-wait
		// state is deferred and should not be run automatically. Instead, the
		// caller of DeferCommitWait has assumed responsibility for performing
		// the commit-wait.
		commitWaitDeferred bool
	}

	// A pointer member to the creating factory provides access to
	// immutable factory settings.
	*TxnCoordSenderFactory

	// An ordered stack of pluggable request interceptors that can transform
	// batch requests and responses while each maintaining targeted state.
	// The stack is stored in a slice backed by the interceptorAlloc.arr and each
	// txnInterceptor implementation is embedded in the interceptorAlloc struct,
	// so the entire stack is allocated together with TxnCoordSender without any
	// additional heap allocations necessary.
	interceptorStack []txnInterceptor
	interceptorAlloc struct {
		arr [6]txnInterceptor
		txnHeartbeater
		txnSeqNumAllocator
		txnPipeliner
		txnSpanRefresher
		txnCommitter
		txnMetricRecorder
		txnLockGatekeeper // not in interceptorStack array.
	}

	// typ specifies whether this transaction is the top level,
	// or one of potentially many distributed transactions.
	typ kv.TxnType
}

var _ kv.TxnSender = &TxnCoordSender{}

// txnInterceptors are pluggable request interceptors that transform requests
// and responses and can perform operations in the context of a transaction. A
// TxnCoordSender maintains a stack of txnInterceptors that it calls into under
// lock whenever it sends a request.
type txnInterceptor interface {
	lockedSender

	// setWrapped sets the txnInterceptor wrapped lockedSender.
	setWrapped(wrapped lockedSender)

	// populateLeafInputState populates the given input payload
	// for a LeafTxn.
	populateLeafInputState(*roachpb.LeafTxnInputState)

	// populateLeafFinalState populates the final payload
	// for a LeafTxn to bring back into a RootTxn.
	populateLeafFinalState(*roachpb.LeafTxnFinalState)

	// importLeafFinalState updates any internal state held inside the
	// interceptor from the given LeafTxn final state.
	importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState)

	// epochBumpedLocked resets the interceptor in the case of a txn epoch
	// increment.
	epochBumpedLocked()

	// createSavepointLocked is used to populate a savepoint with all the state
	// that needs to be restored on a rollback.
	createSavepointLocked(context.Context, *savepoint)

	// rollbackToSavepointLocked is used to restore the state previously saved by
	// createSavepointLocked().
	rollbackToSavepointLocked(context.Context, savepoint)

	// closeLocked closes the interceptor. It is called when the TxnCoordSender
	// shuts down due to either a txn commit or a txn abort. The method will
	// be called exactly once from cleanupTxnLocked.
	closeLocked()
}

func newRootTxnCoordSender(
	tcf *TxnCoordSenderFactory, txn *roachpb.Transaction, pri roachpb.UserPriority,
) kv.TxnSender {
	txn.AssertInitialized(context.TODO())

	if txn.Status != roachpb.PENDING {
		log.Fatalf(context.TODO(), "unexpected non-pending txn in RootTransactionalSender: %s", txn)
	}
	if txn.Sequence != 0 {
		log.Fatalf(context.TODO(), "cannot initialize root txn with seq != 0: %s", txn)
	}

	tcs := &TxnCoordSender{
		typ:                   kv.RootTxn,
		TxnCoordSenderFactory: tcf,
	}
	tcs.mu.txnState = txnPending
	tcs.mu.userPriority = pri

	// Create a stack of request/response interceptors. All of the objects in
	// this stack are pre-allocated on the TxnCoordSender struct, so this just
	// initializes the interceptors and pieces them together. It then adds a
	// txnLockGatekeeper at the bottom of the stack to connect it with the
	// TxnCoordSender's wrapped sender. First, each of the interceptor objects
	// is initialized.
	tcs.interceptorAlloc.txnHeartbeater.init(
		tcf.AmbientContext,
		tcs.stopper,
		tcs.clock,
		&tcs.metrics,
		tcs.heartbeatInterval,
		&tcs.interceptorAlloc.txnLockGatekeeper,
		&tcs.mu.Mutex,
		&tcs.mu.txn,
	)
	tcs.interceptorAlloc.txnCommitter = txnCommitter{
		st:      tcf.st,
		stopper: tcs.stopper,
		mu:      &tcs.mu.Mutex,
	}
	tcs.interceptorAlloc.txnMetricRecorder = txnMetricRecorder{
		metrics: &tcs.metrics,
		clock:   tcs.clock,
		txn:     &tcs.mu.txn,
	}
	tcs.initCommonInterceptors(tcf, txn, kv.RootTxn)

	// Once the interceptors are initialized, piece them all together in the
	// correct order.
	tcs.interceptorAlloc.arr = [...]txnInterceptor{
		&tcs.interceptorAlloc.txnHeartbeater,
		// Various interceptors below rely on sequence number allocation,
		// so the sequence number allocator is near the top of the stack.
		&tcs.interceptorAlloc.txnSeqNumAllocator,
		// The pipeliner sits above the span refresher because it will
		// never generate transaction retry errors that could be avoided
		// with a refresh.
		&tcs.interceptorAlloc.txnPipeliner,
		// The span refresher may resend entire batches to avoid transaction
		// retries. Because of that, we need to be careful which interceptors
		// sit below it in the stack.
		&tcs.interceptorAlloc.txnSpanRefresher,
		// The committer sits beneath the span refresher so that any
		// retryable errors that it generates have a chance of being
		// "refreshed away" without the need for a txn restart. Because the
		// span refresher can re-issue batches, it needs to be careful about
		// what parts of the batch it mutates. Any mutation needs to be
		// idempotent and should avoid writing to memory when not changing
		// it to avoid looking like a data race.
		&tcs.interceptorAlloc.txnCommitter,
		// The metrics recorder sits at the bottom of the stack so that it
		// can observe all transformations performed by other interceptors.
		&tcs.interceptorAlloc.txnMetricRecorder,
	}
	tcs.interceptorStack = tcs.interceptorAlloc.arr[:]

	tcs.connectInterceptors()

	tcs.mu.txn.Update(txn)
	return tcs
}

func (tc *TxnCoordSender) initCommonInterceptors(
	tcf *TxnCoordSenderFactory, txn *roachpb.Transaction, typ kv.TxnType,
) {
	var riGen rangeIteratorFactory
	if ds, ok := tcf.wrapped.(*DistSender); ok {
		riGen.ds = ds
	}
	tc.interceptorAlloc.txnPipeliner = txnPipeliner{
		st:                     tcf.st,
		riGen:                  riGen,
		txnMetrics:             &tc.metrics,
		condensedIntentsEveryN: &tc.TxnCoordSenderFactory.condensedIntentsEveryN,
	}
	tc.interceptorAlloc.txnSpanRefresher = txnSpanRefresher{
		st:    tcf.st,
		knobs: &tcf.testingKnobs,
		riGen: riGen,
		// We can only allow refresh span retries on root transactions
		// because those are the only places where we have all of the
		// refresh spans. If this is a leaf, as in a distributed sql flow,
		// we need to propagate the error to the root for an epoch restart.
		canAutoRetry:                  typ == kv.RootTxn,
		refreshSuccess:                tc.metrics.RefreshSuccess,
		refreshFail:                   tc.metrics.RefreshFail,
		refreshFailWithCondensedSpans: tc.metrics.RefreshFailWithCondensedSpans,
		refreshMemoryLimitExceeded:    tc.metrics.RefreshMemoryLimitExceeded,
		refreshAutoRetries:            tc.metrics.RefreshAutoRetries,
	}
	tc.interceptorAlloc.txnLockGatekeeper = txnLockGatekeeper{
		wrapped:                 tc.wrapped,
		mu:                      &tc.mu.Mutex,
		allowConcurrentRequests: typ == kv.LeafTxn,
	}
	tc.interceptorAlloc.txnSeqNumAllocator.writeSeq = txn.Sequence
}

func (tc *TxnCoordSender) connectInterceptors() {
	for i, reqInt := range tc.interceptorStack {
		if i < len(tc.interceptorStack)-1 {
			reqInt.setWrapped(tc.interceptorStack[i+1])
		} else {
			reqInt.setWrapped(&tc.interceptorAlloc.txnLockGatekeeper)
		}
	}
}

func newLeafTxnCoordSender(
	tcf *TxnCoordSenderFactory, tis *roachpb.LeafTxnInputState,
) kv.TxnSender {
	txn := &tis.Txn
	// 19.2 roots might have this flag set. In 20.1, the flag is only set by the
	// server and terminated by the client in the span refresher interceptor. If
	// the root is a 19.2 node, we reset the flag because it only confuses
	// that interceptor and provides no benefit.
	txn.WriteTooOld = false
	txn.AssertInitialized(context.TODO())

	if txn.Status != roachpb.PENDING {
		log.Fatalf(context.TODO(), "unexpected non-pending txn in LeafTransactionalSender: %s", tis)
	}

	tcs := &TxnCoordSender{
		typ:                   kv.LeafTxn,
		TxnCoordSenderFactory: tcf,
	}
	tcs.mu.txnState = txnPending
	// No need to initialize tcs.mu.userPriority here,
	// as this field is only used in root txns.

	// Create a stack of request/response interceptors. All of the objects in
	// this stack are pre-allocated on the TxnCoordSender struct, so this just
	// initializes the interceptors and pieces them together. It then adds a
	// txnLockGatekeeper at the bottom of the stack to connect it with the
	// TxnCoordSender's wrapped sender. First, each of the interceptor objects
	// is initialized.
	tcs.initCommonInterceptors(tcf, txn, kv.LeafTxn)

	// Per-interceptor leaf initialization. If/when more interceptors
	// need leaf initialization, this should be turned into an interface
	// method on txnInterceptor with a loop here.
	tcs.interceptorAlloc.txnPipeliner.initializeLeaf(tis)
	tcs.interceptorAlloc.txnSeqNumAllocator.initializeLeaf(tis)

	// Once the interceptors are initialized, piece them all together in the
	// correct order.
	tcs.interceptorAlloc.arr = [cap(tcs.interceptorAlloc.arr)]txnInterceptor{
		// LeafTxns never perform writes so the sequence number allocator
		// should never increment its sequence number counter over its
		// lifetime, but it still plays the important role of assigning each
		// read request the latest sequence number.
		&tcs.interceptorAlloc.txnSeqNumAllocator,
		// The pipeliner is needed on leaves to ensure that in-flight writes
		// are chained onto by reads that should see them.
		&tcs.interceptorAlloc.txnPipeliner,
		// The span refresher may be needed for accumulating the spans to
		// be reported to the Root. See also: #24798.
		//
		// Note: this interceptor must be the last in the list; it is
		// only conditionally included in the stack. See below.
		&tcs.interceptorAlloc.txnSpanRefresher,
	}
	// All other interceptors are absent from a LeafTxn's interceptor stack
	// because they do not serve a role on leaves.

	// If the root has informed us that the read spans are not needed by
	// the root, we don't need the txnSpanRefresher.
	if tis.RefreshInvalid {
		tcs.interceptorStack = tcs.interceptorAlloc.arr[:2]
	} else {
		tcs.interceptorStack = tcs.interceptorAlloc.arr[:3]
	}

	tcs.connectInterceptors()

	tcs.mu.txn.Update(txn)
	return tcs
}

// DisablePipelining is part of the client.TxnSender interface.
func (tc *TxnCoordSender) DisablePipelining() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.active {
		return errors.Errorf("cannot disable pipelining on a running transaction")
	}
	tc.interceptorAlloc.txnPipeliner.disabled = true
	return nil
}

func generateTxnDeadlineExceededErr(
	txn *roachpb.Transaction, deadline hlc.Timestamp,
) *roachpb.Error {
	exceededBy := txn.WriteTimestamp.GoTime().Sub(deadline.GoTime())
	extraMsg := fmt.Sprintf(
		"txn timestamp pushed too much; deadline exceeded by %s (%s > %s)",
		exceededBy, txn.WriteTimestamp, deadline)
	return roachpb.NewErrorWithTxn(
		roachpb.NewTransactionRetryError(roachpb.RETRY_COMMIT_DEADLINE_EXCEEDED, extraMsg), txn)
}

// finalizeNonLockingTxnLocked finalizes a non-locking txn, either marking it as
// committed or aborted. It is equivalent, but cheaper than, sending an
// EndTxnRequest. A non-locking txn doesn't have a transaction record, so
// there's no need to send any request to the server. An EndTxnRequest for a
// non-locking txn is elided by the txnCommitter interceptor. However, calling
// this and short-circuting even earlier is even more efficient (and shows in
// benchmarks).
// TODO(nvanbenschoten): we could have this call into txnCommitter's
// sendLockedWithElidedEndTxn method, but we would want to confirm
// that doing so doesn't cut into the speed-up we see from this fast-path.
func (tc *TxnCoordSender) finalizeNonLockingTxnLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) *roachpb.Error {
	et := ba.Requests[0].GetEndTxn()
	if et.Commit {
		deadline := et.Deadline
		if deadline != nil && !deadline.IsEmpty() && deadline.LessEq(tc.mu.txn.WriteTimestamp) {
			txn := tc.mu.txn.Clone()
			pErr := generateTxnDeadlineExceededErr(txn, *deadline)
			// We need to bump the epoch and transform this retriable error.
			ba.Txn = txn
			return tc.updateStateLocked(ctx, ba, nil /* br */, pErr)
		}
		// Mark the transaction as committed so that, in case this commit is done by
		// the closure passed to db.Txn()), db.Txn() doesn't attempt to commit again.
		// Also so that the correct metric gets incremented.
		tc.mu.txn.Status = roachpb.COMMITTED
	} else {
		tc.mu.txn.Status = roachpb.ABORTED
	}
	tc.finalizeAndCleanupTxnLocked(ctx)
	if et.Commit {
		if err := tc.maybeCommitWait(ctx, false /* deferred */); err != nil {
			return roachpb.NewError(err)
		}
	}
	return nil
}

// Send is part of the client.TxnSender interface.
func (tc *TxnCoordSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// NOTE: The locking here is unusual. Although it might look like it, we are
	// NOT holding the lock continuously for the duration of the Send. We lock
	// here, and unlock at the botton of the interceptor stack, in the
	// txnLockGatekeeper. The we lock again in that interceptor when the response
	// comes, and unlock again in the defer below.
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.active = true

	if pErr := tc.maybeRejectClientLocked(ctx, &ba); pErr != nil {
		return nil, pErr
	}

	if ba.IsSingleEndTxnRequest() && !tc.interceptorAlloc.txnPipeliner.hasAcquiredLocks() {
		return nil, tc.finalizeNonLockingTxnLocked(ctx, ba)
	}

	ctx, sp := tc.AnnotateCtxWithSpan(ctx, OpTxnCoordSender)
	defer sp.Finish()

	// Associate the txnID with the trace.
	if tc.mu.txn.ID == (uuid.UUID{}) {
		log.Fatalf(ctx, "cannot send transactional request through unbound TxnCoordSender")
	}
	if sp.IsVerbose() {
		sp.SetTag("txnID", attribute.StringValue(tc.mu.txn.ID.String()))
		ctx = logtags.AddTag(ctx, "txn", uuid.ShortStringer(tc.mu.txn.ID))
		if log.V(2) {
			ctx = logtags.AddTag(ctx, "ts", tc.mu.txn.WriteTimestamp)
		}
	}

	// It doesn't make sense to use inconsistent reads in a transaction. However,
	// we still need to accept it as a parameter for this to compile.
	if ba.ReadConsistency != roachpb.CONSISTENT {
		return nil, roachpb.NewErrorf("cannot use %s ReadConsistency in txn",
			ba.ReadConsistency)
	}

	lastIndex := len(ba.Requests) - 1
	if lastIndex < 0 {
		return nil, nil
	}

	// Clone the Txn's Proto so that future modifications can be made without
	// worrying about synchronization.
	ba.Txn = tc.mu.txn.Clone()

	// Send the command through the txnInterceptor stack.
	br, pErr := tc.interceptorStack[0].SendLocked(ctx, ba)

	pErr = tc.updateStateLocked(ctx, ba, br, pErr)

	// If we succeeded to commit, or we attempted to rollback, we move to
	// txnFinalized.
	if req, ok := ba.GetArg(roachpb.EndTxn); ok {
		et := req.(*roachpb.EndTxnRequest)
		if (et.Commit && pErr == nil) || !et.Commit {
			tc.finalizeAndCleanupTxnLocked(ctx)
			if et.Commit {
				if err := tc.maybeCommitWait(ctx, false /* deferred */); err != nil {
					return nil, roachpb.NewError(err)
				}
			}
		}
	}

	if pErr != nil {
		return nil, pErr
	}

	if br != nil && br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(nil /* culprit */, br))
	}

	return br, nil
}

// maybeCommitWait performs a "commit-wait" sleep, if doing so is deemed
// necessary for consistency.
//
// By default, commit-wait is only necessary for transactions that commit
// with a future-time timestamp that leads the local HLC clock. This is
// because CockroachDB's consistency model depends on all transactions
// waiting until their commit timestamp is below their gateway clock. In
// doing so, transactions ensure that at the time that they complete, all
// other clocks in the system (i.e. on all possible gateways) will be no
// more than the max_offset below the transaction's commit timestamp. This
// property ensures that all causally dependent transactions will have an
// uncertainty interval (see GlobalUncertaintyLimit) that exceeds the
// original transaction's commit timestamp, preventing stale reads. Without
// the wait, it would be possible for a read-write transaction to write a
// future-time value and then for a causally dependent transaction to read
// below that future-time value, violating "read your writes".
//
// The property must also hold for read-only transactions, which may have a
// commit timestamp in the future due to an uncertainty restart after
// observing a future-time value in their uncertainty interval. In such
// cases, the property that the transaction must wait for the local HLC
// clock to exceed its commit timestamp is not necessary to prevent stale
// reads, but it is necessary to ensure monotonic reads. Without the wait,
// it would be possible for a read-only transaction coordinated on a gateway
// with a fast clock to return a future-time value and then for a causally
// dependent read-only transaction coordinated on a gateway with a slow
// clock to read below that future-time value, violating "monotonic reads".
//
// In practice, most transactions do not need to wait at all, because their
// commit timestamps were pulled from an HLC clock (i.e. are not synthetic)
// and so they will be guaranteed to lead the local HLC's clock, assuming
// proper HLC time propagation. Only transactions whose commit timestamps
// were pushed into the future will need to wait, like those who wrote to a
// global_read range and got bumped by the closed timestamp or those who
// conflicted (write-read or write-write) with an existing future-time
// value.
//
// However, CockroachDB also supports a stricter model of consistency
// through its "linearizable" flag. When in linearizable mode (also known as
// "strict serializable" mode), all writing transactions (but not read-only
// transactions) must wait an additional max_offset after committing to
// ensure that their commit timestamp is below the current HLC clock time of
// any other node in the system. In doing so, all causally dependent
// transactions are guaranteed to start with higher timestamps, regardless
// of the gateway they use. This ensures that all causally dependent
// transactions commit with higher timestamps, even if their read and writes
// sets do not conflict with the original transaction's. This prevents the
// "causal reverse" anomaly which can be observed by a third, concurrent
// transaction.
//
// Even when in linearizable mode and performing this extra wait on the commit
// of read-write transactions, uncertainty intervals are still necessary. This
// is to ensure that any two reads that touch overlapping keys but are executed
// on different nodes obey real-time ordering and do not violate the "monotonic
// reads" property. Without uncertainty intervals, it would be possible for a
// read on a node with a fast clock (ts@15) to observe a committed value (ts@10)
// and then a later read on a node with a slow clock (ts@5) to miss the
// committed value. When contrasting this with Google Spanner, we notice that
// Spanner performs a similar commit-wait but then does not include uncertainty
// intervals. The reason this works in Spanner is that read-write transactions
// in Spanner hold their locks across the commit-wait duration, which blocks
// concurrent readers and enforces real-time ordering between any two readers as
// well between the writer and any future reader. Read-write transactions in
// CockroachDB do not hold locks across commit-wait (they release them before),
// so the uncertainty interval is still needed.
//
// For more, see https://www.cockroachlabs.com/blog/consistency-model/ and
// docs/RFCS/20200811_non_blocking_txns.md.
func (tc *TxnCoordSender) maybeCommitWait(ctx context.Context, deferred bool) error {
	if tc.mu.txn.Status != roachpb.COMMITTED {
		log.Fatalf(ctx, "maybeCommitWait called when not committed")
	}
	if tc.mu.commitWaitDeferred && !deferred {
		// If this is an automatic commit-wait call and the user of this
		// transaction has opted to defer the commit-wait and handle it
		// externally, there's nothing to do yet.
		return nil
	}

	commitTS := tc.mu.txn.WriteTimestamp
	readOnly := tc.mu.txn.Sequence == 0
	linearizable := tc.linearizable
	needWait := commitTS.Synthetic || (linearizable && !readOnly)
	if !needWait {
		// No need to wait. If !Synthetic then we know the commit timestamp
		// leads the local HLC clock, and since that's all we'd need to wait
		// for, we can short-circuit.
		return nil
	}

	waitUntil := commitTS
	if linearizable && !readOnly {
		waitUntil = waitUntil.Add(tc.clock.MaxOffset().Nanoseconds(), 0)
	}

	before := tc.clock.PhysicalTime()
	est := waitUntil.GoTime().Sub(before)
	log.VEventf(ctx, 2, "performing commit-wait sleep for ~%s", est)

	// NB: unlock while sleeping to avoid holding the lock for commit-wait.
	tc.mu.Unlock()
	err := tc.clock.SleepUntil(ctx, waitUntil)
	tc.mu.Lock()
	if err != nil {
		return err
	}

	after := tc.clock.PhysicalTime()
	log.VEventf(ctx, 2, "completed commit-wait sleep, took %s", after.Sub(before))
	tc.metrics.CommitWaits.Inc(1)
	return nil
}

// maybeRejectClientLocked checks whether the transaction is in a state that
// prevents it from continuing, such as the heartbeat having detected the
// transaction to have been aborted.
//
// ba is the batch that the client is trying to send. It's inspected because
// rollbacks are always allowed. Can be nil.
func (tc *TxnCoordSender) maybeRejectClientLocked(
	ctx context.Context, ba *roachpb.BatchRequest,
) *roachpb.Error {
	if ba != nil && ba.IsSingleAbortTxnRequest() && tc.mu.txn.Status != roachpb.COMMITTED {
		// As a special case, we allow rollbacks to be sent at any time. Any
		// rollback attempt moves the TxnCoordSender state to txnFinalized, but higher
		// layers are free to retry rollbacks if they want (and they do, for
		// example, when the context was canceled while txn.Rollback() was running).
		//
		// However, we reject this if we know that the transaction has been
		// committed, to avoid sending the rollback concurrently with the
		// txnCommitter asynchronously making the commit explicit. See:
		// https://github.com/cockroachdb/cockroach/issues/68643
		return nil
	}

	// Check the transaction coordinator state.
	switch tc.mu.txnState {
	case txnPending:
		// All good.
	case txnRetryableError:
		return roachpb.NewError(tc.mu.storedRetryableErr)
	case txnError:
		return tc.mu.storedErr
	case txnFinalized:
		msg := fmt.Sprintf("client already committed or rolled back the transaction. "+
			"Trying to execute: %s", ba.Summary())
		stack := string(debug.Stack())
		log.Errorf(ctx, "%s. stack:\n%s", msg, stack)
		reason := roachpb.TransactionStatusError_REASON_UNKNOWN
		if tc.mu.txn.Status == roachpb.COMMITTED {
			reason = roachpb.TransactionStatusError_REASON_TXN_COMMITTED
		}
		return roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(reason, msg), &tc.mu.txn)
	}

	// Check the transaction proto state, along with any finalized transaction
	// status observed by the transaction heartbeat loop.
	protoStatus := tc.mu.txn.Status
	hbObservedStatus := tc.interceptorAlloc.txnHeartbeater.mu.finalObservedStatus
	switch {
	case protoStatus == roachpb.ABORTED:
		// The transaction was rolled back synchronously.
		fallthrough
	case protoStatus != roachpb.COMMITTED && hbObservedStatus == roachpb.ABORTED:
		// The transaction heartbeat observed an aborted transaction record and
		// this was not due to a synchronous transaction commit and transaction
		// record garbage collection.
		// See the comment on txnHeartbeater.mu.finalObservedStatus for more details.
		abortedErr := roachpb.NewErrorWithTxn(
			roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_CLIENT_REJECT), &tc.mu.txn)
		return roachpb.NewError(tc.handleRetryableErrLocked(ctx, abortedErr))
	case protoStatus != roachpb.PENDING || hbObservedStatus != roachpb.PENDING:
		// The transaction proto is in an unexpected state.
		return roachpb.NewErrorf(
			"unexpected txn state: %s; heartbeat observed status: %s", tc.mu.txn, hbObservedStatus)
	default:
		// All good.
	}
	return nil
}

// finalizeAndCleanupTxnLocked marks the transaction state as finalized and
// closes all interceptors.
func (tc *TxnCoordSender) finalizeAndCleanupTxnLocked(ctx context.Context) {
	tc.mu.txnState = txnFinalized
	tc.cleanupTxnLocked(ctx)
}

// cleanupTxnLocked closes all the interceptors.
func (tc *TxnCoordSender) cleanupTxnLocked(ctx context.Context) {
	if tc.mu.closed {
		return
	}
	tc.mu.closed = true
	// Close each interceptor.
	for _, reqInt := range tc.interceptorStack {
		reqInt.closeLocked()
	}
}

// UpdateStateOnRemoteRetryableErr is part of the TxnSender interface.
func (tc *TxnCoordSender) UpdateStateOnRemoteRetryableErr(
	ctx context.Context, pErr *roachpb.Error,
) *roachpb.Error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return roachpb.NewError(tc.handleRetryableErrLocked(ctx, pErr))
}

// handleRetryableErrLocked takes a retriable error and creates a
// TransactionRetryWithProtoRefreshError containing the transaction that needs
// to be used by the next attempt. It also handles various aspects of updating
// the TxnCoordSender's state. Depending on the error, the TxnCoordSender might
// not be usable afterwards (in case of TransactionAbortedError). The caller is
// expected to check the ID of the resulting transaction. If the TxnCoordSender
// can still be used, it will have been prepared for a new epoch.
func (tc *TxnCoordSender) handleRetryableErrLocked(
	ctx context.Context, pErr *roachpb.Error,
) *roachpb.TransactionRetryWithProtoRefreshError {
	// If the error is a transaction retry error, update metrics to
	// reflect the reason for the restart. More details about the
	// different error types are documented above on the metaRestart
	// variables.
	switch tErr := pErr.GetDetail().(type) {
	case *roachpb.TransactionRetryError:
		switch tErr.Reason {
		case roachpb.RETRY_WRITE_TOO_OLD:
			tc.metrics.RestartsWriteTooOld.Inc()
		case roachpb.RETRY_SERIALIZABLE:
			tc.metrics.RestartsSerializable.Inc()
		case roachpb.RETRY_ASYNC_WRITE_FAILURE:
			tc.metrics.RestartsAsyncWriteFailure.Inc()
		case roachpb.RETRY_COMMIT_DEADLINE_EXCEEDED:
			tc.metrics.RestartsCommitDeadlineExceeded.Inc()
		default:
			tc.metrics.RestartsUnknown.Inc()
		}

	case *roachpb.WriteTooOldError:
		tc.metrics.RestartsWriteTooOldMulti.Inc()

	case *roachpb.ReadWithinUncertaintyIntervalError:
		tc.metrics.RestartsReadWithinUncertainty.Inc()

	case *roachpb.TransactionAbortedError:
		tc.metrics.RestartsTxnAborted.Inc()

	case *roachpb.TransactionPushError:
		tc.metrics.RestartsTxnPush.Inc()

	default:
		tc.metrics.RestartsUnknown.Inc()
	}
	errTxnID := pErr.GetTxn().ID
	newTxn := roachpb.PrepareTransactionForRetry(ctx, pErr, tc.mu.userPriority, tc.clock)

	// We'll pass a TransactionRetryWithProtoRefreshError up to the next layer.
	retErr := roachpb.NewTransactionRetryWithProtoRefreshError(
		pErr.String(),
		errTxnID, // the id of the transaction that encountered the error
		newTxn)

	// Move to a retryable error state, where all Send() calls fail until the
	// state is cleared.
	tc.mu.txnState = txnRetryableError
	tc.mu.storedRetryableErr = retErr

	// If the ID changed, it means we had to start a new transaction and the
	// old one is toast. This TxnCoordSender cannot be used any more - future
	// Send() calls will be rejected; the client is supposed to create a new
	// one.
	if errTxnID != newTxn.ID {
		// Remember that this txn is aborted to reject future requests.
		tc.mu.txn.Status = roachpb.ABORTED
		// Abort the old txn. The client is not supposed to use use this
		// TxnCoordSender any more.
		tc.interceptorAlloc.txnHeartbeater.abortTxnAsyncLocked(ctx)
		tc.cleanupTxnLocked(ctx)
		return retErr
	}

	// This is where we get a new epoch.
	tc.mu.txn.Update(&newTxn)

	// Reset state as this is a retryable txn error that is incrementing
	// the transaction's epoch.
	log.VEventf(ctx, 2, "resetting epoch-based coordinator state on retry")
	for _, reqInt := range tc.interceptorStack {
		reqInt.epochBumpedLocked()
	}
	return retErr
}

// updateStateLocked updates the transaction state in both the success and error
// cases. It also updates retryable errors with the updated transaction for use
// by client restarts.
func (tc *TxnCoordSender) updateStateLocked(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) *roachpb.Error {

	// We handle a couple of different cases:
	// 1) A successful response. If that response carries a transaction proto,
	// we'll use it to update our proto.
	// 2) A non-retriable error. We move to the txnError state and we cleanup. If
	// the error carries a transaction in it, we update our proto with it
	// (although Andrei doesn't know if that serves any purpose).
	// 3) A retriable error. We "handle" it, in the sense that we call
	// handleRetryableErrLocked() to transform the error. If the error instructs
	// the client to start a new transaction (i.e. TransactionAbortedError), then
	// the current transaction is automatically rolled-back. Otherwise, we update
	// our proto for a new epoch.
	// NOTE: We'd love to move to state txnError in case of new error but alas
	// with the current interface we can't: there's no way for the client to ack
	// the receipt of the error and control the switching to the new epoch. This
	// is a major problem of the current txn interface - it means that concurrent
	// users of a txn might operate at the wrong epoch if they race with the
	// receipt of such an error.

	if pErr == nil {
		tc.mu.txn.Update(br.Txn)
		return nil
	}

	if pErr.TransactionRestart() != roachpb.TransactionRestart_NONE {
		if tc.typ == kv.LeafTxn {
			// Leaves handle retriable errors differently than roots. The leaf
			// transaction is not supposed to be used any more after a retriable
			// error. Separately, the error needs to make its way back to the root.

			// From now on, clients will get this error whenever they Send(). We want
			// clients to get the same retriable error so we don't wrap it in
			// TxnAlreadyEncounteredErrorError as we do elsewhere.
			tc.mu.txnState = txnError
			tc.mu.storedErr = pErr

			// Cleanup.
			tc.mu.txn.Update(pErr.GetTxn())
			tc.cleanupTxnLocked(ctx)
			return pErr
		}

		txnID := ba.Txn.ID
		errTxnID := pErr.GetTxn().ID // The ID of the txn that needs to be restarted.
		if errTxnID != txnID {
			// KV should not return errors for transactions other than the one in
			// the BatchRequest.
			log.Fatalf(ctx, "retryable error for the wrong txn. ba.Txn: %s. pErr: %s",
				ba.Txn, pErr)
		}
		return roachpb.NewError(tc.handleRetryableErrLocked(ctx, pErr))
	}

	// This is the non-retriable error case.

	// Most errors cause the transaction to not accept further requests (except a
	// rollback), but some errors are safe to allow continuing (in particular
	// ConditionFailedError). In particular, SQL can recover by rolling back to a
	// savepoint.
	if roachpb.ErrPriority(pErr.GoError()) != roachpb.ErrorScoreUnambiguousError {
		tc.mu.txnState = txnError
		tc.mu.storedErr = roachpb.NewError(&roachpb.TxnAlreadyEncounteredErrorError{
			PrevError: pErr.String(),
		})
	}

	// Update our transaction with any information the error has.
	if errTxn := pErr.GetTxn(); errTxn != nil {
		if err := sanityCheckErrWithTxn(ctx, pErr, ba, &tc.testingKnobs); err != nil {
			return roachpb.NewError(err)
		}
		tc.mu.txn.Update(errTxn)
	}
	return pErr
}

// sanityCheckErrWithTxn verifies whether the error (which must have a txn
// attached) contains a COMMITTED transaction. Only rollbacks should be able to
// encounter such errors. Marking a transaction as explicitly-committed can also
// encounter these errors, but those errors don't make it to the TxnCoordSender.
//
// Returns the passed-in error or fatals (depending on DisableCommitSanityCheck
// env var), wrapping the input error in case of an assertion violation.
//
// The assertion is known to have failed in the wild, see:
// https://github.com/cockroachdb/cockroach/issues/67765
func sanityCheckErrWithTxn(
	ctx context.Context,
	pErrWithTxn *roachpb.Error,
	ba roachpb.BatchRequest,
	knobs *ClientTestingKnobs,
) error {
	txn := pErrWithTxn.GetTxn()
	if txn.Status != roachpb.COMMITTED {
		return nil
	}
	// The only case in which an error can have a COMMITTED transaction in it is
	// when the request was a rollback. Rollbacks can race with commits if a
	// context timeout expires while a commit request is in flight.
	if ba.IsSingleAbortTxnRequest() {
		return nil
	}

	// Finding out about our transaction being committed indicates a serious bug.
	// Requests are not supposed to be sent on transactions after they are
	// committed.
	err := errors.Wrapf(pErrWithTxn.GoError(),
		"transaction unexpectedly committed, ba: %s. txn: %s",
		ba, pErrWithTxn.GetTxn(),
	)
	err = errors.WithAssertionFailure(
		errors.WithIssueLink(err, errors.IssueLink{
			IssueURL: "https://github.com/cockroachdb/cockroach/issues/67765",
			Detail: "you have encountered a known bug in CockroachDB, please consider " +
				"reporting on the Github issue or reach out via Support. " +
				"This assertion can be disabled by setting the environment variable " +
				"COCKROACH_DISABLE_COMMIT_SANITY_CHECK=true",
		}))
	if !DisableCommitSanityCheck && !knobs.DisableCommitSanityCheck {
		log.Fatalf(ctx, "%s", err)
	}
	return err
}

// setTxnAnchorKey sets the key at which to anchor the transaction record. The
// transaction anchor key defaults to the first key written in a transaction.
func (tc *TxnCoordSender) setTxnAnchorKeyLocked(key roachpb.Key) error {
	if len(tc.mu.txn.Key) != 0 {
		return errors.Errorf("transaction anchor key already set")
	}
	tc.mu.txn.Key = key
	return nil
}

// AnchorOnSystemConfigRange is part of the client.TxnSender interface.
func (tc *TxnCoordSender) AnchorOnSystemConfigRange() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// Allow this to be called more than once.
	if bytes.Equal(tc.mu.txn.Key, keys.SystemConfigSpan.Key) {
		return nil
	}
	// The system-config trigger must be run on the system-config range which
	// means any transaction with the trigger set needs to be anchored to the
	// system-config range.
	return tc.setTxnAnchorKeyLocked(keys.SystemConfigSpan.Key)
}

// TxnStatus is part of the client.TxnSender interface.
func (tc *TxnCoordSender) TxnStatus() roachpb.TransactionStatus {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.Status
}

// SetUserPriority is part of the client.TxnSender interface.
func (tc *TxnCoordSender) SetUserPriority(pri roachpb.UserPriority) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.active && pri != tc.mu.userPriority {
		return errors.New("cannot change the user priority of a running transaction")
	}
	tc.mu.userPriority = pri
	tc.mu.txn.Priority = roachpb.MakePriority(pri)
	return nil
}

// SetDebugName is part of the client.TxnSender interface.
func (tc *TxnCoordSender) SetDebugName(name string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.txn.Name == name {
		return
	}

	if tc.mu.active {
		panic("cannot change the debug name of a running transaction")
	}
	tc.mu.txn.Name = name
}

// String is part of the client.TxnSender interface.
func (tc *TxnCoordSender) String() string {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.String()
}

// ReadTimestamp is part of the client.TxnSender interface.
func (tc *TxnCoordSender) ReadTimestamp() hlc.Timestamp {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.ReadTimestamp
}

// ProvisionalCommitTimestamp is part of the client.TxnSender interface.
func (tc *TxnCoordSender) ProvisionalCommitTimestamp() hlc.Timestamp {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.WriteTimestamp
}

// CommitTimestamp is part of the client.TxnSender interface.
func (tc *TxnCoordSender) CommitTimestamp() hlc.Timestamp {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	txn := &tc.mu.txn
	tc.mu.txn.CommitTimestampFixed = true
	return txn.ReadTimestamp
}

// CommitTimestampFixed is part of the client.TxnSender interface.
func (tc *TxnCoordSender) CommitTimestampFixed() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.CommitTimestampFixed
}

// SetFixedTimestamp is part of the client.TxnSender interface.
func (tc *TxnCoordSender) SetFixedTimestamp(ctx context.Context, ts hlc.Timestamp) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// The transaction must not have already been used in this epoch.
	if !tc.interceptorAlloc.txnSpanRefresher.refreshFootprint.empty() {
		return errors.WithContextTags(errors.AssertionFailedf(
			"cannot set fixed timestamp, txn %s already performed reads", tc.mu.txn), ctx)
	}
	if tc.mu.txn.Sequence != 0 {
		return errors.WithContextTags(errors.AssertionFailedf(
			"cannot set fixed timestamp, txn %s already performed writes", tc.mu.txn), ctx)
	}

	tc.mu.txn.ReadTimestamp = ts
	tc.mu.txn.WriteTimestamp = ts
	tc.mu.txn.GlobalUncertaintyLimit = ts
	tc.mu.txn.CommitTimestampFixed = true

	// Set the MinTimestamp to the minimum of the existing MinTimestamp and the fixed
	// timestamp. This ensures that the MinTimestamp is always <= the other timestamps.
	tc.mu.txn.MinTimestamp.Backward(ts)
	return nil
}

// RequiredFrontier is part of the client.TxnSender interface.
func (tc *TxnCoordSender) RequiredFrontier() hlc.Timestamp {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.RequiredFrontier()
}

// ManualRestart is part of the client.TxnSender interface.
func (tc *TxnCoordSender) ManualRestart(
	ctx context.Context, pri roachpb.UserPriority, ts hlc.Timestamp,
) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.txnState == txnFinalized {
		log.Fatalf(ctx, "ManualRestart called on finalized txn: %s", tc.mu.txn)
	}

	// Invalidate any writes performed by any workers after the retry updated
	// the txn's proto but before we synchronized (some of these writes might
	// have been performed at the wrong epoch).
	tc.mu.txn.Restart(pri, 0 /* upgradePriority */, ts)

	for _, reqInt := range tc.interceptorStack {
		reqInt.epochBumpedLocked()
	}

	// The txn might have entered the txnError state after the epoch was bumped.
	// Reset the state for the retry.
	tc.mu.txnState = txnPending
}

// IsSerializablePushAndRefreshNotPossible is part of the client.TxnSender interface.
func (tc *TxnCoordSender) IsSerializablePushAndRefreshNotPossible() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	isTxnPushed := tc.mu.txn.WriteTimestamp != tc.mu.txn.ReadTimestamp
	refreshAttemptNotPossible := tc.interceptorAlloc.txnSpanRefresher.refreshInvalid ||
		tc.mu.txn.CommitTimestampFixed
	// We check CommitTimestampFixed here because, if that's set, refreshing
	// of reads is not performed.
	return isTxnPushed && refreshAttemptNotPossible
}

// Epoch is part of the client.TxnSender interface.
func (tc *TxnCoordSender) Epoch() enginepb.TxnEpoch {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.Epoch
}

// IsLocking is part of the client.TxnSender interface.
func (tc *TxnCoordSender) IsLocking() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.IsLocking()
}

// IsTracking returns true if the heartbeat loop is running.
func (tc *TxnCoordSender) IsTracking() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.interceptorAlloc.txnHeartbeater.heartbeatLoopRunningLocked()
}

// Active returns true if requests were sent already. Rolling back to a
// savepoint taken before any requests were sent resets this to false.
func (tc *TxnCoordSender) Active() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.active
}

// GetLeafTxnInputState is part of the client.TxnSender interface.
func (tc *TxnCoordSender) GetLeafTxnInputState(
	ctx context.Context, opt kv.TxnStatusOpt,
) (*roachpb.LeafTxnInputState, error) {
	tis := new(roachpb.LeafTxnInputState)
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.checkTxnStatusLocked(ctx, opt); err != nil {
		return nil, err
	}

	// Copy mutable state so access is safe for the caller.
	tis.Txn = tc.mu.txn
	for _, reqInt := range tc.interceptorStack {
		reqInt.populateLeafInputState(tis)
	}

	// Also mark the TxnCoordSender as "active".  This prevents changing
	// the priority after a leaf has been created. It als conservatively
	// ensures that Active() returns true if there's maybe a command
	// being executed concurrently by a leaf.
	tc.mu.active = true

	return tis, nil
}

// GetLeafTxnFinalState is part of the client.TxnSender interface.
func (tc *TxnCoordSender) GetLeafTxnFinalState(
	ctx context.Context, opt kv.TxnStatusOpt,
) (*roachpb.LeafTxnFinalState, error) {
	tfs := new(roachpb.LeafTxnFinalState)
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.checkTxnStatusLocked(ctx, opt); err != nil {
		return nil, err
	}

	// For compatibility with pre-20.1 nodes: populate the command
	// count.
	// TODO(knz,andrei): Remove this and the command count
	// field in 20.2.
	if tc.mu.active {
		tfs.DeprecatedCommandCount = 1
	}

	// Copy mutable state so access is safe for the caller.
	tfs.Txn = tc.mu.txn
	for _, reqInt := range tc.interceptorStack {
		reqInt.populateLeafFinalState(tfs)
	}

	return tfs, nil
}

func (tc *TxnCoordSender) checkTxnStatusLocked(ctx context.Context, opt kv.TxnStatusOpt) error {
	switch opt {
	case kv.AnyTxnStatus:
		// Nothing to check.
	case kv.OnlyPending:
		// Check the coordinator's proto status.
		rejectErr := tc.maybeRejectClientLocked(ctx, nil /* ba */)
		if rejectErr != nil {
			return rejectErr.GoError()
		}
	default:
		panic("unreachable")
	}
	return nil
}

// UpdateRootWithLeafFinalState is part of the client.TxnSender interface.
func (tc *TxnCoordSender) UpdateRootWithLeafFinalState(
	ctx context.Context, tfs *roachpb.LeafTxnFinalState,
) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.txn.ID == (uuid.UUID{}) {
		log.Fatalf(ctx, "cannot UpdateRootWithLeafFinalState on unbound TxnCoordSender. input id: %s", tfs.Txn.ID)
	}

	// Sanity check: don't combine if the tfs is for a different txn ID.
	if tc.mu.txn.ID != tfs.Txn.ID {
		return
	}

	// If the LeafTxnFinalState is telling us the transaction has been
	// aborted, it's better if we don't ingest it. Ingesting it would
	// possibly put us in an inconsistent state, with an ABORTED proto
	// but with the heartbeat loop still running. It presumably follows
	// a TxnAbortedError that was also received. If that error was also
	// passed to us, then we've already aborted the txn and importing
	// the leaf statewould be OK. However, as it stands, if the
	// TxnAbortedError followed a non-retriable error, than we don't get
	// the aborted error (in fact, we don't get either of the errors;
	// the client is responsible for rolling back).
	//
	// TODO(andrei): A better design would be to abort the txn as soon
	// as any error is received from DistSQL, which would eliminate
	// qualms about what error comes first.
	if tfs.Txn.Status != roachpb.PENDING {
		return
	}

	tc.mu.txn.Update(&tfs.Txn)
	for _, reqInt := range tc.interceptorStack {
		reqInt.importLeafFinalState(ctx, tfs)
	}
}

// TestingCloneTxn is part of the client.TxnSender interface.
// This is for use by tests only. To derive leaf TxnCoordSenders,
// use GetLeafTxnInitialState instead.
func (tc *TxnCoordSender) TestingCloneTxn() *roachpb.Transaction {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.Clone()
}

// PrepareRetryableError is part of the client.TxnSender interface.
func (tc *TxnCoordSender) PrepareRetryableError(ctx context.Context, msg string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return roachpb.NewTransactionRetryWithProtoRefreshError(
		msg, tc.mu.txn.ID, tc.mu.txn)
}

// Step is part of the TxnSender interface.
func (tc *TxnCoordSender) Step(ctx context.Context) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.interceptorAlloc.txnSeqNumAllocator.stepLocked(ctx)
}

// SetReadSeqNum is part of the TxnSender interface.
func (tc *TxnCoordSender) SetReadSeqNum(seq enginepb.TxnSeq) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if seq < 0 || seq > tc.interceptorAlloc.txnSeqNumAllocator.writeSeq {
		return errors.AssertionFailedf("invalid read seq num < 0 || > writeSeq (%d): %d",
			tc.interceptorAlloc.txnSeqNumAllocator.writeSeq, seq)
	}
	tc.interceptorAlloc.txnSeqNumAllocator.readSeq = seq
	return nil
}

// ConfigureStepping is part of the TxnSender interface.
func (tc *TxnCoordSender) ConfigureStepping(
	ctx context.Context, mode kv.SteppingMode,
) (prevMode kv.SteppingMode) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.interceptorAlloc.txnSeqNumAllocator.configureSteppingLocked(mode)
}

// GetSteppingMode is part of the TxnSender interface.
func (tc *TxnCoordSender) GetSteppingMode(ctx context.Context) (curMode kv.SteppingMode) {
	curMode = kv.SteppingDisabled
	if tc.interceptorAlloc.txnSeqNumAllocator.steppingModeEnabled {
		curMode = kv.SteppingEnabled
	}
	return curMode
}

// ManualRefresh is part of the TxnSender interface.
func (tc *TxnCoordSender) ManualRefresh(ctx context.Context) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Hijack the pre-emptive refresh code path to perform the refresh but
	// provide the force flag to ensure that the refresh occurs unconditionally.
	// We provide an empty BatchRequest - maybeRefreshPreemptivelyLocked just
	// needs the transaction proto. The function then returns a BatchRequest
	// with the updated transaction proto. We use this updated proto to call
	// into updateStateLocked directly.
	var ba roachpb.BatchRequest
	ba.Txn = tc.mu.txn.Clone()
	const force = true
	refreshedBa, pErr := tc.interceptorAlloc.txnSpanRefresher.maybeRefreshPreemptivelyLocked(ctx, ba, force)
	if pErr != nil {
		pErr = tc.updateStateLocked(ctx, ba, nil, pErr)
	} else {
		var br roachpb.BatchResponse
		br.Txn = refreshedBa.Txn
		pErr = tc.updateStateLocked(ctx, ba, &br, nil)
	}
	return pErr.GoError()
}

// DeferCommitWait is part of the TxnSender interface.
func (tc *TxnCoordSender) DeferCommitWait(ctx context.Context) func(context.Context) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.commitWaitDeferred = true
	return func(ctx context.Context) error {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		if tc.mu.txn.Status != roachpb.COMMITTED {
			// If transaction has not committed, there's nothing to do.
			return nil
		}
		return tc.maybeCommitWait(ctx, true /* deferred */)
	}
}

// GetTxnRetryableErr is part of the TxnSender interface.
func (tc *TxnCoordSender) GetTxnRetryableErr(
	ctx context.Context,
) *roachpb.TransactionRetryWithProtoRefreshError {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txnState == txnRetryableError {
		return tc.mu.storedRetryableErr
	}
	return nil
}

// ClearTxnRetryableErr is part of the TxnSender interface.
func (tc *TxnCoordSender) ClearTxnRetryableErr(ctx context.Context) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txnState == txnRetryableError {
		tc.mu.storedRetryableErr = nil
		tc.mu.txnState = txnPending
	}
}
