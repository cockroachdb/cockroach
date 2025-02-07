// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
)

const (
	// OpTxnCoordSender represents a txn coordinator send operation.
	OpTxnCoordSender = "txn coordinator send"
)

// forceTxnRetries enables random transaction retries for test builds
// even if they aren't enabled via testing knobs.
var forceTxnRetries = envutil.EnvOrDefaultBool("COCKROACH_FORCE_RANDOM_TXN_RETRIES", false)

// txnState represents states relating to whether an EndTxn request needs
// to be sent.
//
//go:generate stringer -type=txnState
type txnState int

const (
	// txnPending is the normal state for ongoing transactions.
	txnPending txnState = iota

	// txnRetryableError means that the transaction encountered a
	// TransactionRetryWithProtoRefreshError, and calls to Send() fail in this
	// state. It is possible to move back to txnPending by calling
	// ClearRetryableErr().
	txnRetryableError

	// txnError means that a batch encountered a non-retriable error. Further
	// batches except EndTxn(commit=false) will be rejected.
	txnError

	// txnPrepared means that an EndTxn(commit=true,prepare=true) has been
	// executed successfully. Further batches except EndTxn(commit=*) will
	// be rejected.
	txnPrepared

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

// A TxnCoordSender is the production implementation of kv.TxnSender. It is
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
// the kv.Txn is expected to create a new TxnCoordSender instance transparently
// for the higher-level client).
//
// Since it is stateful, the TxnCoordSender needs to understand when a
// transaction is "finished" and the state can be destroyed. As such there's a
// contract that the kv.Txn needs obey. Read-only transactions don't matter -
// they're stateless. For the others, once an intent write is sent by the
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
		storedRetryableErr *kvpb.TransactionRetryWithProtoRefreshError

		// storedErr is set when txnState == txnError. This storedErr is returned to
		// clients on Send().
		storedErr *kvpb.Error

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
		arr [7]txnInterceptor
		txnHeartbeater
		txnSeqNumAllocator
		txnWriteBuffer
		txnPipeliner
		txnCommitter
		txnSpanRefresher
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
	importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error

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
		tcf.st,
		&tcs.testingKnobs,
	)
	tcs.interceptorAlloc.txnCommitter = txnCommitter{
		st:      tcf.st,
		stopper: tcs.stopper,
		metrics: &tcs.metrics,
		mu:      &tcs.mu.Mutex,
	}
	tcs.interceptorAlloc.txnMetricRecorder = txnMetricRecorder{
		metrics:    &tcs.metrics,
		timeSource: timeutil.DefaultTimeSource{},
		txn:        &tcs.mu.txn,
	}
	tcs.initCommonInterceptors(tcf, txn, kv.RootTxn)

	// Once the interceptors are initialized, piece them all together in the
	// correct order.
	tcs.interceptorAlloc.arr = [...]txnInterceptor{
		&tcs.interceptorAlloc.txnHeartbeater,
		// Various interceptors below rely on sequence number allocation,
		// so the sequence number allocator is near the top of the stack.
		&tcs.interceptorAlloc.txnSeqNumAllocator,
		// The write buffer sits above the pipeliner to ensure it doesn't need to
		// know how to handle QueryIntentRequests, as those are only generated (and
		// handled) by the pipeliner.
		&tcs.interceptorAlloc.txnWriteBuffer,
		// The pipeliner sits above the span refresher because it will
		// never generate transaction retry errors that could be avoided
		// with a refresh.
		&tcs.interceptorAlloc.txnPipeliner,
		// The committer sits above the span refresher because it will
		// never generate transaction retry errors that could be avoided
		// with a refresh. However, it may re-issue parts of "successful"
		// batches that failed to qualify for the parallel commit condition.
		// These re-issued batches benefit from pre-emptive refreshes in the
		// span refresher.
		&tcs.interceptorAlloc.txnCommitter,
		// The span refresher may resend entire batches to avoid transaction
		// retries. Because of that, we need to be careful which interceptors
		// sit below it in the stack.
		// The span refresher sits below the committer, so it must be prepared
		// to see STAGING transaction protos in responses and errors. It should
		// defer to the committer for how to handle those.
		&tcs.interceptorAlloc.txnSpanRefresher,
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
		st:                       tcf.st,
		riGen:                    riGen,
		txnMetrics:               &tc.metrics,
		condensedIntentsEveryN:   &tc.TxnCoordSenderFactory.condensedIntentsEveryN,
		inflightOverBudgetEveryN: &tc.TxnCoordSenderFactory.inflightOverBudgetEveryN,
	}
	tc.interceptorAlloc.txnSpanRefresher = txnSpanRefresher{
		st:      tcf.st,
		knobs:   &tcf.testingKnobs,
		riGen:   riGen,
		metrics: &tc.metrics,
		// We can only allow refresh span retries on root transactions
		// because those are the only places where we have all of the
		// refresh spans. If this is a leaf, as in a distributed sql flow,
		// we need to propagate the error to the root for an epoch restart.
		canAutoRetry: typ == kv.RootTxn,
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

// DisablePipelining is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) DisablePipelining() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.active {
		return errors.Errorf("cannot disable pipelining on a running transaction")
	}
	tc.interceptorAlloc.txnPipeliner.disabled = true
	return nil
}

func generateTxnDeadlineExceededErr(txn *roachpb.Transaction, deadline hlc.Timestamp) *kvpb.Error {
	exceededBy := txn.WriteTimestamp.GoTime().Sub(deadline.GoTime())
	extraMsg := redact.Sprintf(
		"txn timestamp pushed too much; deadline exceeded by %s (%s > %s)",
		exceededBy, txn.WriteTimestamp, deadline)
	return kvpb.NewErrorWithTxn(
		kvpb.NewTransactionRetryError(kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED, extraMsg), txn)
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
	ctx context.Context, ba *kvpb.BatchRequest,
) *kvpb.Error {
	et := ba.Requests[0].GetEndTxn()
	if et.Commit {
		deadline := et.Deadline
		if !deadline.IsEmpty() && deadline.LessEq(tc.mu.txn.WriteTimestamp) {
			txn := tc.mu.txn.Clone()
			pErr := generateTxnDeadlineExceededErr(txn, deadline)
			// We need to bump the epoch and transform this retriable error.
			ba.Txn = txn
			return tc.updateStateLocked(ctx, ba, nil /* br */, pErr)
		}
		tc.interceptorAlloc.txnMetricRecorder.setReadOnlyCommit()
		if et.Prepare {
			tc.mu.txn.Status = roachpb.PREPARED
			tc.markTxnPreparedLocked(ctx)
		} else {
			// Mark the transaction as committed so that, in case this commit is done
			// by the closure passed to db.Txn()), db.Txn() doesn't attempt to commit
			// again. Also, so that the correct metric gets incremented.
			tc.mu.txn.Status = roachpb.COMMITTED
			tc.finalizeAndCleanupTxnLocked(ctx)
		}
		if err := tc.maybeCommitWait(ctx, false /* deferred */); err != nil {
			return kvpb.NewError(err)
		}
	} else {
		tc.mu.txn.Status = roachpb.ABORTED
		tc.finalizeAndCleanupTxnLocked(ctx)
	}
	return nil
}

// Send is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) Send(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	// NOTE: The locking here is unusual. Although it might look like it, we are
	// NOT holding the lock continuously for the duration of the Send. We lock
	// here, and unlock at the bottom of the interceptor stack, in the
	// txnLockGatekeeper. Then we lock again in that interceptor when the response
	// comes, and unlock again in the defer below.
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.active = true

	if pErr := tc.maybeRejectIncompatibleRequest(ctx, ba); pErr != nil {
		return nil, pErr
	}

	if pErr := tc.maybeRejectClientLocked(ctx, ba); pErr != nil {
		return nil, pErr
	}

	if ba.IsSingleEndTxnRequest() && !tc.interceptorAlloc.txnPipeliner.hasAcquiredLocks() &&
		!tc.interceptorAlloc.txnWriteBuffer.hasBufferedWrites() {
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
	if ba.ReadConsistency != kvpb.CONSISTENT {
		return nil, kvpb.NewErrorf("cannot use %s ReadConsistency in txn",
			ba.ReadConsistency)
	}

	lastIndex := len(ba.Requests) - 1
	if lastIndex < 0 {
		return nil, nil
	}

	if tc.shouldStepReadTimestampLocked() {
		tc.maybeAutoStepReadTimestampLocked()
	}

	// Clone the Txn's Proto so that future modifications can be made without
	// worrying about synchronization.
	ba.Txn = tc.mu.txn.Clone()

	// Send the command through the txnInterceptor stack.
	br, pErr := tc.interceptorStack[0].SendLocked(ctx, ba)

	pErr = tc.updateStateLocked(ctx, ba, br, pErr)

	// If we succeeded to commit, or we attempted to rollback, we move to
	// txnFinalized. If we succeeded to prepare, we move to txnPrepared.
	if req, ok := ba.GetArg(kvpb.EndTxn); ok {
		et := req.(*kvpb.EndTxnRequest)
		if et.Commit {
			if pErr == nil {
				if et.Prepare {
					tc.markTxnPreparedLocked(ctx)
				} else {
					tc.finalizeAndCleanupTxnLocked(ctx)
				}
				if err := tc.maybeCommitWait(ctx, false /* deferred */); err != nil {
					return nil, kvpb.NewError(err)
				}
			}
		} else /* !et.Commit */ {
			tc.finalizeAndCleanupTxnLocked(ctx)
		}
	}

	if pErr != nil {
		return nil, pErr
	}

	if br != nil && br.Error != nil {
		panic(kvpb.ErrorUnexpectedlySet(nil /* culprit */, br))
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
// commit timestamps were pulled from an HLC clock (either the local clock
// or a remote clock on a node whom the local node has communicated with)
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
	if tc.mu.txn.Status != roachpb.PREPARED && tc.mu.txn.Status != roachpb.COMMITTED {
		log.Fatalf(ctx, "maybeCommitWait called when not prepared/committed")
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

	waitUntil := commitTS
	if linearizable && !readOnly {
		waitUntil = waitUntil.Add(tc.clock.MaxOffset().Nanoseconds(), 0)
	}
	if waitUntil.LessEq(tc.clock.Now()) {
		// No wait fast-path. This is the common case for most transactions. Only
		// transactions who have their commit timestamp bumped into the future will
		// need to wait.
		return nil
	}
	if fn := tc.testingKnobs.CommitWaitFilter; fn != nil {
		fn()
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

// maybeRejectIncompatibleRequest checks if the TxnCoordSender is compatible with
// a given BatchRequest.
// Specifically, a Leaf TxnCoordSender is not compatible with locking requests.
func (tc *TxnCoordSender) maybeRejectIncompatibleRequest(
	ctx context.Context, ba *kvpb.BatchRequest,
) *kvpb.Error {
	switch tc.typ {
	case kv.RootTxn:
		return nil
	case kv.LeafTxn:
		if ba.IsLocking() {
			return kvpb.NewError(errors.WithContextTags(errors.AssertionFailedf(
				"LeafTxn %s incompatible with locking request %s", tc.mu.txn, ba.Summary()), ctx))
		}
		return nil
	default:
		panic("unexpected TxnType")
	}
}

// maybeRejectClientLocked checks whether the transaction is in a state that
// prevents it from continuing, such as the heartbeat having detected the
// transaction to have been aborted.
//
// ba is the batch that the client is trying to send. It's inspected because
// rollbacks are always allowed. Can be nil.
func (tc *TxnCoordSender) maybeRejectClientLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) *kvpb.Error {
	rollback := ba != nil && ba.IsSingleAbortTxnRequest()
	if rollback && tc.mu.txn.Status != roachpb.COMMITTED {
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
		return kvpb.NewError(tc.mu.storedRetryableErr)
	case txnError:
		return tc.mu.storedErr
	case txnPrepared:
		endTxn := ba != nil && ba.IsSingleEndTxnRequest()
		if endTxn {
			return nil
		}
		msg := redact.Sprintf("client already prepared the transaction. "+
			"Trying to execute: %s", ba.Summary())
		reason := kvpb.TransactionStatusError_REASON_UNKNOWN
		return kvpb.NewErrorWithTxn(kvpb.NewTransactionStatusError(reason, msg), &tc.mu.txn)
	case txnFinalized:
		msg := redact.Sprintf("client already committed or rolled back the transaction. "+
			"Trying to execute: %s", ba.Summary())
		if !rollback {
			// If the client is trying to do anything other than rollback, it is
			// unexpected for it to find the transaction already in a txnFinalized
			// state. This may be a bug, so log a stack trace.
			stack := debugutil.Stack()
			log.Errorf(ctx, "%s. stack:\n%s", msg, stack)
		}
		reason := kvpb.TransactionStatusError_REASON_UNKNOWN
		if tc.mu.txn.Status == roachpb.COMMITTED {
			reason = kvpb.TransactionStatusError_REASON_TXN_COMMITTED
		}
		return kvpb.NewErrorWithTxn(kvpb.NewTransactionStatusError(reason, msg), &tc.mu.txn)
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
		abortedErr := kvpb.NewErrorWithTxn(
			kvpb.NewTransactionAbortedError(kvpb.ABORT_REASON_CLIENT_REJECT), &tc.mu.txn)
		return kvpb.NewError(tc.handleRetryableErrLocked(ctx, abortedErr))
	case protoStatus != roachpb.PENDING || hbObservedStatus != roachpb.PENDING:
		// The transaction proto is in an unexpected state.
		return kvpb.NewErrorf(
			"unexpected txn state: %s; heartbeat observed status: %s", tc.mu.txn, hbObservedStatus)
	default:
		// All good.
	}
	return nil
}

// ClientFinalized is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) ClientFinalized() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txnState == txnFinalized
}

// finalizeAndCleanupTxnLocked marks the transaction state as finalized and
// closes all interceptors.
func (tc *TxnCoordSender) finalizeAndCleanupTxnLocked(ctx context.Context) {
	tc.mu.txnState = txnFinalized
	tc.cleanupTxnLocked(ctx)
}

// markTxnPreparedLocked marks the transaction state as prepared and closes all
// interceptors.
func (tc *TxnCoordSender) markTxnPreparedLocked(ctx context.Context) {
	tc.mu.txnState = txnPrepared
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

// handleRetryableErrLocked takes a retriable error and creates a
// TransactionRetryWithProtoRefreshError containing the transaction that needs
// to be used by the next attempt. It also handles various aspects of updating
// the TxnCoordSender's state. Depending on the error, the TxnCoordSender might
// not be usable afterwards (in case of TransactionAbortedError). The caller is
// expected to check the ID of the resulting transaction. If the TxnCoordSender
// can still be used, it will have been prepared for a new epoch.
func (tc *TxnCoordSender) handleRetryableErrLocked(ctx context.Context, pErr *kvpb.Error) error {
	// If the transaction is already in a retryable state and the provided error
	// does not have a higher priority than the existing error, return the
	// existing error instead of attempting to handle the retryable error. This
	// prevents the TxnCoordSender from losing information about a higher
	// priority error.
	if tc.mu.txnState == txnRetryableError &&
		kvpb.ErrPriority(pErr.GoError()) <= kvpb.ErrPriority(tc.mu.storedRetryableErr) {
		return tc.mu.storedRetryableErr
	}

	// If the error is a transaction retry error, update metrics to
	// reflect the reason for the restart. More details about the
	// different error types are documented above on the metaRestart
	// variables.
	var conflictingTxn *enginepb.TxnMeta
	switch tErr := pErr.GetDetail().(type) {
	case *kvpb.TransactionRetryError:
		conflictingTxn = tErr.ConflictingTxn
		switch tErr.Reason {
		case kvpb.RETRY_WRITE_TOO_OLD:
			tc.metrics.RestartsWriteTooOld.Inc()
		case kvpb.RETRY_SERIALIZABLE:
			tc.metrics.RestartsSerializable.Inc()
		case kvpb.RETRY_ASYNC_WRITE_FAILURE:
			tc.metrics.RestartsAsyncWriteFailure.Inc()
		case kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED:
			tc.metrics.RestartsCommitDeadlineExceeded.Inc()
		default:
			tc.metrics.RestartsUnknown.Inc()
		}

	case *kvpb.WriteTooOldError:
		tc.metrics.RestartsWriteTooOld.Inc()

	case *kvpb.ReadWithinUncertaintyIntervalError:
		tc.metrics.RestartsReadWithinUncertainty.Inc()

	case *kvpb.TransactionAbortedError:
		tc.metrics.RestartsTxnAborted.Inc()

	case *kvpb.TransactionPushError:
		tc.metrics.RestartsTxnPush.Inc()

	default:
		tc.metrics.RestartsUnknown.Inc()
	}

	// Construct the next txn proto using the provided error.
	prevTxn := pErr.GetTxn()
	nextTxn, assertErr := kvpb.PrepareTransactionForRetry(pErr, tc.mu.userPriority, tc.clock)
	if assertErr != nil {
		return assertErr
	}

	// Construct the TransactionRetryWithProtoRefreshError using the next txn proto.
	retErr := kvpb.NewTransactionRetryWithProtoRefreshError(
		redact.Sprint(pErr), /* msg */
		prevTxn.ID,          /* prevTxnID */
		prevTxn.Epoch,       /* prevTxnEpoch */
		nextTxn,             /* nextTxn */
		kvpb.WithConflictingTxn(conflictingTxn),
	)

	// Update the TxnCoordSender's state.
	tc.handleTransactionRetryWithProtoRefreshErrorErrLocked(ctx, retErr)

	// Finally, pass a TransactionRetryWithProtoRefreshError up to the next layer.
	return retErr
}

// handleTransactionRetryWithProtoRefreshErrorErrLocked takes a
// TransactionRetryWithProtoRefreshError containing the transaction that needs
// to be used by the next attempt and handles various aspects of updating the
// TxnCoordSender's state.
func (tc *TxnCoordSender) handleTransactionRetryWithProtoRefreshErrorErrLocked(
	ctx context.Context, retErr *kvpb.TransactionRetryWithProtoRefreshError,
) {
	// Move to a retryable error state, where all Send() calls fail until the
	// state is cleared.
	tc.mu.txnState = txnRetryableError
	tc.mu.storedRetryableErr = retErr

	// If the previous transaction is aborted, it means we had to start a new
	// transaction and the old one is toast. This TxnCoordSender cannot be used
	// any more - future Send() calls will be rejected; the client is supposed to
	// create a new one.
	if retErr.PrevTxnAborted() {
		// Remember that this txn is aborted to reject future requests.
		tc.mu.txn.Status = roachpb.ABORTED
		// Abort the old txn. The client is not supposed to use this
		// TxnCoordSender anymore.
		tc.interceptorAlloc.txnHeartbeater.abortTxnAsyncLocked(ctx)
		tc.cleanupTxnLocked(ctx)
		return
	}

	// This is where we get a new read timestamp or a new epoch.
	tc.mu.txn.Update(&retErr.NextTransaction)

	// Reset state if this is a retryable txn error that is incrementing
	// the transaction's epoch.
	if retErr.PrevTxnEpochBumped() {
		log.VEventf(ctx, 2, "resetting epoch-based coordinator state on retry")
		for _, reqInt := range tc.interceptorStack {
			reqInt.epochBumpedLocked()
		}
	}
}

// updateStateLocked updates the transaction state in both the success and error
// cases. It also updates retryable errors with the updated transaction for use
// by client restarts.
func (tc *TxnCoordSender) updateStateLocked(
	ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse, pErr *kvpb.Error,
) *kvpb.Error {

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

	if pErr.TransactionRestart() != kvpb.TransactionRestart_NONE {
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
		return kvpb.NewError(tc.handleRetryableErrLocked(ctx, pErr))
	}

	// This is the non-retriable error case.

	// Most errors cause the transaction to not accept further requests (except a
	// rollback), but some errors are safe to allow continuing (in particular
	// ConditionFailedError). In particular, SQL can recover by rolling back to a
	// savepoint.
	if kvpb.ErrPriority(pErr.GoError()) != kvpb.ErrorScoreUnambiguousError {
		tc.mu.txnState = txnError
		tc.mu.storedErr = kvpb.NewError(&kvpb.TxnAlreadyEncounteredErrorError{
			PrevError: pErr.String(),
		})
	}

	// Update our transaction with any information the error has.
	if errTxn := pErr.GetTxn(); errTxn != nil {
		if err := sanityCheckErrWithTxn(ctx, pErr, ba, &tc.testingKnobs); err != nil {
			return kvpb.NewError(err)
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
// Wraps the error in case of an assertion violation, otherwise returns as-is.
//
// This checks for the occurence of a known issue involving ambiguous write
// errors that occur alongside commits, which may race with transaction
// recovery requests started by contending operations.
// https://github.com/cockroachdb/cockroach/issues/103817
func sanityCheckErrWithTxn(
	ctx context.Context, pErrWithTxn *kvpb.Error, ba *kvpb.BatchRequest, _ *ClientTestingKnobs,
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

	// Finding out about our transaction being committed indicates a serious but
	// known bug. Requests are not supposed to be sent on transactions after they
	// are committed.
	err := errors.Wrapf(pErrWithTxn.GoError(),
		"transaction unexpectedly committed, ba: %s. txn: %s",
		ba, pErrWithTxn.GetTxn(),
	)
	err = errors.WithAssertionFailure(
		errors.WithIssueLink(err, errors.IssueLink{
			IssueURL: build.MakeIssueURL(103817),
			Detail: "you have encountered a known bug in CockroachDB, please consider " +
				"reporting on the Github issue or reach out via Support.",
		}))
	log.Warningf(ctx, "%v", err)
	return err
}

// TxnStatus is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) TxnStatus() roachpb.TransactionStatus {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.Status
}

// SetIsoLevel is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) SetIsoLevel(isoLevel isolation.Level) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.active && isoLevel != tc.mu.txn.IsoLevel {
		// TODO(nvanbenschoten): this error is currently returned directly to the
		// SQL client issuing the SET TRANSACTION ISOLATION LEVEL statement. We
		// should either wrap this error with a pgcode or catch the condition
		// earlier and make this an assertion failure.
		return errors.New("cannot change the isolation level of a running transaction")
	}
	tc.mu.txn.IsoLevel = isoLevel
	return nil
}

// IsoLevel is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) IsoLevel() isolation.Level {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.IsoLevel
}

// SetUserPriority is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) SetUserPriority(pri roachpb.UserPriority) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.active && pri != tc.mu.userPriority {
		// TODO(nvanbenschoten): this error is currently returned directly to the
		// SQL client issuing the SET TRANSACTION PRIORITY statement. We should
		// either wrap this error with a pgcode or catch the condition earlier and
		// make this an assertion failure.
		return errors.New("cannot change the user priority of a running transaction")
	}
	tc.mu.userPriority = pri
	tc.mu.txn.Priority = roachpb.MakePriority(pri)
	return nil
}

// SetDebugName is part of the kv.TxnSender interface.
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

// GetOmitInRangefeeds is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) GetOmitInRangefeeds() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.OmitInRangefeeds
}

// SetOmitInRangefeeds is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) SetOmitInRangefeeds() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.txn.OmitInRangefeeds {
		return
	}

	if tc.mu.active {
		panic("cannot change OmitInRangefeeds of a running transaction")
	}
	tc.mu.txn.OmitInRangefeeds = true
}

// SetBufferedWritesEnabled is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) SetBufferedWritesEnabled(enabled bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.active && enabled && !tc.interceptorAlloc.txnWriteBuffer.enabled {
		panic("cannot enable buffered writes on a running transaction")
	}
	tc.interceptorAlloc.txnWriteBuffer.enabled = enabled
	// TODO(yuzefovich): flush the buffer when going from "enabled" to
	// "disabled".
}

// BufferedWritesEnabled is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) BufferedWritesEnabled() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	return tc.interceptorAlloc.txnWriteBuffer.enabled
}

// String is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) String() string {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.String()
}

// ReadTimestamp is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) ReadTimestamp() hlc.Timestamp {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.ReadTimestamp
}

// ReadTimestampFixed is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) ReadTimestampFixed() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.ReadTimestampFixed
}

// ProvisionalCommitTimestamp is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) ProvisionalCommitTimestamp() hlc.Timestamp {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.WriteTimestamp
}

// CommitTimestamp is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) CommitTimestamp() (hlc.Timestamp, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	txn := &tc.mu.txn
	switch txn.Status {
	case roachpb.COMMITTED:
		return txn.ReadTimestamp, nil
	case roachpb.ABORTED:
		return hlc.Timestamp{}, errors.Errorf("CommitTimestamp called on aborted transaction")
	default:
		// If the transaction is not yet committed, configure the ReadTimestampFixed
		// flag to ensure that the transaction's read timestamp is not pushed before
		// it commits.
		//
		// This operates by disabling the transaction refresh mechanism. For
		// isolation levels that can tolerate write skew, this is not enough to
		// prevent the transaction from committing with a later timestamp. In fact,
		// it's not even clear what timestamp to consider the "commit timestamp" for
		// these transactions, given that they can read at one or more different
		// timestamps than the one they eventually write at. For this reason, we
		// disable the CommitTimestamp method for these isolation levels.
		if txn.IsoLevel.ToleratesWriteSkew() {
			return hlc.Timestamp{}, errors.Errorf("CommitTimestamp called on weak isolation transaction")
		}
		tc.mu.txn.ReadTimestampFixed = true
		return txn.ReadTimestamp, nil
	}
}

// SetFixedTimestamp is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) SetFixedTimestamp(ctx context.Context, ts hlc.Timestamp) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// The transaction must not have already been used in this epoch.
	if tc.hasPerformedReadsLocked() {
		return errors.WithContextTags(errors.AssertionFailedf(
			"cannot set fixed timestamp, txn %s already performed reads", tc.mu.txn), ctx)
	}
	if tc.hasPerformedWritesLocked() {
		return errors.WithContextTags(errors.AssertionFailedf(
			"cannot set fixed timestamp, txn %s already performed writes", tc.mu.txn), ctx)
	}

	tc.mu.txn.ReadTimestamp = ts
	tc.mu.txn.WriteTimestamp = ts
	tc.mu.txn.ReadTimestampFixed = true
	tc.mu.txn.GlobalUncertaintyLimit = ts

	// Set the MinTimestamp to the minimum of the existing MinTimestamp and the fixed
	// timestamp. This ensures that the MinTimestamp is always <= the other timestamps.
	tc.mu.txn.MinTimestamp.Backward(ts)
	return nil
}

// RequiredFrontier is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) RequiredFrontier() hlc.Timestamp {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.RequiredFrontier()
}

// GenerateForcedRetryableErr is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) GenerateForcedRetryableErr(
	ctx context.Context, ts hlc.Timestamp, mustRestart bool, msg redact.RedactableString,
) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txnState != txnPending && tc.mu.txnState != txnRetryableError {
		return errors.AssertionFailedf("cannot generate retryable error, current state: %s", tc.mu.txnState)
	}

	// Construct the next txn proto.
	prevTxn := tc.mu.txn
	nextTxn := prevTxn.Clone()
	nextTxn.WriteTimestamp.Forward(ts)
	// See kvpb.PrepareTransactionForRetry for an explanation of why the isolation
	// level dictates whether we need to restart or can bump the read timestamp.
	if !nextTxn.IsoLevel.PerStatementReadSnapshot() || mustRestart {
		nextTxn.Restart(tc.mu.userPriority, 0 /* upgradePriority */, nextTxn.WriteTimestamp)
	} else {
		nextTxn.BumpReadTimestamp(nextTxn.WriteTimestamp)
	}

	// Construct the TransactionRetryWithProtoRefreshError using the next txn proto.
	retErr := kvpb.NewTransactionRetryWithProtoRefreshError(
		msg, prevTxn.ID, prevTxn.Epoch, *nextTxn)

	// Update the TxnCoordSender's state.
	tc.handleTransactionRetryWithProtoRefreshErrorErrLocked(ctx, retErr)

	// Finally, pass a TransactionRetryWithProtoRefreshError up to the next layer.
	return retErr
}

// UpdateStateOnRemoteRetryableErr is part of the TxnSender interface.
func (tc *TxnCoordSender) UpdateStateOnRemoteRetryableErr(
	ctx context.Context, pErr *kvpb.Error,
) *kvpb.Error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return kvpb.NewError(tc.handleRetryableErrLocked(ctx, pErr))
}

// GetRetryableErr is part of the TxnSender interface.
func (tc *TxnCoordSender) GetRetryableErr(
	ctx context.Context,
) *kvpb.TransactionRetryWithProtoRefreshError {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txnState == txnRetryableError {
		return tc.mu.storedRetryableErr
	}
	return nil
}

// ClearRetryableErr is part of the TxnSender interface.
func (tc *TxnCoordSender) ClearRetryableErr(ctx context.Context) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txnState != txnRetryableError {
		return errors.AssertionFailedf("cannot clear retryable error, in state: %s", tc.mu.txnState)
	}
	if tc.mu.storedRetryableErr.PrevTxnAborted() {
		return errors.AssertionFailedf("cannot clear retryable error, txn aborted: %s", tc.mu.txn)
	}
	tc.mu.txnState = txnPending
	tc.mu.storedRetryableErr = nil
	return nil
}

// IsSerializablePushAndRefreshNotPossible is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) IsSerializablePushAndRefreshNotPossible() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	isTxnSerializable := tc.mu.txn.IsoLevel == isolation.Serializable
	isTxnPushed := tc.mu.txn.WriteTimestamp != tc.mu.txn.ReadTimestamp
	refreshAttemptNotPossible := tc.interceptorAlloc.txnSpanRefresher.refreshInvalid ||
		tc.mu.txn.ReadTimestampFixed
	// We check ReadTimestampFixed here because, if that's set, refreshing
	// of reads is not performed.
	return isTxnSerializable && isTxnPushed && refreshAttemptNotPossible
}

// Key is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) Key() roachpb.Key {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.Key
}

// Epoch is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) Epoch() enginepb.TxnEpoch {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.Epoch
}

// IsLocking is part of the kv.TxnSender interface.
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

// GetLeafTxnInputState is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) GetLeafTxnInputState(
	ctx context.Context,
) (*roachpb.LeafTxnInputState, error) {
	tis := new(roachpb.LeafTxnInputState)
	tc.mu.Lock()
	defer tc.mu.Unlock()

	pErr := tc.maybeRejectClientLocked(ctx, nil /* ba */)
	if pErr != nil {
		return nil, pErr.GoError()
	}

	// Copy mutable state so access is safe for the caller.
	tis.Txn = tc.mu.txn
	for _, reqInt := range tc.interceptorStack {
		reqInt.populateLeafInputState(tis)
	}

	// Also mark the TxnCoordSender as "active".  This prevents changing
	// the priority after a leaf has been created. It also conservatively
	// ensures that Active() returns true if there's maybe a command
	// being executed concurrently by a leaf.
	tc.mu.active = true

	return tis, nil
}

// GetLeafTxnFinalState is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) GetLeafTxnFinalState(
	ctx context.Context,
) (*roachpb.LeafTxnFinalState, error) {
	tfs := new(roachpb.LeafTxnFinalState)
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// TODO(nvanbenschoten): should we be calling maybeRejectClientLocked here?
	// The caller in execinfra.GetLeafTxnFinalState is not set up to propagate
	// errors, so for now, we don't.
	//
	//   pErr := tc.maybeRejectClientLocked(ctx, nil /* ba */)
	//   if pErr != nil {
	//   	return nil, pErr.GoError()
	//   }

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

// UpdateRootWithLeafFinalState is part of the kv.TxnSender interface.
func (tc *TxnCoordSender) UpdateRootWithLeafFinalState(
	ctx context.Context, tfs *roachpb.LeafTxnFinalState,
) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.txn.ID == (uuid.UUID{}) {
		log.Fatalf(ctx, "cannot UpdateRootWithLeafFinalState on unbound TxnCoordSender. input id: %s", tfs.Txn.ID)
	}

	// Sanity check: don't combine if the tfs is for a different txn ID.
	if tc.mu.txn.ID != tfs.Txn.ID {
		return errors.AssertionFailedf("mismatched root id %s and leaf id %s", tc.mu.txn.ID, tfs.Txn.ID)
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
		return nil
	}

	tc.mu.txn.Update(&tfs.Txn)
	for _, reqInt := range tc.interceptorStack {
		err := reqInt.importLeafFinalState(ctx, tfs)
		if err != nil {
			return err
		}
	}
	return nil
}

// TestingCloneTxn is part of the kv.TxnSender interface.
// This is for use by tests only. To derive leaf TxnCoordSenders,
// use GetLeafTxnInitialState instead.
func (tc *TxnCoordSender) TestingCloneTxn() *roachpb.Transaction {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.Clone()
}

// Step is part of the TxnSender interface.
func (tc *TxnCoordSender) Step(ctx context.Context, allowReadTimestampStep bool) error {
	// TODO(nvanbenschoten): it should be possible to make this assertion, but
	// the API is currently misused by the connExecutor. See #86162.
	// if tc.typ != kv.RootTxn {
	//	return errors.AssertionFailedf("cannot step in non-root txn")
	// }
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if allowReadTimestampStep && tc.shouldStepReadTimestampLocked() {
		tc.manualStepReadTimestampLocked()
	}
	return tc.interceptorAlloc.txnSeqNumAllocator.manualStepReadSeqLocked(ctx)
}

// GetReadSeqNum is part of the TxnSender interface.
func (tc *TxnCoordSender) GetReadSeqNum() enginepb.TxnSeq {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.interceptorAlloc.txnSeqNumAllocator.readSeq
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
	return tc.interceptorAlloc.txnSeqNumAllocator.steppingMode
}

// manualStepReadTimestampLocked advances the transaction's read timestamp to a
// timestamp taken from the local clock and resets refresh span tracking.
//
//gcassert:inline
func (tc *TxnCoordSender) manualStepReadTimestampLocked() {
	tc.stepReadTimestampLocked()
}

// maybeAutoStepReadTimestampLocked advances the transaction's read timestamp to
// a timestamp taken from the local clock and resets refresh span tracking, if
// manual stepping is disabled and the transaction is expected to automatically
// capture a new read snapshot on each batch.
//
//gcassert:inline
func (tc *TxnCoordSender) maybeAutoStepReadTimestampLocked() {
	if tc.typ != kv.RootTxn {
		return // only root transactions auto-step
	}
	if tc.interceptorAlloc.txnSeqNumAllocator.steppingMode == kv.SteppingEnabled {
		return // only manual stepping allowed
	}
	tc.stepReadTimestampLocked()
}

// stepReadTimestampLocked advances the transaction's read timestamp to a
// timestamp taken from the local clock and resets refresh span tracking.
//
// Doing so establishes a new external "read snapshot" from which all future
// reads in the transaction will operate. This read snapshot will be at least as
// recent as the previous read snapshot, and will typically be more recent (i.e.
// it will never regress). Consistency with prior reads in the transaction is
// not maintained, so reads of previously read keys may not be "repeatable" and
// may observe "phantoms". On the other hand, by not maintaining consistency
// between read snapshots, isolation-related retries (write-write conflicts) and
// consistency-related retries (uncertainty errors) have a higher chance of
// being refreshed away (client-side or server-side) without need for client
// intervention (i.e. without requiring a statement-level retry).
//
// Note that the transaction's uncertainty interval is not reset by this method
// to now()+max_offset, even though doing so would be necessary to strictly
// guarantee real-time ordering between the commit of a writer transaction and
// the subsequent establishment of a new read snapshot in a reader transaction.
// By not resetting the uncertainty interval, we allow for the possibility that
// a reader transaction may establish a new read snapshot after the writer has
// committed (on a node with a fast clock) and yet not observe that writer's
// writes.
//
// This decision not to reset the transaction's uncertainty interval is
// discussed in the Read Committed RFC (section "Read Uncertainty Intervals"):
//
// > Read Committed transactions have the option to provide the same "no stale
// > reads" guarantee at the level of each individual statement. Doing so would
// > require transactions to reset their `GlobalUncertaintyLimit` and
// > `ObservedTimestamps` on each statement boundary, setting their
// > `GlobalUncertaintyLimit` to `hlc.Now() + hlc.MaxOffset()` and clearing all
// > `ObservedTimestamps`.
// >
// > We propose that Read Committed transactions do not do this. The cost of
// > resetting a transaction's uncertainty interval on each statement boundary is
// > likely greater than the benefit. Doing so increases the chance that
// > individual statements retry due to `ReadWithinUncertaintyInterval` errors. In
// > the worst case, each statement will need to traverse (through retries) an
// > entire uncertainty interval before converging to a "certain" read snapshot.
// > While these retries will be scoped to a single statement and should not
// > escape to the client, they do still have a latency cost.
// >
// > We make this decision because we do not expect that applications rely on
// > strong consistency guarantees between the commit of one transaction and the
// > start of an individual statement within another in-progress transaction. To
// > rely on such guarantees would require complex and surprising application-side
// > synchronization.
func (tc *TxnCoordSender) stepReadTimestampLocked() {
	now := tc.clock.Now()
	tc.mu.txn.BumpReadTimestamp(now)
	tc.interceptorAlloc.txnSpanRefresher.resetRefreshSpansLocked()
}

// shouldStepReadTimestampLocked returns true if the transaction's read
// timestamp should be advanced on each step, based on the transaction's
// isolation level and whether its read timestamp has been fixed.
//
// The specific approach to stepping (manual vs. automatic) depends on the
// configured the stepping mode.
func (tc *TxnCoordSender) shouldStepReadTimestampLocked() bool {
	return tc.mu.txn.IsoLevel.PerStatementReadSnapshot() && !tc.mu.txn.ReadTimestampFixed
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

// HasPerformedReads is part of the TxnSender interface.
func (tc *TxnCoordSender) HasPerformedReads() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.hasPerformedReadsLocked()
}

// HasPerformedWrites is part of the TxnSender interface.
func (tc *TxnCoordSender) HasPerformedWrites() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.hasPerformedWritesLocked()
}

// TestingShouldRetry is part of the TxnSender interface.
func (tc *TxnCoordSender) TestingShouldRetry() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txnState == txnFinalized {
		return false
	}
	if filter := tc.testingKnobs.TransactionRetryFilter; filter != nil && filter(tc.mu.txn) {
		return true
	}
	if forceTxnRetries && buildutil.CrdbTestBuild {
		return rand.Float64() < kv.RandomTxnRetryProbability
	}
	return false
}

func (tc *TxnCoordSender) hasPerformedReadsLocked() bool {
	return !tc.interceptorAlloc.txnSpanRefresher.refreshFootprint.empty()
}

func (tc *TxnCoordSender) hasPerformedWritesLocked() bool {
	return tc.mu.txn.Sequence != 0
}
