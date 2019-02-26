// Copyright 2014 The Cockroach Authors.
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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

const (
	opTxnCoordSender = "txn coordinator send"
)

// !!!
// // txnState represents states relating to whether Begin/EndTxn requests need to
// // be sent.
// //go:generate stringer -type=txnState
// type txnState int
//
// const (
//   // txnPending is the normal state for ongoing transactions.
//   txnPending txnState = iota
//
//   // !!!
//   // // txnErrorClientNotified means that a batch encountered a non-retriable
//   // // error, or a TransactionAbortedError (which is retriable, but not with this
//   // // TxnCoordSender). An async rollback has been issued. Further batches except
//   // // EndTransaction(commit=false) will be rejected; see
//   // // TxnCoordSender.storedErr.
//   // txnErrorClientNotified
//
//   txnErrorClientNotNotified
//
//   // !!! comment
//   // txnFinalized means that an EndTransaction(commit=true) has been executed
//   // successfully, or an EndTransaction(commit=false) was sent - regardless of
//   // whether it executed successfully or not. Further batches except
//   // EndTransaction(commit=false) will be rejected; a second rollback is allowed
//   // in case the first one fails.
//   txnFinalized
// )

type txnAlreadyFinalizedError struct {
	committed     bool
	rollbackCause string
	// leafErr is set if this Leaf txn previously got an error. The transaction
	// may or may not have been rolled back yet (the Root is in charge of that),
	// but in any case this Leaf is not to be used any more.
	leafErr string
	// req is the request that generated this error
	req string
}

func (e txnAlreadyFinalizedError) Error() string {
	if e.rollbackCause != "" {
		return fmt.Sprintf(
			"transaction already rolled back (rollback reason: ) while trying to execute: %s",
			e.rollbackCause, e.req)
	}
	if e.committed {
		return fmt.Sprintf(
			"transaction already committed while trying to execute: %s",
			e.rollbackCause, e.req)
	}
	if e.leafErr != "" {
		return fmt.Sprintf(
			"leaf transaction already encountered error %s while trying to execute: %s",
			e.leafErr, e.req)
	}
	panic("not reached")
}

func newTxnAlreadyFinalizedError(committed bool, rollbackCause, leafErr, req string) error {
	if committed && rollbackCause != "" {
		log.Fatalf(context.TODO(), "both committed and rollbackCause specified")
	}
	return txnAlreadyFinalizedError{
		committed:     committed,
		rollbackCause: rollbackCause,
		leafErr:       leafErr,
		req:           req}
}

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
// - Accumulating intent spans.
// - Attaching intent spans to EndTransaction requests, for intent cleanup.
// - Handles retriable errors by either bumping the transaction's epoch or, in
// case of TransactionAbortedErrors, cleaning up the transaction (in this case,
// the client.Txn is expected to create a new TxnCoordSender instance
// transparently for the higher-level client).
//
// Since it is stateful, the TxnCoordSender needs to understand when a
// transaction is "finished" and the state can be destroyed. As such there's a
// contract that the client.Txn needs obey. Read-only transactions don't matter
// - they're stateless. For the others, once a BeginTransaction is sent by the
// client, the TxnCoordSender considers the transactions completed in the
// following situations:
// - A batch containing an EndTransactions (commit or rollback) succeeds.
// - A batch containing an EndTransaction(commit=false) succeeds or fails. Only
// more rollback attempts can follow a rollback attempt.
// - A batch returns a TransactionAbortedError. As mentioned above, the client
// is expected to create a new TxnCoordSender for the next transaction attempt.
//
// Note that "1PC" batches (i.e. batches containing both a Begin and an
// EndTransaction) are no exception from the contract - if the batch fails, the
// client is expected to send a rollback (or perform another transaction attempt
// in case of retriable errors).
type TxnCoordSender struct {
	mu struct {
		syncutil.Mutex

		clientRejectState clientRejectState

		// active is set whenever the transaction has sent any requests.
		active bool

		// closed is set once we've cleaned up this TxnCoordSender and all the
		// interceptors.
		closed bool

		// systemConfigTrigger is set to true when modifying keys from the
		// SystemConfig span. This sets the SystemConfigTrigger on
		// EndTransactionRequest.
		systemConfigTrigger bool

		// txn is the Transaction proto attached to all the requests and updated on
		// all the responses.
		txn roachpb.Transaction

		// userPriority is the txn's priority. Used when restarting the transaction.
		userPriority roachpb.UserPriority

		// onFinishFn is a closure invoked when state changes to done or aborted.
		onFinishFn func(error)
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
		txnIntentCollector
		txnPipeliner
		txnSpanRefresher
		txnCommitter
		txnMetricRecorder
		txnLockGatekeeper // not in interceptorStack array.
	}

	// typ specifies whether this transaction is the top level,
	// or one of potentially many distributed transactions.
	typ client.TxnType
}

var _ client.TxnSender = &TxnCoordSender{}

type clientRejectState struct {
	// committed is set if this transaction was previously committed. Future
	// Send() calls will be rejected with corresponding messages.
	committed bool

	// rollbackCause is set if this transaction was previously rolled back. It
	// indicates why the transaction was rolled back (i.e. did the client
	// request the rollback, or did an error cause it and, if so, which
	// error).
	rollbackCause string

	// asyncAbortErr is set when a particular error needs to be returned to the
	// client on the next Send(). This is used when async processes (i.e. the
	// heartbeat loop) discover some error condition (in particular,
	// TransactionAbortedError).
	//
	// This asyncAbortErr is returned directly to clients on Send() (possibly
	// modified to a new transaction in case of TransactionAbortedError).
	//
	// When asyncAbortErr is set, the transaction has been rolled back (but the
	// client hasn't found out yet). Once the error is returned to the client,
	// asyncAbortErr will be set to nil and rollbackCause will be set instead. The
	// idea is that the client is only notified of this error once; other future
	// requests will be rejected as they normally are after the transaction is
	// rolled back.
	asyncAbortErr *client.TxnRestartError
}

// lockedSender is like a client.Sender but requires the caller to hold the
// TxnCoordSender lock to send requests.
type lockedSender interface {
	// SendLocked sends the batch request and receives a batch response. It
	// requires that the TxnCoordSender lock be held when called, but this lock
	// is not held for the entire duration of the call. Instead, the lock is
	// released immediately before the batch is sent to a lower-level Sender and
	// is re-acquired when the response is returned.
	// WARNING: because the lock is released when calling this method and
	// re-acquired before it returned, callers cannot rely on a single mutual
	// exclusion zone mainted across the call.
	SendLocked(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
}

// txnInterceptors are pluggable request interceptors that transform requests
// and responses and can perform operations in the context of a transaction. A
// TxnCoordSender maintains a stack of txnInterceptors that it calls into under
// lock whenever it sends a request.
type txnInterceptor interface {
	lockedSender

	// setWrapped sets the txnInterceptor wrapped lockedSender.
	setWrapped(wrapped lockedSender)

	// populateMetaLocked populates the provided TxnCoordMeta with any
	// internal state that the txnInterceptor contains. This is used
	// to serialize the interceptor's state so that it can be passed to
	// other TxnCoordSenders within a distributed transaction.
	populateMetaLocked(meta *roachpb.TxnCoordMeta)

	// augmentMetaLocked updates any internal state held inside the
	// interceptor that is a function of the TxnCoordMeta. This is used
	// to deserialize the interceptor's state when it is provided by
	// another TxnCoordSender within a distributed transaction.
	augmentMetaLocked(meta roachpb.TxnCoordMeta)

	// epochBumpedLocked resets the interceptor in the case of a txn epoch
	// increment.
	epochBumpedLocked()

	// closeLocked closes the interceptor. It is called when the TxnCoordSender
	// shuts down due to either a txn commit or a txn abort. The method will
	// be called exactly once from cleanupTxnLocked.
	closeLocked()
}

// txnLockGatekeeper is a lockedSender that sits at the bottom of the
// TxnCoordSender's interceptor stack and handles unlocking the TxnCoordSender's
// mutex when sending a request and locking the TxnCoordSender's mutex when
// receiving a response. It allows the entire txnInterceptor stack to operate
// under lock without needing to worry about unlocking at the correct time.
type txnLockGatekeeper struct {
	wrapped client.Sender
	mu      sync.Locker // shared with TxnCoordSender
}

// SendLocked implements the lockedSender interface.
func (gs *txnLockGatekeeper) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Note the funky locking here: we unlock for the duration of the call and the
	// lock again.
	gs.mu.Unlock()
	defer gs.mu.Lock()
	return gs.wrapped.Send(ctx, ba)
}

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts      *metric.Counter
	Commits     *metric.Counter
	Commits1PC  *metric.Counter // Commits which finished in a single phase
	AutoRetries *metric.Counter // Auto retries which avoid client-side restarts
	Durations   *metric.Histogram

	// Restarts is the number of times we had to restart the transaction.
	Restarts *metric.Histogram

	// Counts of restart types.
	RestartsWriteTooOld       *metric.Counter
	RestartsSerializable      *metric.Counter
	RestartsPossibleReplay    *metric.Counter
	RestartsAsyncWriteFailure *metric.Counter
}

var (
	metaAbortsRates = metric.Metadata{
		Name:        "txn.aborts",
		Help:        "Number of aborted KV transactions",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaCommitsRates = metric.Metadata{
		Name:        "txn.commits",
		Help:        "Number of committed KV transactions (including 1PC)",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	// NOTE: The 1PC rate is arguably not accurate because it counts batches
	// containing both BeginTransaction and EndTransaction without caring if the
	// DistSender had to split it for touching multiple ranges.
	metaCommits1PCRates = metric.Metadata{
		Name:        "txn.commits1PC",
		Help:        "Number of committed one-phase KV transactions",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaAutoRetriesRates = metric.Metadata{
		Name:        "txn.autoretries",
		Help:        "Number of automatic retries to avoid serializable restarts",
		Measurement: "Retries",
		Unit:        metric.Unit_COUNT,
	}
	metaDurationsHistograms = metric.Metadata{
		Name:        "txn.durations",
		Help:        "KV transaction durations",
		Measurement: "KV Txn Duration",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRestartsHistogram = metric.Metadata{
		Name:        "txn.restarts",
		Help:        "Number of restarted KV transactions",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsWriteTooOld = metric.Metadata{
		Name:        "txn.restarts.writetooold",
		Help:        "Number of restarts due to a concurrent writer committing first",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsSerializable = metric.Metadata{
		Name:        "txn.restarts.serializable",
		Help:        "Number of restarts due to a forwarded commit timestamp and isolation=SERIALIZABLE",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsPossibleReplay = metric.Metadata{
		Name:        "txn.restarts.possiblereplay",
		Help:        "Number of restarts due to possible replays of command batches at the storage layer",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsAsyncWriteFailure = metric.Metadata{
		Name:        "txn.restarts.asyncwritefailure",
		Help:        "Number of restarts due to async consensus writes that failed to leave intents",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
)

// MakeTxnMetrics returns a TxnMetrics struct that contains metrics whose
// windowed portions retain data for approximately histogramWindow.
func MakeTxnMetrics(histogramWindow time.Duration) TxnMetrics {
	return TxnMetrics{
		Aborts:                    metric.NewCounter(metaAbortsRates),
		Commits:                   metric.NewCounter(metaCommitsRates),
		Commits1PC:                metric.NewCounter(metaCommits1PCRates),
		AutoRetries:               metric.NewCounter(metaAutoRetriesRates),
		Durations:                 metric.NewLatency(metaDurationsHistograms, histogramWindow),
		Restarts:                  metric.NewHistogram(metaRestartsHistogram, histogramWindow, 100, 3),
		RestartsWriteTooOld:       metric.NewCounter(metaRestartsWriteTooOld),
		RestartsSerializable:      metric.NewCounter(metaRestartsSerializable),
		RestartsPossibleReplay:    metric.NewCounter(metaRestartsPossibleReplay),
		RestartsAsyncWriteFailure: metric.NewCounter(metaRestartsAsyncWriteFailure),
	}
}

// TxnCoordSenderFactory implements client.TxnSenderFactory.
type TxnCoordSenderFactory struct {
	log.AmbientContext

	st                *cluster.Settings
	wrapped           client.Sender
	clock             *hlc.Clock
	heartbeatInterval time.Duration
	linearizable      bool // enables linearizable behavior
	stopper           *stop.Stopper
	metrics           TxnMetrics

	testingKnobs ClientTestingKnobs
}

var _ client.TxnSenderFactory = &TxnCoordSenderFactory{}

// TxnCoordSenderFactoryConfig holds configuration and auxiliary objects that can be passed
// to NewTxnCoordSenderFactory.
type TxnCoordSenderFactoryConfig struct {
	AmbientCtx log.AmbientContext

	Settings *cluster.Settings
	Clock    *hlc.Clock
	Stopper  *stop.Stopper

	HeartbeatInterval time.Duration
	Linearizable      bool
	Metrics           TxnMetrics

	TestingKnobs ClientTestingKnobs
}

// NewTxnCoordSenderFactory creates a new TxnCoordSenderFactory. The
// factory creates new instances of TxnCoordSenders.
func NewTxnCoordSenderFactory(
	cfg TxnCoordSenderFactoryConfig, wrapped client.Sender,
) *TxnCoordSenderFactory {
	tcf := &TxnCoordSenderFactory{
		AmbientContext:    cfg.AmbientCtx,
		st:                cfg.Settings,
		wrapped:           wrapped,
		clock:             cfg.Clock,
		stopper:           cfg.Stopper,
		linearizable:      cfg.Linearizable,
		heartbeatInterval: cfg.HeartbeatInterval,
		metrics:           cfg.Metrics,
		testingKnobs:      cfg.TestingKnobs,
	}
	if tcf.st == nil {
		tcf.st = cluster.MakeTestingClusterSettings()
	}
	if tcf.heartbeatInterval == 0 {
		tcf.heartbeatInterval = base.DefaultHeartbeatInterval
	}
	if tcf.metrics == (TxnMetrics{}) {
		tcf.metrics = MakeTxnMetrics(metric.TestSampleInterval)
	}
	return tcf
}

// TransactionalSender is part of the TxnSenderFactory interface.
func (tcf *TxnCoordSenderFactory) TransactionalSender(
	typ client.TxnType, meta roachpb.TxnCoordMeta,
) client.TxnSender {
	meta.Txn.AssertInitialized(context.TODO())
	tcs := &TxnCoordSender{
		typ:                   typ,
		TxnCoordSenderFactory: tcf,
	}

	// Create a stack of request/response interceptors. All of the objects in
	// this stack are pre-allocated on the TxnCoordSender struct, so this just
	// initializes the interceptors and pieces them together. It then adds a
	// txnLockGatekeeper at the bottom of the stack to connect it with the
	// TxnCoordSender's wrapped sender. First, each of the interceptor objects
	// is initialized.
	var ri *RangeIterator
	if ds, ok := tcf.wrapped.(*DistSender); ok {
		ri = NewRangeIterator(ds)
	}
	// Some interceptors are only needed by roots.
	if typ == client.RootTxn {
		tcs.interceptorAlloc.txnHeartbeater.init(
			&tcs.mu.Mutex,
			&tcs.mu.txn,
			tcf.st,
			tcs.clock,
			tcs.heartbeatInterval,
			&tcs.interceptorAlloc.txnLockGatekeeper,
			&tcs.metrics,
			// txnAbortDetectedLocked
			func(ctx context.Context) {
				// If the heartbeat loop tells us that it detected the transaction to
				// be aborted, we'll want to rollback. When the client Send()s the next
				// batch, it will be rejected with the TxnRestartError we build here.
				abortedErr := roachpb.NewErrorWithTxn(
					roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_HB_LOOP_DETECTED),
					&tcs.mu.txn)
				updatedTxn, asyncErr := tcs.handleRetryableErrLocked(ctx, abortedErr)
				if updatedTxn == nil {
					// We expect updateTxn == nil and asyncErr.NewTxn to be set.
					log.Fatalf(ctx, "unexpected updatedTxn when heartbeat loop detected aborted txn")
				}
				tcs.rollbackAsyncLockedClientNotNotified(ctx, asyncErr)
			},
			tcs.stopper,
		)
		tcs.interceptorAlloc.txnIntentCollector = txnIntentCollector{
			st: tcf.st,
			ri: ri,
		}
		tcs.interceptorAlloc.txnMetricRecorder = txnMetricRecorder{
			metrics: &tcs.metrics,
			clock:   tcs.clock,
			txn:     &tcs.mu.txn,
		}
	}
	tcs.interceptorAlloc.txnPipeliner = txnPipeliner{
		st: tcf.st,
	}
	tcs.interceptorAlloc.txnSpanRefresher = txnSpanRefresher{
		st:    tcf.st,
		knobs: &tcf.testingKnobs,
		// We can only allow refresh span retries on root transactions
		// because those are the only places where we have all of the
		// refresh spans. If this is a leaf, as in a distributed sql flow,
		// we need to propagate the error to the root for an epoch restart.
		canAutoRetry:     typ == client.RootTxn,
		autoRetryCounter: tcs.metrics.AutoRetries,
	}
	tcs.interceptorAlloc.txnLockGatekeeper = txnLockGatekeeper{
		wrapped: tcs.wrapped,
		mu:      &tcs.mu.Mutex,
	}

	// Once the interceptors are initialized, piece them all together in the
	// correct order.
	switch typ {
	case client.RootTxn:
		tcs.interceptorAlloc.arr = [...]txnInterceptor{
			&tcs.interceptorAlloc.txnHeartbeater,
			// Various interceptors below rely on sequence number allocation,
			// so the sequence number allocator is near the top of the stack.
			&tcs.interceptorAlloc.txnSeqNumAllocator,
			&tcs.interceptorAlloc.txnIntentCollector,
			// The pipelinger sits above the span refresher because it will
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
	case client.LeafTxn:
		tcs.interceptorAlloc.arr = [cap(tcs.interceptorAlloc.arr)]txnInterceptor{
			// LeafTxns never perform writes so the sequence number allocator
			// should never increment its sequence number counter over its
			// lifetime, but it still plays the important role of assigning each
			// read request the latest sequence number.
			&tcs.interceptorAlloc.txnSeqNumAllocator,
			// The pipeliner is needed on leaves to ensure that in-flight writes
			// are chained onto by reads that should see them.
			&tcs.interceptorAlloc.txnPipeliner,
			// The span refresher was configured above to not actually perform
			// refreshes for leaves. It is still needed for accumulating the
			// spans to be reported to the Root. But the gateway doesn't do much
			// with them; see #24798.
			&tcs.interceptorAlloc.txnSpanRefresher,
		}
		// All other interceptors are absent from a LeafTxn's interceptor stack
		// because they do not serve a role on leaves.
		tcs.interceptorStack = tcs.interceptorAlloc.arr[:3]
	default:
		panic(fmt.Sprintf("unknown TxnType %v", typ))
	}
	for i, reqInt := range tcs.interceptorStack {
		if i < len(tcs.interceptorStack)-1 {
			reqInt.setWrapped(tcs.interceptorStack[i+1])
		} else {
			reqInt.setWrapped(&tcs.interceptorAlloc.txnLockGatekeeper)
		}
	}

	tcs.augmentMetaLocked(context.TODO(), meta)
	return tcs
}

// NonTransactionalSender is part of the TxnSenderFactory interface.
func (tcf *TxnCoordSenderFactory) NonTransactionalSender() client.Sender {
	return tcf.wrapped
}

// Metrics returns the factory's metrics struct.
func (tcf *TxnCoordSenderFactory) Metrics() TxnMetrics {
	return tcf.metrics
}

// GetMeta is part of the client.TxnSender interface.
func (tc *TxnCoordSender) GetMeta(
	ctx context.Context, opt client.TxnStatusOpt,
) (roachpb.TxnCoordMeta, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// Copy mutable state so access is safe for the caller.
	var meta roachpb.TxnCoordMeta
	meta.Txn = tc.mu.txn.Clone()
	for _, reqInt := range tc.interceptorStack {
		reqInt.populateMetaLocked(&meta)
	}
	if opt == client.OnlyPending && meta.Txn.Status != roachpb.PENDING {
		rejectErr := tc.maybeRejectClientLocked(ctx, nil /* ba */)
		if rejectErr == nil {
			log.Fatal(ctx, "expected non-nil rejectErr")
		}
		return roachpb.TxnCoordMeta{}, rejectErr
	}
	return meta, nil
}

// AugmentMeta is part of the client.TxnSender interface.
func (tc *TxnCoordSender) AugmentMeta(ctx context.Context, meta roachpb.TxnCoordMeta) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.txn.ID == (uuid.UUID{}) {
		log.Fatalf(ctx, "cannot AugmentMeta on unbound TxnCoordSender. meta id: %s", meta.Txn.ID)
	}

	// Sanity check: don't combine if the meta is for a different txn ID.
	if tc.mu.txn.ID != meta.Txn.ID {
		return
	}
	tc.augmentMetaLocked(ctx, meta)
}

func (tc *TxnCoordSender) augmentMetaLocked(ctx context.Context, meta roachpb.TxnCoordMeta) {
	if meta.Txn.Status != roachpb.PENDING {
		// Non-pending transactions should only come in errors, which are not
		// handled by this method.
		log.Fatalf(ctx, "unexpected non-pending txn in augmentMetaLocked: %s", meta.Txn)
	}
	tc.mu.txn.Update(&meta.Txn)
	for _, reqInt := range tc.interceptorStack {
		reqInt.augmentMetaLocked(meta)
	}
}

// OnFinish is part of the client.TxnSender interface.
func (tc *TxnCoordSender) OnFinish(onFinishFn func(error)) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.onFinishFn = onFinishFn
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

// commitReadOnlyTxnLocked "commits" a read-only txn. It is equivalent, but
// cheaper than, sending an EndTransactionRequest. A read-only txn doesn't have
// a transaction record, so there's no need to send any request to the server.
// An EndTransactionRequest for a read-only txn is elided by the txnCommitter
// interceptor. However, calling this and short-circuting even earlier is
// even more efficient (and shows in benchmarks).
// TODO(nvanbenschoten): we could have this call into txnCommitter's
// sendLockedWithElidedEndTransaction method, but we would want to confirm
// that doing so doesn't cut into the speed-up we see from this fast-path.
func (tc *TxnCoordSender) commitReadOnlyTxnLocked(
	ctx context.Context, deadline *hlc.Timestamp,
) *client.ErrWithIndex {
	if deadline != nil && deadline.Less(tc.mu.txn.Timestamp) {
		return client.NewErrWithIndex(
			roachpb.NewTransactionStatusError("deadline exceeded before transaction finalization"), -1)
	}
	tc.mu.clientRejectState.committed = true
	// Mark the transaction as committed so that, in case this commit is done by
	// the closure passed to db.Txn()), db.Txn() doesn't attempt to commit again.
	// Also so that the correct metric gets incremented.
	tc.mu.txn.Status = roachpb.COMMITTED
	tc.cleanupTxnLocked(ctx)
	return nil
}

// Send is part of the client.TxnSender interface.
func (tc *TxnCoordSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *client.ErrWithIndex) {
	// NOTE: The locking here is unusual. Although it might look like it, we are
	// NOT holding the lock continuously for the duration of the Send. We lock
	// here, and unlock at the botton of the interceptor stack, in the
	// txnLockGatekeeper. The we lock again in that interceptor when the response
	// comes, and unlock again in the defer below.
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.maybeRejectClientLocked(ctx, &ba); err != nil {
		return nil, client.NewErrWithIndex(err, -1)
	}

	if ba.IsSingleEndTransactionRequest() {
		et := ba.Requests[0].GetEndTransaction()
		// !!!
		// if !et.Commit &&
		//   (tc.mu.txnState == txnErrorClientNotified || tc.mu.txnState == txnErrorClientNotNotified) {
		//   // A rollback when we're in an error state is a no-op. An async rollback
		//   // has already been sent.
		//   // !!! maybe I should not handle these rollbacks specially, and I should
		//   // detach this TxnCoordSender from the client.Txn when the error is
		//   // returned.
		//   return nil, nil
		// }
		if !tc.interceptorAlloc.txnIntentCollector.haveIntents() {
			return nil, tc.commitReadOnlyTxnLocked(ctx, et.Deadline)
		}
	}

	startNs := tc.clock.PhysicalNow()

	if _, ok := ba.GetArg(roachpb.BeginTransaction); ok {
		return nil, client.NewErrWithIndex(
			errors.Errorf("BeginTransaction added before the TxnCoordSender"), -1)
	}

	ctx, sp := tc.AnnotateCtxWithSpan(ctx, opTxnCoordSender)
	defer sp.Finish()

	// Associate the txnID with the trace.
	if tc.mu.txn.ID == (uuid.UUID{}) {
		log.Fatalf(ctx, "cannot send transactional request through unbound TxnCoordSender")
	}
	if !tracing.IsBlackHoleSpan(sp) {
		sp.SetBaggageItem("txnID", tc.mu.txn.ID.String())
	}
	ctx = logtags.AddTag(ctx, "txn", uuid.ShortStringer(tc.mu.txn.ID))
	if log.V(2) {
		ctx = logtags.AddTag(ctx, "ts", tc.mu.txn.Timestamp)
	}

	// It doesn't make sense to use inconsistent reads in a transaction. However,
	// we still need to accept it as a parameter for this to compile.
	if ba.ReadConsistency != roachpb.CONSISTENT {
		return nil, client.NewErrWithIndex(
			errors.Errorf("cannot use %s ReadConsistency in txn", ba.ReadConsistency),
			-1)
	}

	lastIndex := len(ba.Requests) - 1
	if lastIndex < 0 {
		return nil, nil
	}

	if !tc.mu.active {
		tc.mu.active = true
		// If we haven't generate a transaction priority before, do it now.
		//
		// NOTE(andrei): Unfortunately, as of August 2018, txn.Priority == 0 is also
		// true when the priority has been generated from MinUserPriority. In that
		// case, we'll generate it again.
		if tc.mu.txn.Priority == 0 {
			tc.mu.txn.Priority = roachpb.MakePriority(tc.mu.userPriority)
		}
	}
	// Clone the Txn's Proto so that future modifications can be made without
	// worrying about synchronization.
	newTxn := tc.mu.txn.Clone()
	ba.Txn = &newTxn

	// Send the command through the txnInterceptor stack.
	br, pErr := tc.interceptorStack[0].SendLocked(ctx, ba)

	// Check if the transaction was aborted while this request was in flight. If
	// it was, we don't want to return the result to the client even if this
	// request succeeded: it might have raced with the cleaning up of intents and
	// so it might have missed to see previous writes in this transaction.
	if txnFinishedErr := tc.maybeRejectClientLocked(ctx, &ba); txnFinishedErr != nil {
		return nil, client.NewErrWithIndex(txnFinishedErr, -1)
	}

	errWIdx := tc.updateStateLocked(ctx, startNs, ba, br, pErr)

	// If we succeeded to commit, or we attempted to rollback, we move to
	// txnFinalized.
	if req, ok := ba.GetArg(roachpb.EndTransaction); ok {
		etReq := req.(*roachpb.EndTransactionRequest)
		if etReq.Commit {
			if errWIdx == nil {
				tc.mu.clientRejectState.committed = true
				// !!! tc.mu.txnState = txnFinalized
				tc.cleanupTxnLocked(ctx)
				tc.maybeSleepForLinearizable(ctx, br, startNs)
			}
		} else {
			// Rollbacks always move us to txnFinalized.
			// !!! tc.mu.txnState = txnFinalized
			tc.mu.clientRejectState.rollbackCause = "client rollback"
			tc.cleanupTxnLocked(ctx)
		}
	}

	if errWIdx != nil {
		return nil, errWIdx
	}

	if br != nil && br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(nil /* culprit */, br))
	}

	return br, nil
}

// maybeSleepForLinearizable sleeps if the linearizable flag is set. We want to
// make sure that all the clocks in the system are past the commit timestamp of
// the transaction. This is guaranteed if either:
// - the commit timestamp is MaxOffset behind startNs
// - MaxOffset ns were spent in this function when returning to the
// client.
// Below we choose the option that involves less waiting, which is likely the
// first one unless a transaction commits with an odd timestamp.
//
// Can't use linearizable mode with clockless reads since in that case we don't
// know how long to sleep - could be forever!
func (tc *TxnCoordSender) maybeSleepForLinearizable(
	ctx context.Context, br *roachpb.BatchResponse, startNs int64,
) {
	if tsNS := br.Txn.Timestamp.WallTime; startNs > tsNS {
		startNs = tsNS
	}
	maxOffset := tc.clock.MaxOffset()
	sleepNS := maxOffset -
		time.Duration(tc.clock.PhysicalNow()-startNs)

	if maxOffset != timeutil.ClocklessMaxOffset && tc.linearizable && sleepNS > 0 {
		// TODO(andrei): perhaps we shouldn't sleep with the lock held.
		log.VEventf(ctx, 2, "%v: waiting %s on EndTransaction for linearizability",
			br.Txn.Short(), duration.Truncate(sleepNS, time.Millisecond))
		time.Sleep(sleepNS)
	}
}

// maybeRejectClientLocked checks whether the transaction is in a state that
// prevents it from continuing, such as the heartbeat having detected the
// transaction to have been aborted.
//
// ba is the batch that the client is trying to send. It's inspected because
// rollbacks are always allowed. Can be nil.
func (tc *TxnCoordSender) maybeRejectClientLocked(
	ctx context.Context, ba *roachpb.BatchRequest,
) error {
	// !!!
	// if singleRollback := ba != nil &&
	//   ba.IsSingleEndTransactionRequest() &&
	//   !ba.Requests[0].GetInner().(*roachpb.EndTransactionRequest).Commit; singleRollback {
	//   // As a special case, we allow rollbacks to be sent at any time. Any
	//   // rollback attempt moves the TxnCoordSender state to txnFinalized, but higher
	//   // layers are free to retry rollbacks if they want (and they do, for
	//   // example, when the context was canceled while txn.Rollback() was running).
	//   return nil
	// }

	req := "<none>"
	if ba != nil {
		req = ba.String()
	}
	if tc.mu.clientRejectState.committed {
		return newTxnAlreadyFinalizedError(
			true /* committed */, "" /* rollbackCause */, "" /* leafErr */, req)
	}
	if rbc := tc.mu.clientRejectState.rollbackCause; rbc != "" {
		if tc.typ == client.LeafTxn {
			return newTxnAlreadyFinalizedError(
				false /* committed */, "" /* rollbackCause */, rbc, req)
		}
		return newTxnAlreadyFinalizedError(
			false /* committed */, rbc, "" /* leafErr */, req)
	}
	if asyncAbortErr := tc.mu.clientRejectState.asyncAbortErr; asyncAbortErr != nil {
		// We're now notifying the client about the "async error", so we move it to
		// rollbackCause. Future calls will be rejected with a
		// txnAlreadyFinalizedError.
		tc.mu.clientRejectState.asyncAbortErr = nil
		tc.mu.clientRejectState.rollbackCause = asyncAbortErr.Error()
		return asyncAbortErr
	}

	return nil
}

// cleanupTxnLocked calls onFinishFn and closes all the interceptors.
func (tc *TxnCoordSender) cleanupTxnLocked(ctx context.Context) {
	if tc.mu.closed {
		return
	}
	tc.mu.closed = true
	if tc.mu.onFinishFn != nil {
		rejectErr := tc.maybeRejectClientLocked(ctx, nil /* ba */)
		if rejectErr == nil {
			log.Fatal(ctx, "expected non-nil rejectErr")
		}
		tc.mu.onFinishFn(rejectErr)
		tc.mu.onFinishFn = nil
	}
	// Close each interceptor.
	for _, reqInt := range tc.interceptorStack {
		reqInt.closeLocked()
	}
}

func (tc *TxnCoordSender) RollbackAsync(ctx context.Context) {
	log.VEvent(ctx, 2, "client-initiated rollback")
	tc.mu.Lock()
	tc.rollbackAsyncLockedClientNotified(ctx, "transaction already rolled back by client")
	tc.mu.Unlock()
}

// rollbackAsyncClientNotNotified takes in an error that was generated by an
// async process (e.g. the heartbeat loop), sets the state so that the next
// Send() operation returns an appropriate error, and rolls back the txn.
// restartErr is expected to have its NewTxn field set. In other words it needs
// to have come from a TransactionAbortedError (otherwise we wouldn't have
// rolled back the txn).
func (tc *TxnCoordSender) rollbackAsyncLockedClientNotNotified(
	ctx context.Context, restartErr client.TxnRestartError,
) {
	if tc.typ == client.LeafTxn {
		log.Fatalf(ctx, "unexpected rollbackAsync called in leaf txn: %s", restartErr)
	}
	if restartErr.NewTxn == nil {
		log.Fatalf(ctx, "expected restartErr.NewTxn to be set: %s", restartErr)
	}
	tc.mu.clientRejectState.asyncAbortErr = &restartErr
	tc.rollbackAsyncLocked(ctx, "async err: "+restartErr.Error())
}

// rollbackAsyncClientNotified sets the state so that future requests get
// TxnAlreadyFinalizedError and rolls back the txn.
//
// For leaf transactions, there's no rollback, just some cleanup.
func (tc *TxnCoordSender) rollbackAsyncLockedClientNotified(
	ctx context.Context, rollbackCause string,
) {
	tc.mu.clientRejectState.rollbackCause = rollbackCause
	if tc.typ == client.LeafTxn {
		// Leafs can't rollback; that's the root's job.
		tc.cleanupTxnLocked(ctx)
		return
	}
	tc.rollbackAsyncLocked(ctx, rollbackCause)
}

// !!! don't call directly. Go through rollbackAsyncLockedClientNotified.
// rollbackAsyncLocked sends an EndTransaction(commmit=false) asynchronously.
//
// The state is moved to txnError and future batches sent through this
// TxnCoorSender will be rejected with pErr (or an error derived from it in case
// of TransactionAbortedError).
// !!! comment about txnError.
//
// NB: After the transaction is rolled back, care should be taken to not return
// more results to the client, as those results might have raced with the
// cleaning up of intents. The Send() method checks the clientRejectState after
// every request to ensure that the transaction hasn't been rolled back while
// the request was in flight.
func (tc *TxnCoordSender) rollbackAsyncLocked(ctx context.Context, rollbackCause string) {
	log.VEventf(ctx, 2, "rolling back transaction because of: %s", rollbackCause)
	if tc.mu.clientRejectState == (clientRejectState{}) {
		log.Fatal(ctx, "expected something to be set in clientRejectState")
	}

	// !!!
	// tc.mu.txnState = nextState
	// tc.mu.storedErr = pErr

	// NB: We use context.Background() here because we don't want a canceled
	// context to interrupt the aborting.
	// !!! ctx = tc.AnnotateCtx(context.Background())

	// We don't have a client whose context we can attach to, but we do want to
	// limit how long this request is going to be around or it could leak a
	// goroutine (in case of a long-lived network partition).
	ctx, cancel := tc.stopper.WithCancelOnQuiesce(tc.AnnotateCtx(context.Background()))

	log.VEventf(ctx, 2, "async abort for txn: %s", tc.mu.txn)
	if err := tc.stopper.RunAsyncTask(
		ctx, "txnHeartbeat: aborting txn", func(ctx context.Context) {
			defer cancel()
			_ = contextutil.RunWithTimeout(ctx, "async txn rollback", 3*time.Second, func(ctx context.Context) error {
				// Construct a batch with an EndTransaction request.
				ba := roachpb.BatchRequest{}
				txn := tc.mu.txn.Clone()
				ba.Header = roachpb.Header{Txn: &txn}
				ba.Add(&roachpb.EndTransactionRequest{
					Commit: false,
				})
				// Send the abort request through the interceptor stack. This is important
				// because we need the txnIntentCollector to append intents to the
				// EndTransaction request.
				tc.mu.Lock()
				defer tc.mu.Unlock()
				_, pErr := tc.interceptorStack[0].SendLocked(ctx, ba)
				if pErr != nil {
					log.VErrEventf(ctx, 1, "async abort failed for %s: %s ", txn, pErr)
				}
				return pErr.GoError()
			})
		},
	); err != nil {
		cancel()
		log.Warning(ctx, err)
	}

	tc.cleanupTxnLocked(ctx)
}

// UpdateStateOnRemoteRetryableErr is part of the TxnSender interface.
func (tc *TxnCoordSender) UpdateStateOnRemoteRetryableErr(
	ctx context.Context, pErr *roachpb.Error,
) client.TxnRestartError {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.typ == client.LeafTxn {
		log.Fatalf(ctx, "unexpected call on leaf txn")
	}

	updatedTxn, err := tc.handleRetryableErrLocked(ctx, pErr)
	// We'll update our txn and possible rollback (in case we got a
	// TransactionAbortedError).
	if updatedTxn != nil {
		tc.mu.txn.Update(updatedTxn)
	} else {
		// Set the proto status to ABORTED, for sanity.
		tc.mu.txn.Status = roachpb.ABORTED
		// Rollback. This txn, and this TxnCoordSender, are not supposed to be used
		// any more.
		// The contract is that the caller needs to see if it gets a new txn id
		// after this call (which it does in this case) and, if so, stop using this
		// TxnCoordSender.
		// !!! comment
		// !!! what message do I pass here? Is pErr.Error() correct?
		tc.rollbackAsyncLockedClientNotified(ctx, err.Error())
	}
	return err
}

// handleRetryableErrLocked takes a retriable pErr and creates a TxnRestartError
// to be passed to the client. Sometimes the TxnCoordSender needs to update its
// Transaction before continuing; in these cases a Transaction is also returned.
// In other cases (i.e. TransactionAbortedError), the TxnCoordSender needs to
// rollback the existing transaction and shut down.  The client will create a
// new TxnCoordSender. In these cases, the returned Transaction is nil
// (TxnRestartError.NewTxn will be populated for the client, but the
// TxnCoordSender shouldn't care about that).
func (tc *TxnCoordSender) handleRetryableErrLocked(
	ctx context.Context, pErr *roachpb.Error,
) (*roachpb.Transaction, client.TxnRestartError) {
	if tc.typ == client.LeafTxn {
		log.Fatalf(ctx, "LeafTxns are not supposed to handle retriable errors")
	}

	// If the error is a transaction retry error, update metrics to
	// reflect the reason for the restart.
	if tErr, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); ok {
		switch tErr.Reason {
		case roachpb.RETRY_WRITE_TOO_OLD:
			tc.metrics.RestartsWriteTooOld.Inc(1)
		case roachpb.RETRY_SERIALIZABLE:
			tc.metrics.RestartsSerializable.Inc(1)
		case roachpb.RETRY_POSSIBLE_REPLAY:
			tc.metrics.RestartsPossibleReplay.Inc(1)
		case roachpb.RETRY_ASYNC_WRITE_FAILURE:
			tc.metrics.RestartsAsyncWriteFailure.Inc(1)
		}
	}
	errTxnID := pErr.GetTxn().ID
	newTxn := roachpb.PrepareTransactionForRetry(ctx, pErr, tc.mu.userPriority, tc.clock)

	// Are we suppose to start a new transaction?
	var retErr client.TxnRestartError
	if errTxnID != newTxn.ID {
		retErr = client.NewTxnRestartError(pErr.Message, errTxnID, &newTxn)
		// Reset state as this is a retryable txn error that is incrementing
		// the transaction's epoch.
		log.VEventf(ctx, 2, "resetting epoch-based coordinator state on retry")
		for _, reqInt := range tc.interceptorStack {
			reqInt.epochBumpedLocked()
		}
		return nil, retErr
	} else {
		retErr = client.NewTxnRestartError(pErr.Message, errTxnID, nil /* newTxn */)
		return &newTxn, retErr
	}
}

// updateStateLocked updates the transaction state in both the success and error
// cases. It returns the error to be passed on to the client.
//
// startNS is the time when the request that's updating the state has been sent.
// This is not used if the request is known to not be the one in charge of
// starting tracking the transaction - i.e. this is the case for DistSQL, which
// just does reads and passes 0.
func (tc *TxnCoordSender) updateStateLocked(
	ctx context.Context,
	startNS int64,
	ba roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	pErr *roachpb.Error,
) *client.ErrWithIndex {

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
	// NOTE: We'd love to move to state txnError in case of all retriable errors
	// but alas with the current interface we can't: there's no way for the client
	// to ack the receipt of the error and control the switching to the new epoch.
	// This is a major problem of the current txn interface - it means that
	// concurrent users of a txn might operate at the wrong epoch if they race
	// with the receipt of such an error.

	if pErr == nil {
		tc.mu.txn.Update(br.Txn)
		return nil
	}

	if pErr.TransactionRestart != roachpb.TransactionRestart_NONE {
		if tc.typ == client.LeafTxn {
			// Leaves handle retriable errors differently than roots. The leaf
			// transaction is not supposed to be used any more after a retriable
			// error. Separately, the error needs to make its way back to the root.

			// NB: For leaves, rollbackAsync doesn't actually rollback.
			tc.rollbackAsyncLockedClientNotified(ctx, pErr.Message)
			return client.GoErrorWithIdx(pErr)
		}

		txnID := ba.Txn.ID
		errTxnID := pErr.GetTxn().ID // The ID of the txn that needs to be restarted.
		if errTxnID != txnID {
			// KV should not return errors for transactions other than the one in
			// the BatchRequest.
			log.Fatalf(ctx, "retryable error for the wrong txn. ba.Txn: %s. pErr: %s",
				ba.Txn, pErr)
		}
		updatedTxn, err := tc.handleRetryableErrLocked(ctx, pErr)
		// We'll update our txn, unless this was an abort error. If it was an abort
		// error, the transaction will be rolled back.
		if updatedTxn != nil {
			tc.mu.txn.Update(updatedTxn)
		} else {
			// Set the proto status to ABORTED, for sanity.
			tc.mu.txn.Status = roachpb.ABORTED
			// Rollback. The client is not supposed to use this transaction, or this
			// TxnCoordSender, any more.
			tc.rollbackAsyncLockedClientNotified(ctx, err.RestartCause())
		}
		return client.NewErrWithIndex(err, -1 /* we don't keep track of indexes for TxnRestartErr */)
	}

	// This is the non-retriable error case.
	if errTxn := pErr.GetTxn(); errTxn != nil {
		// Rollback the transaction.
		cp := errTxn.Clone()
		tc.mu.txn.Update(&cp)
		tc.rollbackAsyncLockedClientNotified(ctx, pErr.Message)
	}
	return client.GoErrorWithIdx(pErr)
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

// SetSystemConfigTrigger is part of the client.TxnSender interface.
func (tc *TxnCoordSender) SetSystemConfigTrigger() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if !tc.mu.systemConfigTrigger {
		tc.mu.systemConfigTrigger = true
		// The system-config trigger must be run on the system-config range which
		// means any transaction with the trigger set needs to be anchored to the
		// system-config range.
		return tc.setTxnAnchorKeyLocked(keys.SystemConfigSpan.Key)
	}
	return nil
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

	// Negative priorities come from txn.InternalSetPriority.
	if tc.mu.active && pri > 0 {
		return errors.Errorf("cannot change the user priority of a running transaction")
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

// OrigTimestamp is part of the client.TxnSender interface.
func (tc *TxnCoordSender) OrigTimestamp() hlc.Timestamp {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.OrigTimestamp
}

// CommitTimestamp is part of the client.TxnSender interface.
func (tc *TxnCoordSender) CommitTimestamp() hlc.Timestamp {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.txn.OrigTimestampWasObserved = true
	return tc.mu.txn.OrigTimestamp
}

// CommitTimestampFixed is part of the client.TxnSender interface.
func (tc *TxnCoordSender) CommitTimestampFixed() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.OrigTimestampWasObserved
}

// SetFixedTimestamp is part of the client.TxnSender interface.
func (tc *TxnCoordSender) SetFixedTimestamp(ctx context.Context, ts hlc.Timestamp) {
	tc.mu.Lock()
	tc.mu.txn.Timestamp = ts
	tc.mu.txn.OrigTimestamp = ts
	tc.mu.txn.MaxTimestamp = ts
	tc.mu.txn.OrigTimestampWasObserved = true
	tc.mu.Unlock()
}

// ManualRestart is part of the client.TxnSender interface.
func (tc *TxnCoordSender) ManualRestart(
	ctx context.Context, pri roachpb.UserPriority, ts hlc.Timestamp,
) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Invalidate any writes performed by any workers after the retry updated
	// the txn's proto but before we synchronized (some of these writes might
	// have been performed at the wrong epoch).
	tc.mu.txn.Restart(pri, 0 /* upgradePriority */, ts)

	for _, reqInt := range tc.interceptorStack {
		reqInt.epochBumpedLocked()
	}
}

// IsSerializablePushAndRefreshNotPossible is part of the client.TxnSender interface.
func (tc *TxnCoordSender) IsSerializablePushAndRefreshNotPossible() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	origTimestamp := tc.mu.txn.OrigTimestamp
	origTimestamp.Forward(tc.mu.txn.RefreshedTimestamp)
	isTxnPushed := tc.mu.txn.Timestamp != origTimestamp
	refreshAttemptNotPossible := tc.interceptorAlloc.txnSpanRefresher.refreshInvalid ||
		tc.mu.txn.OrigTimestampWasObserved
	// We check OrigTimestampWasObserved here because, if that's set, refreshing
	// of reads is not performed.
	return isTxnPushed && refreshAttemptNotPossible
}

// Epoch is part of the client.TxnSender interface.
func (tc *TxnCoordSender) Epoch() uint32 {
	return tc.mu.txn.Epoch
}

// SerializeTxn is part of the client.TxnSender interface.
func (tc *TxnCoordSender) SerializeTxn() *roachpb.Transaction {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	cpy := tc.mu.txn.Clone()
	return &cpy
}
