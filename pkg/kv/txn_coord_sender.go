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
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	opTxnCoordSender = "txn coordinator"
	opHeartbeatLoop  = "heartbeat txn"
)

// txnCoordState represents the state of the transaction coordinator.
// It is an intermediate state which indicates we've finished the
// transaction at the coordinator level and it's no longer legitimate
// for sending requests, even though we don't yet know for sure that
// the transaction record has been aborted / committed.
type txnCoordState int

const (
	_ txnCoordState = iota
	// done indicates the transaction has been completed via end
	// transaction and can no longer be used.
	done
	// aborted indicates the transaction was aborted or abandoned (e.g.
	// from timeout, heartbeat failure, context cancelation, txn abort
	// or restart, etc.)
	aborted
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
// - Accumulating intent spans.
// - Attaching intent spans to EndTransaction requests, for intent cleanup.
// - Handles retriable errors by either bumping the transaction's epoch or, in
// case of TransactionAbortedErrors, cleaning up the transaction (in this case,
// the client.Txn is expected to create a new TxnCoordSender instance
// transparently for the higher-level client).
// - Ensures atomic execution for non-transactional (write) batches by transparently
// wrapping them in transactions when the DistSender is forced to split them for
// multiple ranges. For this reason, generally even non-transactional batches
// need to be sent through a TxnCoordSender.
//
// Since it is stateful, the TxnCoordSender needs to understand when a
// transaction is "finished" and the state can be destroyed. As such there's a
// contract that the client.Txn needs obey. Read-only transactions don't matter
// - they're stateless. For the others, once a BeginTransaction is sent by the
// client, the TxnCoordSender considers the transactions completed in the
// following situations:
// - A batch containing an EndTransactions (commit or rollback) succeeds.
// - A batch containing an EndTransaction(commit=false) succeeds or fails. I.e.
// nothing is expected to follow a rollback attempt.
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

		// txn is a copy of the transaction record, updated with each request.
		txn roachpb.Transaction

		// hbRunning is set if the TxnCoordSender has a heartbeat loop running for
		// the transaction record.
		hbRunning bool

		// commandCount indicates how many requests have been sent through
		// this transaction. Reset on retryable txn errors.
		// TODO(andrei): let's get rid of this. It should be maintained
		// in the SQL level.
		commandCount int32
		// lastUpdateNanos is the latest wall time in nanos the client sent
		// transaction operations to this coordinator. Accessed and updated
		// atomically.
		lastUpdateNanos int64
		// Analogous to lastUpdateNanos, this is the wall time at which the
		// transaction was instantiated.
		firstUpdateNanos int64
		// txnEnd is closed when the transaction is aborted or committed,
		// terminating the heartbeat loop.
		txnEnd chan struct{}
		// state indicates the state of the transaction coordinator, which
		// may briefly diverge from the state of the transaction record if
		// the coordinator is aborted after a failed heartbeat, but before
		// we've gotten a response with the updated transaction state.
		state txnCoordState
		// onFinishFn is a closure invoked when state changes to done or aborted.
		onFinishFn func(error)
	}

	// A pointer member to the creating factory provides access to
	// immutable factory settings.
	*TxnCoordSenderFactory

	// An ordered stack of pluggable request interceptors that can transform
	// batch requests and responses while each maintaining targeted state.
	// The stack is stored in an array and each txnReqInterceptor implementation
	// is embedded in the interceptorAlloc struct, so the entire stack is
	// allocated together with TxnCoordSender without any additional heap
	// allocations necessary.
	interceptorStack [3]txnReqInterceptor
	interceptorAlloc struct {
		txnIntentCollector
		txnSpanRefresher
		txnBatchWrapper
	}

	// typ specifies whether this transaction is the top level,
	// or one of potentially many distributed transactions.
	typ client.TxnType
}

var _ client.TxnSender = &TxnCoordSender{}

// txnReqInterceptors are pluggable request interceptors that transform requests
// and responses and can perform operations in the context of a transaction.
type txnReqInterceptor interface {
	// beforeSendLocked transforms a request batch before it is sent. It will
	// always be called under the TxnCoordSender's lock.
	beforeSendLocked(
		ctx context.Context, ba roachpb.BatchRequest,
	) (roachpb.BatchRequest, *roachpb.Error)

	// maybeRetrySend inspects the response batch and error and optionally
	// retries the send.
	maybeRetrySend(
		ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
	) (*roachpb.BatchResponse, *roachpb.Error)

	// afterSendLocked transforms a response batch and error after it is sent.
	// It will always be called under the TxnCoordSender's lock.
	afterSendLocked(
		ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
	) (*roachpb.BatchResponse, *roachpb.Error)

	// populateMetaLocked populates the provided TxnCoordMeta with any
	// internal state that the txnReqInterceptor contains. This is used
	// to serialize the interceptor's state so that it can be passed to
	// other TxnCoordSenders within a distributed transaction.
	populateMetaLocked(meta *roachpb.TxnCoordMeta)

	// refreshMetaLocked refreshes any internal state held inside the
	// interceptor that is a function of the TxnCoordMeta. It is called
	// when the TxnCoordMeta updates its TxnCoordMeta manually.
	augmentMetaLocked(meta roachpb.TxnCoordMeta)

	// epochRetryLocked resets the interceptor in the case of a txn epoch
	// increment.
	epochRetryLocked()

	// closeLocked closes the interceptor. It is called when the TxnCoordSender
	// shuts down due to either a txn commit or a txn abort.
	// TODO(nvanbenschoten): this won't be used until txnPipeliner. Remove
	// if that never goes in.
	closeLocked()
}

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts      *metric.CounterWithRates
	Commits     *metric.CounterWithRates
	Commits1PC  *metric.CounterWithRates // Commits which finished in a single phase
	AutoRetries *metric.CounterWithRates // Auto retries which avoid client-side restarts
	Durations   *metric.Histogram

	// Restarts is the number of times we had to restart the transaction.
	Restarts *metric.Histogram

	// Counts of restart types.
	RestartsWriteTooOld    *metric.Counter
	RestartsDeleteRange    *metric.Counter
	RestartsSerializable   *metric.Counter
	RestartsPossibleReplay *metric.Counter
}

var (
	metaAbortsRates = metric.Metadata{
		Name: "txn.aborts",
		Help: "Number of aborted KV transactions"}
	metaCommitsRates = metric.Metadata{
		Name: "txn.commits",
		Help: "Number of committed KV transactions (including 1PC)"}
	// NOTE: The 1PC rate is arguably not accurate because it counts batches
	// containing both BeginTransaction and EndTransaction without caring if the
	// DistSender had to split it for touching multiple ranges.
	metaCommits1PCRates = metric.Metadata{
		Name: "txn.commits1PC",
		Help: "Number of committed one-phase KV transactions"}
	metaAutoRetriesRates = metric.Metadata{
		Name: "txn.autoretries",
		Help: "Number of automatic retries to avoid serializable restarts"}
	metaDurationsHistograms = metric.Metadata{
		Name: "txn.durations",
		Help: "KV transaction durations in nanoseconds"}
	metaRestartsHistogram = metric.Metadata{
		Name: "txn.restarts",
		Help: "Number of restarted KV transactions"}
	metaRestartsWriteTooOld = metric.Metadata{
		Name: "txn.restarts.writetooold",
		Help: "Number of restarts due to a concurrent writer committing first"}
	metaRestartsDeleteRange = metric.Metadata{
		Name: "txn.restarts.deleterange",
		Help: "Number of restarts due to a forwarded commit timestamp and a DeleteRange command"}
	metaRestartsSerializable = metric.Metadata{
		Name: "txn.restarts.serializable",
		Help: "Number of restarts due to a forwarded commit timestamp and isolation=SERIALIZABLE"}
	metaRestartsPossibleReplay = metric.Metadata{
		Name: "txn.restarts.possiblereplay",
		Help: "Number of restarts due to possible replays of command batches at the storage layer"}
)

// MakeTxnMetrics returns a TxnMetrics struct that contains metrics whose
// windowed portions retain data for approximately histogramWindow.
func MakeTxnMetrics(histogramWindow time.Duration) TxnMetrics {
	return TxnMetrics{
		Aborts:                 metric.NewCounterWithRates(metaAbortsRates),
		Commits:                metric.NewCounterWithRates(metaCommitsRates),
		Commits1PC:             metric.NewCounterWithRates(metaCommits1PCRates),
		AutoRetries:            metric.NewCounterWithRates(metaAutoRetriesRates),
		Durations:              metric.NewLatency(metaDurationsHistograms, histogramWindow),
		Restarts:               metric.NewHistogram(metaRestartsHistogram, histogramWindow, 100, 3),
		RestartsWriteTooOld:    metric.NewCounter(metaRestartsWriteTooOld),
		RestartsDeleteRange:    metric.NewCounter(metaRestartsDeleteRange),
		RestartsSerializable:   metric.NewCounter(metaRestartsSerializable),
		RestartsPossibleReplay: metric.NewCounter(metaRestartsPossibleReplay),
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
}

var _ client.TxnSenderFactory = &TxnCoordSenderFactory{}

// NewTxnCoordSenderFactory creates a new TxnCoordSenderFactory. The
// factory creates new instances of TxnCoordSenders.
//
// TODO(spencer): move these settings into a configuration object and
// supply that to each sender.
func NewTxnCoordSenderFactory(
	ambient log.AmbientContext,
	st *cluster.Settings,
	wrapped client.Sender,
	clock *hlc.Clock,
	linearizable bool,
	stopper *stop.Stopper,
	txnMetrics TxnMetrics,
) *TxnCoordSenderFactory {
	return &TxnCoordSenderFactory{
		AmbientContext:    ambient,
		st:                st,
		wrapped:           wrapped,
		clock:             clock,
		heartbeatInterval: base.DefaultHeartbeatInterval,
		linearizable:      linearizable,
		stopper:           stopper,
		metrics:           txnMetrics,
	}
}

// New is part of the TxnSenderFactory interface.
func (tcf *TxnCoordSenderFactory) New(
	typ client.TxnType, txn *roachpb.Transaction,
) client.TxnSender {
	tcs := &TxnCoordSender{
		typ: typ,
		TxnCoordSenderFactory: tcf,
	}

	// Create a stack of request/response interceptors.
	var ri *RangeIterator
	if ds, ok := tcf.wrapped.(*DistSender); ok {
		ri = NewRangeIterator(ds)
	}
	tcs.interceptorAlloc.txnIntentCollector = txnIntentCollector{
		st: tcf.st,
		ri: ri,
	}
	tcs.interceptorAlloc.txnSpanRefresher = txnSpanRefresher{
		st:           tcf.st,
		mu:           &tcs.mu,
		refreshValid: true,
		// We can only allow refresh span retries on root transactions
		// because those are the only places where we have all of the
		// refresh spans. If this is a leaf, as in a distributed sql flow,
		// we need to propagate the error to the root for an epoch restart.
		canAutoRetry:     typ == client.RootTxn,
		wrapped:          tcs.wrapped,
		autoRetryCounter: tcs.metrics.AutoRetries,
	}
	tcs.interceptorAlloc.txnBatchWrapper = txnBatchWrapper{
		tcf: tcf,
	}
	tcs.interceptorStack = [...]txnReqInterceptor{
		&tcs.interceptorAlloc.txnIntentCollector,
		&tcs.interceptorAlloc.txnSpanRefresher,
		&tcs.interceptorAlloc.txnBatchWrapper,
	}

	// If a transaction was passed in bind the TxnCoordSender to it.
	// TODO(andrei): Ideally, if a transaction is not passed it, we should take
	// that to mean that a TxnCoordSender is not needed and we should return the
	// wrapped sender directly. However, there are tests that pass nil and still
	// send transactional requests. That's why the TxnCoordSender is still
	// littered with code handling the case where it is not yet bound to a
	// transaction.
	if txn != nil {
		tcs.mu.txn = txn.Clone()
	}
	return tcs
}

// WrappedSender is part of the TxnSenderFactory interface.
func (tcf *TxnCoordSenderFactory) WrappedSender() client.Sender {
	return tcf.wrapped
}

// Metrics returns the factory's metrics struct.
func (tcf *TxnCoordSenderFactory) Metrics() TxnMetrics {
	return tcf.metrics
}

// GetMeta is part of the client.TxnSender interface.
func (tc *TxnCoordSender) GetMeta() roachpb.TxnCoordMeta {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// Copy mutable state so access is safe for the caller.
	var meta roachpb.TxnCoordMeta
	meta.Txn = tc.mu.txn.Clone()
	meta.CommandCount = tc.mu.commandCount
	for _, reqInt := range tc.interceptorStack {
		reqInt.populateMetaLocked(&meta)
	}
	return meta
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
	tc.mu.txn.Update(&meta.Txn)
	tc.mu.commandCount += meta.CommandCount
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

// Send implements the batch.Sender interface.
//
// Read/write mutating requests have their key or key range added to the
// transaction's interval tree of key ranges for eventual cleanup via resolved
// write intents; they're tagged to an outgoing EndTransaction request, with the
// receiving replica in charge of resolving them.
func (tc *TxnCoordSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	ctx = tc.AnnotateCtx(ctx)

	// Start new or pick up active trace. From here on, there's always an active
	// Trace, though its overhead is small unless it's sampled.
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		sp = tc.AmbientContext.Tracer.StartSpan(opTxnCoordSender)
		defer sp.Finish()
		ctx = opentracing.ContextWithSpan(ctx, sp)
	}

	startNS := tc.clock.PhysicalNow()

	if ba.Txn != nil {
		if tc.mu.txn.ID == (uuid.UUID{}) {
			log.Fatalf(ctx, "cannot send transactional request through unbound TxnCoordSender")
		}

		ctx = log.WithLogTag(ctx, "txn", uuid.ShortStringer(ba.Txn.ID))
		if log.V(2) {
			ctx = log.WithLogTag(ctx, "ts", ba.Txn.Timestamp)
		}

		// If this request is part of a transaction...
		if err := tc.validateTxnForBatch(ctx, &ba); err != nil {
			return nil, roachpb.NewError(err)
		}

		// Associate the txnID with the trace.
		txnIDStr := ba.Txn.ID.String()
		sp.SetBaggageItem("txnID", txnIDStr)

		_, hasBegin := ba.GetArg(roachpb.BeginTransaction)
		if hasBegin {
			// If there's a BeginTransaction, we need to start the heartbeat loop.
			// Perhaps surprisingly, this needs to be done even if the batch has both
			// a BeginTransaction and an EndTransaction. Although on batch success the
			// heartbeat loop will be stopped right away, on retriable errors we need the
			// heartbeat loop: the intents and txn record should be kept in place just
			// like for non-1PC txns.
			if err := tc.startHeartbeatLoop(ctx); err != nil {
				return nil, roachpb.NewError(err)
			}
		}

		var pErr *roachpb.Error
		ba, pErr = func() (roachpb.BatchRequest, *roachpb.Error) {
			tc.mu.Lock()
			defer tc.mu.Unlock()

			// Copy a few fields from the request's txn. This is technically only
			// required during the first send, as these fields are set before
			// the first send and can't change afterwards. Keeping these fields in
			// sync between the TxnCoordSender and the client.Txn is needed because,
			// when the TxnCoordSender synthesizes TransactionAbortedErrors, it
			// creates a new proto that it passes to the client.Txn and then these
			// fields are used when creating that proto that will then be used for the
			// client.Txn. On subsequent retries of the transaction, it's important
			// for the values of these fields to have been preserved because that
			// makes future calls to txn.SetIsolation() and such no-ops.
			// If this makes no sense it's because the TxnCoordSender having a copy of
			// the Transaction proto generally makes no sense.
			tc.mu.txn.Name = ba.Txn.Name
			tc.mu.txn.Isolation = ba.Txn.Isolation
			tc.mu.txn.Priority = ba.Txn.Priority

			if pErr := tc.maybeRejectClientLocked(ctx, ba.Txn.ID); pErr != nil {
				return ba, pErr
			}

			// Update the command count.
			tc.mu.commandCount += int32(len(ba.Requests))

			// Allow each interceptor to modify the request.
			for _, reqInt := range tc.interceptorStack {
				ba, pErr = reqInt.beforeSendLocked(ctx, ba)
				if pErr != nil {
					return ba, pErr
				}
			}
			return ba, nil
		}()
		if pErr != nil {
			return nil, pErr
		}
	}

	// Send the command through wrapped sender, handling retry
	// opportunities in case of error.
	br, pErr := tc.wrapped.Send(ctx, ba)
	if pErr != nil {
		// With mixed success, we can't attempt a retry without potentially
		// succeeding at the same conditional put or increment request
		// twice; return the wrapped error instead. Because the dist sender
		// splits up batches to send to multiple ranges in parallel, and
		// then combines the results, partial success makes it very
		// difficult to determine what can be retried.
		if msErr, ok := pErr.GetDetail().(*roachpb.MixedSuccessError); ok {
			pErr = msErr.Wrapped
			log.VEventf(ctx, 2, "got partial success; cannot retry %s (pErr=%s)", ba, pErr)
		} else {
			// Allow each interceptor to decide whether to retry the request.
			for i := len(tc.interceptorStack) - 1; i >= 0; i-- {
				reqInt := tc.interceptorStack[i]
				br, pErr = reqInt.maybeRetrySend(ctx, &ba, br, pErr)
			}
		}
	}

	br, pErr = func() (*roachpb.BatchResponse, *roachpb.Error) {
		if ba.Txn == nil {
			// Not a transactional request.
			// TODO(andrei): this should be removed once non-transactional
			// requests are eliminated from using the TxnCoordSender.
			return br, pErr
		}

		tc.mu.Lock()
		defer tc.mu.Unlock()

		// Allow each interceptor to update internal state based on the response
		// and optionally modify it. Iterate in reverse order to "unwrap" the
		// interceptor stack.
		for i := len(tc.interceptorStack) - 1; i >= 0; i-- {
			reqInt := tc.interceptorStack[i]
			br, pErr = reqInt.afterSendLocked(ctx, ba, br, pErr)
		}

		return br, tc.updateStateLocked(ctx, startNS, ba, br, pErr)
	}()
	if pErr != nil {
		log.VEventf(ctx, 2, "error: %s", pErr)
		return nil, pErr
	}

	if br.Txn == nil {
		return br, nil
	}
	if _, ok := ba.GetArg(roachpb.EndTransaction); !ok {
		return br, nil
	}
	// If the linearizable flag is set, we want to make sure that all the
	// clocks in the system are past the commit timestamp of the transaction.
	// This is guaranteed if either - the commit timestamp is MaxOffset behind
	// startNS - MaxOffset ns were spent in this function when returning to the
	// client. Below we choose the option that involves less waiting, which is
	// likely the first one unless a transaction commits with an odd timestamp.
	//
	// Can't use linearizable mode with clockless reads since in that case we
	// don't know how long to sleep - could be forever!
	if tsNS := br.Txn.Timestamp.WallTime; startNS > tsNS {
		startNS = tsNS
	}
	maxOffset := tc.clock.MaxOffset()
	sleepNS := maxOffset -
		time.Duration(tc.clock.PhysicalNow()-startNS)

	if maxOffset != timeutil.ClocklessMaxOffset && tc.linearizable && sleepNS > 0 {
		defer func() {
			if log.V(1) {
				log.Infof(ctx, "%v: waiting %s on EndTransaction for linearizability", br.Txn.Short(), duration.Truncate(sleepNS, time.Millisecond))
			}
			time.Sleep(sleepNS)
		}()
	}
	if br.Txn.Status != roachpb.PENDING {
		tc.mu.Lock()
		tc.mu.txn = br.Txn.Clone()
		_, hasBT := ba.GetArg(roachpb.BeginTransaction)
		onePC := br.Txn.Status == roachpb.COMMITTED && hasBT
		if onePC {
			tc.metrics.Commits1PC.Inc(1)
		}
		tc.cleanupTxnLocked(ctx, done)
		tc.mu.Unlock()
	}
	return br, nil
}

// maybeRejectClientLocked checks whether the (transactional) request is in a
// state that prevents it from continuing, such as the coordinator having
// considered the client abandoned, or a heartbeat having reported an error.
func (tc *TxnCoordSender) maybeRejectClientLocked(
	ctx context.Context, txnID uuid.UUID,
) *roachpb.Error {
	// Check whether the transaction is still tracked and has a chance of
	// completing. It's possible that the coordinator learns about the
	// transaction having terminated from a heartbeat, and GC queue correctness
	// (along with common sense) mandates that we don't let the client
	// continue.
	switch {
	case tc.mu.state == aborted:
		fallthrough
	case tc.mu.txn.Status == roachpb.ABORTED:
		abortedErr := roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(), &tc.mu.txn)
		// TODO(andrei): figure out a UserPriority to use here.
		newTxn := roachpb.PrepareTransactionForRetry(
			ctx, abortedErr,
			// priority is not used for aborted errors
			roachpb.NormalUserPriority,
			tc.clock)
		return roachpb.NewError(roachpb.NewHandledRetryableTxnError(
			abortedErr.Message, txnID, newTxn))

	case tc.mu.txn.Status == roachpb.COMMITTED:
		return roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(
			"transaction is already committed"), &tc.mu.txn)

	default:
		return nil
	}
}

// validateTxn validates properties of a txn specified on a request.
// The transaction is expected to be initialized by the time it reaches
// the TxnCoordSender.
func (tc *TxnCoordSender) validateTxnForBatch(ctx context.Context, ba *roachpb.BatchRequest) error {
	if len(ba.Requests) == 0 {
		return errors.Errorf("empty batch with txn")
	}
	ba.Txn.AssertInitialized(ctx)

	var haveBeginTxn bool
	for _, req := range ba.Requests {
		args := req.GetInner()
		if _, ok := args.(*roachpb.BeginTransactionRequest); ok {
			if haveBeginTxn {
				return errors.Errorf("begin transaction requested twice in the same batch: %s", ba.Txn)
			}
			if ba.Txn.Key == nil {
				return errors.Errorf("transaction with BeginTxnRequest missing anchor key: %v", ba)
			}
			haveBeginTxn = true
		}
	}
	return nil
}

// cleanupTxnLocked is called when a transaction ends. The heartbeat
// goroutine is signaled to stop. The TxnCoordSender's state is set to `state`.
// Future Send() calls are rejected.
func (tc *TxnCoordSender) cleanupTxnLocked(ctx context.Context, state txnCoordState) {
	tc.mu.state = state
	if tc.mu.onFinishFn != nil {
		// rejectErr is guaranteed to be non-nil because state is done or
		// aborted on cleanup.
		rejectErr := tc.maybeRejectClientLocked(ctx, tc.mu.txn.ID).GetDetail()
		if rejectErr == nil {
			log.Fatalf(ctx, "expected non-nil rejectErr on txn coord state %v", state)
		}
		tc.mu.onFinishFn(rejectErr)
	}

	// The heartbeat might've already removed the record. Or we may have already
	// closed txnEnd but we are racing with the heartbeat cleanup.
	if tc.mu.txnEnd == nil {
		return
	}
	// Trigger heartbeat shutdown.
	log.VEvent(ctx, 2, "coordinator stops")
	close(tc.mu.txnEnd)
	tc.mu.txnEnd = nil
	// Close each interceptor.
	for _, reqInt := range tc.interceptorStack {
		reqInt.closeLocked()
	}
}

// finalTxnStatsLocked collects a transaction's final statistics. Returns
// the duration, restarts, and finalized txn status.
func (tc *TxnCoordSender) finalTxnStatsLocked() (duration, restarts int64, status roachpb.TransactionStatus) {
	duration = tc.clock.PhysicalNow() - tc.mu.firstUpdateNanos
	restarts = int64(tc.mu.txn.Epoch)
	status = tc.mu.txn.Status
	return duration, restarts, status
}

// heartbeatLoop periodically sends a HeartbeatTxn RPC to an extant transaction,
// stopping in the event the transaction is aborted or committed after
// attempting to resolve the intents. When the heartbeat stops, the transaction
// stats are updated based on its final disposition.
//
// TODO(wiz): Update (*DBServer).Batch to not use context.TODO().
func (tc *TxnCoordSender) heartbeatLoop(ctx context.Context) {
	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(tc.heartbeatInterval)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	// TODO(tschottdorf): this should join to the trace of the request
	// which starts this goroutine.
	sp := tc.AmbientContext.Tracer.StartSpan(opHeartbeatLoop)
	defer sp.Finish()
	ctx = opentracing.ContextWithSpan(ctx, sp)

	defer func() {
		tc.mu.Lock()
		if tc.mu.txnEnd != nil {
			tc.mu.txnEnd = nil
		}
		duration, restarts, status := tc.finalTxnStatsLocked()
		tc.mu.hbRunning = false
		tc.mu.Unlock()
		tc.updateStats(duration, restarts, status)
	}()

	var closer <-chan struct{}
	{
		tc.mu.Lock()
		closer = tc.mu.txnEnd
		tc.mu.Unlock()
		if closer == nil {
			return
		}
	}
	// Loop with ticker for periodic heartbeats.
	for {
		select {
		case <-tickChan:
			if !tc.heartbeat(ctx) {
				return
			}
		case <-closer:
			// Transaction finished normally.
			return
		case <-tc.stopper.ShouldQuiesce():
			return
		}
	}
}

// abortTxnAsyncLocked sends an EndTransaction asynchronously to the wrapped
// Sender.
func (tc *TxnCoordSender) abortTxnAsyncLocked() {
	// NB: We use context.Background() here because we don't want a canceled
	// context to interrupt the aborting.
	ctx := tc.AnnotateCtx(context.Background())

	tc.cleanupTxnLocked(ctx, aborted)
	txn := tc.mu.txn.Clone()

	// Construct a batch with an EndTransaction request.
	ba := roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.EndTransactionRequest{
		Commit: false,
		// Resolved intents should maintain an abort span entry to
		// prevent concurrent requests from failing to notice the
		// transaction was aborted.
		Poison: true,
	})

	// Allow each interceptor to modify the request, as usual. This is
	// important because we need the txnIntentCollector to append intents
	// to the EndTransaction request.
	for _, reqInt := range tc.interceptorStack {
		var pErr *roachpb.Error
		ba, pErr = reqInt.beforeSendLocked(ctx, ba)
		if pErr != nil {
			log.Warning(ctx, pErr)
			return
		}
	}

	log.VEventf(ctx, 2, "async abort for txn: %s", txn)
	if err := tc.stopper.RunAsyncTask(
		ctx, "kv.TxnCoordSender: aborting txn", func(ctx context.Context) {
			// Use the wrapped sender since we already cleaned up the txn
			// and have prevented any other requests from being send through
			// the TxnCoordSender.
			_, pErr := tc.wrapped.Send(ctx, ba)
			if pErr != nil {
				log.VErrEventf(ctx, 1,
					"abort due to inactivity failed for %s: %s ", txn, pErr)
			}
		},
	); err != nil {
		log.Warning(ctx, err)
	}
}

// heartbeat sends a HeartbeatTxnRequest to the txn record.
// Errors that carry update txn information (e.g.  TransactionAbortedError) will
// update the txn. Other errors are swallowed.
// Returns true if heartbeating should continue, false if the transaction is no
// longer Pending and so there's no point in heartbeating further.
func (tc *TxnCoordSender) heartbeat(ctx context.Context) bool {
	tc.mu.Lock()
	txn := tc.mu.txn.Clone()
	tc.mu.Unlock()

	if txn.Status != roachpb.PENDING {
		// A previous iteration has already determined that the transaction is
		// already finalized.
		return false
	}

	ba := roachpb.BatchRequest{}
	ba.Txn = &txn

	hb := &roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Now: tc.clock.Now(),
	}
	ba.Add(hb)

	log.VEvent(ctx, 2, "heartbeat")
	br, pErr := tc.wrapped.Send(ctx, ba)

	if pErr != nil {
		log.VEventf(ctx, 2, "heartbeat failed: %s", pErr)

		// If the heartbeat request arrived to find a missing transaction record
		// then we ignore the error. This is possible if the heartbeat loop was
		// started before a BeginTxn request succeeds because of ambiguity in the
		// first write request's response.
		if tse, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); ok &&
			tse.Reason == roachpb.TransactionStatusError_REASON_TXN_NOT_FOUND {
			return true
		}

		// If the error contains updated txn info, ingest it. For example, we might
		// find out this way that the transaction has been Aborted in the meantime
		// (e.g. someone else pushed it).
		if errTxn := pErr.GetTxn(); errTxn != nil {
			tc.mu.Lock()
			tc.mu.txn.Update(errTxn)
			tc.mu.Unlock()
		}

		return tc.mu.txn.Status == roachpb.PENDING
	}
	txn.Update(br.Responses[0].GetInner().(*roachpb.HeartbeatTxnResponse).Txn)

	// Give the news to the txn in the txns map. This will update long-running
	// transactions (which may find out that they have to restart in that way),
	// but in particular makes sure that they notice when they've been aborted
	// (in which case we'll give them an error on their next request).
	tc.mu.Lock()
	tc.mu.txn.Update(&txn)
	tc.mu.Unlock()

	return true
}

// startHeartbeatLoop starts a heartbeat loop in a different goroutine.
func (tc *TxnCoordSender) startHeartbeatLoop(ctx context.Context) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.mu.hbRunning = true
	tc.mu.firstUpdateNanos = tc.clock.PhysicalNow()

	// Only heartbeat the txn record if we're the root transaction.
	if tc.typ != client.RootTxn {
		return nil
	}

	log.VEventf(ctx, 2, "coordinator spawns heartbeat loop")
	// Create a channel to stop the heartbeat with the lock held
	// to avoid a race between the async task and a subsequent commit.
	tc.mu.txnEnd = make(chan struct{})
	// Create a new context so that the heartbeat loop doesn't inherit the
	// caller's cancelation.
	hbCtx := tc.AnnotateCtx(context.Background())
	if err := tc.stopper.RunAsyncTask(
		ctx, "kv.TxnCoordSender: heartbeat loop", func(ctx context.Context) {
			tc.heartbeatLoop(hbCtx)
		}); err != nil {
		// The system is already draining and we can't start the
		// heartbeat. We refuse new transactions for now because
		// they're likely not going to have all intents committed.
		// In principle, we can relax this as needed though.
		tc.cleanupTxnLocked(ctx, aborted)
		duration, restarts, status := tc.finalTxnStatsLocked()
		tc.updateStats(duration, restarts, status)
		return err
	}
	return nil
}

// IsTracking returns true if the heartbeat loop is running.
func (tc *TxnCoordSender) IsTracking() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.hbRunning
}

// updateStateLocked updates the transaction state in both the success and error
// cases, applying those updates to the corresponding txnMeta object when
// adequate. It also updates retryable errors with the updated transaction for
// use by client restarts.
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
) *roachpb.Error {
	txnID := ba.Txn.ID
	var newTxn roachpb.Transaction
	if pErr == nil {
		newTxn.Update(ba.Txn)
		newTxn.Update(br.Txn)
	} else {
		// Only handle transaction retry errors if this is a root transaction.
		if pErr.TransactionRestart != roachpb.TransactionRestart_NONE &&
			tc.typ == client.RootTxn {
			errTxnID := pErr.GetTxn().ID // The ID of the txn that needs to be restarted.
			if errTxnID != txnID {
				// KV should not return errors for transactions other than the one in
				// the BatchRequest.
				log.Fatalf(ctx, "retryable error for the wrong txn. ba.Txn: %s. pErr: %s",
					ba.Txn, pErr)
			}
			// If the error is a transaction retry error, update metrics to
			// reflect the reason for the restart.
			// TODO(spencer): this code path does not account for retry errors
			//   experienced by dist sql (see internal/client/txn.go).
			if tErr, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); ok {
				switch tErr.Reason {
				case roachpb.RETRY_WRITE_TOO_OLD:
					tc.metrics.RestartsWriteTooOld.Inc(1)
				case roachpb.RETRY_DELETE_RANGE:
					tc.metrics.RestartsDeleteRange.Inc(1)
				case roachpb.RETRY_SERIALIZABLE:
					tc.metrics.RestartsSerializable.Inc(1)
				case roachpb.RETRY_POSSIBLE_REPLAY:
					tc.metrics.RestartsPossibleReplay.Inc(1)
				}
			}
			newTxn = roachpb.PrepareTransactionForRetry(ctx, pErr, ba.UserPriority, tc.clock)

			// We'll pass a HandledRetryableTxnError up to the next layer.
			pErr = roachpb.NewError(
				roachpb.NewHandledRetryableTxnError(
					pErr.Message,
					errTxnID, // the id of the transaction that encountered the error
					newTxn))

			// If the ID changed, it means we had to start a new transaction and the
			// old one is toast. This TxnCoordSender cannot be used any more - future
			// Send() calls will be rejected; the client is supposed to create a new
			// one.
			if errTxnID != newTxn.ID {
				tc.abortTxnAsyncLocked()
				return pErr
			}

			// Reset state as this is a retryable txn error that is incrementing
			// the transaction's epoch.
			log.VEventf(ctx, 2, "resetting epoch-based coordinator state on retry")
			tc.mu.commandCount = 0
			for _, reqInt := range tc.interceptorStack {
				reqInt.epochRetryLocked()
			}
		} else {
			// We got a non-retryable error, or a retryable error at a leaf
			// transaction, and need to pass responsibility for handling it
			// up to the root transaction.

			newTxn.Update(ba.Txn)
			if errTxn := pErr.GetTxn(); errTxn != nil {
				newTxn.Update(errTxn)
			}

			// Update the txn in the error to reflect the TxnCoordSender's state.
			//
			// Avoid changing existing errors because sometimes they escape into
			// goroutines and data races can occur.
			pErrShallow := *pErr
			pErrShallow.SetTxn(&newTxn) // SetTxn clones newTxn
			pErr = &pErrShallow
		}
	}

	// Update our record of this transaction, even on error.
	tc.mu.txn.Update(&newTxn)
	tc.mu.lastUpdateNanos = tc.clock.PhysicalNow()

	if pErr != nil {
		// On rollback error, stop the heartbeat loop. No more requests can come
		// after a rollback, and there's nobody else to stop the heartbeat loop.
		// The rollback success, like the commit success, is handled similarly
		// below.
		et, isEnding := ba.GetArg(roachpb.EndTransaction)
		if isEnding && !et.(*roachpb.EndTransactionRequest).Commit {
			tc.cleanupTxnLocked(ctx, aborted)
		}
	}

	return pErr
}

// updateStats updates transaction metrics after a transaction finishes.
func (tc *TxnCoordSender) updateStats(duration, restarts int64, status roachpb.TransactionStatus) {
	tc.metrics.Durations.RecordValue(duration)
	tc.metrics.Restarts.RecordValue(restarts)
	switch status {
	case roachpb.ABORTED:
		tc.metrics.Aborts.Inc(1)
	case roachpb.PENDING:
		// NOTE(andrei): Getting a PENDING status here is possible if the heartbeat
		// loop has stopped because the stopper is quiescing.
	case roachpb.COMMITTED:
		tc.metrics.Commits.Inc(1)
	}
}
