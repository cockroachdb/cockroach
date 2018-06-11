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
	"sync"
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
	// aborted indicates the transaction was aborted. All requests other than
	// rollbacks are rejected.
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
	// The stack is stored in an array and each txnInterceptor implementation
	// is embedded in the interceptorAlloc struct, so the entire stack is
	// allocated together with TxnCoordSender without any additional heap
	// allocations necessary.
	interceptorStack [3]txnInterceptor
	interceptorAlloc struct {
		txnIntentCollector
		txnPipeliner
		txnSpanRefresher
		txnLockGatekeeper // not in interceptorStack array.
	}

	// typ specifies whether this transaction is the top level,
	// or one of potentially many distributed transactions.
	typ client.TxnType
}

var _ client.TxnSender = &TxnCoordSender{}

// lockedSender is like a client.Sender but requires the caller to hold the
// TxnCoordSender lock to send requests.
type lockedSender interface {
	// SendLocked sends the batch request and receives a batch response. It
	// requires that the TxnCoordSender lock be held when called, but this lock
	// is not heldfor the entire duration of the call. Instead, the lock is
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
	// shuts down due to either a txn commit or a txn abort.
	// TODO(nvanbenschoten): this won't be used until txnPipeliner. Remove
	// if that never goes in.
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
	gs.mu.Unlock()
	defer gs.mu.Lock()
	return gs.wrapped.Send(ctx, ba)
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
	metaRestartsDeleteRange = metric.Metadata{
		Name:        "txn.restarts.deleterange",
		Help:        "Number of restarts due to a forwarded commit timestamp and a DeleteRange command",
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
	tcs := &TxnCoordSender{
		typ: typ,
		TxnCoordSenderFactory: tcf,
	}

	// Create a stack of request/response interceptors. All of the objects in
	// this stack are pre-allocated on the TxnCoordSender struct, so this just
	// initializes the interceptors and pieces them together. It then adds a
	// txnLockGatekeeper at the bottom of the stack to connect it with the
	// TxnCoordSender's wrapped sender.
	var ri *RangeIterator
	if ds, ok := tcf.wrapped.(*DistSender); ok {
		ri = NewRangeIterator(ds)
	}
	tcs.interceptorAlloc.txnIntentCollector = txnIntentCollector{
		st: tcf.st,
		ri: ri,
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
		mu:      &tcs.mu,
		wrapped: tcs.wrapped,
	}
	tcs.interceptorStack = [...]txnInterceptor{
		&tcs.interceptorAlloc.txnIntentCollector,
		&tcs.interceptorAlloc.txnPipeliner,
		&tcs.interceptorAlloc.txnSpanRefresher,
	}
	for i, reqInt := range tcs.interceptorStack {
		if i < len(tcs.interceptorStack)-1 {
			reqInt.setWrapped(tcs.interceptorStack[i+1])
		} else {
			reqInt.setWrapped(&tcs.interceptorAlloc.txnLockGatekeeper)
		}
	}

	tcs.augmentMetaLocked(meta)
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
	tc.augmentMetaLocked(meta)
}

func (tc *TxnCoordSender) augmentMetaLocked(meta roachpb.TxnCoordMeta) {
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

	if ba.Txn == nil {
		log.Fatalf(ctx, "can't send non-transactional request through a TxnCoordSender: %s", ba)
	}

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

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.txn.ID == (uuid.UUID{}) {
		log.Fatalf(ctx, "cannot send transactional request through unbound TxnCoordSender")
	}

	// Update our copy of the transaction. It will be further updated with the
	// result in updateStateLocked().
	// Besides seeming like the sane thing to do, updating the txn here assures
	// that, if the heartbeat loop is started, it's assured to have a transaction
	// anchor key to operate on.
	tc.mu.txn.Update(ba.Txn)

	ctx = log.WithLogTag(ctx, "txn", uuid.ShortStringer(ba.Txn.ID))
	if log.V(2) {
		ctx = log.WithLogTag(ctx, "ts", ba.Txn.Timestamp)
	}

	// If this request is part of a transaction...
	if err := tc.validateTxnForBatchLocked(ctx, &ba); err != nil {
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
		if err := tc.startHeartbeatLoopLocked(ctx); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

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

	if pErr := tc.maybeRejectClientLocked(ctx, ba.Txn.ID, &ba); pErr != nil {
		return nil, pErr
	}

	// Update the command count.
	tc.mu.commandCount += int32(len(ba.Requests))

	// Send the command through the txnInterceptor stack.
	br, pErr := tc.interceptorStack[0].SendLocked(ctx, ba)
	pErr = tc.updateStateLocked(ctx, startNS, ba, br, pErr)
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
		tc.mu.txn = br.Txn.Clone()
		_, hasBT := ba.GetArg(roachpb.BeginTransaction)
		onePC := br.Txn.Status == roachpb.COMMITTED && hasBT
		if onePC {
			tc.metrics.Commits1PC.Inc(1)
		}
		tc.cleanupTxnLocked(ctx, done)
	}
	return br, nil
}

// maybeRejectClientLocked checks whether the (transactional) request is in a
// state that prevents it from continuing, such as the coordinator having
// considered the client abandoned, or a heartbeat having reported an error.
func (tc *TxnCoordSender) maybeRejectClientLocked(
	ctx context.Context, txnID uuid.UUID, ba *roachpb.BatchRequest,
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
		// As a special case, we allow rollbacks to be sent at any time. Any
		// rollback attempt moves the TxnCoordSender state to aborted, but higher
		// layers are free to retry rollbacks if they want (and they do, for
		// example, when the context was canceled while txn.Rollback() was running).
		singleAbort := ba != nil &&
			ba.IsSingleEndTransactionRequest() &&
			!ba.Requests[0].GetInner().(*roachpb.EndTransactionRequest).Commit
		if singleAbort {
			return nil
		}

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

// validateTxnForBatchLocked validates properties of a txn specified on
// a request. The transaction is expected to be initialized by the time
// it reaches the TxnCoordSender.
func (tc *TxnCoordSender) validateTxnForBatchLocked(
	ctx context.Context, ba *roachpb.BatchRequest,
) error {
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
		rejectErr := tc.maybeRejectClientLocked(ctx, tc.mu.txn.ID, nil /* ba */).GetDetail()
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

	log.VEventf(ctx, 2, "async abort for txn: %s", txn)
	if err := tc.stopper.RunAsyncTask(
		ctx, "kv.TxnCoordSender: aborting txn", func(ctx context.Context) {
			// Send the abort request through the interceptor stack. This is
			// important because we need the txnIntentCollector to append
			// intents to the EndTransaction request.
			tc.mu.Lock()
			defer tc.mu.Unlock()
			_, pErr := tc.interceptorStack[0].SendLocked(ctx, ba)
			if pErr != nil {
				log.VErrEventf(ctx, 1,
					"async abort failed for %s: %s ", txn, pErr)
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

	if txn.Key == nil {
		log.Fatalf(ctx, "attempting to heartbeat txn without anchor key: %v", txn)
	}

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

		tc.mu.Lock()
		// If the error contains updated txn info, ingest it. For example, we might
		// find out this way that the transaction has been Aborted in the meantime
		// (e.g. someone else pushed it), in which case it's up to us to clean up.
		if errTxn := pErr.GetTxn(); errTxn != nil {
			tc.mu.txn.Update(errTxn)
		}
		status := tc.mu.txn.Status
		if status == roachpb.ABORTED {
			log.VEventf(ctx, 1, "Heartbeat detected aborted txn. Cleaning up.")
			tc.abortTxnAsyncLocked()
		}
		tc.mu.Unlock()

		return status == roachpb.PENDING
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

// startHeartbeatLoopLocked starts a heartbeat loop in a different goroutine.
func (tc *TxnCoordSender) startHeartbeatLoopLocked(ctx context.Context) error {
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
// cases. It also updates retryable errors with the updated transaction for use
// by client restarts.
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
	var responseTxn *roachpb.Transaction
	if pErr == nil {
		responseTxn = br.Txn
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
			nextTxn := roachpb.PrepareTransactionForRetry(ctx, pErr, ba.UserPriority, tc.clock)
			responseTxn = &nextTxn

			// We'll pass a HandledRetryableTxnError up to the next layer.
			pErr = roachpb.NewError(
				roachpb.NewHandledRetryableTxnError(
					pErr.Message,
					errTxnID, // the id of the transaction that encountered the error
					nextTxn))

			// If the ID changed, it means we had to start a new transaction and the
			// old one is toast. This TxnCoordSender cannot be used any more - future
			// Send() calls will be rejected; the client is supposed to create a new
			// one.
			if errTxnID != nextTxn.ID {
				tc.abortTxnAsyncLocked()
				return pErr
			}

			// Reset state as this is a retryable txn error that is incrementing
			// the transaction's epoch.
			log.VEventf(ctx, 2, "resetting epoch-based coordinator state on retry")
			tc.mu.commandCount = 0
			for _, reqInt := range tc.interceptorStack {
				reqInt.epochBumpedLocked()
			}
		} else {
			// We got a non-retryable error, or a retryable error at a leaf
			// transaction, and need to pass responsibility for handling it
			// up to the root transaction.

			if errTxn := pErr.GetTxn(); errTxn != nil {
				responseTxn = errTxn
			}
		}
	}

	// Update our record of this transaction, even on error.
	if responseTxn != nil {
		tc.mu.txn.Update(responseTxn)
	}
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
