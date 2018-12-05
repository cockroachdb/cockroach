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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

// txnState represents states relating to whether Begin/EndTxn requests need to
// be sent.
//go:generate stringer -type=txnState
type txnState int

const (
	// txnPending is the normal state for ongoing transactions.
	txnPending txnState = iota

	// txnError means that a batch encountered a non-retriable error. Further
	// batches except EndTransaction(commit=false) will be rejected.
	txnError

	// txnFinalized means that an EndTransaction(commit=true) has been executed
	// successfully, or an EndTransaction(commit=false) was sent - regardless of
	// whether it executed successfully or not. Further batches except
	// EndTransaction(commit=false) will be rejected; a second rollback is allowed
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

		txnState txnState

		// active is set whenever the transaction has sent any requests.
		active bool

		// closed is set once this transaction has either committed or rolled back
		// (including when the heartbeat loop cleans it up asynchronously). If the
		// client sends anything other than a rollback, it will get an error
		// (a retryable TransactionAbortedError in case of the async abort).
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
	// The stack is stored in an array and each txnInterceptor implementation
	// is embedded in the interceptorAlloc struct, so the entire stack is
	// allocated together with TxnCoordSender without any additional heap
	// allocations necessary.
	interceptorStack [6]txnInterceptor
	interceptorAlloc struct {
		txnHeartbeat
		txnIntentCollector
		txnPipeliner
		txnSpanRefresher
		txnSeqNumAllocator
		txnMetrics
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
	// shuts down due to either a txn commit or a txn abort.
	//
	// This method can be called multiple times (e.g. if the txn is aborted by the
	// heartbeat loop and then upon a client rollback); implementers beware.
	//
	// Note that EndTransaction(commit=false) requests can still be sent (via the
	// lockedSender interface) after this is called, and they're expected to be
	// forwarded along. The idea for this method is to stop background tasks.
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
		typ: typ,
		TxnCoordSenderFactory: tcf,
	}
	tcs.mu.txnState = txnPending

	// Create a stack of request/response interceptors. All of the objects in
	// this stack are pre-allocated on the TxnCoordSender struct, so this just
	// initializes the interceptors and pieces them together. It then adds a
	// txnLockGatekeeper at the bottom of the stack to connect it with the
	// TxnCoordSender's wrapped sender.
	var ri *RangeIterator
	if ds, ok := tcf.wrapped.(*DistSender); ok {
		ri = NewRangeIterator(ds)
	}
	tcs.interceptorAlloc.txnHeartbeat.init(
		&tcs.mu.Mutex,
		&tcs.mu.txn,
		tcs.clock,
		tcs.heartbeatInterval,
		&tcs.interceptorAlloc.txnLockGatekeeper,
		&tcs.metrics,
		tcs.stopper,
		tcs.cleanupTxnLocked,
	)
	tcs.interceptorAlloc.txnMetrics.init(&tcs.mu.txn, tcs.clock, &tcs.metrics)
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
		wrapped: tcs.wrapped,
		mu:      &tcs.mu,
	}
	tcs.interceptorStack = [...]txnInterceptor{
		&tcs.interceptorAlloc.txnHeartbeat,
		// The seq num allocator is the below the txnHeartbeat so that it sees the
		// BeginTransaction prepended by that interceptor. (An alternative would be
		// to not assign seq nums to BeginTransaction; it doesn't need it.)
		// Note though that it skips assigning seq nums to heartbeats.
		&tcs.interceptorAlloc.txnSeqNumAllocator,
		&tcs.interceptorAlloc.txnIntentCollector,
		&tcs.interceptorAlloc.txnPipeliner,
		&tcs.interceptorAlloc.txnSpanRefresher,
		&tcs.interceptorAlloc.txnMetrics,
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
		return roachpb.TxnCoordMeta{}, rejectErr.GoError()
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
	tc.augmentMetaLocked(meta)
}

func (tc *TxnCoordSender) augmentMetaLocked(meta roachpb.TxnCoordMeta) {
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
// An EndTransactionRequest for a read-only txn is elided by the txnHeartbeat
// interceptor. However, calling this and short-circuting even earlier is
// even more efficient (and shows in benchmarks).
func (tc *TxnCoordSender) commitReadOnlyTxnLocked(
	ctx context.Context, deadline *hlc.Timestamp,
) *roachpb.Error {
	if deadline != nil && deadline.Less(tc.mu.txn.Timestamp) {
		return roachpb.NewError(
			roachpb.NewTransactionStatusError("deadline exceeded before transaction finalization"))
	}
	tc.mu.txnState = txnFinalized
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
) (*roachpb.BatchResponse, *roachpb.Error) {
	// NOTE: The locking here is unusual. Although it might look like it, we are
	// NOT holding the lock continuously for the duration of the Send. We lock
	// here, and unlock at the botton of the interceptor stack, in the
	// txnLockGatekeeper. The we lock again in that interceptor when the response
	// comes, and unlock again in the defer below.
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if pErr := tc.maybeRejectClientLocked(ctx, &ba); pErr != nil {
		return nil, pErr
	}

	if ba.IsSingleEndTransactionRequest() && !tc.interceptorAlloc.txnHeartbeat.mu.everSentBeginTxn {
		return nil, tc.commitReadOnlyTxnLocked(ctx, ba.Requests[0].GetEndTransaction().Deadline)
	}

	startNs := tc.clock.PhysicalNow()

	if _, ok := ba.GetArg(roachpb.BeginTransaction); ok {
		return nil, roachpb.NewErrorf("BeginTransaction added before the TxnCoordSender")
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
		return nil, roachpb.NewErrorf("cannot use %s ReadConsistency in txn",
			ba.ReadConsistency)
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

	pErr = tc.updateStateLocked(ctx, startNs, ba, br, pErr)

	// If we succeeded to commit, or we attempted to rollback, we move to
	// txnFinalized.
	if req, ok := ba.GetArg(roachpb.EndTransaction); ok {
		etReq := req.(*roachpb.EndTransactionRequest)
		if etReq.Commit {
			if pErr == nil {
				tc.mu.txnState = txnFinalized
				tc.cleanupTxnLocked(ctx)
				tc.maybeSleepForLinearizable(ctx, br, startNs)
			}
		} else {
			// Rollbacks always move us to txnFinalized.
			tc.mu.txnState = txnFinalized
			tc.cleanupTxnLocked(ctx)
		}
	}

	// Move to the error state on non-retriable errors.
	if pErr != nil {
		log.VEventf(ctx, 2, "failed batch: %s", pErr)
		var retriable bool
		// Note that unhandled retryable txn errors are allowed from leaf
		// transactions. We pass them up through distributed SQL flows to
		// the root transactions, at the receiver.
		if pErr.TransactionRestart != roachpb.TransactionRestart_NONE {
			retriable = true
			if tc.typ == client.RootTxn {
				log.Fatalf(ctx,
					"unexpected retryable error at the client.Txn level: (%T) %s",
					pErr.GetDetail(), pErr)
			}
		} else if _, ok := pErr.GetDetail().(*roachpb.HandledRetryableTxnError); ok {
			retriable = true
		}

		if !retriable {
			tc.mu.txnState = txnError
		}

		return nil, pErr
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
) *roachpb.Error {
	if singleRollback := ba != nil &&
		ba.IsSingleEndTransactionRequest() &&
		!ba.Requests[0].GetInner().(*roachpb.EndTransactionRequest).Commit; singleRollback {
		// As a special case, we allow rollbacks to be sent at any time. Any
		// rollback attempt moves the TxnCoordSender state to txnFinalized, but higher
		// layers are free to retry rollbacks if they want (and they do, for
		// example, when the context was canceled while txn.Rollback() was running).
		return nil
	}

	if tc.mu.txnState == txnFinalized {
		return roachpb.NewErrorWithTxn(
			roachpb.NewTransactionStatusError(
				"client already committed or rolled back the transaction"),
			&tc.mu.txn)
	}
	if tc.mu.txnState == txnError {
		return roachpb.NewError(&roachpb.TxnAlreadyEncounteredErrorError{})
	}
	if tc.mu.txn.Status == roachpb.ABORTED {
		abortedErr := roachpb.NewErrorWithTxn(
			roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_CLIENT_REJECT), &tc.mu.txn)
		if tc.typ == client.LeafTxn {
			// Leaf txns return raw retriable errors (which get handled by the
			// root) rather than HandledRetryableTxnError.
			return abortedErr
		}
		newTxn := roachpb.PrepareTransactionForRetry(
			ctx, abortedErr,
			// priority is not used for aborted errors
			roachpb.NormalUserPriority,
			tc.clock)
		return roachpb.NewError(roachpb.NewHandledRetryableTxnError(
			abortedErr.Message, tc.mu.txn.ID, newTxn))
	}

	if tc.mu.txn.Status != roachpb.PENDING {
		log.Fatalf(ctx, "unexpected txn state: %s", tc.mu.txn)
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
		rejectErr := tc.maybeRejectClientLocked(ctx, nil /* ba */).GetDetail()
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

// UpdateStateOnRemoteRetryableErr is part of the TxnSender interface.
func (tc *TxnCoordSender) UpdateStateOnRemoteRetryableErr(
	ctx context.Context, pErr *roachpb.Error,
) *roachpb.Error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	err := tc.handleRetryableErrLocked(ctx, pErr)
	tc.mu.txn.Update(&err.Transaction)
	return roachpb.NewError(err)
}

// handleRetryableErrLocked takes a retriable error and creates a
// HandledRetryableError containing the transaction that needs to be used by the
// next attempt. It also handles various aspects of updating the
// TxnCoordSender's state, but notably it does not update its proto: the caller
// needs to call tc.mu.txn.Update(pErr.GetTxn()).
func (tc *TxnCoordSender) handleRetryableErrLocked(
	ctx context.Context, pErr *roachpb.Error,
) *roachpb.HandledRetryableTxnError {
	// If the error is a transaction retry error, update metrics to
	// reflect the reason for the restart.
	// TODO(spencer): this code path does not account for retry errors
	//   experienced by dist sql (see internal/client/txn.go).
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

	// We'll pass a HandledRetryableTxnError up to the next layer.
	retErr := roachpb.NewHandledRetryableTxnError(
		pErr.Message,
		errTxnID, // the id of the transaction that encountered the error
		newTxn)

	// If the ID changed, it means we had to start a new transaction and the
	// old one is toast. This TxnCoordSender cannot be used any more - future
	// Send() calls will be rejected; the client is supposed to create a new
	// one.
	if errTxnID != newTxn.ID {
		// Remember that this txn is aborted to reject future requests.
		tc.mu.txn.Status = roachpb.ABORTED
		// Abort the old txn. The client is not supposed to use use this
		// TxnCoordSender any more.
		tc.interceptorAlloc.txnHeartbeat.abortTxnAsyncLocked(ctx)
		return retErr
	}

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

			err := tc.handleRetryableErrLocked(ctx, pErr)
			if err.Transaction.ID == ba.Txn.ID {
				// We'll update our txn, unless this was an abort error.
				cp := err.Transaction.Clone()
				responseTxn = &cp
			}
			pErr = roachpb.NewError(err)
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
	// Note that multiple retriable errors for the same epoch might arrive; also
	// we might get retriable errors for old epochs. We rely on the associativity
	// of Transaction.Update to sort out this lack of ordering guarantee.
	if responseTxn != nil {
		tc.mu.txn.Update(responseTxn)
	}
	return pErr
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
