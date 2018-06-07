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

// A TxnCoordSender is an implementation of client.Sender which wraps
// a lower-level Sender (either a storage.Stores or a DistSender) to
// which it sends commands. It acts as a man-in-the-middle,
// coordinating transaction state for clients. Unlike other senders,
// the TxnCoordSender is stateful and holds information about an
// ongoing transaction. Among other things, it records the intent
// spans of keys mutated by the transaction for later
// resolution.
//
// After a transaction has begun writing, the TxnCoordSender may start
// sending periodic heartbeat messages to that transaction's txn
// record, to keep it live. Note that heartbeating is done only from
// the root transaction coordinator, in the event that multiple
// coordinators are active (i.e. in a distributed SQL flow).
type TxnCoordSender struct {
	mu struct {
		syncutil.Mutex

		// tracking is set if the TxnCoordSender has a heartbeat loop running for
		// the transaction record. It also means that the TxnCoordSender is
		// accumulating intents for the transaction.
		// tracking is set by the client just before a BeginTransaction request is
		// sent. If set, an EndTransaction will also be sent eventually to clean up.
		tracking bool

		// meta contains all coordinator state which may be passed between
		// distributed TxnCoordSenders via MetaRelease() and MetaAugment().
		// Most of the txnReqInterceptors hold references to this meta and
		// perform mutations to it under lock.
		meta roachpb.TxnCoordMeta

		// lastUpdateNanos is the latest wall time in nanos the client sent
		// transaction operations to this coordinator. Accessed and updated
		// atomically.
		lastUpdateNanos int64
		// Analogous to lastUpdateNanos, this is the wall time at which the
		// transaction was instantiated.
		firstUpdateNanos int64
		// txnEnd is closed when the transaction is aborted or committed,
		// terminating the associated heartbeat instance.
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
	// requests and responses.
	interceptors []txnReqInterceptor

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

	// maybeRetrySend inspects the esponse batch and error and optionally
	// retries the send.
	maybeRetrySend(
		ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
	) (*roachpb.BatchResponse, *roachpb.Error)

	// afterSendLocked transforms a response batch and error after it is sent.
	// It will always be called under the TxnCoordSender's lock.
	afterSendLocked(
		ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
	) (*roachpb.BatchResponse, *roachpb.Error)

	// refreshMetaLocked refreshes any internal state held inside the
	// interceptor that is a function of the TxnCoordMeta. It is called
	// when the TxnCoordMeta updates its TxnCoordMeta manually.
	refreshMetaLocked()
}

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts      *metric.CounterWithRates
	Commits     *metric.CounterWithRates
	Commits1PC  *metric.CounterWithRates // Commits which finished in a single phase
	AutoRetries *metric.CounterWithRates // Auto retries which avoid client-side restarts
	Abandons    *metric.CounterWithRates
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
	metaCommits1PCRates = metric.Metadata{
		Name: "txn.commits1PC",
		Help: "Number of committed one-phase KV transactions"}
	metaAutoRetriesRates = metric.Metadata{
		Name: "txn.autoretries",
		Help: "Number of automatic retries to avoid serializable restarts"}
	metaAbandonsRates = metric.Metadata{
		Name: "txn.abandons",
		Help: "Number of abandoned KV transactions"}
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
		Abandons:               metric.NewCounterWithRates(metaAbandonsRates),
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
	clientTimeout     time.Duration
	linearizable      bool // enables linearizable behavior
	stopper           *stop.Stopper
	metrics           TxnMetrics
}

var _ client.TxnSenderFactory = &TxnCoordSenderFactory{}

const defaultClientTimeout = 10 * time.Second

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
		clientTimeout:     defaultClientTimeout,
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
	tcs.mu.meta.RefreshValid = true

	// Create a stack of request/response interceptors.
	var ri *RangeIterator
	if ds, ok := tcf.wrapped.(*DistSender); ok {
		ri = NewRangeIterator(ds)
	}
	tcs.interceptors = []txnReqInterceptor{
		&txnIntentCollector{
			st:   tcf.st,
			ri:   ri,
			meta: &tcs.mu.meta,
		},
		&txnSpanRefresher{
			st:   tcf.st,
			mu:   &tcs.mu,
			meta: &tcs.mu.meta,
			// We can only allow refresh span retries on root transactions
			// because those are the only places where we have all of the
			// refresh spans. If this is a leaf, as in a distributed sql flow,
			// we need to propagate the error to the root for an epoch restart.
			canAutoRetry:     typ == client.RootTxn,
			wrapped:          tcs.wrapped,
			autoRetryCounter: tcs.metrics.AutoRetries,
		},
		&txnBatchWrapper{
			tcf: tcf,
		},
	}

	// If a transaction was passed in bind the TxnCoordSender to it.
	// TODO(andrei): Ideally, if a transaction is not passed it, we should take
	// that to mean that a TxnCoordSender is not needed and we should return the
	// wrapped sender directly. However, there are tests that pass nil and still
	// send transactional requests. That's why the TxnCoordSender is still
	// littered with code handling the case where it is not yet bound to a
	// transaction.
	if txn != nil {
		tcs.mu.meta.Txn = txn.Clone()
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
	meta := tc.mu.meta
	meta.Txn = tc.mu.meta.Txn.Clone()
	meta.Intents = append([]roachpb.Span(nil), tc.mu.meta.Intents...)
	if tc.mu.meta.RefreshValid {
		meta.RefreshReads = append([]roachpb.Span(nil), tc.mu.meta.RefreshReads...)
		meta.RefreshWrites = append([]roachpb.Span(nil), tc.mu.meta.RefreshWrites...)
	}
	return meta
}

// AugmentMeta is part of the client.TxnSender interface.
func (tc *TxnCoordSender) AugmentMeta(ctx context.Context, meta roachpb.TxnCoordMeta) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.meta.Txn.ID == (uuid.UUID{}) {
		log.Fatalf(ctx, "cannot AugmentMeta on unbound TxnCoordSender. meta id: %s", meta.Txn.ID)
	}

	// Sanity check: don't combine if the meta is for a different txn ID.
	if tc.mu.meta.Txn.ID != meta.Txn.ID {
		return
	}
	tc.mu.meta.Txn.Update(&meta.Txn)
	// Do not modify existing span slices when copying.
	tc.mu.meta.Intents, _ = roachpb.MergeSpans(
		append(append([]roachpb.Span(nil), tc.mu.meta.Intents...), meta.Intents...),
	)
	if !meta.RefreshValid {
		tc.mu.meta.RefreshValid = false
		tc.mu.meta.RefreshReads = nil
		tc.mu.meta.RefreshWrites = nil
	} else if tc.mu.meta.RefreshValid {
		tc.mu.meta.RefreshReads, _ = roachpb.MergeSpans(
			append(append([]roachpb.Span(nil), tc.mu.meta.RefreshReads...), meta.RefreshReads...),
		)
		tc.mu.meta.RefreshWrites, _ = roachpb.MergeSpans(
			append(append([]roachpb.Span(nil), tc.mu.meta.RefreshWrites...), meta.RefreshWrites...),
		)
	}
	tc.mu.meta.CommandCount += meta.CommandCount
	// Refresh the interceptors.
	for _, reqInt := range tc.interceptors {
		reqInt.refreshMetaLocked()
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
		if tc.mu.meta.Txn.ID == (uuid.UUID{}) {
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

		txnID := ba.Txn.ID

		// Associate the txnID with the trace.
		txnIDStr := txnID.String()
		sp.SetBaggageItem("txnID", txnIDStr)

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
			tc.mu.meta.Txn.Name = ba.Txn.Name
			tc.mu.meta.Txn.Isolation = ba.Txn.Isolation
			tc.mu.meta.Txn.Priority = ba.Txn.Priority

			if pErr := tc.maybeRejectClientLocked(ctx, ba.Txn.ID); pErr != nil {
				return ba, pErr
			}
			tc.mu.meta.CommandCount += int32(len(ba.Requests))

			// Allow each interceptor to modify the request.
			for _, reqInt := range tc.interceptors {
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
			for i := len(tc.interceptors) - 1; i >= 0; i-- {
				reqInt := tc.interceptors[i]
				br, pErr = reqInt.maybeRetrySend(ctx, &ba, br, pErr)
			}
		}
	}

	br, pErr = func() (*roachpb.BatchResponse, *roachpb.Error) {
		if ba.Txn == nil {
			// Not a transactional request.
			// TODO(andreimatei): this should be removed once non-transactional
			// requests are eliminated from using the TxnCoordSender.
			return br, pErr
		}

		tc.mu.Lock()
		defer tc.mu.Unlock()

		// Allow each interceptor to update internal state based on the response
		// and optionally modify it. Iterate in reverse order to "unwrap" the
		// interceptor stack.
		for i := len(tc.interceptors) - 1; i >= 0; i-- {
			reqInt := tc.interceptors[i]
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
		tc.mu.meta.Txn = br.Txn.Clone()
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
	case tc.mu.meta.Txn.Status == roachpb.ABORTED:
		abortedErr := roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(), &tc.mu.meta.Txn)
		// TODO(andrei): figure out a UserPriority to use here.
		newTxn := roachpb.PrepareTransactionForRetry(
			ctx, abortedErr,
			// priority is not used for aborted errors
			roachpb.NormalUserPriority,
			tc.clock)
		return roachpb.NewError(roachpb.NewHandledRetryableTxnError(
			abortedErr.Message, txnID, newTxn))

	case tc.mu.meta.Txn.Status == roachpb.COMMITTED:
		return roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(
			"transaction is already committed"), &tc.mu.meta.Txn)

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
// goroutine is signaled to clean up the transaction gracefully.
func (tc *TxnCoordSender) cleanupTxnLocked(ctx context.Context, state txnCoordState) {
	tc.mu.state = state
	if tc.mu.onFinishFn != nil {
		// rejectErr is guaranteed to be non-nil because state is done or
		// aborted on cleanup.
		rejectErr := tc.maybeRejectClientLocked(ctx, tc.mu.meta.Txn.ID).GetDetail()
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
}

// finalTxnStatsLocked collects a transaction's final statistics. Returns
// the duration, restarts, and finalized txn status.
func (tc *TxnCoordSender) finalTxnStatsLocked() (duration, restarts int64, status roachpb.TransactionStatus) {
	duration = tc.clock.PhysicalNow() - tc.mu.firstUpdateNanos
	restarts = int64(tc.mu.meta.Txn.Epoch)
	status = tc.mu.meta.Txn.Status
	return duration, restarts, status
}

// heartbeatLoop periodically sends a HeartbeatTxn RPC to an extant transaction,
// stopping in the event the transaction is aborted or committed after
// attempting to resolve the intents. When the heartbeat stops, the transaction
// stats are updated based on its final disposition.
//
// TODO(dan): The Context we use for this is currently the one from the first
// request in a Txn, but the semantics of this aren't good. Each context has its
// own associated lifetime and we're ignoring all but the first. It happens now
// that we pass the same one in every request, but it's brittle to rely on this
// forever.
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
			close(tc.mu.txnEnd)
			tc.mu.txnEnd = nil
		}
		duration, restarts, status := tc.finalTxnStatsLocked()
		tc.mu.Unlock()
		tc.updateStats(duration, restarts, status, false)
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
		case <-ctx.Done():
			// Note that if ctx is not cancelable, then ctx.Done() returns a nil
			// channel, which blocks forever. In this case, the heartbeat loop is
			// responsible for timing out transactions. If ctx.Done() is not nil, then
			// then heartbeat loop ignores the timeout check and this case is
			// responsible for client timeouts.
			log.VEventf(ctx, 2, "transaction heartbeat stopped: %s", ctx.Err())

			// Check if the closer channel had also been closed; in that case, that
			// takes priority.
			select {
			case <-closer:
				// Transaction finished normally.
				return
			default:
				tc.tryAsyncAbort(ctx)
				return
			}
		case <-tc.stopper.ShouldQuiesce():
			return
		}
	}
}

// tryAsyncAbort (synchronously) grabs a copy of the txn proto and the
// intents (which it then clears from meta), and asynchronously tries
// to abort the transaction.
func (tc *TxnCoordSender) tryAsyncAbort(ctx context.Context) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.tryAsyncAbortLocked(ctx)
}

// tryAsyncAbortLocked is like tryAsyncAbort but assumes that the
// TxnCoordSender lock is already held.
func (tc *TxnCoordSender) tryAsyncAbortLocked(ctx context.Context) {
	// Clone the intents and the txn to avoid data races.
	intentSpans, _ := roachpb.MergeSpans(append([]roachpb.Span(nil), tc.mu.meta.Intents...))
	tc.cleanupTxnLocked(ctx, aborted)
	txn := tc.mu.meta.Txn.Clone()

	// Since we don't hold the lock continuously, it's possible that two aborts
	// raced here. That's fine (and probably better than the alternative, which
	// is missing new intents sometimes). Note that the txn may be uninitialized
	// here if a failure occurred before the first write succeeded.
	if txn.Status != roachpb.PENDING {
		return
	}

	// Update out status to Aborted, since we're about to send a rollback. Besides
	// being sane, this prevents the heartbeat loop from incrementing an
	// "Abandons" metric.
	tc.mu.meta.Txn.Status = roachpb.ABORTED

	// NB: use context.Background() here because we may be called when the
	// caller's context has been canceled.
	if err := tc.stopper.RunAsyncTask(
		tc.AnnotateCtx(context.Background()), "kv.TxnCoordSender: aborting txn", func(ctx context.Context) {
			// Use the wrapped sender since the normal Sender does not allow
			// clients to specify intents.
			resp, pErr := client.SendWrappedWith(
				ctx, tc.wrapped, roachpb.Header{Txn: &txn}, &roachpb.EndTransactionRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: txn.Key,
					},
					Commit:      false,
					IntentSpans: intentSpans,
					// Resolved intents should maintain an abort span entry to
					// prevent concurrent requests from failing to notice the
					// transaction was aborted.
					Poison: true,
				},
			)
			tc.mu.Lock()
			defer tc.mu.Unlock()
			if pErr != nil {
				if log.V(1) {
					log.Warningf(ctx, "abort due to inactivity failed for %s: %s ", txn, pErr)
				}
				if errTxn := pErr.GetTxn(); errTxn != nil {
					tc.mu.meta.Txn.Update(errTxn)
				}
			} else {
				tc.mu.meta.Txn.Update(resp.(*roachpb.EndTransactionResponse).Txn)
			}
		},
	); err != nil {
		log.Warning(ctx, err)
	}
}

func (tc *TxnCoordSender) heartbeat(ctx context.Context) bool {
	tc.mu.Lock()
	txn := tc.mu.meta.Txn.Clone()
	timeout := tc.clock.PhysicalNow() - tc.clientTimeout.Nanoseconds()
	hasAbandoned := tc.mu.lastUpdateNanos < timeout
	tc.mu.Unlock()

	if txn.Status != roachpb.PENDING {
		// A previous iteration has already determined that the transaction is
		// already finalized, so we wait for the client to realize that and
		// want to keep our state for the time being (to dish out the right
		// error once it returns).
		return true
	}

	// Before we send a heartbeat, determine whether this transaction should be
	// considered abandoned. If so, exit heartbeat. If ctx.Done() is not nil, then
	// it is a cancellable Context and we skip this check and use the ctx lifetime
	// instead of a timeout.
	//
	// TODO(andrei): We should disallow non-cancellable contexts in the heartbeat
	// goroutine and enforce that our kv client cancels the context when it's
	// done. We get non-cancellable contexts from remote clients
	// (roachpb.ExternalClient) because we override the gRPC context to make it
	// non-cancellable in DBServer.Batch (as that context is not tied to a txn
	// lifetime).
	// Further note that, unfortunately, the Sender interface generally makes it
	// difficult for the TxnCoordSender to get a context with the same lifetime as
	// the transaction (the TxnCoordSender associates the context of the txn's
	// first write with the txn). We should move to using only use local clients
	// (i.e. merge, or at least co-locate client.Txn and the TxnCoordSender). At
	// that point, we probably don't even need to deal with context cancellation
	// any more; the client will be trusted to always send an EndRequest when it's
	// done with a transaction.
	if ctx.Done() == nil && hasAbandoned {
		log.VEvent(ctx, 2, "transaction abandoned heartbeat stopped")
		tc.tryAsyncAbort(ctx)
		return false
	}

	ba := roachpb.BatchRequest{}
	ba.Txn = &txn

	hb := &roachpb.HeartbeatTxnRequest{
		Now: tc.clock.Now(),
	}
	hb.Key = txn.Key
	ba.Add(hb)

	log.VEvent(ctx, 2, "heartbeat")
	br, pErr := tc.wrapped.Send(ctx, ba)

	// Correctness mandates that when we can't heartbeat the transaction, we
	// make sure the client doesn't keep going. This is particularly relevant
	// in the case of an ABORTED transaction, but event if we can't reach the
	// transaction record at all, we have to assume it's been aborted as well.
	if pErr != nil {
		log.VEventf(ctx, 2, "heartbeat failed: %s", pErr)

		// If the heartbeat request arrived to find a missing transaction record
		// then we ignore the error and continue the heartbeat loop. This is
		// possible if the heartbeat loop was started before a BeginTxn request
		// succeeds because of ambiguity in the first write request's response.
		if tse, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); ok &&
			tse.Reason == roachpb.TransactionStatusError_REASON_TXN_NOT_FOUND {
			return true
		}

		if errTxn := pErr.GetTxn(); errTxn != nil {
			tc.mu.Lock()
			tc.mu.meta.Txn.Update(errTxn)
			tc.mu.Unlock()
		}
		// We're not going to let the client carry out additional requests, so
		// try to clean up if the known txn disposition remains PENDING.
		if txn.Status == roachpb.PENDING {
			log.VEventf(ctx, 2, "transaction heartbeat failed: %s", pErr)
			tc.tryAsyncAbort(ctx)
		}
		// Stop the heartbeat.
		return false
	}
	txn.Update(br.Responses[0].GetInner().(*roachpb.HeartbeatTxnResponse).Txn)

	// Give the news to the txn in the txns map. This will update long-running
	// transactions (which may find out that they have to restart in that way),
	// but in particular makes sure that they notice when they've been aborted
	// (in which case we'll give them an error on their next request).
	tc.mu.Lock()
	tc.mu.meta.Txn.Update(&txn)
	tc.mu.Unlock()

	return true
}

// StartTracking is part of the client.TxnSender interface.
func (tc *TxnCoordSender) StartTracking(ctx context.Context) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.mu.tracking = true
	tc.mu.firstUpdateNanos = tc.clock.PhysicalNow()

	// Only heartbeat the txn record if we're the root transaction.
	if tc.typ != client.RootTxn {
		return nil
	}

	log.VEventf(ctx, 2, "coordinator spawns heartbeat loop")
	// Create a channel to stop the heartbeat with the lock held
	// to avoid a race between the async task and a subsequent commit.
	tc.mu.txnEnd = make(chan struct{})
	if err := tc.stopper.RunAsyncTask(
		ctx, "kv.TxnCoordSender: heartbeat loop", func(ctx context.Context) {
			tc.heartbeatLoop(ctx)
		}); err != nil {
		// The system is already draining and we can't start the
		// heartbeat. We refuse new transactions for now because
		// they're likely not going to have all intents committed.
		// In principle, we can relax this as needed though.
		tc.cleanupTxnLocked(ctx, aborted)
		duration, restarts, status := tc.finalTxnStatsLocked()
		tc.updateStats(duration, restarts, status, false /* onePC */)
		return err
	}
	return nil
}

// updateStateLocked updates the transaction state in both the success and
// error cases, applying those updates to the corresponding txnMeta
// object when adequate. It also updates retryable errors with the
// updated transaction for use by client restarts.
//
// startNS is the time when the request that's updating the state has
// been sent.  This is not used if the request is known to not be the
// one in charge of starting tracking the transaction - i.e. this is
// the case for DistSQL, which just does reads and passes 0.
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

			// Reset state as this is a retryable txn error. Note that
			// intents are tracked cumulatively across epochs on retries.
			log.VEventf(ctx, 2, "resetting epoch-based coordinator state on retry")
			tc.mu.meta.CommandCount = 0
			tc.mu.meta.RefreshReads = nil
			tc.mu.meta.RefreshWrites = nil
			tc.mu.meta.RefreshValid = true
			for _, reqInt := range tc.interceptors {
				reqInt.refreshMetaLocked()
			}

			// Pass a HandledRetryableTxnError up to the next layer.
			pErr = roachpb.NewError(
				roachpb.NewHandledRetryableTxnError(
					pErr.Message,
					errTxnID, // the id of the transaction that encountered the error
					newTxn))

			// If the ID changed, it means we had to start a new transaction
			// and the old one is toast. Try an asynchronous abort of the
			// bound transaction to clean up its intents immediately, which
			// likely will otherwise require synchronous cleanup by the
			// restated transaction and return without any update to
			if errTxnID != newTxn.ID {
				tc.tryAsyncAbortLocked(ctx)
				return pErr
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

	// If this was a successful one phase commit, update stats
	// directly as they won't otherwise be updated on heartbeat
	// loop shutdown.
	_, isBeginning := ba.GetArg(roachpb.BeginTransaction)
	_, isEnding := ba.GetArg(roachpb.EndTransaction)
	if pErr == nil && isBeginning && isEnding {
		etArgs, ok := br.Responses[len(br.Responses)-1].GetInner().(*roachpb.EndTransactionResponse)
		tc.updateStats(tc.clock.PhysicalNow()-startNS, 0, newTxn.Status, ok && etArgs.OnePhaseCommit)
	}

	// Update our record of this transaction, even on error.
	tc.mu.meta.Txn.Update(&newTxn)
	tc.mu.lastUpdateNanos = tc.clock.PhysicalNow()

	return pErr
}

// updateStats updates transaction metrics after a transaction finishes.
func (tc *TxnCoordSender) updateStats(
	duration, restarts int64, status roachpb.TransactionStatus, onePC bool,
) {
	tc.metrics.Durations.RecordValue(duration)
	tc.metrics.Restarts.RecordValue(restarts)
	switch status {
	case roachpb.ABORTED:
		tc.metrics.Aborts.Inc(1)
	case roachpb.PENDING:
		tc.metrics.Abandons.Inc(1)
	case roachpb.COMMITTED:
		tc.metrics.Commits.Inc(1)
		if onePC {
			tc.metrics.Commits1PC.Inc(1)
		}
	}
}
