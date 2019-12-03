// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

const (
	opTxnCoordSender = "txn coordinator send"
)

// txnState represents states relating to whether an EndTransaction request
// needs to be sent.
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
// - they're stateless. For the others, once an intent write is sent by the
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
		// storedErr is set when txnState == txnError. This storedErr is returned to
		// clients on Send().
		storedErr *roachpb.Error

		// active is set whenever the transaction has sent any requests.
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

	// If set, concurrent requests are allowed. If not set, concurrent requests
	// result in an assertion error. Only leaf transactions are supposed allow
	// concurrent requests - leaves don't restart the transaction and they don't
	// bump the read timestamp through refreshes.
	allowConcurrentRequests bool
	// requestInFlight is set while a request is being processed by the wrapped
	// sender. Used to detect and prevent concurrent txn use.
	requestInFlight bool
}

// SendLocked implements the lockedSender interface.
func (gs *txnLockGatekeeper) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// If so configured, protect against concurrent use of the txn. Concurrent
	// requests don't work generally because of races between clients sending
	// requests and the TxnCoordSender restarting the transaction, and also
	// concurrent requests are not compatible with the span refresher in
	// particular since refreshing is invalid if done concurrently with requests
	// in flight whose spans haven't been accounted for.
	//
	// As a special case, allow for async rollbacks and heartbeats to be sent
	// whenever. As another special case, we allow concurrent requests on AS OF
	// SYSTEM TIME transactions. I think these transactions can't experience
	// retriable errors, so they're safe. And they're used concurrently by schema
	// change code.
	if !gs.allowConcurrentRequests && !ba.Txn.CommitTimestampFixed {
		asyncRequest := ba.IsSingleAbortTransactionRequest() || ba.IsSingleHeartbeatTxnRequest()
		if !asyncRequest {
			if gs.requestInFlight {
				return nil, roachpb.NewError(
					errors.AssertionFailedf("concurrent txn use detected. ba: %s", ba))
			}
			gs.requestInFlight = true
			defer func() {
				gs.requestInFlight = false
			}()
		}
	}

	// Note the funky locking here: we unlock for the duration of the call and the
	// lock again.
	gs.mu.Unlock()
	defer gs.mu.Lock()
	return gs.wrapped.Send(ctx, ba)
}

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts          *metric.Counter
	Commits         *metric.Counter
	Commits1PC      *metric.Counter // Commits which finished in a single phase
	ParallelCommits *metric.Counter // Commits which entered the STAGING state
	AutoRetries     *metric.Counter // Auto retries which avoid client-side restarts
	Durations       *metric.Histogram

	// Restarts is the number of times we had to restart the transaction.
	Restarts *metric.Histogram

	// Counts of restart types.
	RestartsWriteTooOld           telemetry.CounterWithMetric
	RestartsWriteTooOldMulti      telemetry.CounterWithMetric
	RestartsSerializable          telemetry.CounterWithMetric
	RestartsPossibleReplay        telemetry.CounterWithMetric
	RestartsAsyncWriteFailure     telemetry.CounterWithMetric
	RestartsReadWithinUncertainty telemetry.CounterWithMetric
	RestartsTxnAborted            telemetry.CounterWithMetric
	RestartsTxnPush               telemetry.CounterWithMetric
	RestartsUnknown               telemetry.CounterWithMetric
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
	metaCommits1PCRates = metric.Metadata{
		Name:        "txn.commits1PC",
		Help:        "Number of KV transaction on-phase commit attempts",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaParallelCommitsRates = metric.Metadata{
		Name:        "txn.parallelcommits",
		Help:        "Number of KV transaction parallel commit attempts",
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
	// There are two ways we can get "write too old" restarts. In both
	// cases, a WriteTooOldError is generated in the MVCC layer. This is
	// intercepted on the way out by the Store, which performs a single
	// retry at a pushed timestamp. If the retry succeeds, the immediate
	// operation succeeds but the WriteTooOld flag is set on the
	// Transaction, which causes EndTransaction to return a
	// TransactionRetryError with RETRY_WRITE_TOO_OLD. These are
	// captured as txn.restarts.writetooold.
	//
	// If the Store's retried operation generates a second
	// WriteTooOldError (indicating a conflict with a third transaction
	// with a higher timestamp than the one that caused the first
	// WriteTooOldError), the store doesn't retry again, and the
	// WriteTooOldError will be returned up the stack to be retried at
	// this level. These are captured as txn.restarts.writetoooldmulti.
	// This path is inefficient, and if it turns out to be common we may
	// want to do something about it.
	metaRestartsWriteTooOld = metric.Metadata{
		Name:        "txn.restarts.writetooold",
		Help:        "Number of restarts due to a concurrent writer committing first",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsWriteTooOldMulti = metric.Metadata{
		Name:        "txn.restarts.writetoooldmulti",
		Help:        "Number of restarts due to multiple concurrent writers committing first",
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
	metaRestartsReadWithinUncertainty = metric.Metadata{
		Name:        "txn.restarts.readwithinuncertainty",
		Help:        "Number of restarts due to reading a new value within the uncertainty interval",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsTxnAborted = metric.Metadata{
		Name:        "txn.restarts.txnaborted",
		Help:        "Number of restarts due to an abort by a concurrent transaction (usually due to deadlock)",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	// TransactionPushErrors at this level are unusual. They are
	// normally handled at the Store level with the txnwait and
	// contention queues. However, they can reach this level and be
	// retried in tests that disable the store-level retries, and
	// there may be edge cases that allow them to reach this point in
	// production.
	metaRestartsTxnPush = metric.Metadata{
		Name:        "txn.restarts.txnpush",
		Help:        "Number of restarts due to a transaction push failure",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsUnknown = metric.Metadata{
		Name:        "txn.restarts.unknown",
		Help:        "Number of restarts due to a unknown reasons",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
)

// MakeTxnMetrics returns a TxnMetrics struct that contains metrics whose
// windowed portions retain data for approximately histogramWindow.
func MakeTxnMetrics(histogramWindow time.Duration) TxnMetrics {
	return TxnMetrics{
		Aborts:                        metric.NewCounter(metaAbortsRates),
		Commits:                       metric.NewCounter(metaCommitsRates),
		Commits1PC:                    metric.NewCounter(metaCommits1PCRates),
		ParallelCommits:               metric.NewCounter(metaParallelCommitsRates),
		AutoRetries:                   metric.NewCounter(metaAutoRetriesRates),
		Durations:                     metric.NewLatency(metaDurationsHistograms, histogramWindow),
		Restarts:                      metric.NewHistogram(metaRestartsHistogram, histogramWindow, 100, 3),
		RestartsWriteTooOld:           telemetry.NewCounterWithMetric(metaRestartsWriteTooOld),
		RestartsWriteTooOldMulti:      telemetry.NewCounterWithMetric(metaRestartsWriteTooOldMulti),
		RestartsSerializable:          telemetry.NewCounterWithMetric(metaRestartsSerializable),
		RestartsPossibleReplay:        telemetry.NewCounterWithMetric(metaRestartsPossibleReplay),
		RestartsAsyncWriteFailure:     telemetry.NewCounterWithMetric(metaRestartsAsyncWriteFailure),
		RestartsReadWithinUncertainty: telemetry.NewCounterWithMetric(metaRestartsReadWithinUncertainty),
		RestartsTxnAborted:            telemetry.NewCounterWithMetric(metaRestartsTxnAborted),
		RestartsTxnPush:               telemetry.NewCounterWithMetric(metaRestartsTxnPush),
		RestartsUnknown:               telemetry.NewCounterWithMetric(metaRestartsUnknown),
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
		tcf.heartbeatInterval = base.DefaultTxnHeartbeatInterval
	}
	if tcf.metrics == (TxnMetrics{}) {
		tcf.metrics = MakeTxnMetrics(metric.TestSampleInterval)
	}
	return tcf
}

// TransactionalSender is part of the TxnSenderFactory interface.
func (tcf *TxnCoordSenderFactory) TransactionalSender(
	typ client.TxnType, meta roachpb.TxnCoordMeta, pri roachpb.UserPriority,
) client.TxnSender {
	meta.Txn.AssertInitialized(context.TODO())
	tcs := &TxnCoordSender{
		typ:                   typ,
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
	var riGen RangeIteratorGen
	if ds, ok := tcf.wrapped.(*DistSender); ok {
		riGen = ds.rangeIteratorGen
	}
	// Some interceptors are only needed by roots.
	if typ == client.RootTxn {
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
	}
	tcs.interceptorAlloc.txnPipeliner = txnPipeliner{
		st:    tcf.st,
		riGen: riGen,
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
		wrapped:                 tcs.wrapped,
		mu:                      &tcs.mu.Mutex,
		allowConcurrentRequests: typ == client.LeafTxn,
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
	meta.Txn = tc.mu.txn
	for _, reqInt := range tc.interceptorStack {
		reqInt.populateMetaLocked(&meta)
	}
	switch opt {
	case client.AnyTxnStatus:
		// Nothing to check.
	case client.OnlyPending:
		// Check the coordinator's proto status.
		rejectErr := tc.maybeRejectClientLocked(ctx, nil /* ba */)
		if rejectErr != nil {
			return roachpb.TxnCoordMeta{}, rejectErr.GoError()
		}
	default:
		panic("unreachable")
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

	// If the TxnCoordMeta is telling us the transaction has been aborted, it's
	// better if we don't ingest it. Ingesting it would possibly put us in an
	// inconsistent state, with an ABORTED proto but with the heartbeat loop still
	// running. When this TxnCoordMeta was received from DistSQL, it presumably
	// followed a TxnAbortedError that was also received. If that error was also
	// passed to us, then we've already aborted the txn and ingesting the meta
	// would be OK. However, as it stands, if the TxnAbortedError followed a
	// non-retriable error, than we don't get the aborted error (in fact, we don't
	// get either of the errors; the client is responsible for rolling back).
	// TODO(andrei): A better design would be to abort the txn as soon as any
	// error is received from DistSQL, which would eliminate qualms about what
	// error comes first.
	if meta.Txn.Status != roachpb.PENDING {
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
	ctx context.Context, ba roachpb.BatchRequest,
) *roachpb.Error {
	deadline := ba.Requests[0].GetEndTransaction().Deadline
	if deadline != nil && !tc.mu.txn.WriteTimestamp.Less(*deadline) {
		txn := tc.mu.txn.Clone()
		pErr := generateTxnDeadlineExceededErr(txn, *deadline)
		// We need to bump the epoch and transform this retriable error.
		ba.Txn = txn
		return tc.updateStateLocked(ctx, ba, nil /* br */, pErr)
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
	tc.mu.active = true

	if pErr := tc.maybeRejectClientLocked(ctx, &ba); pErr != nil {
		return nil, pErr
	}

	if ba.IsSingleEndTransactionRequest() && !tc.interceptorAlloc.txnPipeliner.haveWrites() {
		return nil, tc.commitReadOnlyTxnLocked(ctx, ba)
	}

	startNs := tc.clock.PhysicalNow()

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
		ctx = logtags.AddTag(ctx, "ts", tc.mu.txn.WriteTimestamp)
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

	if pErr != nil {
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
	if tsNS := br.Txn.WriteTimestamp.WallTime; startNs > tsNS {
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
	if ba != nil && ba.IsSingleAbortTransactionRequest() {
		// As a special case, we allow rollbacks to be sent at any time. Any
		// rollback attempt moves the TxnCoordSender state to txnFinalized, but higher
		// layers are free to retry rollbacks if they want (and they do, for
		// example, when the context was canceled while txn.Rollback() was running).
		return nil
	}

	// Check the transaction coordinator state.
	switch tc.mu.txnState {
	case txnPending:
		// All good.
	case txnError:
		return tc.mu.storedErr
	case txnFinalized:
		msg := fmt.Sprintf("client already committed or rolled back the transaction. "+
			"Trying to execute: %s", ba.Summary())
		stack := string(debug.Stack())
		log.Errorf(ctx, "%s. stack:\n%s", msg, stack)
		return roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(msg), &tc.mu.txn)
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
		// See the comment on txnHeartbeater.mu.finalizedStatus for more details.
		abortedErr := roachpb.NewErrorWithTxn(
			roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_CLIENT_REJECT), &tc.mu.txn)
		if tc.typ == client.LeafTxn {
			// Leaf txns return raw retriable errors (which get handled by the
			// root) rather than TransactionRetryWithProtoRefreshError.
			return abortedErr
		}
		// Root txns handle retriable errors.
		newTxn := roachpb.PrepareTransactionForRetry(
			ctx, abortedErr, roachpb.NormalUserPriority, tc.clock)
		return roachpb.NewError(roachpb.NewTransactionRetryWithProtoRefreshError(
			abortedErr.Message, tc.mu.txn.ID, newTxn))
	case protoStatus != roachpb.PENDING || hbObservedStatus != roachpb.PENDING:
		// The transaction proto is in an unexpected state.
		return roachpb.NewErrorf(
			"unexpected txn state: %s; heartbeat observed status: %s", tc.mu.txn, hbObservedStatus)
	default:
		// All good.
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
	txnID := tc.mu.txn.ID
	err := tc.handleRetryableErrLocked(ctx, pErr)
	// We'll update our txn, unless this was an abort error. If it was an abort
	// error, the transaction has been rolled back and the state was updated in
	// handleRetryableErrLocked().
	if err.Transaction.ID == txnID {
		// This is where we get a new epoch.
		tc.mu.txn.Update(&err.Transaction)
	}
	return roachpb.NewError(err)
}

// handleRetryableErrLocked takes a retriable error and creates a
// TransactionRetryWithProtoRefreshError containing the transaction that needs to be used by the
// next attempt. It also handles various aspects of updating the
// TxnCoordSender's state, but notably it does not update its proto: the caller
// needs to call tc.mu.txn.Update(pErr.GetTxn()).
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
		case roachpb.RETRY_POSSIBLE_REPLAY:
			tc.metrics.RestartsPossibleReplay.Inc()
		case roachpb.RETRY_ASYNC_WRITE_FAILURE:
			tc.metrics.RestartsAsyncWriteFailure.Inc()
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
		tc.interceptorAlloc.txnHeartbeater.abortTxnAsyncLocked(ctx)
		tc.cleanupTxnLocked(ctx)
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

	if pErr.TransactionRestart != roachpb.TransactionRestart_NONE {
		if tc.typ == client.LeafTxn {
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
		err := tc.handleRetryableErrLocked(ctx, pErr)
		// We'll update our txn, unless this was an abort error. If it was an abort
		// error, the transaction has been rolled back and the state was updated in
		// handleRetryableErrLocked().
		if err.Transaction.ID == ba.Txn.ID {
			// This is where we get a new epoch.
			tc.mu.txn.Update(&err.Transaction)
		}
		return roachpb.NewError(err)
	}

	// This is the non-retriable error case.
	if errTxn := pErr.GetTxn(); errTxn != nil {
		tc.mu.txnState = txnError
		tc.mu.storedErr = roachpb.NewError(&roachpb.TxnAlreadyEncounteredErrorError{
			PrevError: pErr.String(),
		})
		// Cleanup.
		tc.mu.txn.Update(errTxn)
		tc.cleanupTxnLocked(ctx)
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

// ReadTimestamp is part of the client.TxnSender interface.
func (tc *TxnCoordSender) ReadTimestamp() hlc.Timestamp {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.ReadTimestamp
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
func (tc *TxnCoordSender) SetFixedTimestamp(ctx context.Context, ts hlc.Timestamp) {
	tc.mu.Lock()
	tc.mu.txn.ReadTimestamp = ts
	tc.mu.txn.WriteTimestamp = ts
	tc.mu.txn.MaxTimestamp = ts
	tc.mu.txn.CommitTimestampFixed = true

	// Set the MinTimestamp to the minimum of the existing MinTimestamp and the fixed
	// timestamp. This ensures that the MinTimestamp is always <= the other timestamps.
	tc.mu.txn.MinTimestamp.Backward(ts)

	// For backwards compatibility with 19.2, set the DeprecatedOrigTimestamp too.
	tc.mu.txn.DeprecatedOrigTimestamp = ts

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

	isTxnPushed := tc.mu.txn.WriteTimestamp != tc.mu.txn.ReadTimestamp
	refreshAttemptNotPossible := tc.interceptorAlloc.txnSpanRefresher.refreshInvalid ||
		tc.mu.txn.CommitTimestampFixed
	// We check CommitTimestampFixed here because, if that's set, refreshing
	// of reads is not performed.
	return isTxnPushed && refreshAttemptNotPossible
}

// Epoch is part of the client.TxnSender interface.
func (tc *TxnCoordSender) Epoch() enginepb.TxnEpoch {
	return tc.mu.txn.Epoch
}

// SerializeTxn is part of the client.TxnSender interface.
func (tc *TxnCoordSender) SerializeTxn() *roachpb.Transaction {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.txn.Clone()
}

// IsTracking returns true if the heartbeat loop is running.
func (tc *TxnCoordSender) IsTracking() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.interceptorAlloc.txnHeartbeater.heartbeatLoopRunningLocked()
}
