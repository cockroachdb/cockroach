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
	"sort"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
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
	opHeartbeatLoop  = "heartbeat"
)

// maxTxnIntentsBytes is a threshold in bytes for intent spans stored
// on the coordinator during the lifetime of a transaction. Intents
// are included with a transaction on commit or abort, to be cleaned
// up asynchronously. If they exceed this threshold, they're condensed
// to avoid memory blowup both on the coordinator and (critically) on
// the EndTransaction command at the Raft group responsible for the
// transaction record.
var maxTxnIntentsBytes = settings.RegisterIntSetting(
	"kv.transaction.max_intents_bytes",
	"maximum number of bytes used to track write intents in transactions",
	256*1000,
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
		// meta contains all coordinator state which may be passed between
		// distributed TxnCoordSenders via MetaRelease() and MetaAugment().
		meta roachpb.TxnCoordMeta

		// intentsSizeBytes is the size in bytes of the intent spans in the
		// meta, maintained to efficiently check the threshold.
		intentsSizeBytes int64
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

	// typ specifies whether this transaction is the top level,
	// or one of potentially many distributed transactions.
	typ client.TxnType
}

var _ client.TxnSender = &TxnCoordSender{}

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts     *metric.CounterWithRates
	Commits    *metric.CounterWithRates
	Commits1PC *metric.CounterWithRates // Commits which finished in a single phase
	Abandons   *metric.CounterWithRates
	Durations  *metric.Histogram

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
	metaAbandonsRates = metric.Metadata{
		Name: "txn.abandons",
		Help: "Number of abandoned KV transactions"}
	metaDurationsHistograms = metric.Metadata{
		Name: "txn.durations",
		Help: "KV transaction durations"}
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

// New is part of the TxnCoordSenderFactory interface.
func (tcf *TxnCoordSenderFactory) New(typ client.TxnType) client.TxnSender {
	return &TxnCoordSender{
		typ: typ,
		TxnCoordSenderFactory: tcf,
	}
}

// GetMeta is part of the client.TxnSender interface.
func (tc *TxnCoordSender) GetMeta() roachpb.TxnCoordMeta {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// Copy mutable state so access is safe for the caller.
	meta := tc.mu.meta
	meta.Txn = tc.mu.meta.Txn.Clone()
	meta.Intents = append([]roachpb.Span(nil), tc.mu.meta.Intents...)
	return meta
}

// AugmentMeta is part of the client.TxnSender interface.
func (tc *TxnCoordSender) AugmentMeta(meta roachpb.TxnCoordMeta) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// Sanity check: don't combine if the meta is for a different txn ID.
	if tc.mu.meta.Txn.ID != meta.Txn.ID {
		return
	}
	tc.mu.meta.Txn.Update(&meta.Txn)
	// Do not modify existing span slices when copying.
	tc.mu.meta.Intents, _ = roachpb.MergeSpans(
		append(append([]roachpb.Span(nil), tc.mu.meta.Intents...), meta.Intents...),
	)
	tc.mu.meta.CommandCount += meta.CommandCount

	// Recompute the size of the intents.
	tc.mu.intentsSizeBytes = 0
	for _, i := range tc.mu.meta.Intents {
		tc.mu.intentsSizeBytes += int64(len(i.Key) + len(i.EndKey))
	}
}

// OnFinish is part of the client.TxnSender interface.
func (tc *TxnCoordSender) OnFinish(onFinishFn func(error)) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.onFinishFn = onFinishFn
}

// Send implements the batch.Sender interface. If the request is part of a
// transaction, the TxnCoordSender adds the transaction to a map of active
// transactions and begins heartbeating it. Every subsequent request for the
// same transaction updates the lastUpdate timestamp to prevent live
// transactions from being considered abandoned and garbage collected.
// Read/write mutating requests have their key or key range added to the
// transaction's interval tree of key ranges for eventual cleanup via resolved
// write intents; they're tagged to an outgoing EndTransaction request, with
// the receiving replica in charge of resolving them.
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
		// If this request is part of a transaction...
		if err := tc.validateTxnForBatch(ctx, &ba); err != nil {
			return nil, roachpb.NewError(err)
		}

		txnID := ba.Txn.ID

		// Associate the txnID with the trace. We need to do this after the
		// maybeBeginTxn call.
		txnIDStr := txnID.String()
		sp.SetBaggageItem("txnID", txnIDStr)

		var et *roachpb.EndTransactionRequest
		var hasET bool
		{
			var rArgs roachpb.Request
			rArgs, hasET = ba.GetArg(roachpb.EndTransaction)
			if hasET {
				et = rArgs.(*roachpb.EndTransactionRequest)
				if len(et.Key) != 0 {
					return nil, roachpb.NewErrorf("EndTransaction must not have a Key set")
				}
				et.Key = ba.Txn.Key
				if len(et.IntentSpans) > 0 {
					// TODO(tschottdorf): it may be useful to allow this later.
					// That would be part of a possible plan to allow txns which
					// write on multiple coordinators.
					return nil, roachpb.NewErrorf("client must not pass intents to EndTransaction")
				}
			}
		}

		if pErr := func() *roachpb.Error {
			tc.mu.Lock()
			defer tc.mu.Unlock()
			if ba.Txn.Writing {
				if pErr := tc.maybeRejectClientLocked(ctx, ba.Txn.ID); pErr != nil {
					return pErr
				}
			}
			tc.mu.meta.CommandCount += int32(len(ba.Requests))

			if !hasET {
				return nil
			}
			// Everything below is carried out only when trying to finish a txn.
			if tc.typ == client.LeafTxn {
				return roachpb.NewErrorf("cannot commit on a leaf transaction coordinator")
			}

			// Populate et.IntentSpans, taking into account both any existing
			// and new writes, and taking care to perform proper deduplication.
			et.IntentSpans = tc.mu.meta.Intents
			intentsSizeBytes := tc.mu.intentsSizeBytes
			// Defensively set distinctSpans to false if we had any previous
			// writes in this transaction. This effectively limits the distinct
			// spans optimization to 1pc transactions.
			distinctSpans := len(tc.mu.meta.Intents) == 0

			// We can't pass in a batch response here to better limit the key
			// spans as we don't know what is going to be affected. This will
			// affect queries such as `DELETE FROM my.table LIMIT 10` when
			// executed as a 1PC transaction. e.g.: a (BeginTransaction,
			// DeleteRange, EndTransaction) batch.
			ba.IntentSpanIterate(nil, func(key, endKey roachpb.Key) {
				et.IntentSpans = append(et.IntentSpans, roachpb.Span{
					Key:    key,
					EndKey: endKey,
				})
				intentsSizeBytes += int64(len(key) + len(endKey))
			})
			// The request might already be used by an outgoing goroutine,
			// so we can't safely mutate anything in-place (as
			// maybeCondenseIntentSpans and MergeSpans do).
			et.IntentSpans = append([]roachpb.Span(nil), et.IntentSpans...)
			var err error
			if et.IntentSpans, intentsSizeBytes, err = tc.maybeCondenseIntentSpans(
				ctx, et.IntentSpans, intentsSizeBytes,
			); err != nil {
				return roachpb.NewError(err)
			}
			// TODO(peter): Populate DistinctSpans on all batches, not just batches
			// which contain an EndTransactionRequest.
			var distinct bool
			et.IntentSpans, distinct = roachpb.MergeSpans(et.IntentSpans)
			ba.Header.DistinctSpans = distinct && distinctSpans
			if len(et.IntentSpans) == 0 {
				// If there aren't any intents, then there's factually no
				// transaction to end. Read-only txns have all of their state
				// in the client.
				return roachpb.NewErrorf("cannot commit a read-only transaction")
			}
			tc.mu.meta.Intents = et.IntentSpans
			tc.mu.intentsSizeBytes = intentsSizeBytes

			return nil
		}(); pErr != nil {
			return nil, pErr
		}

		if hasET && log.V(1) {
			for _, intent := range et.IntentSpans {
				log.Eventf(ctx, "intent: [%s,%s)", intent.Key, intent.EndKey)
			}
		}
	}

	// Send the command through wrapped sender, taking appropriate measures
	// on error.
	var br *roachpb.BatchResponse
	{
		var pErr *roachpb.Error
		br, pErr = tc.wrapped.Send(ctx, ba)

		if _, ok := pErr.GetDetail().(*roachpb.OpRequiresTxnError); ok {
			br, pErr = tc.resendWithTxn(ctx, ba)
		}

		if pErr = tc.updateState(ctx, startNS, ba, br, pErr); pErr != nil {
			log.Eventf(ctx, "error: %s", pErr)
			return nil, pErr
		}
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

type spanBucket struct {
	rangeID roachpb.RangeID
	size    int64
	spans   []roachpb.Span
}

// maybeCondenseIntentSpans avoids sending massive EndTransaction
// requests which can consume excessive memory at evaluation time and
// in the txn coordinator sender itself. Spans are condensed based on
// current range boundaries. Returns the condensed set of spans and
// the new total spans size.
func (tc *TxnCoordSender) maybeCondenseIntentSpans(
	ctx context.Context, spans []roachpb.Span, spansSize int64,
) ([]roachpb.Span, int64, error) {
	if spansSize < maxTxnIntentsBytes.Get(&tc.st.SV) {
		return spans, spansSize, nil
	}
	// Only condense if the wrapped sender is a distributed sender.
	ds, ok := tc.wrapped.(*DistSender)
	if !ok {
		return spans, spansSize, nil
	}
	// Sort the spans by start key.
	sort.Slice(spans, func(i, j int) bool { return spans[i].Key.Compare(spans[j].Key) < 0 })

	// Divide them by range boundaries and condense. Iterate over spans
	// using a range iterator and add each to a bucket keyed by range
	// ID. Local keys are kept in a new slice and not added to buckets.
	buckets := []*spanBucket{}
	localSpans := []roachpb.Span{}
	ri := NewRangeIterator(ds)
	for _, s := range spans {
		if keys.IsLocal(s.Key) {
			localSpans = append(localSpans, s)
			continue
		}
		ri.Seek(ctx, roachpb.RKey(s.Key), Ascending)
		if !ri.Valid() {
			return nil, 0, ri.Error().GoError()
		}
		rangeID := ri.Desc().RangeID
		if l := len(buckets); l > 0 && buckets[l-1].rangeID == rangeID {
			buckets[l-1].spans = append(buckets[l-1].spans, s)
		} else {
			buckets = append(buckets, &spanBucket{rangeID: rangeID, spans: []roachpb.Span{s}})
		}
		buckets[len(buckets)-1].size += int64(len(s.Key) + len(s.EndKey))
	}

	// Sort the buckets by size and collapse from largest to smallest
	// until total size of uncondensed spans no longer exceeds threshold.
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].size > buckets[j].size })
	spans = localSpans // reset to hold just the local spans; will add newly condensed and remainder
	for _, bucket := range buckets {
		// Condense until we get to half the threshold.
		if spansSize <= maxTxnIntentsBytes.Get(&tc.st.SV)/2 {
			// Collect remaining spans from each bucket into uncondensed slice.
			spans = append(spans, bucket.spans...)
			continue
		}
		spansSize -= bucket.size
		// TODO(spencer): consider further optimizations here to create
		// more than one span out of a bucket to avoid overly broad span
		// combinations.
		cs := bucket.spans[0]
		for _, s := range bucket.spans[1:] {
			cs = cs.Combine(s)
			if !cs.Valid() {
				return nil, 0, errors.Errorf("combining span %s yielded invalid result", s)
			}
		}
		spansSize += int64(len(cs.Key) + len(cs.EndKey))
		spans = append(spans, cs)
	}

	return spans, spansSize, nil
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
// the TxnCoordSender. Furthermore, no transactional writes are allowed
// unless preceded by a begin transaction request within the same batch.
// The exception is if the transaction is already in state txn.Writing=true.
func (tc *TxnCoordSender) validateTxnForBatch(ctx context.Context, ba *roachpb.BatchRequest) error {
	if len(ba.Requests) == 0 {
		return errors.Errorf("empty batch with txn")
	}
	ba.Txn.AssertInitialized(ctx)

	// Check for a begin transaction to set txn key based on the key of
	// the first transactional write. Also enforce that no transactional
	// writes occur before a begin transaction.
	var haveBeginTxn bool
	for _, req := range ba.Requests {
		args := req.GetInner()
		if _, ok := args.(*roachpb.BeginTransactionRequest); ok {
			if haveBeginTxn || ba.Txn.Writing {
				return errors.Errorf("begin transaction requested twice in the same txn: %s", ba.Txn)
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
		if rejectErr != nil {
			log.Fatalf(ctx, "expected non-nil rejectErr on txn coord state %v", state)
		}
		tc.mu.onFinishFn(rejectErr)
	}
	tc.mu.meta.Intents = nil
	tc.mu.intentsSizeBytes = 0

	// The heartbeat might've already removed the record. Or we may have already
	// closed txnEnd but we are racing with the heartbeat cleanup.
	if tc.mu.txnEnd == nil {
		return
	}
	// Trigger heartbeat shutdown.
	log.Event(ctx, "coordinator stops")
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
		tc.mu.txnEnd = make(chan struct{})
		closer = tc.mu.txnEnd
		tc.mu.Unlock()
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
			log.Eventf(ctx, "transaction heartbeat stopped: %s", ctx.Err())
			tc.tryAsyncAbort(ctx)
			return
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
	// Clone the intents and the txn to avoid data races.
	intentSpans, _ := roachpb.MergeSpans(append([]roachpb.Span(nil), tc.mu.meta.Intents...))
	tc.cleanupTxnLocked(ctx, aborted)
	txn := tc.mu.meta.Txn.Clone()

	// Since we don't hold the lock continuously, it's possible that two aborts
	// raced here. That's fine (and probably better than the alternative, which
	// is missing new intents sometimes). Note that the txn may be uninitialized
	// here if a failure occurred before the first write succeeded.
	if txn.Status != roachpb.PENDING || txn.ID == (uuid.UUID{}) {
		return
	}

	// NB: use context.Background() here because we may be called when the
	// caller's context has been canceled.
	if err := tc.stopper.RunAsyncTask(
		tc.AnnotateCtx(context.Background()), "kv.TxnCoordSender: aborting txn", func(ctx context.Context) {
			// Use the wrapped sender since the normal Sender does not allow
			// clients to specify intents.
			resp, pErr := client.SendWrappedWith(
				ctx, tc.wrapped, roachpb.Header{Txn: &txn}, &roachpb.EndTransactionRequest{
					Span: roachpb.Span{
						Key: txn.Key,
					},
					Commit:      false,
					IntentSpans: intentSpans,
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
		if log.V(1) {
			log.Infof(ctx, "transaction %s abandoned; stopping heartbeat", txn)
		}
		log.Eventf(ctx, "transaction abandoned heartbeat stopped")
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

	log.Event(ctx, "heartbeat")
	br, pErr := tc.wrapped.Send(ctx, ba)

	// Correctness mandates that when we can't heartbeat the transaction, we
	// make sure the client doesn't keep going. This is particularly relevant
	// in the case of an ABORTED transaction, but event if we can't reach the
	// transaction record at all, we have to assume it's been aborted as well.
	if pErr != nil {
		log.Eventf(ctx, "heartbeat failed to %s: %s", txn, pErr)
		if errTxn := pErr.GetTxn(); errTxn != nil {
			tc.mu.Lock()
			tc.mu.meta.Txn.Update(errTxn)
			tc.mu.Unlock()
		}
		// We're not going to let the client carry out additional requests, so
		// try to clean up if the known txn disposition remains PENDING.
		if txn.Status == roachpb.PENDING {
			log.Eventf(ctx, "transaction heartbeat failed: %s", pErr)
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

// updateState updates the transaction state in both the success and
// error cases, applying those updates to the corresponding txnMeta
// object when adequate. It also updates retryable errors with the
// updated transaction for use by client restarts.
//
// startNS is the time when the request that's updating the state has
// been sent.  This is not used if the request is known to not be the
// one in charge of starting tracking the transaction - i.e. this is
// the case for DistSQL, which just does reads and passes 0.
func (tc *TxnCoordSender) updateState(
	ctx context.Context,
	startNS int64,
	ba roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	pErr *roachpb.Error,
) *roachpb.Error {

	if ba.Txn == nil {
		// Not a transactional request.
		return pErr
	}

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

			// Reset the statement count as this is a retryable txn error.
			tc.mu.Lock()
			tc.mu.meta.CommandCount = 0
			tc.mu.Unlock()

			// If the ID changed, it means we had to start a new transaction
			// and the old one is toast. Try an asynchronous abort of the
			// prior transaction to clean up its intents immediately, which
			// likely will otherwise require synchronous cleanup by the
			// restated transaction.
			if errTxnID != newTxn.ID {
				tc.tryAsyncAbort(ctx)
			}
			// Pass a HandledRetryableTxnError up to the next layer.
			pErr = roachpb.NewError(
				roachpb.NewHandledRetryableTxnError(
					pErr.Message,
					errTxnID, // the id of the transaction that encountered the error
					newTxn))
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

	tc.mu.Lock()
	defer tc.mu.Unlock()

	// For successful transactional requests, keep the written intents and
	// the updated transaction record to be sent along with the reply.
	// The transaction metadata is created with the first writing operation.
	//
	// A tricky edge case is that of a transaction which "fails" on the
	// first writing request, but actually manages to write some intents
	// (for example, due to being multi-range). In this case, there will
	// be an error, but the transaction will be marked as Writing and the
	// coordinator must track the state, for the client's retry will be
	// performed with a Writing transaction which the coordinator rejects
	// unless it is tracking it (on top of it making sense to track it;
	// after all, it **has** laid down intents and only the coordinator
	// can augment a potential EndTransaction call). See #3303.
	//
	// An extension of this case is that of a transaction which receives an
	// ambiguous result error on its first writing request. Here, the
	// transaction will not be marked as Writing, but still could have laid
	// down intents (we don't know, it's ambiguous!). As with the other case,
	// we still track the possible writes so they can be cleaned up cleanup
	// to avoid dangling intents. However, since the Writing flag is not
	// set in these cases, it may be possible that the request was read-only.
	// This is ok, since the following block will be a no-op if the batch
	// contained no transactional write requests.
	_, ambiguousErr := pErr.GetDetail().(*roachpb.AmbiguousResultError)
	if pErr == nil || ambiguousErr || newTxn.Writing {
		// Adding the intents even on error reduces the likelihood of dangling
		// intents blocking concurrent writers for extended periods of time.
		// See #3346.
		intents := tc.mu.meta.Intents
		intentsSizeBytes := tc.mu.intentsSizeBytes
		ba.IntentSpanIterate(br, func(key, endKey roachpb.Key) {
			intents = append(intents, roachpb.Span{Key: key, EndKey: endKey})
			intentsSizeBytes += int64(len(key) + len(endKey))
		})
		if condensedIntents, condensedIntentsSize, err := tc.maybeCondenseIntentSpans(ctx, intents, intentsSizeBytes); err != nil {
			log.ErrEventf(ctx, "failed to condense intent spans (%s); skipping", err)
		} else {
			intents, intentsSizeBytes = condensedIntents, condensedIntentsSize
		}

		// Initialize the first update time and maybe start the heartbeat.
		if tc.mu.firstUpdateNanos == 0 && len(intents) > 0 {
			// If the transaction is already over, there's no point in
			// launching a one-off heartbeat which will shut down right
			// away. If we ended up here with an error, we'll always start
			// the coordinator - the transaction has laid down intents, so
			// we expect it to be committed/aborted at some point in the
			// future.
			if _, isEnding := ba.GetArg(roachpb.EndTransaction); pErr != nil || !isEnding {
				log.Event(ctx, "coordinator spawns")
				tc.mu.firstUpdateNanos = startNS

				// Only heartbeat the txn record if we're the root transaction.
				if tc.typ == client.RootTxn {
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
						tc.updateStats(duration, restarts, status, false)
						return roachpb.NewError(err)
					}
				}
			} else {
				// If this was a successful one phase commit, update stats
				// directly as they won't otherwise be updated on heartbeat
				// loop shutdown.
				etArgs, ok := br.Responses[len(br.Responses)-1].GetInner().(*roachpb.EndTransactionResponse)
				tc.updateStats(tc.clock.PhysicalNow()-startNS, 0, newTxn.Status, ok && etArgs.OnePhaseCommit)
			}
		}
		tc.mu.meta.Intents = intents
		tc.mu.intentsSizeBytes = intentsSizeBytes
	}

	// Update our record of this transaction, even on error.
	tc.mu.meta.Txn.Update(&newTxn)
	tc.mu.lastUpdateNanos = tc.clock.PhysicalNow()

	return pErr
}

// TODO(tschottdorf): this method is somewhat awkward but unless we want to
// give this error back to the client, our options are limited. We'll have to
// run the whole thing for them, or any restart will still end up at the client
// which will not be prepared to be handed a Txn.
func (tc *TxnCoordSender) resendWithTxn(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Run a one-off transaction with that single command.
	if log.V(1) {
		log.Infof(ctx, "%s: auto-wrapping in txn and re-executing: ", ba)
	}
	// TODO(bdarnell): need to be able to pass other parts of DBContext
	// through here.
	dbCtx := client.DefaultDBContext()
	dbCtx.UserPriority = ba.UserPriority
	tmpDB := client.NewDBWithContext(tc.TxnCoordSenderFactory, tc.clock, dbCtx)
	var br *roachpb.BatchResponse
	err := tmpDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetDebugName("auto-wrap")
		b := txn.NewBatch()
		b.Header = ba.Header
		for _, arg := range ba.Requests {
			req := arg.GetInner()
			b.AddRawRequest(req)
		}
		err := txn.CommitInBatch(ctx, b)
		br = b.RawResponse()
		return err
	})
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	br.Txn = nil // hide the evidence
	return br, nil
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
