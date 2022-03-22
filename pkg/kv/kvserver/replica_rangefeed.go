// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// RangefeedEnabled is a cluster setting that enables rangefeed requests.
// Certain ranges have span configs that specifically enable rangefeeds (system
// ranges and ranges covering tables in the system database); this setting
// covers everything else.
var RangefeedEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"kv.rangefeed.enabled",
	"if set, rangefeed registration is enabled",
	false,
).WithPublic()

// RangeFeedRefreshInterval controls the frequency with which we deliver closed
// timestamp updates to rangefeeds.
var RangeFeedRefreshInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"kv.rangefeed.closed_timestamp_refresh_interval",
	"the interval at which closed-timestamp updates"+
		"are delivered to rangefeeds; set to 0 to use kv.closed_timestamp.side_transport_interval",
	0,
	settings.NonNegativeDuration,
)

// RangefeedTBIEnabled controls whether or not we use a TBI during catch-up scan.
var RangefeedTBIEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"kv.rangefeed.catchup_scan_iterator_optimization.enabled",
	"if true, rangefeeds will use time-bound iterators for catchup-scans when possible",
	util.ConstantWithMetamorphicTestBool("kv.rangefeed.catchup_scan_iterator_optimization.enabled", true),
)

// lockedRangefeedStream is an implementation of rangefeed.Stream which provides
// support for concurrent calls to Send. Note that the default implementation of
// grpc.Stream is not safe for concurrent calls to Send.
type lockedRangefeedStream struct {
	wrapped rangefeed.Stream
	sendMu  syncutil.Mutex
}

func (s *lockedRangefeedStream) Context() context.Context {
	return s.wrapped.Context()
}

func (s *lockedRangefeedStream) Send(e *roachpb.RangeFeedEvent) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.wrapped.Send(e)
}

// rangefeedTxnPusher is a shim around intentResolver that implements the
// rangefeed.TxnPusher interface.
type rangefeedTxnPusher struct {
	ir *intentresolver.IntentResolver
	r  *Replica
}

// PushTxns is part of the rangefeed.TxnPusher interface. It performs a
// high-priority push at the specified timestamp to each of the specified
// transactions.
func (tp *rangefeedTxnPusher) PushTxns(
	ctx context.Context, txns []enginepb.TxnMeta, ts hlc.Timestamp,
) ([]*roachpb.Transaction, error) {
	pushTxnMap := make(map[uuid.UUID]*enginepb.TxnMeta, len(txns))
	for i := range txns {
		txn := &txns[i]
		pushTxnMap[txn.ID] = txn
	}

	h := roachpb.Header{
		Timestamp: ts,
		Txn: &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Priority: enginepb.MaxTxnPriority,
			},
		},
	}

	pushedTxnMap, pErr := tp.ir.MaybePushTransactions(
		ctx, pushTxnMap, h, roachpb.PUSH_TIMESTAMP, false, /* skipIfInFlight */
	)
	if pErr != nil {
		return nil, pErr.GoError()
	}

	pushedTxns := make([]*roachpb.Transaction, 0, len(pushedTxnMap))
	for _, txn := range pushedTxnMap {
		pushedTxns = append(pushedTxns, txn)
	}
	return pushedTxns, nil
}

// ResolveIntents is part of the rangefeed.TxnPusher interface.
func (tp *rangefeedTxnPusher) ResolveIntents(
	ctx context.Context, intents []roachpb.LockUpdate,
) error {
	return tp.ir.ResolveIntents(ctx, intents,
		// NB: Poison is ignored for non-ABORTED intents.
		intentresolver.ResolveOptions{Poison: true},
	).GoError()
}

// RangeFeed registers a rangefeed over the specified span. It sends updates to
// the provided stream and returns with an optional error when the rangefeed is
// complete. The provided ConcurrentRequestLimiter is used to limit the number
// of rangefeeds using catch-up iterators at the same time.
func (r *Replica) RangeFeed(
	args *roachpb.RangeFeedRequest, stream rangefeed.Stream,
) *roachpb.Error {
	return r.rangeFeedWithRangeID(r.RangeID, args, stream)
}

func (r *Replica) rangeFeedWithRangeID(
	_forStacks roachpb.RangeID, args *roachpb.RangeFeedRequest, stream rangefeed.Stream,
) *roachpb.Error {
	if !r.isRangefeedEnabled() && !RangefeedEnabled.Get(&r.store.cfg.Settings.SV) {
		return roachpb.NewErrorf("rangefeeds require the kv.rangefeed.enabled setting. See %s",
			docs.URL(`change-data-capture.html#enable-rangefeeds-to-reduce-latency`))
	}
	ctx := r.AnnotateCtx(stream.Context())

	rSpan, err := keys.SpanAddr(args.Span)
	if err != nil {
		return roachpb.NewError(err)
	}

	if err := r.ensureClosedTimestampStarted(ctx); err != nil {
		if err := stream.Send(&roachpb.RangeFeedEvent{Error: &roachpb.RangeFeedError{
			Error: *err,
		}}); err != nil {
			return roachpb.NewError(err)
		}
		return nil
	}

	// If the RangeFeed is performing a catch-up scan then it will observe all
	// values above args.Timestamp. If the RangeFeed is requesting previous
	// values for every update then it will also need to look for the version
	// proceeding each value observed during the catch-up scan timestamp. This
	// means that the earliest value observed by the catch-up scan will be
	// args.Timestamp.Next and the earliest timestamp used to retrieve the
	// previous version of a value will be args.Timestamp, so this is the
	// timestamp we must check against the GCThreshold.
	checkTS := args.Timestamp
	if checkTS.IsEmpty() {
		// If no timestamp was provided then we're not going to run a catch-up
		// scan, so make sure the GCThreshold in requestCanProceed succeeds.
		checkTS = r.Clock().Now()
	}

	lockedStream := &lockedRangefeedStream{wrapped: stream}
	errC := make(chan *roachpb.Error, 1)

	// If we will be using a catch-up iterator, wait for the limiter here before
	// locking raftMu.
	usingCatchUpIter := false
	var iterSemRelease func()
	if !args.Timestamp.IsEmpty() {
		usingCatchUpIter = true
		alloc, err := r.store.limiters.ConcurrentRangefeedIters.Begin(ctx)
		if err != nil {
			return roachpb.NewError(err)
		}
		// Finish the iterator limit if we exit before the iterator finishes.
		// The release function will be hooked into the Close method on the
		// iterator below. The sync.Once prevents any races between exiting early
		// from this call and finishing the catch-up scan underneath the
		// rangefeed.Processor. We need to release here in case we fail to
		// register the processor, or, more perniciously, in the case where the
		// processor gets registered by shut down before starting the catch-up
		// scan.
		var iterSemReleaseOnce sync.Once
		iterSemRelease = func() {
			iterSemReleaseOnce.Do(alloc.Release)
		}
		defer iterSemRelease()
	}

	// Lock the raftMu, then register the stream as a new rangefeed registration.
	// raftMu is held so that the catch-up iterator is captured in the same
	// critical-section as the registration is established. This ensures that
	// the registration doesn't miss any events.
	r.raftMu.Lock()
	if err := r.checkExecutionCanProceedForRangeFeed(ctx, rSpan, checkTS); err != nil {
		r.raftMu.Unlock()
		return roachpb.NewError(err)
	}

	// Register the stream with a catch-up iterator.
	var catchUpIterFunc rangefeed.CatchUpIteratorConstructor
	if usingCatchUpIter {
		catchUpIterFunc = func() *rangefeed.CatchUpIterator {
			// Assert that we still hold the raftMu when this is called to ensure
			// that the catchUpIter reads from the current snapshot.
			r.raftMu.AssertHeld()
			return rangefeed.NewCatchUpIterator(r.Engine(),
				args, RangefeedTBIEnabled.Get(&r.store.cfg.Settings.SV), iterSemRelease)
		}
	}
	p := r.registerWithRangefeedRaftMuLocked(
		ctx, rSpan, args.Timestamp, catchUpIterFunc, args.WithDiff, lockedStream, errC,
	)
	r.raftMu.Unlock()

	// When this function returns, attempt to clean up the rangefeed.
	defer r.maybeDisconnectEmptyRangefeed(p)

	// Block on the registration's error channel. Note that the registration
	// observes stream.Context().Done.
	return <-errC
}

func (r *Replica) getRangefeedProcessorAndFilter() (*rangefeed.Processor, *rangefeed.Filter) {
	r.rangefeedMu.RLock()
	defer r.rangefeedMu.RUnlock()
	return r.rangefeedMu.proc, r.rangefeedMu.opFilter
}

func (r *Replica) getRangefeedProcessor() *rangefeed.Processor {
	p, _ := r.getRangefeedProcessorAndFilter()
	return p
}

func (r *Replica) setRangefeedProcessor(p *rangefeed.Processor) {
	r.rangefeedMu.Lock()
	defer r.rangefeedMu.Unlock()
	r.rangefeedMu.proc = p
	r.store.addReplicaWithRangefeed(r.RangeID)
}

func (r *Replica) unsetRangefeedProcessorLocked(p *rangefeed.Processor) {
	if r.rangefeedMu.proc != p {
		// The processor was already unset.
		return
	}
	r.rangefeedMu.proc = nil
	r.rangefeedMu.opFilter = nil
	r.store.removeReplicaWithRangefeed(r.RangeID)
}

func (r *Replica) unsetRangefeedProcessor(p *rangefeed.Processor) {
	r.rangefeedMu.Lock()
	defer r.rangefeedMu.Unlock()
	r.unsetRangefeedProcessorLocked(p)
}

func (r *Replica) setRangefeedFilterLocked(f *rangefeed.Filter) {
	if f == nil {
		panic("filter nil")
	}
	r.rangefeedMu.opFilter = f
}

func (r *Replica) updateRangefeedFilterLocked() bool {
	f := r.rangefeedMu.proc.Filter()
	// Return whether the update to the filter was successful or not. If
	// the processor was already stopped then we can't update the filter.
	if f != nil {
		r.setRangefeedFilterLocked(f)
		return true
	}
	return false
}

// The size of an event is 112 bytes, so this will result in an allocation on
// the order of ~512KB per RangeFeed. That's probably ok given the number of
// ranges on a node that we'd like to support with active rangefeeds, but it's
// certainly on the upper end of the range.
//
// TODO(dan): Everyone seems to agree that this memory limit would be better set
// at a store-wide level, but there doesn't seem to be an easy way to accomplish
// that.
const defaultEventChanCap = 4096

// Rangefeed registration takes place under the raftMu, so log if we ever hold
// the mutex for too long, as this could affect foreground traffic.
//
// At the time of writing (09/2021), the blocking call to Processor.syncEventC
// in Processor.Register appears to be mildly concerning, but we have no reason
// to believe that is blocks the raftMu in practice.
func logSlowRangefeedRegistration(ctx context.Context) func() {
	const slowRaftMuWarnThreshold = 20 * time.Millisecond
	start := timeutil.Now()
	return func() {
		elapsed := timeutil.Since(start)
		if elapsed >= slowRaftMuWarnThreshold {
			log.Warningf(ctx, "rangefeed registration took %s", elapsed)
		}
	}
}

// registerWithRangefeedRaftMuLocked sets up a Rangefeed registration over the
// provided span. It initializes a rangefeed for the Replica if one is not
// already running. Requires raftMu be locked.
func (r *Replica) registerWithRangefeedRaftMuLocked(
	ctx context.Context,
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	catchUpIter rangefeed.CatchUpIteratorConstructor,
	withDiff bool,
	stream rangefeed.Stream,
	errC chan<- *roachpb.Error,
) *rangefeed.Processor {
	defer logSlowRangefeedRegistration(ctx)()

	// Attempt to register with an existing Rangefeed processor, if one exists.
	// The locking here is a little tricky because we need to handle the case
	// of concurrent processor shutdowns (see maybeDisconnectEmptyRangefeed).
	r.rangefeedMu.Lock()
	p := r.rangefeedMu.proc
	if p != nil {
		reg, filter := p.Register(span, startTS, catchUpIter, withDiff, stream, errC)
		if reg {
			// Registered successfully with an existing processor.
			// Update the rangefeed filter to avoid filtering ops
			// that this new registration might be interested in.
			r.setRangefeedFilterLocked(filter)
			r.rangefeedMu.Unlock()
			return p
		}
		// If the registration failed, the processor was already being shut
		// down. Help unset it and then continue on with initializing a new
		// processor.
		r.unsetRangefeedProcessorLocked(p)
		p = nil
	}
	r.rangefeedMu.Unlock()

	feedBudget := r.store.GetStoreConfig().RangefeedBudgetFactory.CreateBudget(r.startKey)

	// Create a new rangefeed.
	desc := r.Desc()
	tp := rangefeedTxnPusher{ir: r.store.intentResolver, r: r}
	cfg := rangefeed.Config{
		AmbientContext:   r.AmbientContext,
		Clock:            r.Clock(),
		RangeID:          r.RangeID,
		Span:             desc.RSpan(),
		TxnPusher:        &tp,
		PushTxnsInterval: r.store.TestingKnobs().RangeFeedPushTxnsInterval,
		PushTxnsAge:      r.store.TestingKnobs().RangeFeedPushTxnsAge,
		EventChanCap:     defaultEventChanCap,
		EventChanTimeout: 50 * time.Millisecond,
		Metrics:          r.store.metrics.RangeFeedMetrics,
		MemBudget:        feedBudget,
	}
	p = rangefeed.NewProcessor(cfg)

	// Start it with an iterator to initialize the resolved timestamp.
	rtsIter := func() rangefeed.IntentScanner {
		// Assert that we still hold the raftMu when this is called to ensure
		// that the rtsIter reads from the current snapshot. The replica
		// synchronizes with the rangefeed Processor calling this function by
		// waiting for the Register call below to return.
		r.raftMu.AssertHeld()

		lowerBound, _ := keys.LockTableSingleKey(desc.StartKey.AsRawKey(), nil)
		upperBound, _ := keys.LockTableSingleKey(desc.EndKey.AsRawKey(), nil)
		iter := r.Engine().NewEngineIterator(storage.IterOptions{
			LowerBound: lowerBound,
			UpperBound: upperBound,
		})
		return rangefeed.NewSeparatedIntentScanner(iter)
	}

	// NB: This only errors if the stopper is stopping, and we have to return here
	// in that case. We do check ShouldQuiesce() below, but that's not sufficient
	// because the stopper has two states: stopping and quiescing. If this errors
	// due to stopping, but before it enters the quiescing state, then the select
	// below will fall through to the panic.
	if err := p.Start(r.store.Stopper(), rtsIter); err != nil {
		errC <- roachpb.NewError(err)
		return nil
	}

	// Register with the processor *before* we attach its reference to the
	// Replica struct. This ensures that the registration is in place before
	// any other goroutines are able to stop the processor. In other words,
	// this ensures that the only time the registration fails is during
	// server shutdown.
	reg, filter := p.Register(span, startTS, catchUpIter, withDiff, stream, errC)
	if !reg {
		select {
		case <-r.store.Stopper().ShouldQuiesce():
			errC <- roachpb.NewError(&roachpb.NodeUnavailableError{})
			return nil
		default:
			panic("unexpected Stopped processor")
		}
	}

	// Set the rangefeed processor and filter reference. We know that no other
	// registration process could have raced with ours because calling this
	// method requires raftMu to be exclusively locked.
	r.setRangefeedProcessor(p)
	r.setRangefeedFilterLocked(filter)

	// Check for an initial closed timestamp update immediately to help
	// initialize the rangefeed's resolved timestamp as soon as possible.
	r.handleClosedTimestampUpdateRaftMuLocked(ctx, r.GetClosedTimestamp(ctx))

	return p
}

// maybeDisconnectEmptyRangefeed tears down the provided Processor if it is
// still active and if it no longer has any registrations.
func (r *Replica) maybeDisconnectEmptyRangefeed(p *rangefeed.Processor) {
	r.rangefeedMu.Lock()
	defer r.rangefeedMu.Unlock()
	if p == nil || p != r.rangefeedMu.proc {
		// The processor has already been removed or replaced.
		return
	}
	if p.Len() == 0 || !r.updateRangefeedFilterLocked() {
		// Stop the rangefeed processor if it has no registrations or if we are
		// unable to update the operation filter.
		p.Stop()
		r.unsetRangefeedProcessorLocked(p)
	}
}

// disconnectRangefeedWithErr broadcasts the provided error to all rangefeed
// registrations and tears down the provided rangefeed Processor.
func (r *Replica) disconnectRangefeedWithErr(p *rangefeed.Processor, pErr *roachpb.Error) {
	p.StopWithErr(pErr)
	r.unsetRangefeedProcessor(p)
}

// disconnectRangefeedSpanWithErr broadcasts the provided error to all rangefeed
// registrations that overlap the given span. Tears down the rangefeed Processor
// if it has no remaining registrations.
func (r *Replica) disconnectRangefeedSpanWithErr(span roachpb.Span, pErr *roachpb.Error) {
	p := r.getRangefeedProcessor()
	if p == nil {
		return
	}
	p.DisconnectSpanWithErr(span, pErr)
	r.maybeDisconnectEmptyRangefeed(p)
}

// disconnectRangefeedWithReason broadcasts the provided rangefeed retry reason
// to all rangefeed registrations and tears down the active rangefeed Processor.
// No-op if a rangefeed is not active.
func (r *Replica) disconnectRangefeedWithReason(reason roachpb.RangeFeedRetryError_Reason) {
	p := r.getRangefeedProcessor()
	if p == nil {
		return
	}
	pErr := roachpb.NewError(roachpb.NewRangeFeedRetryError(reason))
	r.disconnectRangefeedWithErr(p, pErr)
}

// numRangefeedRegistrations returns the number of registrations attached to the
// Replica's rangefeed processor.
func (r *Replica) numRangefeedRegistrations() int {
	p := r.getRangefeedProcessor()
	if p == nil {
		return 0
	}
	return p.Len()
}

// populatePrevValsInLogicalOpLogRaftMuLocked updates the provided logical op
// log with previous values read from the reader, which is expected to reflect
// the state of the Replica before the operations in the logical op log are
// applied. No-op if a rangefeed is not active. Requires raftMu to be locked.
func (r *Replica) populatePrevValsInLogicalOpLogRaftMuLocked(
	ctx context.Context, ops *kvserverpb.LogicalOpLog, prevReader storage.Reader,
) {
	p, filter := r.getRangefeedProcessorAndFilter()
	if p == nil {
		return
	}

	// Read from the Reader to populate the PrevValue fields.
	for _, op := range ops.Ops {
		var key []byte
		var ts hlc.Timestamp
		var prevValPtr *[]byte
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			key, ts, prevValPtr = t.Key, t.Timestamp, &t.PrevValue
		case *enginepb.MVCCCommitIntentOp:
			key, ts, prevValPtr = t.Key, t.Timestamp, &t.PrevValue
		case *enginepb.MVCCWriteIntentOp,
			*enginepb.MVCCUpdateIntentOp,
			*enginepb.MVCCAbortIntentOp,
			*enginepb.MVCCAbortTxnOp:
			// Nothing to do.
			continue
		default:
			panic(errors.AssertionFailedf("unknown logical op %T", t))
		}

		// Don't read previous values from the reader for operations that are
		// not needed by any rangefeed registration.
		if !filter.NeedPrevVal(roachpb.Span{Key: key}) {
			continue
		}

		// Read the previous value from the prev Reader. Unlike the new value
		// (see handleLogicalOpLogRaftMuLocked), this one may be missing.
		prevVal, _, err := storage.MVCCGet(
			ctx, prevReader, key, ts, storage.MVCCGetOptions{Tombstones: true, Inconsistent: true},
		)
		if err != nil {
			r.disconnectRangefeedWithErr(p, roachpb.NewErrorf(
				"error consuming %T for key %v @ ts %v: %v", op, key, ts, err,
			))
			return
		}
		if prevVal != nil {
			*prevValPtr = prevVal.RawBytes
		} else {
			*prevValPtr = nil
		}
	}
}

// handleLogicalOpLogRaftMuLocked passes the logical op log to the active
// rangefeed, if one is running. The method accepts a reader, which is used to
// look up the values associated with key-value writes in the log before handing
// them to the rangefeed processor. No-op if a rangefeed is not active. Requires
// raftMu to be locked.
func (r *Replica) handleLogicalOpLogRaftMuLocked(
	ctx context.Context, ops *kvserverpb.LogicalOpLog, reader storage.Reader,
) {
	p, filter := r.getRangefeedProcessorAndFilter()
	if p == nil {
		return
	}
	if ops == nil {
		// Rangefeeds can't be turned on unless RangefeedEnabled is set to true,
		// after which point new Raft proposals will include logical op logs.
		// However, there's a race present where old Raft commands without a
		// logical op log might be passed to a rangefeed. Since the effect of
		// these commands was not included in the catch-up scan of current
		// registrations, we're forced to throw an error. The rangefeed clients
		// can reconnect at a later time, at which point all new Raft commands
		// should have logical op logs.
		r.disconnectRangefeedWithReason(roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING)
		return
	}
	if len(ops.Ops) == 0 {
		return
	}

	// When reading straight from the Raft log, some logical ops will not be
	// fully populated. Read from the Reader to populate all fields.
	for _, op := range ops.Ops {
		var key []byte
		var ts hlc.Timestamp
		var valPtr *[]byte
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			key, ts, valPtr = t.Key, t.Timestamp, &t.Value
		case *enginepb.MVCCCommitIntentOp:
			key, ts, valPtr = t.Key, t.Timestamp, &t.Value
		case *enginepb.MVCCWriteIntentOp,
			*enginepb.MVCCUpdateIntentOp,
			*enginepb.MVCCAbortIntentOp,
			*enginepb.MVCCAbortTxnOp:
			// Nothing to do.
			continue
		default:
			panic(errors.AssertionFailedf("unknown logical op %T", t))
		}

		// Don't read values from the reader for operations that are not needed
		// by any rangefeed registration. We still need to inform the rangefeed
		// processor of the changes to intents so that it can track unresolved
		// intents, but we don't need to provide values.
		//
		// We could filter out MVCCWriteValueOp operations entirely at this
		// point if they are not needed by any registration, but as long as we
		// avoid the value lookup here, doing any more doesn't seem worth it.
		if !filter.NeedVal(roachpb.Span{Key: key}) {
			continue
		}

		// Read the value directly from the Reader. This is performed in the
		// same raftMu critical section that the logical op's corresponding
		// WriteBatch is applied, so the value should exist.
		val, _, err := storage.MVCCGet(ctx, reader, key, ts, storage.MVCCGetOptions{Tombstones: true})
		if val == nil && err == nil {
			err = errors.New("value missing in reader")
		}
		if err != nil {
			r.disconnectRangefeedWithErr(p, roachpb.NewErrorf(
				"error consuming %T for key %v @ ts %v: %v", op, key, ts, err,
			))
			return
		}
		*valPtr = val.RawBytes
	}

	// Pass the ops to the rangefeed processor.
	if !p.ConsumeLogicalOps(ctx, ops.Ops...) {
		// Consumption failed and the rangefeed was stopped.
		r.unsetRangefeedProcessor(p)
	}
}

// handleSSTableRaftMuLocked emits an ingested SSTable from AddSSTable via the
// rangefeed. These can be expected to have timestamps at the write timestamp
// (i.e. submitted with SSTTimestampToRequestTimestamp) since we assert
// elsewhere that MVCCHistoryMutation commands disconnect rangefeeds.
//
// NB: We currently don't have memory budgeting for rangefeeds, instead using a
// large buffered channel, so this can easily OOM the node. This is "fine" for
// now, since we do not currently expect AddSSTable across spans with
// rangefeeds, but must be added before we start publishing SSTables in earnest.
// See: https://github.com/cockroachdb/cockroach/issues/73616
func (r *Replica) handleSSTableRaftMuLocked(
	ctx context.Context, sst []byte, sstSpan roachpb.Span, writeTS hlc.Timestamp,
) {
	p, _ := r.getRangefeedProcessorAndFilter()
	if p == nil {
		return
	}
	if !p.ConsumeSSTable(ctx, sst, sstSpan, writeTS) {
		r.unsetRangefeedProcessor(p)
	}
}

// handleClosedTimestampUpdate takes the a closed timestamp for the replica
// and informs the rangefeed, if one is running. No-op if a
// rangefeed is not active.
//
// closeTS is generally expected to be the highest closed timestamp known, but
// it doesn't need to be - handleClosedTimestampUpdate can be called with
// updates out of order.
func (r *Replica) handleClosedTimestampUpdate(ctx context.Context, closedTS hlc.Timestamp) {
	ctx = r.AnnotateCtx(ctx)
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.handleClosedTimestampUpdateRaftMuLocked(ctx, closedTS)
}

// handleClosedTimestampUpdateRaftMuLocked is like handleClosedTimestampUpdate,
// but it requires raftMu to be locked.
func (r *Replica) handleClosedTimestampUpdateRaftMuLocked(
	ctx context.Context, closedTS hlc.Timestamp,
) {
	p := r.getRangefeedProcessor()
	if p == nil {
		return
	}

	// If the closed timestamp is sufficiently stale, signal that we want an
	// update to the leaseholder so that it will eventually begin to progress
	// again.
	behind := r.Clock().PhysicalTime().Sub(closedTS.GoTime())
	slowClosedTSThresh := 5 * closedts.TargetDuration.Get(&r.store.cfg.Settings.SV)
	if behind > slowClosedTSThresh {
		m := r.store.metrics.RangeFeedMetrics
		if m.RangeFeedSlowClosedTimestampLogN.ShouldLog() {
			if closedTS.IsEmpty() {
				log.Infof(ctx, "RangeFeed closed timestamp is empty")
			} else {
				log.Infof(ctx, "RangeFeed closed timestamp %s is behind by %s", closedTS, behind)
			}
		}

		// Asynchronously attempt to nudge the closed timestamp in case it's stuck.
		key := fmt.Sprintf(`rangefeed-slow-closed-timestamp-nudge-r%d`, r.RangeID)
		// Ignore the result of DoChan since, to keep this all async, it always
		// returns nil and any errors are logged by the closure passed to the
		// `DoChan` call.
		taskCtx, sp := tracing.EnsureForkSpan(ctx, r.AmbientContext.Tracer, key)
		_, leader := m.RangeFeedSlowClosedTimestampNudge.DoChan(key, func() (interface{}, error) {
			defer sp.Finish()
			// Also ignore the result of RunTask, since it only returns errors when
			// the task didn't start because we're shutting down.
			_ = r.store.stopper.RunTask(taskCtx, key, func(ctx context.Context) {
				// Limit the amount of work this can suddenly spin up. In particular,
				// this is to protect against the case of a system-wide slowdown on
				// closed timestamps, which would otherwise potentially launch a huge
				// number of lease acquisitions all at once.
				select {
				case <-ctx.Done():
					// Don't need to do this anymore.
					return
				case m.RangeFeedSlowClosedTimestampNudgeSem <- struct{}{}:
				}
				defer func() { <-m.RangeFeedSlowClosedTimestampNudgeSem }()
				if err := r.ensureClosedTimestampStarted(ctx); err != nil {
					log.Infof(ctx, `RangeFeed failed to nudge: %s`, err)
				}
			})
			return nil, nil
		})
		if !leader {
			// In the leader case, we've passed ownership of sp to the task. If the
			// task was not triggered, though, it's up to us to Finish() it.
			sp.Finish()
		}
	}

	// If the closed timestamp is not empty, inform the Processor.
	if closedTS.IsEmpty() {
		return
	}
	if !p.ForwardClosedTS(ctx, closedTS) {
		// Consumption failed and the rangefeed was stopped.
		r.unsetRangefeedProcessor(p)
	}
}

// ensureClosedTimestampStarted does its best to make sure that this node is
// receiving closed timestamp updates for this replica's range. Note that this
// forces a valid lease to exist on the range and so can be reasonably expensive
// if there is not already a valid lease.
func (r *Replica) ensureClosedTimestampStarted(ctx context.Context) *roachpb.Error {
	// Make sure there's a valid lease. If there's no lease, nobody's sending
	// closed timestamp updates.
	lease := r.CurrentLeaseStatus(ctx)

	if !lease.IsValid() {
		// Send a cheap request that needs a leaseholder, in order to ensure a
		// lease. We don't care about the request's result, only its routing. We're
		// employing higher-level machinery here (the DistSender); there's no better
		// way to ensure that someone (potentially another replica) takes a lease.
		// In particular, r.redirectOnOrAcquireLease() doesn't work because, if the
		// current lease is invalid and the current replica is not a leader, the
		// current replica will not take a lease.
		log.VEventf(ctx, 2, "ensuring lease for rangefeed range. current lease invalid: %s", lease.Lease)
		err := contextutil.RunWithTimeout(ctx, "read forcing lease acquisition", 5*time.Second,
			func(ctx context.Context) error {
				var b kv.Batch
				liReq := &roachpb.LeaseInfoRequest{}
				liReq.Key = r.Desc().StartKey.AsRawKey()
				b.AddRawRequest(liReq)
				return r.store.DB().Run(ctx, &b)
			})
		if err != nil {
			if errors.HasType(err, (*contextutil.TimeoutError)(nil)) {
				err = &roachpb.RangeFeedRetryError{
					Reason: roachpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER,
				}
			}
			return roachpb.NewError(err)
		}
	}
	return nil
}
