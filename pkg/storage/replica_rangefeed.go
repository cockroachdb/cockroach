// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/storage/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// RangefeedEnabled is a cluster setting that enables rangefeed requests.
var RangefeedEnabled = settings.RegisterBoolSetting(
	"kv.rangefeed.enabled",
	"if set, rangefeed registration is enabled",
	false,
)

// lockedRangefeedStream is an implementation of rangefeed.Stream which provides
// support for concurrent calls to Send. Note that the default implementation of
// grpc.Stream is not safe for concurrent calls to Send.
type lockedRangefeedStream struct {
	wrapped roachpb.Internal_RangeFeedServer
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
) ([]roachpb.Transaction, error) {
	pushTxnMap := make(map[uuid.UUID]enginepb.TxnMeta, len(txns))
	for _, txn := range txns {
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

	pushedTxns := make([]roachpb.Transaction, 0, len(pushedTxnMap))
	for _, txn := range pushedTxnMap {
		pushedTxns = append(pushedTxns, txn)
	}
	return pushedTxns, nil
}

// CleanupTxnIntentsAsync is part of the rangefeed.TxnPusher interface.
func (tp *rangefeedTxnPusher) CleanupTxnIntentsAsync(
	ctx context.Context, txns []roachpb.Transaction,
) error {
	endTxns := make([]result.EndTxnIntents, len(txns))
	for i, txn := range txns {
		endTxns[i].Txn = txn
	}
	return tp.ir.CleanupTxnIntentsAsync(ctx, tp.r.RangeID, endTxns, true /* allowSyncProcessing */)
}

type iteratorWithCloser struct {
	engine.SimpleIterator
	close func()
}

func (i iteratorWithCloser) Close() {
	i.SimpleIterator.Close()
	i.close()
}

// RangeFeed registers a rangefeed over the specified span. It sends updates to
// the provided stream and returns with an optional error when the rangefeed is
// complete. The provided ConcurrentRequestLimiter is used to limit the number
// of rangefeeds using catchup iterators at the same time.
func (r *Replica) RangeFeed(
	args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer,
) *roachpb.Error {
	if !RangefeedEnabled.Get(&r.store.cfg.Settings.SV) {
		return roachpb.NewErrorf("rangefeeds require the kv.rangefeed.enabled setting. See " +
			base.DocsURL(`change-data-capture.html#enable-rangefeeds-to-reduce-latency`))
	}
	ctx := r.AnnotateCtx(stream.Context())

	var rspan roachpb.RSpan
	var err error
	rspan.Key, err = keys.Addr(args.Span.Key)
	if err != nil {
		return roachpb.NewError(err)
	}
	rspan.EndKey, err = keys.Addr(args.Span.EndKey)
	if err != nil {
		return roachpb.NewError(err)
	}

	if err := r.ensureClosedTimestampStarted(ctx); err != nil {
		return err
	}

	checkTS := args.Timestamp
	if checkTS.IsEmpty() {
		checkTS = r.Clock().Now()
	}

	lockedStream := &lockedRangefeedStream{wrapped: stream}
	errC := make(chan *roachpb.Error, 1)

	// If we will be using a catch-up iterator, wait for the limiter here before
	// locking raftMu.
	usingCatchupIter := false
	var iterSemRelease func()
	if !args.Timestamp.IsEmpty() {
		usingCatchupIter = true
		lim := &r.store.limiters.ConcurrentRangefeedIters
		if err := lim.Begin(ctx); err != nil {
			return roachpb.NewError(err)
		}
		// Finish the iterator limit, but only if we exit before
		// creating the iterator itself.
		iterSemRelease = lim.Finish
		defer func() {
			if iterSemRelease != nil {
				iterSemRelease()
			}
		}()
	}

	// Lock the raftMu, then register the stream as a new rangefeed registration.
	// raftMu is held so that the catch-up iterator is captured in the same
	// critical-section as the registration is established. This ensures that
	// the registration doesn't miss any events.
	r.raftMu.Lock()
	if err := r.requestCanProceed(rspan, checkTS); err != nil {
		r.raftMu.Unlock()
		return roachpb.NewError(err)
	}

	// Ensure that the range does not require an expiration-based lease. If it
	// does, it will never get closed timestamp updates and the rangefeed will
	// never be able to advance its resolved timestamp.
	if r.requiresExpiringLease() {
		r.raftMu.Unlock()
		return roachpb.NewErrorf("expiration-based leases are incompatible with rangefeeds")
	}

	// Register the stream with a catch-up iterator.
	var catchUpIter engine.SimpleIterator
	if usingCatchupIter {
		innerIter := r.Engine().NewIterator(engine.IterOptions{
			UpperBound: args.Span.EndKey,
			// RangeFeed originally intended to use the time-bound iterator
			// performance optimization. However, they've had correctness issues in
			// the past (#28358, #34819) and no-one has the time for the due-diligence
			// necessary to be confidant in their correctness going forward. Not using
			// them causes the total time spent in RangeFeed catchup on changefeed
			// over tpcc-1000 to go from 40s -> 4853s, which is quite large but still
			// workable. See #35122 for details.
			// MinTimestampHint: args.Timestamp,
		})
		catchUpIter = iteratorWithCloser{
			SimpleIterator: innerIter,
			close:          iterSemRelease,
		}
		// Responsibility for releasing the semaphore now passes to the iterator.
		iterSemRelease = nil
	}
	p := r.registerWithRangefeedRaftMuLocked(
		ctx, rspan, args.Timestamp, catchUpIter, lockedStream, errC,
	)
	r.raftMu.Unlock()

	// When this function returns, attempt to clean up the rangefeed.
	defer r.maybeDisconnectEmptyRangefeed(p)

	// Block on the registration's error channel. Note that the registration
	// observes stream.Context().Done.
	return <-errC
}

func (r *Replica) getRangefeedProcessor() *rangefeed.Processor {
	r.rangefeedMu.RLock()
	defer r.rangefeedMu.RUnlock()
	return r.rangefeedMu.proc
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
	r.store.removeReplicaWithRangefeed(r.RangeID)
}

func (r *Replica) unsetRangefeedProcessor(p *rangefeed.Processor) {
	r.rangefeedMu.Lock()
	defer r.rangefeedMu.Unlock()
	r.unsetRangefeedProcessorLocked(p)
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

// registerWithRangefeedRaftMuLocked sets up a Rangefeed registration over the
// provided span. It initializes a rangefeed for the Replica if one is not
// already running. Requires raftMu be locked.
func (r *Replica) registerWithRangefeedRaftMuLocked(
	ctx context.Context,
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	catchupIter engine.SimpleIterator,
	stream rangefeed.Stream,
	errC chan<- *roachpb.Error,
) *rangefeed.Processor {
	// Attempt to register with an existing Rangefeed processor, if one exists.
	// The locking here is a little tricky because we need to handle the case
	// of concurrent processor shutdowns (see maybeDisconnectEmptyRangefeed).
	r.rangefeedMu.RLock()
	p := r.rangefeedMu.proc
	if p != nil {
		reg := p.Register(span, startTS, catchupIter, stream, errC)
		r.rangefeedMu.RUnlock()
		if reg {
			// Registered successfully with an existing processor.
			return p
		}
		// If the registration failed, the processor was already being shut
		// down. Help unset it and then continue on with initializing a new
		// processor.
		r.unsetRangefeedProcessor(p)
		p = nil
	} else {
		r.rangefeedMu.RUnlock()
	}

	// Create a new rangefeed.
	desc := r.Desc()
	tp := rangefeedTxnPusher{ir: r.store.intentResolver, r: r}
	cfg := rangefeed.Config{
		AmbientContext:   r.AmbientContext,
		Clock:            r.Clock(),
		Span:             desc.RSpan(),
		TxnPusher:        &tp,
		EventChanCap:     defaultEventChanCap,
		EventChanTimeout: 50 * time.Millisecond,
		Metrics:          r.store.metrics.RangeFeedMetrics,
	}
	p = rangefeed.NewProcessor(cfg)

	// Start it with an iterator to initialize the resolved timestamp.
	rtsIter := r.Engine().NewIterator(engine.IterOptions{
		UpperBound: desc.EndKey.AsRawKey(),
		// TODO(nvanbenschoten): To facilitate fast restarts of rangefeed
		// we should periodically persist the resolved timestamp so that we
		// can initialize the rangefeed using an iterator that only needs to
		// observe timestamps back to the last recorded resolved timestamp.
		// This is safe because we know that there are no unresolved intents
		// at times before a resolved timestamp.
		// MinTimestampHint: r.ResolvedTimestamp,
	})
	p.Start(r.store.Stopper(), rtsIter)

	// Register with the processor *before* we attach its reference to the
	// Replica struct. This ensures that the registration is in place before
	// any other goroutines are able to stop the processor. In other words,
	// this ensures that the only time the registration fails is during
	// server shutdown.
	reg := p.Register(span, startTS, catchupIter, stream, errC)
	if !reg {
		catchupIter.Close() // clean up
		select {
		case <-r.store.Stopper().ShouldQuiesce():
			errC <- roachpb.NewError(&roachpb.NodeUnavailableError{})
			return nil
		default:
			panic("unexpected Stopped processor")
		}
	}

	// Set the rangefeed reference. We know that no other registration
	// process could have raced with ours because calling this method
	// requires raftMu to be exclusively locked.
	r.setRangefeedProcessor(p)

	// Check for an initial closed timestamp update immediately to help
	// initialize the rangefeed's resolved timestamp as soon as possible.
	r.handleClosedTimestampUpdateRaftMuLocked(ctx)

	return p
}

// maybeDisconnectEmptyRangefeed tears down the provided Processor if it is
// still active and if it no longer has any registrations.
func (r *Replica) maybeDisconnectEmptyRangefeed(p *rangefeed.Processor) {
	if p == nil || p != r.getRangefeedProcessor() {
		// The processor has already been removed or replaced.
		return
	}
	if p.Len() == 0 {
		r.rangefeedMu.Lock()
		defer r.rangefeedMu.Unlock()
		// Check length again under lock to ensure that we're not shutting down
		// a rangefeed processor that has new registrations. Registration on an
		// existing rangefeed processor takes place under read lock.
		if p.Len() == 0 {
			p.Stop()
			r.unsetRangefeedProcessorLocked(p)
		}
	}
}

// disconnectRangefeedWithErr broadcasts the provided error to all rangefeed
// registrations and tears down the provided rangefeed Processor.
func (r *Replica) disconnectRangefeedWithErr(p *rangefeed.Processor, pErr *roachpb.Error) {
	p.StopWithErr(pErr)
	r.unsetRangefeedProcessor(p)
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

// handleLogicalOpLogRaftMuLocked passes the logical op log to the active
// rangefeed, if one is running. The method accepts a reader, which is used to
// look up the values associated with key-value writes in the log before handing
// them to the rangefeed processor. No-op if a rangefeed is not active. Requires
// raftMu to be locked.
func (r *Replica) handleLogicalOpLogRaftMuLocked(
	ctx context.Context, ops *storagepb.LogicalOpLog, reader engine.Reader,
) {
	p := r.getRangefeedProcessor()
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
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		// Read the value directly from the Reader. This is performed in the
		// same raftMu critical section that the logical op's corresponding
		// WriteBatch is applied, so the value should exist.
		val, _, err := engine.MVCCGet(ctx, reader, key, ts, engine.MVCCGetOptions{Tombstones: true})
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
	if !p.ConsumeLogicalOps(ops.Ops...) {
		// Consumption failed and the rangefeed was stopped.
		r.unsetRangefeedProcessor(p)
	}
}

// handleClosedTimestampUpdate determines the current maximum closed timestamp
// for the replica and informs the rangefeed, if one is running. No-op if a
// rangefeed is not active.
func (r *Replica) handleClosedTimestampUpdate(ctx context.Context) {
	ctx = r.AnnotateCtx(ctx)
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.handleClosedTimestampUpdateRaftMuLocked(ctx)
}

// handleClosedTimestampUpdateRaftMuLocked is like handleClosedTimestampUpdate,
// but it requires raftMu to be locked.
func (r *Replica) handleClosedTimestampUpdateRaftMuLocked(ctx context.Context) {
	p := r.getRangefeedProcessor()
	if p == nil {
		return
	}

	// Determine what the maximum closed timestamp is for this replica.
	closedTS := r.maxClosed(ctx)

	// If the closed timestamp is sufficiently stale, signal that we want an
	// update to the leaseholder so that it will eventually begin to progress
	// again.
	slowClosedTSThresh := 5 * closedts.TargetDuration.Get(&r.store.cfg.Settings.SV)
	if d := timeutil.Since(closedTS.GoTime()); d > slowClosedTSThresh {
		m := r.store.metrics.RangeFeedMetrics
		if m.RangeFeedSlowClosedTimestampLogN.ShouldLog() {
			if closedTS.IsEmpty() {
				log.Infof(ctx, "RangeFeed closed timestamp is empty")
			} else {
				log.Infof(ctx, "RangeFeed closed timestamp %s is behind by %s", closedTS, d)
			}
		}

		// Asynchronously attempt to nudge the closed timestamp in case it's stuck.
		key := fmt.Sprintf(`rangefeed-slow-closed-timestamp-nudge-r%d`, r.RangeID)
		// Ignore the result of DoChan since, to keep this all async, it always
		// returns nil and any errors are logged by the closure passed to the
		// `DoChan` call.
		_, _ = m.RangeFeedSlowClosedTimestampNudge.DoChan(key, func() (interface{}, error) {
			// Also ignore the result of RunTask, since it only returns errors when
			// the task didn't start because we're shutting down.
			_ = r.store.stopper.RunTask(ctx, key, func(context.Context) {
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
	}

	// If the closed timestamp is not empty, inform the Processor.
	if closedTS.IsEmpty() {
		return
	}
	if !p.ForwardClosedTS(closedTS) {
		// Consumption failed and the rangefeed was stopped.
		r.unsetRangefeedProcessor(p)
	}
}

// ensureClosedTimestampStarted does its best to make sure that this node is
// receiving closed timestamp updated for this replica's range. Note that this
// forces a valid lease to exist on the range and so can be reasonably expensive
// if there is not already a valid lease.
func (r *Replica) ensureClosedTimestampStarted(ctx context.Context) *roachpb.Error {
	// Make sure there's a leaseholder. If there's no leaseholder, there's no
	// closed timestamp updates.
	var leaseholderNodeID roachpb.NodeID
	_, err := r.redirectOnOrAcquireLease(ctx)
	if err == nil {
		// We have the lease. Request is essentially a wrapper for calling EmitMLAI
		// on a remote node, so cut out the middleman.
		r.EmitMLAI()
		return nil
	} else if lErr, ok := err.GetDetail().(*roachpb.NotLeaseHolderError); ok {
		if lErr.LeaseHolder == nil {
			// It's possible for redirectOnOrAcquireLease to return
			// NotLeaseHolderErrors with LeaseHolder unset, but these should be
			// transient conditions. If this method is being called by RangeFeed to
			// nudge a stuck closedts, then essentially all we can do here is nothing
			// and assume that redirectOnOrAcquireLease will do something different
			// the next time it's called.
			return nil
		}
		leaseholderNodeID = lErr.LeaseHolder.NodeID
	} else {
		return err
	}
	// Request fixes any issues where we've missed a closed timestamp update or
	// where we're not connected to receive them from this node in the first
	// place.
	r.store.cfg.ClosedTimestamp.Clients.Request(leaseholderNodeID, r.RangeID)
	return nil
}
