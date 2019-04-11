// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/storage/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	args *roachpb.RangeFeedRequest,
	stream roachpb.Internal_RangeFeedServer,
	iteratorLimiter limit.ConcurrentRequestLimiter,
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

	// Make sure there's a leaseholder, otherwise nothing will close timestamps
	// for this range, which means we won't make checkpoints.
	if _, err := r.redirectOnOrAcquireLease(ctx); err != nil {
		// We don't care if this replica is the leaseholder, only that someone is,
		// so ignore NotLeaseHolderError.
		if _, ok := err.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
			return err
		}
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
		if err := iteratorLimiter.Begin(ctx); err != nil {
			return roachpb.NewError(err)
		}
		// Finish the iterator limit, but only if we exit before
		// creating the iterator itself.
		iterSemRelease = iteratorLimiter.Finish
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
		rspan, args.Timestamp, catchUpIter, lockedStream, errC,
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
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	catchupIter engine.SimpleIterator,
	stream rangefeed.Stream,
	errC chan<- *roachpb.Error,
) *rangefeed.Processor {
	// Attempt to register with an existing Rangefeed processor, if one exists.
	// The locking here is a little tricky because we need to handle the case
	// of concurrent processor shutdowns.
	p := r.getRangefeedProcessor()
	if p != nil {
		if p.Register(span, startTS, catchupIter, stream, errC) {
			// Registered successfully with an existing processor.
			return p
		}
		// If the registration failed, the processor was already being shut
		// down. Help unset it and then continue on with initializing a new
		// processor.
		r.unsetRangefeedProcessor(p)
		p = nil
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
	// this ensures that the the registration never fails.
	reg := p.Register(span, startTS, catchupIter, stream, errC)
	if !reg {
		panic("unexpected Stopped processor")
	}

	// Set the rangefeed reference. We know that no other registration
	// process could have raced with ours because calling this method
	// requires raftMu to be exclusively locked.
	r.setRangefeedProcessor(p)

	// Check for an initial closed timestamp update immediately to help
	// initialize the rangefeed's resolved timestamp as soon as possible.
	r.handleClosedTimestampUpdateRaftMuLocked()

	return p
}

// maybeDisconnectEmptyRangefeed tears down the provided Processor if it is
// still active and if it no longer has any registrations.
func (r *Replica) maybeDisconnectEmptyRangefeed(p *rangefeed.Processor) {
	if p != r.getRangefeedProcessor() {
		// The processor has already been removed or replaced.
		return
	}
	if p.Len() == 0 {
		r.rangefeedMu.Lock()
		defer r.rangefeedMu.Unlock()
		// Check length again under lock to ensure that we're not shutting down
		// a rangefeed processor that has new registrations.
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
// rangefeed, if one is running. No-op if a rangefeed is not active. Requires
// raftMu to be locked.
func (r *Replica) handleLogicalOpLogRaftMuLocked(ctx context.Context, ops *storagepb.LogicalOpLog) {
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
		r.disconnectRangefeedWithErr(p, roachpb.NewError(roachpb.NewRangeFeedRetryError(
			roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING,
		)))
		return
	}
	if len(ops.Ops) == 0 {
		return
	}

	// When reading straight from the Raft log, some logical ops will not be
	// fully populated. Read from the engine (under raftMu) to populate all
	// fields.
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

		// Read the value directly from the Engine. This is performed in the
		// same raftMu critical section that the logical op's corresponding
		// WriteBatch is applied, so the value should exist.
		val, _, err := engine.MVCCGet(ctx, r.Engine(), key, ts, engine.MVCCGetOptions{Tombstones: true})
		if val == nil && err == nil {
			err = errors.New("value missing in engine")
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
func (r *Replica) handleClosedTimestampUpdate() {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.handleClosedTimestampUpdateRaftMuLocked()
}

// handleClosedTimestampUpdateRaftMuLocked is like handleClosedTimestampUpdate,
// but it requires raftMu to be locked.
func (r *Replica) handleClosedTimestampUpdateRaftMuLocked() {
	p := r.getRangefeedProcessor()
	if p == nil {
		return
	}

	// Determine what the maximum closed timestamp is for this replica.
	closedTS := r.maxClosed(context.Background())

	// If the closed timestamp is not empty, inform the Processor.
	if closedTS.IsEmpty() {
		return
	}
	if !p.ForwardClosedTS(closedTS) {
		// Consumption failed and the rangefeed was stopped.
		r.unsetRangefeedProcessor(p)
	}
}
