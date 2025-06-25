// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/multiqueue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

const (
	// Messages that provide detail about why a snapshot was rejected.
	storeDrainingMsg = "store is draining"

	// IntersectingSnapshotMsg is part of the error message returned from
	// canAcceptSnapshotLocked and is exposed here so testing can rely on it.
	IntersectingSnapshotMsg = "snapshot intersects existing range"

	// tagSnapshotTiming is the tracing span tag that the *snapshotTimingTag
	// lives under.
	tagSnapshotTiming = "snapshot_timing_tag"
)

// snapshotMetrics contains metrics on the number and size of snapshots in
// progress or in the snapshot queue.
type snapshotMetrics struct {
	QueueLen        *metric.Gauge
	QueueSize       *metric.Gauge
	InProgress      *metric.Gauge
	TotalInProgress *metric.Gauge
}

// incomingSnapshotStream is the minimal interface on a GRPC stream required
// to receive a snapshot over the network.
type incomingSnapshotStream interface {
	Send(*kvserverpb.SnapshotResponse) error
	Recv() (*kvserverpb.SnapshotRequest, error)
}

// outgoingSnapshotStream is the minimal interface on a GRPC stream required
// to send a snapshot over the network.
type outgoingSnapshotStream interface {
	Send(*kvserverpb.SnapshotRequest) error
	Recv() (*kvserverpb.SnapshotResponse, error)
}

// snapshotRecordMetrics is a wrapper function that increments a set of metrics
// related to the number of snapshot bytes sent/received. The definer of the
// function specifies which metrics are incremented.
type snapshotRecordMetrics func(inc int64)

// snapshotTimingTag represents a lazy tracing span tag containing information
// on how long individual parts of a snapshot take. Individual stopwatches can
// be added to a snapshotTimingTag.
type snapshotTimingTag struct {
	mu struct {
		syncutil.Mutex
		stopwatches map[string]*timeutil.StopWatch
	}
}

// newSnapshotTimingTag creates a new snapshotTimingTag.
func newSnapshotTimingTag() *snapshotTimingTag {
	tag := snapshotTimingTag{}
	tag.mu.stopwatches = make(map[string]*timeutil.StopWatch)
	return &tag
}

// addStopwatch adds the given stopwatch to the tag's map of stopwatches.
func (tag *snapshotTimingTag) addStopwatch(name string) {
	tag.mu.Lock()
	defer tag.mu.Unlock()
	tag.mu.stopwatches[name] = timeutil.NewStopWatch()
}

// start begins the stopwatch corresponding to name if the stopwatch
// exists and shouldRecord is true.
func (tag *snapshotTimingTag) start(name string) {
	tag.mu.Lock()
	defer tag.mu.Unlock()
	if stopwatch, ok := tag.mu.stopwatches[name]; ok {
		stopwatch.Start()
	}
}

// stop ends the stopwatch corresponding to name if the stopwatch
// exists and shouldRecord is true.
func (tag *snapshotTimingTag) stop(name string) {
	tag.mu.Lock()
	defer tag.mu.Unlock()
	if stopwatch, ok := tag.mu.stopwatches[name]; ok {
		stopwatch.Stop()
	}
}

// Render implements the tracing.LazyTag interface. It returns a map of each
// stopwatch's name to the stopwatch's elapsed time.
func (tag *snapshotTimingTag) Render() []attribute.KeyValue {
	tag.mu.Lock()
	defer tag.mu.Unlock()
	tags := make([]attribute.KeyValue, 0, len(tag.mu.stopwatches))
	for name, stopwatch := range tag.mu.stopwatches {
		tags = append(tags, attribute.KeyValue{
			Key:   attribute.Key(name),
			Value: attribute.StringValue(string(humanizeutil.Duration(stopwatch.Elapsed()))),
		})
	}
	return tags
}

// stubBackingHandle is a stub implementation of RemoteObjectBackingHandle
// that just wraps a RemoteObjectBacking. This is used by a snapshot receiver as
// it is on a different node than the one that created the original
// RemoteObjectBackingHandle, so the Close() function is a no-op.
type stubBackingHandle struct {
	backing objstorage.RemoteObjectBacking
}

// Get implements the RemoteObjectBackingHandle interface.
func (s stubBackingHandle) Get() (objstorage.RemoteObjectBacking, error) {
	return s.backing, nil
}

// Close implements the RemoteObjectBackingHandle interface.
func (s stubBackingHandle) Close() {
	// No-op.
}

var _ objstorage.RemoteObjectBackingHandle = &stubBackingHandle{}

// reserveReceiveSnapshot reserves space for this snapshot which will attempt to
// prevent overload of system resources as this snapshot is being sent.
// Snapshots are often sent in bulk (due to operations like store decommission)
// so it is necessary to prevent snapshot transfers from overly impacting
// foreground traffic.
func (s *Store) reserveReceiveSnapshot(
	ctx context.Context, header *kvserverpb.SnapshotRequest_Header,
) (_cleanup func(), _err error) {
	ctx, sp := tracing.EnsureChildSpan(ctx, s.cfg.Tracer(), "reserveReceiveSnapshot")
	defer sp.Finish()

	return s.throttleSnapshot(ctx,
		s.snapshotApplyQueue,
		int(header.SenderQueueName),
		header.SenderQueuePriority,
		-1,
		header.RangeSize,
		header.RaftMessageRequest.RangeID,
		snapshotMetrics{
			s.metrics.RangeSnapshotRecvQueueLength,
			s.metrics.RangeSnapshotRecvQueueSize,
			s.metrics.RangeSnapshotRecvInProgress,
			s.metrics.RangeSnapshotRecvTotalInProgress,
		},
	)
}

// reserveSendSnapshot throttles outgoing snapshots.
func (s *Store) reserveSendSnapshot(
	ctx context.Context, req *kvserverpb.DelegateSendSnapshotRequest, rangeSize int64,
) (_cleanup func(), _err error) {
	ctx, sp := tracing.EnsureChildSpan(ctx, s.cfg.Tracer(), "reserveSendSnapshot")
	defer sp.Finish()
	if fn := s.cfg.TestingKnobs.BeforeSendSnapshotThrottle; fn != nil {
		fn()
	}

	return s.throttleSnapshot(ctx,
		s.snapshotSendQueue,
		int(req.SenderQueueName),
		req.SenderQueuePriority,
		req.QueueOnDelegateLen,
		rangeSize,
		req.RangeID,
		snapshotMetrics{
			s.metrics.RangeSnapshotSendQueueLength,
			s.metrics.RangeSnapshotSendQueueSize,
			s.metrics.RangeSnapshotSendInProgress,
			s.metrics.RangeSnapshotSendTotalInProgress,
		},
	)
}

// throttleSnapshot is a helper function to throttle snapshot sending and
// receiving. The returned closure is used to cleanup the reservation and
// release its resources.
func (s *Store) throttleSnapshot(
	ctx context.Context,
	snapshotQueue *multiqueue.MultiQueue,
	requestSource int,
	requestPriority float64,
	maxQueueLength int64,
	rangeSize int64,
	rangeID roachpb.RangeID,
	snapshotMetrics snapshotMetrics,
) (cleanup func(), funcErr error) {

	tBegin := timeutil.Now()
	var permit *multiqueue.Permit
	// Empty snapshots are exempt from rate limits because they're so cheap to
	// apply. This vastly speeds up rebalancing any empty ranges created by a
	// RESTORE or manual SPLIT AT, since it prevents these empty snapshots from
	// getting stuck behind large snapshots managed by the replicate queue.
	if rangeSize != 0 || s.cfg.TestingKnobs.ThrottleEmptySnapshots {
		task, err := snapshotQueue.Add(requestSource, requestPriority, maxQueueLength)
		if err != nil {
			return nil, err
		}
		// After this point, the task is on the queue, so any future errors need to
		// be handled by cancelling the task to release the permit.
		defer func() {
			if funcErr != nil {
				snapshotQueue.Cancel(task)
			}
		}()

		// Total bytes of snapshots waiting in the snapshot queue
		snapshotMetrics.QueueSize.Inc(rangeSize)
		defer snapshotMetrics.QueueSize.Dec(rangeSize)
		// Total number of snapshots waiting in the snapshot queue
		snapshotMetrics.QueueLen.Inc(1)
		defer snapshotMetrics.QueueLen.Dec(1)

		queueCtx := ctx
		if deadline, ok := queueCtx.Deadline(); ok {
			// Enforce a more strict timeout for acquiring the snapshot reservation to
			// ensure that if the reservation is acquired, the snapshot has sufficient
			// time to complete. See the comment on snapshotReservationQueueTimeoutFraction
			// and TestReserveSnapshotQueueTimeout.
			timeoutFrac := snapshotReservationQueueTimeoutFraction.Get(&s.ClusterSettings().SV)
			timeout := time.Duration(timeoutFrac * float64(timeutil.Until(deadline)))
			var cancel func()
			queueCtx, cancel = context.WithTimeout(queueCtx, timeout) // nolint:context
			defer cancel()
		}
		select {
		case permit = <-task.GetWaitChan():
			// Got a spot in the snapshotQueue, continue with sending the snapshot.
			if fn := s.cfg.TestingKnobs.AfterSnapshotThrottle; fn != nil {
				fn()
			}
			log.Event(ctx, "acquired spot in the snapshot snapshotQueue")
		case <-queueCtx.Done():
			// We need to cancel the task so that it doesn't ever get a permit.
			if err := ctx.Err(); err != nil {
				return nil, errors.Wrap(err, "acquiring snapshot reservation")
			}
			return nil, errors.Wrapf(
				queueCtx.Err(),
				"giving up during snapshot reservation due to cluster setting %q",
				snapshotReservationQueueTimeoutFraction.Name(),
			)
		case <-s.stopper.ShouldQuiesce():
			return nil, errors.Errorf("stopped")
		}

		// Counts non-empty in-progress snapshots.
		snapshotMetrics.InProgress.Inc(1)
	}
	// Counts all in-progress snapshots.
	snapshotMetrics.TotalInProgress.Inc(1)

	// The choice here is essentially arbitrary, but with a default range size of 128mb-512mb and the
	// Raft snapshot rate limiting of 32mb/s, we expect to spend less than 16s per snapshot.
	// which is what we want to log.
	const snapshotReservationWaitWarnThreshold = 32 * time.Second
	elapsed := timeutil.Since(tBegin)
	// NB: this log message is skipped in test builds as many tests do not mock
	// all of the objects being logged.
	if elapsed > snapshotReservationWaitWarnThreshold && !buildutil.CrdbTestBuild {
		log.Infof(
			ctx,
			"waited for %.1fs to acquire snapshot reservation to r%d",
			elapsed.Seconds(),
			rangeID,
		)
	}

	s.metrics.ReservedReplicaCount.Inc(1)
	s.metrics.Reserved.Inc(rangeSize)
	return func() {
		s.metrics.ReservedReplicaCount.Dec(1)
		s.metrics.Reserved.Dec(rangeSize)
		snapshotMetrics.TotalInProgress.Dec(1)

		if rangeSize != 0 || s.cfg.TestingKnobs.ThrottleEmptySnapshots {
			snapshotMetrics.InProgress.Dec(1)
			snapshotQueue.Release(permit)
		}
	}, nil
}

// canAcceptSnapshotLocked returns (_, nil) if the snapshot can be applied to
// this store's replica (i.e. the snapshot is not from an older incarnation of
// the replica) and a placeholder that can be (but is not yet) added to the
// replicasByKey map (if necessary).
//
// Both the store mu and the raft mu for the existing replica (which must exist)
// must be held.
func (s *Store) canAcceptSnapshotLocked(
	ctx context.Context, snapHeader *kvserverpb.SnapshotRequest_Header,
) (*ReplicaPlaceholder, error) {
	// TODO(tbg): see the comment on desc.Generation for what seems to be a much
	// saner way to handle overlap via generational semantics.
	desc := *snapHeader.State.Desc

	// First, check for an existing Replica.
	existingRepl, ok := s.mu.replicasByRangeID.Load(desc.RangeID)
	if !ok {
		return nil, errors.Errorf("canAcceptSnapshotLocked requires a replica present")
	}
	// The raftMu is held which allows us to use the existing replica as a
	// placeholder when we decide that the snapshot can be applied. As long as the
	// caller releases the raftMu only after feeding the snapshot into the
	// replica, this is safe. This is true even when the snapshot spans a merge,
	// because we will be guaranteed to have the subsumed (initialized) Replicas
	// in place as well. This is because they are present when the merge first
	// commits, and cannot have been replicaGC'ed yet (see replicaGCQueue.process).
	existingRepl.raftMu.AssertHeld()

	existingRepl.mu.RLock()
	existingDesc := existingRepl.shMu.state.Desc
	existingIsInitialized := existingDesc.IsInitialized()
	existingDestroyStatus := existingRepl.mu.destroyStatus
	existingRepl.mu.RUnlock()

	if existingIsInitialized {
		// Regular Raft snapshots can't be refused at this point,
		// even if they widen the existing replica. See the comments
		// in Replica.maybeAcquireSnapshotMergeLock for how this is
		// made safe.
		//
		// NB: The snapshot must be intended for this replica as
		// withReplicaForRequest ensures that requests with a non-zero replica
		// id are passed to a replica with a matching id.
		return nil, nil
	}

	// If we are not alive then we should not apply a snapshot as our removal
	// is imminent.
	if existingDestroyStatus.Removed() {
		return nil, existingDestroyStatus.err
	}

	// We have a key span [desc.StartKey,desc.EndKey) which we want to apply a
	// snapshot for. Is there a conflicting existing placeholder or an
	// overlapping range?
	if err := s.checkSnapshotOverlapLocked(ctx, snapHeader); err != nil {
		return nil, err
	}

	placeholder := &ReplicaPlaceholder{
		rangeDesc: desc,
	}
	return placeholder, nil
}

// checkSnapshotOverlapLocked returns an error if the snapshot overlaps an
// existing replica or placeholder. Any replicas that do overlap have a good
// chance of being abandoned, so they're proactively handed to the replica GC
// queue.
func (s *Store) checkSnapshotOverlapLocked(
	ctx context.Context, snapHeader *kvserverpb.SnapshotRequest_Header,
) error {
	desc := *snapHeader.State.Desc

	// NB: this check seems redundant since placeholders are also represented in
	// replicasByKey (and thus returned in getOverlappingKeyRangeLocked).
	if exRng, ok := s.mu.replicaPlaceholders[desc.RangeID]; ok {
		return errors.Mark(errors.Errorf(
			"%s: canAcceptSnapshotLocked: cannot add placeholder, have an existing placeholder %s %v",
			s, exRng, snapHeader.RaftMessageRequest.FromReplica),
			errMarkSnapshotError)
	}

	// TODO(benesch): consider discovering and GC'ing *all* overlapping ranges,
	// not just the first one that getOverlappingKeyRangeLocked happens to return.
	if it := s.getOverlappingKeyRangeLocked(&desc); !it.isEmpty() {
		// We have a conflicting range, so we must block the snapshot.
		// When such a conflict exists, it will be resolved by one range
		// either being split or garbage collected.
		exReplica, err := s.GetReplica(it.Desc().RangeID)
		msg := IntersectingSnapshotMsg
		if err != nil {
			log.Warningf(ctx, "unable to look up overlapping replica on %s: %v", exReplica, err)
		} else {
			inactive := func(r *Replica) bool {
				if r.RaftStatus() == nil {
					return true
				}
				// TODO(benesch): this check does not detect inactivity on
				// replicas with epoch-based leases. Since the validity of an
				// epoch-based lease is tied to the owning node's liveness, the
				// lease can be valid well after the leader of the range has cut
				// off communication with this replica. Expiration based leases,
				// by contrast, will expire quickly if the leader of the range
				// stops sending this replica heartbeats.
				return !r.CurrentLeaseStatus(ctx).IsValid()
			}
			// We unconditionally send this replica through the replica GC queue. It's
			// reasonably likely that the replica GC queue will do nothing because the
			// replica needs to split instead, but better to err on the side of
			// queueing too frequently. Blocking Raft snapshots for too long can wedge
			// a cluster, and if the replica does need to be GC'd, this might be the
			// only code path that notices in a timely fashion.
			//
			// We're careful to avoid starving out other replicas in the replica GC
			// queue by queueing at a low priority unless we can prove that the range
			// is inactive and thus unlikely to be about to process a split.
			gcPriority := replicaGCPriorityDefault
			if inactive(exReplica) {
				gcPriority = replicaGCPrioritySuspect
			}

			msg += "; initiated GC:"
			s.replicaGCQueue.AddAsync(ctx, exReplica, gcPriority)
		}
		return errors.Mark(
			errors.Errorf("%s %v (incoming %v)", msg, exReplica, snapHeader.State.Desc.RSpan()), // exReplica can be nil
			errMarkSnapshotError,
		)
	}
	return nil
}

// getLocalityComparison takes two nodeIDs as input and returns the locality
// comparison result between their corresponding nodes. This result indicates
// whether the two nodes are located in different regions or zones.
func (s *Store) getLocalityComparison(
	fromNodeID roachpb.NodeID, toNodeID roachpb.NodeID,
) roachpb.LocalityComparisonType {
	firstLocality := s.cfg.StorePool.GetNodeLocality(fromNodeID)
	secLocality := s.cfg.StorePool.GetNodeLocality(toNodeID)
	return firstLocality.Compare(secLocality)
}

// receiveSnapshot receives an incoming snapshot via a pre-opened GRPC stream.
func (s *Store) receiveSnapshot(
	ctx context.Context, header *kvserverpb.SnapshotRequest_Header, stream incomingSnapshotStream,
) error {
	// Draining nodes will generally not be rebalanced to (see the filtering that
	// happens in getStoreListFromIDs()), but in case they are, they should
	// reject the incoming rebalancing snapshots.
	if s.IsDraining() {
		switch t := header.SenderQueueName; t {
		case kvserverpb.SnapshotRequest_RAFT_SNAPSHOT_QUEUE:
			// We can not reject Raft snapshots because draining nodes may have
			// replicas in `StateSnapshot` that need to catch up.
		case kvserverpb.SnapshotRequest_REPLICATE_QUEUE:
			// Only reject if these are "rebalance" snapshots, not "recovery"
			// snapshots. We use the priority 0 to differentiate the types.
			if header.SenderQueuePriority == 0 {
				return sendSnapshotError(ctx, s, stream, errors.New(storeDrainingMsg))
			}
		case kvserverpb.SnapshotRequest_OTHER:
			return sendSnapshotError(ctx, s, stream, errors.New(storeDrainingMsg))
		default:
			// If this a new snapshot type that this cockroach version does not know
			// about, we let it through.
		}
	}

	if fn := s.cfg.TestingKnobs.ReceiveSnapshot; fn != nil {
		if err := fn(ctx, header); err != nil {
			// NB: we intentionally don't mark this error as errMarkSnapshotError so
			// that we don't end up retrying injected errors in tests.
			return sendSnapshotError(ctx, s, stream, err)
		}
	}

	// Defensive check that any snapshot contains this store in the	descriptor.
	storeID := s.StoreID()
	if _, ok := header.State.Desc.GetReplicaDescriptor(storeID); !ok {
		return errors.AssertionFailedf(
			`snapshot from queue %s was sent to s%d which did not contain it as a replica: %s`,
			header.SenderQueueName, storeID, header.State.Desc.Replicas())
	}

	cleanup, err := s.reserveReceiveSnapshot(ctx, header)
	if err != nil {
		return err
	}
	defer cleanup()

	// The comment on ReplicaPlaceholder motivates and documents
	// ReplicaPlaceholder semantics. Please be familiar with them
	// before making any changes.
	var placeholder *ReplicaPlaceholder
	if pErr := s.withReplicaForRequest(
		ctx, &header.RaftMessageRequest, func(ctx context.Context, r *Replica,
		) *kvpb.Error {
			var err error
			s.mu.Lock()
			defer s.mu.Unlock()
			placeholder, err = s.canAcceptSnapshotLocked(ctx, header)
			if err != nil {
				return kvpb.NewError(err)
			}
			if placeholder != nil {
				if err := s.addPlaceholderLocked(placeholder); err != nil {
					return kvpb.NewError(err)
				}
			}
			return nil
		}); pErr != nil {
		log.Infof(ctx, "cannot accept snapshot: %s", pErr)
		return sendSnapshotError(ctx, s, stream, pErr.GoError())
	}

	defer func() {
		if placeholder != nil {
			// Remove the placeholder, if it's still there. Most of the time it will
			// have been filled and this is a no-op.
			if _, err := s.removePlaceholder(ctx, placeholder, removePlaceholderFailed); err != nil {
				log.Fatalf(ctx, "unable to remove placeholder: %s", err)
			}
		}
	}()

	snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
	if err != nil {
		err = errors.Wrap(err, "invalid snapshot")
		return sendSnapshotError(ctx, s, stream, err)
	}

	ss := &kvBatchSnapshotStrategy{
		scratch:      s.sstSnapshotStorage.NewScratchSpace(header.State.Desc.RangeID, snapUUID),
		sstChunkSize: snapshotSSTWriteSyncRate.Get(&s.cfg.Settings.SV),
		st:           s.ClusterSettings(),
		clusterID:    s.ClusterID(),
	}
	defer ss.Close(ctx)

	if err := stream.Send(&kvserverpb.SnapshotResponse{Status: kvserverpb.SnapshotResponse_ACCEPTED}); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "accepted snapshot reservation for r%d", header.State.Desc.RangeID)
	}

	comparisonResult := s.getLocalityComparison(header.RaftMessageRequest.FromReplica.NodeID,
		header.RaftMessageRequest.ToReplica.NodeID)

	recordBytesReceived := func(inc int64) {
		s.metrics.RangeSnapshotRcvdBytes.Inc(inc)
		s.metrics.updateCrossLocalityMetricsOnSnapshotRcvd(comparisonResult, inc)

		// This logic for metrics should match what is in replica_command.
		if header.SenderQueueName == kvserverpb.SnapshotRequest_RAFT_SNAPSHOT_QUEUE {
			s.metrics.RangeSnapshotRecoveryRcvdBytes.Inc(inc)
		} else if header.SenderQueueName == kvserverpb.SnapshotRequest_OTHER {
			s.metrics.RangeSnapshotRebalancingRcvdBytes.Inc(inc)
		} else {
			// Replicate queue does both types, so split based on priority.
			// See AllocatorAction.Priority
			if header.SenderQueuePriority > 0 {
				s.metrics.RangeSnapshotUpreplicationRcvdBytes.Inc(inc)
			} else {
				s.metrics.RangeSnapshotRebalancingRcvdBytes.Inc(inc)
			}
		}
	}
	inSnap, err := ss.Receive(ctx, s, stream, *header, recordBytesReceived)
	if err != nil {
		return err
	}
	inSnap.placeholder = placeholder

	// Use a background context for applying the snapshot, as handleRaftReady is
	// not prepared to deal with arbitrary context cancellation. Also, we've
	// already received the entire snapshot here, so there's no point in
	// abandoning application half-way through if the caller goes away.
	applyCtx := s.AnnotateCtx(context.Background())
	msgAppResp, pErr := s.processRaftSnapshotRequest(applyCtx, header, inSnap)
	if pErr != nil {
		err := pErr.GoError()
		// We mark this error as a snapshot error which will be interpreted by the
		// sender as this being a retriable error, see isSnapshotError().
		err = errors.Mark(err, errMarkSnapshotError)
		err = errors.Wrap(err, "failed to apply snapshot")
		return sendSnapshotError(ctx, s, stream, err)
	}
	return stream.Send(&kvserverpb.SnapshotResponse{
		Status:         kvserverpb.SnapshotResponse_APPLIED,
		CollectedSpans: tracing.SpanFromContext(ctx).GetConfiguredRecording(),
		MsgAppResp:     msgAppResp,
	})
}

// sendSnapshotError sends an error response back to the sender of this snapshot
// to signify that it can not accept this snapshot. Internally it increments the
// statistic tracking how many invalid snapshots it received.
func sendSnapshotError(
	ctx context.Context, s *Store, stream incomingSnapshotStream, err error,
) error {
	s.metrics.RangeSnapshotRecvFailed.Inc(1)
	resp := snapRespErr(err)
	resp.CollectedSpans = tracing.SpanFromContext(ctx).GetConfiguredRecording()

	return stream.Send(resp)
}

func snapRespErr(err error) *kvserverpb.SnapshotResponse {
	return &kvserverpb.SnapshotResponse{
		Status:            kvserverpb.SnapshotResponse_ERROR,
		EncodedError:      errors.EncodeError(context.Background(), err),
		DeprecatedMessage: err.Error(),
	}
}

func maybeHandleDeprecatedSnapErr(deprecated bool, err error) error {
	if !deprecated {
		return err
	}
	return errors.Mark(err, errMarkSnapshotError)
}

// SnapshotStorePool narrows StorePool to make sendSnapshotUsingDelegate easier to test.
type SnapshotStorePool interface {
	Throttle(reason storepool.ThrottleReason, why string, toStoreID roachpb.StoreID)
}

// minSnapshotRate defines the minimum value that the rate limit for rebalance
// and recovery snapshots can be configured to. Any value below this lower bound
// is considered unsafe for use, as it can lead to excessively long-running
// snapshots. The sender of Raft snapshots holds resources (e.g. LSM snapshots,
// LSM iterators until #75824 is addressed) and blocks Raft log truncation, so
// it is not safe to let a single snapshot run for an unlimited period of time.
//
// The value was chosen based on a maximum range size of 512mb and a desire to
// prevent a single snapshot for running for more than 10 minutes. With a rate
// limit of 1mb/s, a 512mb snapshot will take just under 9 minutes to send.
const minSnapshotRate = 1 << 20 // 1mb/s

// SendEmptySnapshot creates an OutgoingSnapshot for the input range
// descriptor and seeds it with an empty range. Then, it sends this
// snapshot to the replica specified in the input.
func SendEmptySnapshot(
	ctx context.Context,
	clusterID uuid.UUID,
	st *cluster.Settings,
	tracer *tracing.Tracer,
	mrc RPCMultiRaftClient,
	now hlc.Timestamp,
	desc roachpb.RangeDescriptor,
	to roachpb.ReplicaDescriptor,
) error {
	// Create an engine to use as a buffer for the empty snapshot.
	eng, err := storage.Open(
		context.Background(),
		storage.InMemory(),
		cluster.MakeClusterSettings(),
		storage.CacheSize(1<<20 /* 1 MiB */),
		storage.MaxSizeBytes(512<<20 /* 512 MiB */))
	if err != nil {
		return err
	}
	defer eng.Close()

	var ms enginepb.MVCCStats
	// Seed an empty range into the new engine.
	if err := storage.MVCCPutProto(
		ctx, eng, keys.RangeDescriptorKey(desc.StartKey), now, &desc, storage.MVCCWriteOptions{Stats: &ms},
	); err != nil {
		return err
	}

	ms, err = stateloader.WriteInitialReplicaState(
		ctx,
		eng,
		ms,
		desc,
		roachpb.Lease{},
		hlc.Timestamp{}, // gcThreshold
		roachpb.GCHint{},
		st.Version.ActiveVersionOrEmpty(ctx).Version,
	)
	if err != nil {
		return err
	}

	// Use stateloader to load state out of memory from the previously created engine.
	sl := stateloader.Make(desc.RangeID)
	state, err := sl.Load(ctx, eng, &desc)
	if err != nil {
		return err
	}
	// See comment on DeprecatedUsingAppliedStateKey for why we need to set this
	// explicitly for snapshots going out to followers.
	state.DeprecatedUsingAppliedStateKey = true

	snapUUID := uuid.NewV4()

	// The snapshot must use a Pebble snapshot, since it requires consistent
	// iterators.
	//
	// NB: Using a regular snapshot as opposed to an EventuallyFileOnlySnapshot
	// is alright here as there should be no keys in this span to begin with,
	// and this snapshot should be very short-lived.
	engSnapshot := eng.NewSnapshot()

	// Create an OutgoingSnapshot to send.
	outgoingSnap, err := snapshot(
		ctx,
		snapUUID,
		sl,
		engSnapshot,
		desc.StartKey,
	)
	if err != nil {
		// Close() is not idempotent, and will be done by outgoingSnap.Close() if
		// the snapshot was successfully created.
		engSnapshot.Close()
		return err
	}
	defer outgoingSnap.Close()

	// From and to replica descriptors are the same because we have
	// to send the snapshot from a member of the range descriptor.
	// Sending it from the current replica ensures that. Otherwise,
	// it would be a malformed request if it came from a non-member.
	from := to
	req := kvserverpb.RaftMessageRequest{
		RangeID:     desc.RangeID,
		FromReplica: from,
		ToReplica:   to,
		Message: raftpb.Message{
			Type:     raftpb.MsgSnap,
			To:       raftpb.PeerID(to.ReplicaID),
			From:     raftpb.PeerID(from.ReplicaID),
			Term:     outgoingSnap.RaftSnap.Metadata.Term,
			Snapshot: &outgoingSnap.RaftSnap,
		},
	}

	header := kvserverpb.SnapshotRequest_Header{
		State:              state,
		RaftMessageRequest: req,
		RangeSize:          ms.Total(),
		RangeKeysInOrder:   true,
	}

	stream, err := mrc.RaftSnapshot(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := stream.CloseSend(); err != nil {
			log.Warningf(ctx, "failed to close snapshot stream: %+v", err)
		}
	}()

	if _, err := sendSnapshot(
		ctx,
		clusterID,
		st,
		tracer,
		stream,
		noopStorePool{},
		header,
		&outgoingSnap,
		eng.NewWriteBatch,
		func() {},
		nil, /* recordBytesSent */
	); err != nil {
		return err
	}
	return nil
}

// noopStorePool is a hollowed out StorePool that does not throttle. It's used in recovery scenarios.
type noopStorePool struct{}

func (n noopStorePool) Throttle(storepool.ThrottleReason, string, roachpb.StoreID) {}

// sendSnapshot sends an outgoing snapshot via a pre-opened GRPC stream.
func sendSnapshot(
	ctx context.Context,
	clusterID uuid.UUID,
	st *cluster.Settings,
	tracer *tracing.Tracer,
	stream outgoingSnapshotStream,
	storePool SnapshotStorePool,
	header kvserverpb.SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newWriteBatch func() storage.WriteBatch,
	sent func(),
	recordBytesSent snapshotRecordMetrics,
) (*kvserverpb.SnapshotResponse, error) {
	if recordBytesSent == nil {
		// NB: Some tests and an offline tool (ResetQuorum) call into `sendSnapshotUsingDelegate`
		// with a nil metrics tracking function. We pass in a fake metrics tracking function here that isn't
		// hooked up to anything.
		recordBytesSent = func(inc int64) {}
	}
	ctx, sp := tracing.EnsureChildSpan(ctx, tracer, "sending snapshot")
	defer sp.Finish()

	start := timeutil.Now()
	to := header.RaftMessageRequest.ToReplica
	if err := stream.Send(&kvserverpb.SnapshotRequest{Header: &header}); err != nil {
		return nil, err
	}
	log.Event(ctx, "sent SNAPSHOT_REQUEST message to server")
	// Wait until we get a response from the server. The recipient may queue us
	// (only a limited number of snapshots are allowed concurrently) or flat-out
	// reject the snapshot. After the initial message exchange, we'll go and send
	// the actual snapshot (if not rejected).
	resp, err := stream.Recv()
	if err != nil {
		storePool.Throttle(storepool.ThrottleFailed, err.Error(), to.StoreID)
		return nil, err
	}
	switch resp.Status {
	case kvserverpb.SnapshotResponse_ERROR:
		sp.ImportRemoteRecording(resp.CollectedSpans)
		storePool.Throttle(storepool.ThrottleFailed, resp.DeprecatedMessage, to.StoreID)
		return nil, errors.Wrapf(maybeHandleDeprecatedSnapErr(resp.Error()), "%s: remote couldn't accept %s", to, snap)
	case kvserverpb.SnapshotResponse_ACCEPTED:
		// This is the response we're expecting. Continue with snapshot sending.
		log.Event(ctx, "received SnapshotResponse_ACCEPTED message from server")
	default:
		err := errors.Errorf("%s: server sent an invalid status while negotiating %s: %s",
			to, snap, resp.Status)
		storePool.Throttle(storepool.ThrottleFailed, err.Error(), to.StoreID)
		return nil, err
	}

	durQueued := timeutil.Since(start)
	start = timeutil.Now()

	// Consult cluster settings to determine rate limits and batch sizes.
	targetRate := rate.Limit(rebalanceSnapshotRate.Get(&st.SV))
	batchSize := snapshotSenderBatchSize.Get(&st.SV)

	// Convert the bytes/sec rate limit to batches/sec.
	//
	// TODO(peter): Using bytes/sec for rate limiting seems more natural but has
	// practical difficulties. We either need to use a very large burst size
	// which seems to disable the rate limiting, or call WaitN in smaller than
	// burst size chunks which caused excessive slowness in testing. Would be
	// nice to figure this out, but the batches/sec rate limit works for now.
	limiter := rate.NewLimiter(targetRate/rate.Limit(batchSize), 1 /* burst size */)

	ss := &kvBatchSnapshotStrategy{
		batchSize:     batchSize,
		limiter:       limiter,
		newWriteBatch: newWriteBatch,
		st:            st,
		clusterID:     clusterID,
	}

	// Record timings for snapshot send if kv.trace.snapshot.enable_threshold is enabled
	numBytesSent, err := ss.Send(ctx, stream, header, snap, recordBytesSent)
	if err != nil {
		return nil, err
	}
	durSent := timeutil.Since(start)

	// Notify the sent callback before the final snapshot request is sent so that
	// the snapshots generated metric gets incremented before the snapshot is
	// applied.
	sent()
	if err := stream.Send(&kvserverpb.SnapshotRequest{Final: true}); err != nil {
		return nil, err
	}
	log.KvDistribution.Infof(
		ctx,
		"streamed %s to %s with %s in %.2fs @ %s/s: %s, rate-limit: %s/s, queued: %.2fs",
		snap,
		to,
		humanizeutil.IBytes(numBytesSent),
		durSent.Seconds(),
		humanizeutil.IBytes(int64(float64(numBytesSent)/durSent.Seconds())),
		ss.Status(),
		humanizeutil.IBytes(int64(targetRate)),
		durQueued.Seconds(),
	)

	resp, err = stream.Recv()
	if err != nil {
		return nil, errors.Wrapf(err, "%s: remote failed to apply snapshot", to)
	}
	sp.ImportRemoteRecording(resp.CollectedSpans)
	// NB: wait for EOF which ensures that all processing on the server side has
	// completed (such as defers that might be run after the previous message was
	// received).
	if unexpectedResp, err := stream.Recv(); err != io.EOF {
		if err != nil {
			return nil, errors.Wrapf(err, "%s: expected EOF, got resp=%v with error", to, unexpectedResp)
		}
		return nil, errors.Newf("%s: expected EOF, got resp=%v", to, unexpectedResp)
	}
	switch resp.Status {
	case kvserverpb.SnapshotResponse_ERROR:
		return nil, errors.Wrapf(
			maybeHandleDeprecatedSnapErr(resp.Error()), "%s: remote failed to apply snapshot", to,
		)
	case kvserverpb.SnapshotResponse_APPLIED:
		return resp, nil
	default:
		return nil, errors.Errorf("%s: server sent an invalid status during finalization: %s",
			to, resp.Status,
		)
	}
}
