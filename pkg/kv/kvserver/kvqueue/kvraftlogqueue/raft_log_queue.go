// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvraftlogqueue

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvqueue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicasideload"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storemetrics"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Overview of Raft log truncation:
//
// The safety requirement for truncation is that the entries being truncated
// are already durably applied to the state machine. This is because after a
// truncation, the only remaining source of information regarding the data in
// the truncated entries is the state machine, which represents a prefix of
// the log. If we truncated entries that were not durably applied to the state
// machine, a crash would create a gap in what the state machine knows and the
// first entry in the untruncated log, which prevents any more application.
//
// Initialized replicas may need to provide log entries to slow followers to
// catch up, so for performance reasons they should also base truncation on
// the state of followers. Additionally, truncation should typically do work
// when there are "significant" bytes or number of entries to truncate.
// However, if the replica is quiescent we would like to truncate the whole
// log when it becomes possible.
//
// An attempt is made to add a replica to the queue under two situations:
// - Event occurs that indicates that there are significant bytes/entries that
//   can be truncated. Until the truncation is proposed (see below), these
//   events can keep firing. The queue dedups the additions until the replica
//   has been processed. Note that there is insufficient information at the
//   time of addition to predict that the truncation will actually happen.
//   Only the processing will finally decide whether truncation should happen,
//   hence the deduping cannot happen outside the queue (say by changing the
//   firing condition). If nothing is done when processing the replica, the
//   continued firing of the events will cause the replica to again be added
//   to the queue. In the current code, these events can only trigger after
//   application to the state machine.
//
// - Periodic addition via the replicaScanner: this is helpful in two ways (a)
//   the events in the previous bullet can under-fire if the size estimates
//   are wrong, (b) if the replica becomes quiescent, those events can stop
//   firing but truncation may not have been done due to other constraints
//   (like lagging followers). The periodic addition (polling) takes care of
//   ensuring that when those other constraints are removed, the truncation
//   happens.
//
// The raftLogQueue proposes "replicated" truncation. This is done by the raft
// leader, which has knowledge of the followers and results in a
// TruncateLogRequest. This proposal will be raft replicated and serve as an
// upper bound to all replicas on what can be truncated. Each replica
// remembers in-memory what truncations have been proposed, so that truncation
// can be done independently at each replica when the corresponding
// RaftAppliedIndex is durable (see raftLogTruncator). Note that since raft
// state (including truncated state) is not part of the state machine, this
// loose coupling is fine. The loose coupling is enabled with cluster version
// LooselyCoupledRaftLogTruncation and cluster setting
// kv.raft_log.enable_loosely_coupled_truncation. When not doing loose
// coupling (legacy), the proposal causes immediate truncation -- this is
// correct because other externally maintained invariants ensure that the
// state machine is durable (though we have some concerns in
// https://github.com/cockroachdb/cockroach/issues/38566).
//
// NB: Loosely coupled truncation loses the pending truncations that were
// queued in-memory when a node restarts. This is considered ok for now since
// either (a) the range will keep seeing new writes and eventually another
// truncation will be proposed, (b) if the range becomes quiescent we are
// willing to accept some amount of garbage. (b) can be addressed by
// unilaterally truncating at each follower if the range is quiescent. And
// since we check that the RaftAppliedIndex is durable, it is easy to truncate
// all the entries of the log in this quiescent case.

// This is a temporary cluster setting that we will remove after one release
// cycle of everyone running with the default value of true. It only exists as
// a safety switch in case the new behavior causes unanticipated issues.
// Current plan:
// - v22.1: Has the setting. Expectation is that no one changes to false.
// - v22.2: The code behavior is hard-coded to true, in that the setting has
//   no effect (we can also delete a bunch of legacy code).
// Mixed version clusters:
// - v21.2 and v22.1: Will behave as strongly coupled since the cluster
//   version serves as an additional gate.
// - v22.1 and v22.2: If the setting has been changed to false the v22.1 nodes
//   will do strongly coupled truncation and the v22.2 will do loosely
//   coupled. This co-existence is correct.
var LooselyCoupledTruncationEnabled = func() *settings.BoolSetting {
	s := settings.RegisterBoolSetting(
		settings.SystemOnly,
		"kv.raft_log.loosely_coupled_truncation.enabled",
		"set to true to loosely couple the raft log truncation",
		true)
	s.SetVisibility(settings.Reserved)
	return s
}()

const (
	// raftLogQueueTimerDuration is the duration between truncations.
	raftLogQueueTimerDuration = 0 // zero duration to process truncations greedily

	// Allow a limited number of Raft log truncations to be processed
	// concurrently.
	raftLogQueueConcurrency = 4
	// RaftLogQueuePendingSnapshotGracePeriod indicates the grace period after an
	// in-flight snapshot is marked completed. While a snapshot is in-flight we
	// will not truncate past the snapshot's log index but we also don't want to
	// do so the moment the in-flight snapshot completes, since it is only applied
	// at the receiver a little later. This grace period reduces the probability
	// of an ill-timed log truncation that would necessitate another snapshot.
	RaftLogQueuePendingSnapshotGracePeriod = 3 * time.Second
)

// RaftLogQueue manages a queue of replicas slated to have their raft logs
// truncated by removing unneeded entries.
type RaftLogQueue struct {
	*kvqueue.BaseQueue
	db *kv.DB

	logSnapshots util.EveryN
	metrics      *storemetrics.StoreMetrics
}

// NewRaftLogQueue returns a new instance of RaftLogQueue. Replicas are passed
// to the queue both proactively (triggered by write load) and periodically
// (via the scanner). When processing a replica, the queue decides whether the
// Raft log can be truncated, which is a tradeoff between wanting to keep the
// log short overall and allowing slower followers to catch up before they get
// cut off by a truncation and need a snapshot. See newTruncateDecision for
// details on this decision making process.
func NewRaftLogQueue(
	store kvqueue.Store, db *kv.DB, metrics *storemetrics.StoreMetrics, knobs *kvqueue.TestingKnobs,
) *RaftLogQueue {
	rlq := &RaftLogQueue{
		db:           db,
		logSnapshots: util.Every(10 * time.Second),
		metrics:      metrics,
	}
	rlq.BaseQueue = kvqueue.NewBaseQueue(
		"raftlog", rlq, store,
		kvqueue.Config{
			MaxSize:              kvqueue.DefaultQueueMaxSize,
			MaxConcurrency:       raftLogQueueConcurrency,
			NeedsLease:           false,
			NeedsSystemConfig:    false,
			AcceptsUnsplitRanges: true,
			Successes:            metrics.RaftLogQueueSuccesses,
			Failures:             metrics.RaftLogQueueFailures,
			Pending:              metrics.RaftLogQueuePending,
			ProcessingNanos:      metrics.RaftLogQueueProcessingNanos,
		},
		knobs,
	)
	return rlq
}

// ShouldQueue determines whether a range should be queued for truncating. This
// is true only if the replica is the raft leader and if the total number of
// the range's raft log's stale entries exceeds RaftLogQueueStaleThreshold.
func (rlq *RaftLogQueue) ShouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, r kvqueue.Replica, _ spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	decision, err := r.NewTruncateDecision(ctx)
	if err != nil {
		log.Warningf(ctx, "%v", err)
		return false, 0
	}

	shouldQ, _, prio := rlq.ShouldQueueImpl(ctx, decision)
	return shouldQ, prio
}

// ShouldQueueImpl returns whether the given truncate decision should lead to
// a log truncation. This is either the case if the decision says so or if
// we want to recompute the log size (in which case `recomputeRaftLogSize` and
// `shouldQ` are both true and a reasonable priority is returned).
func (rlq *RaftLogQueue) ShouldQueueImpl(
	ctx context.Context, decision kvqueue.TruncateDecision,
) (shouldQ bool, recomputeRaftLogSize bool, priority float64) {
	if decision.ShouldTruncate() {
		return true, !decision.Input.LogSizeTrusted, float64(decision.Input.LogSize)
	}
	if decision.Input.LogSizeTrusted ||
		decision.Input.LastIndex == decision.Input.FirstIndex {

		return false, false, 0
	}
	// We have a nonempty log (first index != last index) and can't vouch that
	// the bytes in the log are known. Queue the replica; processing it will
	// force a recomputation. For the priority, we have to pick one as we
	// usually use the log size which is not available here. Going half-way
	// between zero and the MaxLogSize should give a good tradeoff between
	// processing the recomputation quickly, and not starving replicas which see
	// a significant amount of write traffic until they run over and truncate
	// more aggressively than they need to.
	// NB: this happens even on followers.
	return true, true, 1.0 + float64(decision.Input.MaxLogSize)/2.0
}

// Process truncates the raft log of the range if the replica is the raft
// leader and if the total number of the range's raft log's stale entries
// exceeds RaftLogQueueStaleThreshold.
func (rlq *RaftLogQueue) Process(
	ctx context.Context, r kvqueue.Replica, _ spanconfig.StoreReader,
) (processed bool, err error) {
	decision, err := r.NewTruncateDecision(ctx)
	if err != nil {
		return false, err
	}

	if _, recompute, _ := rlq.ShouldQueueImpl(ctx, decision); recompute {
		log.VEventf(ctx, 2, "recomputing raft log based on decision %+v", decision)

		// We need to hold raftMu both to access the sideloaded storage and to
		// make sure concurrent Raft activity doesn't foul up our update to the
		// cached in-memory values.
		r.RaftLock()
		n, err := ComputeRaftLogSize(ctx, r.GetRangeID(), rlq.Store.Engine(), r.SideloadedRaftMuLocked())
		if err == nil {
			r.UpdateRaftLogSize(n)
		}
		r.RaftUnlock()

		if err != nil {
			return false, errors.Wrap(err, "recomputing raft log size")
		}

		log.VEventf(ctx, 2, "recomputed raft log size to %s", humanizeutil.IBytes(n))

		// Override the decision, now that an accurate log size is available.
		decision, err = r.NewTruncateDecision(ctx)
		if err != nil {
			return false, err
		}
	}

	// Can and should the raft logs be truncated?
	if !decision.ShouldTruncate() {
		log.VEventf(ctx, 3, "%s", redact.Safe(decision.String()))
		return false, nil
	}

	if n := decision.NumNewRaftSnapshots(); log.V(1) || n > 0 && rlq.logSnapshots.ShouldProcess(timeutil.Now()) {
		log.Infof(ctx, "%v", redact.Safe(decision.String()))
	} else {
		log.VEventf(ctx, 1, "%v", redact.Safe(decision.String()))
	}
	b := &kv.Batch{}
	truncRequest := &roachpb.TruncateLogRequest{
		RequestHeader: roachpb.RequestHeader{Key: r.Desc().StartKey.AsRawKey()},
		Index:         decision.NewFirstIndex,
		RangeID:       r.GetRangeID(),
	}
	if rlq.Settings.Version.IsActive(
		ctx, clusterversion.LooselyCoupledRaftLogTruncation) {
		truncRequest.ExpectedFirstIndex = decision.Input.FirstIndex
	}
	b.AddRawRequest(truncRequest)
	if err := rlq.db.Run(ctx, b); err != nil {
		return false, err
	}
	rlq.metrics.RaftLogTruncated.Inc(int64(decision.NumTruncatableIndexes()))
	return true, nil
}

// Timer returns interval between processing successive queued truncations.
func (*RaftLogQueue) Timer(_ time.Duration) time.Duration {
	return raftLogQueueTimerDuration
}

// PurgatoryChan returns nil.
func (*RaftLogQueue) PurgatoryChan() <-chan time.Time {
	return nil
}

func IsLooselyCoupledRaftLogTruncationEnabled(
	ctx context.Context, settings *cluster.Settings,
) bool {
	return settings.Version.IsActive(
		ctx, clusterversion.LooselyCoupledRaftLogTruncation) &&
		LooselyCoupledTruncationEnabled.Get(&settings.SV)
}

// ComputeRaftLogSize computes the size (in bytes) of the Raft log from the
// storage engine. This will iterate over the Raft log and sideloaded files, so
// depending on the size of these it can be mildly to extremely expensive and
// thus should not be called frequently.
//
// The sideloaded storage may be nil, in which case it is treated as empty.
func ComputeRaftLogSize(
	ctx context.Context,
	rangeID roachpb.RangeID,
	reader storage.Reader,
	sideloaded replicasideload.SideloadStorage,
) (int64, error) {
	prefix := keys.RaftLogPrefix(rangeID)
	prefixEnd := prefix.PrefixEnd()
	iter := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd,
	})
	defer iter.Close()
	ms, err := iter.ComputeStats(prefix, prefixEnd, 0 /* nowNanos */)
	if err != nil {
		return 0, err
	}
	var totalSideloaded int64
	if sideloaded != nil {
		var err error
		// The remaining bytes if one were to truncate [0, 0) gives us the total
		// number of bytes in sideloaded files.
		_, totalSideloaded, err = sideloaded.BytesIfTruncatedFromTo(ctx, 0, 0)
		if err != nil {
			return 0, err
		}
	}
	return ms.SysBytes + totalSideloaded, nil
}
