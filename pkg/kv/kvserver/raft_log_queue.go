// Copyright 2015 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

// Overview of Raft log truncation:

// The safety requirement for truncation is that the entries being truncated
// are already durably applied to the state machine. Initialized replicas may
// need to provide log entries to slow followers to catch up, so for
// performance reasons they should also base truncation on the state of
// followers. Additionally, truncation should typically do work when there are
// "significant" bytes or number of entries to truncate. However, if the
// replica is quiescent we would like to truncate the whole log when it
// becomes possible.
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
//   (lagging followers; latest applied state was not durable). The periodic
//   addition (polling) takes care of ensuring that when those other
//   constraints are removed, the truncation happens.
//
// The raftLogQueue does the following:
// 1. Proposes "replicated" truncation
// This is done by the raft leader, which has knowledge of the followers and
// results in a TruncateLogRequest. This proposal will be raft replicated and
// serve as an upper bound to all replicas on what can be truncated. Each
// replica remembers in-memory what truncations have been proposed, so that
// truncation can be done independently at each replica when the corresponding
// RaftAppliedIndex is durable. Note that since raft state (including truncated
// state) is not part of the state machine, this loose coordination is fine.
//
// 2. Proposes "local" truncation
// This is done at any replica if the replica (a) is quiescent, and (b) the
// raft log is non-empty. We treat this similar to the case where a
// TruncateLogRequest arrived with the latest entry in the raft log. We need
// this path since nodes can fail before they did the actual truncation, and
// forget the "replicated" truncation proposals -- this ensures that such
// replicas will eventually be truncated.
//
// The loose coupling implied by (1) above, and the corresponding need to do
// (2) are enabled with cluster version LooselyCoupledRaftLogTruncation.

// This is a temporary cluster setting that we will remove after one release
// cycle of everyone running with the default value of true. It only exists as
// a safety switch in case the new behavior causes unanticipated issues.
var enableLooselyCoupledTruncation = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.raft_log.enable_loosely_coupled_truncation",
	"set to true to loosely couple the raft log truncation.",
	true)

func init() {
	enableLooselyCoupledTruncation.SetVisibility(settings.Reserved)
}

const (
	// raftLogQueueTimerDuration is the duration between truncations.
	raftLogQueueTimerDuration = 0 // zero duration to process truncations greedily
	// RaftLogQueueStaleThreshold is the minimum threshold for stale raft log
	// entries. A stale entry is one which all replicas of the range have
	// progressed past and thus is no longer needed and can be truncated.
	RaftLogQueueStaleThreshold = 100
	// RaftLogQueueStaleSize is the minimum size of the Raft log that we'll
	// truncate even if there are fewer than RaftLogQueueStaleThreshold entries
	// to truncate. The value of 64 KB was chosen experimentally by looking at
	// when Raft log truncation usually occurs when using the number of entries
	// as the sole criteria.
	RaftLogQueueStaleSize = 64 << 10
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

// raftLogQueue manages a queue of replicas slated to have their raft logs
// truncated by removing unneeded entries.
type raftLogQueue struct {
	*baseQueue
	db *kv.DB

	logSnapshots util.EveryN
}

// newRaftLogQueue returns a new instance of raftLogQueue. Replicas are passed
// to the queue both proactively (triggered by write load) and periodically
// (via the scanner). When processing a replica, the queue decides whether the
// Raft log can be truncated, which is a tradeoff between wanting to keep the
// log short overall and allowing slower followers to catch up before they get
// cut off by a truncation and need a snapshot. See newTruncateDecision for
// details on this decision making process.
func newRaftLogQueue(store *Store, db *kv.DB) *raftLogQueue {
	rlq := &raftLogQueue{
		db:           db,
		logSnapshots: util.Every(10 * time.Second),
	}
	rlq.baseQueue = newBaseQueue(
		"raftlog", rlq, store,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			maxConcurrency:       raftLogQueueConcurrency,
			needsLease:           false,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
			successes:            store.metrics.RaftLogQueueSuccesses,
			failures:             store.metrics.RaftLogQueueFailures,
			pending:              store.metrics.RaftLogQueuePending,
			processingNanos:      store.metrics.RaftLogQueueProcessingNanos,
		},
	)
	return rlq
}

// newTruncateDecision returns a truncateDecision for the given Replica if no
// error occurs. If input data to establish a truncateDecision is missing, a
// zero decision is returned.
//
// At a high level, a truncate decision operates based on the Raft log size, the
// number of entries in the log, and the Raft status of the followers. In an
// ideal world and most of the time, followers are reasonably up to date, and a
// decision to truncate to the index acked on all replicas will be made whenever
// there is at least a little bit of log to truncate (think a hundred records or
// ~100kb of data). If followers fall behind, are offline, or are waiting for a
// snapshot, a second strategy is needed to make sure that the Raft log is
// eventually truncated: when the raft log size exceeds a limit, truncations
// become willing and able to cut off followers as long as a quorum has acked
// the truncation index. The quota pool ensures that the delta between "acked by
// quorum" and "acked by all" is bounded, while Raft limits the size of the
// uncommitted, i.e. not "acked by quorum", part of the log; thus the "quorum"
// truncation strategy bounds the absolute size of the log on all followers.
//
// Exceptions are made for replicas for which information is missing ("probing
// state") as long as they are known to have been online recently, and for
// in-flight snapshots which are not adequately reflected in the Raft status and
// would otherwise be cut off with regularity. Probing live followers should
// only remain in this state for a short moment and so we deny a log truncation
// outright (as there's no safe index to truncate to); for snapshots, we can
// still truncate, but not past the snapshot's index.
//
// A challenge for log truncation is to deal with sideloaded log entries, that
// is, entries which contain SSTables for direct ingestion into the storage
// engine. Such log entries are very large, and failing to account for them in
// the heuristics can trigger overly aggressive truncations.
//
// The raft log size used in the decision making process is principally updated
// in the main Raft command apply loop, and adds a Replica to this queue
// whenever the log size has increased by a non-negligible amount that would be
// worth truncating (~100kb).
//
// Unfortunately, the size tracking is not very robust as it suffers from two
// limitations at the time of writing:
// 1. it may undercount as it is in-memory and incremented only as proposals
//    are handled; that is, a freshly started node will believe its Raft log to be
//    zero-sized independent of its actual size, and
// 2. the addition and corresponding subtraction happen in very different places
//    and are difficult to keep bug-free, meaning that there is low confidence that
//    we maintain the delta in a completely accurate manner over time. One example
//    of potential errors are sideloaded proposals, for which the subtraction needs
//    to load the size of the file on-disk (i.e. supplied by the fs), whereas
//    the addition uses the in-memory representation of the file.
//
// Ideally, a Raft log that grows large for whichever reason (for instance the
// queue being stuck on another replica) wouldn't be more than a nuisance on
// nodes with sufficient disk space. Also, IMPORT/RESTORE's split/scatter
// phase interacts poorly with overly aggressive truncations and can DDOS the
// Raft snapshot queue.
func newTruncateDecision(ctx context.Context, r *Replica) (truncateDecision, error) {
	rangeID := r.RangeID
	now := timeutil.Now()

	// NB: we need an exclusive lock due to grabbing the first index.
	r.mu.Lock()
	raftLogSize := r.mu.pendingLogTruncations.computePostTruncLogSize(r.mu.raftLogSize)
	// A "cooperative" truncation (i.e. one that does not cut off followers from
	// the log) takes place whenever there are more than
	// RaftLogQueueStaleThreshold entries or the log's estimated size is above
	// RaftLogQueueStaleSize bytes. This is fairly aggressive, so under normal
	// conditions, the log is very small.
	//
	// If followers start falling behind, at some point the logs still need to
	// be truncated. We do this either when the size of the log exceeds
	// RaftLogTruncationThreshold (or, in eccentric configurations, the zone's
	// RangeMaxBytes). This captures the heuristic that at some point, it's more
	// efficient to catch up via a snapshot than via applying a long tail of log
	// entries.
	targetSize := r.store.cfg.RaftLogTruncationThreshold
	if targetSize > r.mu.conf.RangeMaxBytes {
		targetSize = r.mu.conf.RangeMaxBytes
	}
	raftStatus := r.raftStatusRLocked()

	firstIndex, err := r.raftFirstIndexLocked()
	firstIndex = r.mu.pendingLogTruncations.computePostTruncFirstIndex(firstIndex)
	const anyRecipientStore roachpb.StoreID = 0
	pendingSnapshotIndex := r.getAndGCSnapshotLogTruncationConstraintsLocked(now, anyRecipientStore)
	lastIndex := r.mu.lastIndex
	// NB: raftLogSize above adjusts for pending truncations that have already
	// been successfully replicated via raft, but logSizeTrusted does not see if
	// those pending truncations would cause a transition from trusted =>
	// !trusted. This is done since we don't want to trigger a recomputation of
	// the raft log size while we still have pending truncations.
	logSizeTrusted := r.mu.raftLogSizeTrusted
	isQuiescent := r.mu.quiescent
	r.mu.Unlock()

	if err != nil {
		return truncateDecision{}, errors.Wrapf(err, "error retrieving first index for r%d", rangeID)
	}

	if raftStatus == nil {
		if log.V(6) {
			log.Infof(ctx, "the raft group doesn't exist for r%d", rangeID)
		}
		return truncateDecision{}, nil
	}

	// Is this the raft leader? We only perform log truncation:
	// - on the raft leader which has the up to date info on followers. This
	//   proposal is replicated via raft.
	// - on a follower (completely local truncation): if the replica is
	//   quiescent.
	if raftStatus.RaftState != raft.StateLeader &&
		(!isQuiescent || !isLooselyCoupledRaftLogTruncationEnabled(ctx, r.ClusterSettings())) {
		return truncateDecision{}, nil
	}

	if raftStatus.RaftState == raft.StateLeader {
		// For all our followers, overwrite the RecentActive field (which is always
		// true since we don't use CheckQuorum) with our own activity check.
		r.mu.RLock()
		log.Eventf(ctx, "raft status before lastUpdateTimes check: %+v", raftStatus.Progress)
		log.Eventf(ctx, "lastUpdateTimes: %+v", r.mu.lastUpdateTimes)
		updateRaftProgressFromActivity(
			ctx, raftStatus.Progress, r.descRLocked().Replicas().Descriptors(),
			func(replicaID roachpb.ReplicaID) bool {
				return r.mu.lastUpdateTimes.isFollowerActiveSince(
					ctx, replicaID, now, r.store.cfg.RangeLeaseActiveDuration())
			},
		)
		log.Eventf(ctx, "raft status after lastUpdateTimes check: %+v", raftStatus.Progress)
		r.mu.RUnlock()

		if pr, ok := raftStatus.Progress[raftStatus.Lead]; ok {
			// TODO(tschottdorf): remove this line once we have picked up
			// https://github.com/etcd-io/etcd/pull/10279
			pr.State = tracker.StateReplicate
			raftStatus.Progress[raftStatus.Lead] = pr
		}
	}
	// Else follower, which doesn't care about the others in the system.
	// TODO(sumeer): check if this will confuse the computation in
	// computeTruncateDecision which does look at RaftStatus.Progress.

	input := truncateDecisionInput{
		RaftStatus:           *raftStatus,
		LogSize:              raftLogSize,
		MaxLogSize:           targetSize,
		LogSizeTrusted:       logSizeTrusted,
		FirstIndex:           firstIndex,
		LastIndex:            lastIndex,
		PendingSnapshotIndex: pendingSnapshotIndex,
	}

	decision := computeTruncateDecision(input)
	return decision, nil
}

func updateRaftProgressFromActivity(
	ctx context.Context,
	prs map[uint64]tracker.Progress,
	replicas []roachpb.ReplicaDescriptor,
	replicaActive func(roachpb.ReplicaID) bool,
) {
	for _, replDesc := range replicas {
		replicaID := replDesc.ReplicaID
		pr, ok := prs[uint64(replicaID)]
		if !ok {
			continue
		}
		pr.RecentActive = replicaActive(replicaID)
		// Override this field for safety since we don't use it. Instead, we use
		// pendingSnapshotIndex from above.
		//
		// NOTE: We don't rely on PendingSnapshot because PendingSnapshot is
		// initialized by the leader when it realizes the follower needs a snapshot,
		// and it isn't initialized with the index of the snapshot that is actually
		// sent by us (out of band), which likely is lower.
		pr.PendingSnapshot = 0
		prs[uint64(replicaID)] = pr
	}
}

const (
	truncatableIndexChosenViaCommitIndex     = "commit"
	truncatableIndexChosenViaFollowers       = "followers"
	truncatableIndexChosenViaProbingFollower = "probing follower"
	truncatableIndexChosenViaPendingSnap     = "pending snapshot"
	truncatableIndexChosenViaFirstIndex      = "first index"
	truncatableIndexChosenViaLastIndex       = "last index"
)

type truncateDecisionInput struct {
	RaftStatus            raft.Status
	LogSize, MaxLogSize   int64
	LogSizeTrusted        bool // false when LogSize might be off
	FirstIndex, LastIndex uint64
	PendingSnapshotIndex  uint64
}

func (input truncateDecisionInput) LogTooLarge() bool {
	return input.LogSize > input.MaxLogSize
}

// truncateDecision describes a truncation decision.
// Beware: when extending this struct, be sure to adjust .String()
// so that it is guaranteed to not contain any PII or confidential
// cluster data.
type truncateDecision struct {
	Input       truncateDecisionInput
	CommitIndex uint64

	NewFirstIndex uint64 // first index of the resulting log after truncation
	ChosenVia     string
}

func (td *truncateDecision) raftSnapshotsForIndex(index uint64) int {
	var n int
	for _, p := range td.Input.RaftStatus.Progress {
		if p.State != tracker.StateReplicate {
			// If the follower isn't replicating, we can't trust its Match in
			// the first place. But note that this shouldn't matter in practice
			// as we already take care to not cut off these followers when
			// computing the truncate decision. See:
			_ = truncatableIndexChosenViaProbingFollower // guru ref
			continue
		}

		// When a log truncation happens at the "current log index" (i.e. the
		// most recently committed index), it is often still in flight to the
		// followers not required for quorum, and it is likely that they won't
		// need a truncation to catch up. A follower in that state will have a
		// Match equaling committed-1, but a Next of committed+1 (indicating that
		// an append at 'committed' is already ongoing).
		if p.Match < index && p.Next <= index {
			n++
		}
	}
	if td.Input.PendingSnapshotIndex != 0 && td.Input.PendingSnapshotIndex < index {
		n++
	}

	return n
}

func (td *truncateDecision) NumNewRaftSnapshots() int {
	return td.raftSnapshotsForIndex(td.NewFirstIndex) - td.raftSnapshotsForIndex(td.Input.FirstIndex)
}

// String returns a representation for the decision.
// It is guaranteed to not return PII or confidential
// information from the cluster.
func (td *truncateDecision) String() string {
	var buf strings.Builder
	_, _ = fmt.Fprintf(&buf, "should truncate: %t [", td.ShouldTruncate())
	_, _ = fmt.Fprintf(
		&buf,
		"truncate %d entries to first index %d (chosen via: %s)",
		td.NumTruncatableIndexes(), td.NewFirstIndex, td.ChosenVia,
	)
	if td.Input.LogTooLarge() {
		_, _ = fmt.Fprintf(
			&buf,
			"; log too large (%s > %s)",
			humanizeutil.IBytes(td.Input.LogSize),
			humanizeutil.IBytes(td.Input.MaxLogSize),
		)
	}
	if n := td.NumNewRaftSnapshots(); n > 0 {
		_, _ = fmt.Fprintf(&buf, "; implies %d Raft snapshot%s", n, util.Pluralize(int64(n)))
	}
	if !td.Input.LogSizeTrusted {
		_, _ = fmt.Fprintf(&buf, "; log size untrusted")
	}
	buf.WriteRune(']')

	return buf.String()
}

func (td *truncateDecision) NumTruncatableIndexes() int {
	if td.NewFirstIndex < td.Input.FirstIndex {
		return 0
	}
	return int(td.NewFirstIndex - td.Input.FirstIndex)
}

func (td *truncateDecision) ShouldTruncate() bool {
	n := td.NumTruncatableIndexes()
	return n >= RaftLogQueueStaleThreshold ||
		(n > 0 && td.Input.LogSize >= RaftLogQueueStaleSize)
}

// ProtectIndex attempts to "protect" a position in the log by making sure it's
// not truncated away. Specifically it lowers the proposed truncation point
// (which will be the new first index after the truncation) to the given index
// if it would be truncating at a point past it. If a change is made, the
// ChosenVia is updated with the one given. This protection is not guaranteed if
// the protected index is outside of the existing [FirstIndex,LastIndex] bounds.
func (td *truncateDecision) ProtectIndex(index uint64, chosenVia string) {
	if td.NewFirstIndex > index {
		td.NewFirstIndex = index
		td.ChosenVia = chosenVia
	}
}

// computeTruncateDecision returns the oldest index that cannot be
// truncated. If there is a behind node, we want to keep old raft logs so it
// can catch up without having to send a full snapshot. However, if a node down
// is down long enough, sending a snapshot is more efficient and we should
// truncate the log to the next behind node or the quorum committed index. We
// currently truncate when the raft log size is bigger than the range
// size.
//
// Note that when a node is behind we continue to let the raft log build up
// instead of truncating to the commit index. Consider what would happen if we
// truncated to the commit index whenever a node is behind and thus needs to be
// caught up via a snapshot. While we're generating the snapshot, sending it to
// the behind node and waiting for it to be applied we would continue to
// truncate the log. If the snapshot generation and application takes too long
// the behind node will be caught up to a point behind the current first index
// and thus require another snapshot, likely entering a never ending loop of
// snapshots. See #8629.
func computeTruncateDecision(input truncateDecisionInput) truncateDecision {
	decision := truncateDecision{Input: input}
	decision.CommitIndex = input.RaftStatus.Commit

	// The last index is most aggressive possible truncation that we could do.
	// Everything else in this method makes the truncation less aggressive.
	decision.NewFirstIndex = input.LastIndex
	decision.ChosenVia = truncatableIndexChosenViaLastIndex

	// Start by trying to truncate at the commit index. Naively, you would expect
	// LastIndex to never be smaller than the commit index, but
	// RaftStatus.Progress.Match is updated on the leader when a command is
	// proposed and in a single replica Raft group this also means that
	// RaftStatus.Commit is updated at propose time.
	decision.ProtectIndex(decision.CommitIndex, truncatableIndexChosenViaCommitIndex)

	for _, progress := range input.RaftStatus.Progress {
		// Snapshots are expensive, so we try our best to avoid truncating past
		// where a follower is.

		// First, we never truncate off a recently active follower, no matter how
		// large the log gets. Recently active shares the (currently 10s) constant
		// as the quota pool, so the quota pool should put a bound on how much the
		// raft log can grow due to this.
		//
		// For live followers which are being probed (i.e. the leader doesn't know
		// how far they've caught up), the Match index is too large, and so the
		// quorum index can be, too. We don't want these followers to require a
		// snapshot since they are most likely going to be caught up very soon (they
		// respond with the "right index" to the first probe or don't respond, in
		// which case they should end up as not recently active). But we also don't
		// know their index, so we can't possible make a truncation decision that
		// avoids that at this point and make the truncation a no-op.
		//
		// The scenario in which this is most relevant is during restores, where we
		// split off new ranges that rapidly receive very large log entries while
		// the Raft group is still in a state of discovery (a new leader starts
		// probing followers at its own last index). Additionally, these ranges will
		// be split many times over, resulting in a flurry of snapshots with
		// overlapping bounds that put significant stress on the Raft snapshot
		// queue.
		if progress.RecentActive {
			if progress.State == tracker.StateProbe {
				decision.ProtectIndex(input.FirstIndex, truncatableIndexChosenViaProbingFollower)
			} else {
				decision.ProtectIndex(progress.Match, truncatableIndexChosenViaFollowers)
			}
			continue
		}

		// Second, if the follower has not been recently active, we don't
		// truncate it off as long as the raft log is not too large.
		if !input.LogTooLarge() {
			decision.ProtectIndex(progress.Match, truncatableIndexChosenViaFollowers)
		}

		// Otherwise, we let it truncate to the committed index.
	}

	// The pending snapshot index acts as a placeholder for a replica that is
	// about to be added to the range (or is in Raft recovery). We don't want to
	// truncate the log in a way that will require that new replica to be caught
	// up via yet another Raft snapshot.
	if input.PendingSnapshotIndex > 0 {
		decision.ProtectIndex(input.PendingSnapshotIndex, truncatableIndexChosenViaPendingSnap)
	}

	// If new first index dropped below first index, make them equal (resulting
	// in a no-op).
	if decision.NewFirstIndex < input.FirstIndex {
		decision.NewFirstIndex = input.FirstIndex
		decision.ChosenVia = truncatableIndexChosenViaFirstIndex
	}

	// We've inherited the unfortunate semantics for {First,Last}Index from
	// raft.Storage. Specifically, both {First,Last}Index are inclusive, so
	// there's no way to represent an empty log. The way we've initialized
	// repl.FirstIndex is to set it to the first index in the possibly-empty log
	// (TruncatedState.Index + 1), and allowing LastIndex to fall behind it when
	// the log is empty (TruncatedState.Index). The initialization is done when
	// minting a new replica from either the truncated state of incoming
	// snapshot, or using the default initial log index. This makes for the
	// confusing situation where FirstIndex > LastIndex. We can detect this
	// special empty log case by comparing checking if
	// `FirstIndex == LastIndex + 1` (`logEmpty` below). Similar to this, we can
	// have the case that `FirstIndex = CommitIndex + 1` when there are no
	// committed entries (which we check for in `noCommittedEntries` below).
	// Having done that (i.e. if the raft log is not empty, and there are
	// committed entries), we can assert on the following invariants:
	//
	//         FirstIndex    <= LastIndex                                    (0)
	//         NewFirstIndex >= FirstIndex                                   (1)
	//         NewFirstIndex <= LastIndex                                    (2)
	//         NewFirstIndex <= CommitIndex                                  (3)
	//
	// (1) asserts that we're not regressing our FirstIndex
	// (2) asserts that our we don't truncate past the last index we can
	//     truncate away, and
	// (3) is similar to (2) in that we assert that we're not truncating past
	//     the last known CommitIndex.
	//
	// TODO(irfansharif): We should consider cleaning up this mess around
	// {First,Last,Commit}Index by using a sentinel value to represent an empty
	// log (like we do with `invalidLastTerm`). It'd be extra nice if we could
	// safeguard access by relying on the type system to force callers to
	// consider the empty case. Something like
	// https://github.com/nvanbenschoten/optional could help us emulate an
	// `option<uint64>` type if we care enough.
	logEmpty := input.FirstIndex == input.LastIndex+1
	noCommittedEntries := input.FirstIndex == input.RaftStatus.Commit+1

	logIndexValid := logEmpty ||
		(decision.NewFirstIndex >= input.FirstIndex) && (decision.NewFirstIndex <= input.LastIndex)
	commitIndexValid := noCommittedEntries ||
		(decision.NewFirstIndex <= decision.CommitIndex)
	valid := logIndexValid && commitIndexValid
	if !valid {
		err := fmt.Sprintf("invalid truncation decision: output = %d, input: [%d, %d], commit idx = %d",
			decision.NewFirstIndex, input.FirstIndex, input.LastIndex, decision.CommitIndex)
		panic(err)
	}

	return decision
}

// shouldQueue determines whether a range should be queued for truncating. This
// is true only if the replica is the raft leader and if the total number of
// the range's raft log's stale entries exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, r *Replica, _ spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	decision, err := newTruncateDecision(ctx, r)
	if err != nil {
		log.Warningf(ctx, "%v", err)
		return false, 0
	}
	shouldQ, _, prio := rlq.shouldQueueImpl(ctx, decision)
	return shouldQ, prio
}

// shouldQueueImpl returns whether the given truncate decision should lead to
// a log truncation. This is either the case if the decision says so or if
// we want to recompute the log size (in which case `recomputeRaftLogSize` and
// `shouldQ` are both true and a reasonable priority is returned).
func (rlq *raftLogQueue) shouldQueueImpl(
	ctx context.Context, decision truncateDecision,
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

// process truncates the raft log of the range if the replica is the raft
// leader and if the total number of the range's raft log's stale entries
// exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) process(
	ctx context.Context, r *Replica, _ spanconfig.StoreReader,
) (processed bool, err error) {
	decision, err := newTruncateDecision(ctx, r)
	if err != nil {
		return false, err
	}

	if _, recompute, _ := rlq.shouldQueueImpl(ctx, decision); recompute {
		log.VEventf(ctx, 2, "recomputing raft log based on decision %+v", decision)

		// We need to hold raftMu both to access the sideloaded storage and to
		// make sure concurrent Raft activity doesn't foul up our update to the
		// cached in-memory values.
		r.raftMu.Lock()
		n, err := ComputeRaftLogSize(ctx, r.RangeID, r.Engine(), r.raftMu.sideloaded)
		if err == nil {
			r.mu.Lock()
			r.mu.raftLogSize = n
			r.mu.raftLogLastCheckSize = n
			r.mu.raftLogSizeTrusted = true
			r.mu.Unlock()
		}
		r.raftMu.Unlock()

		if err != nil {
			return false, errors.Wrap(err, "recomputing raft log size")
		}

		log.VEventf(ctx, 2, "recomputed raft log size to %s", humanizeutil.IBytes(n))

		// Override the decision, now that an accurate log size is available.
		decision, err = newTruncateDecision(ctx, r)
		if err != nil {
			return false, err
		}
	}

	// Can and should the raft logs be truncated?
	if !decision.ShouldTruncate() {
		log.VEventf(ctx, 3, "%s", log.Safe(decision.String()))
		return false, nil
	}
	if decision.Input.RaftStatus.RaftState != raft.StateLeader {
		// This is a follower and the replica is quiescent. Truncate the whole
		// log, since a quiescent replica means all the followers are up-to-date.
		err := func() error {
			r.raftMu.Lock()
			r.mu.Lock()
			defer r.raftMu.Unlock()
			if r.mu.state.TruncatedState.Index >= r.mu.lastIndex {
				// Already truncated
				r.mu.RUnlock()
				return nil
			}
			term, err := (*replicaRaftStorage)(r).Term(r.mu.lastIndex)
			if err != nil {
				r.mu.RUnlock()
				return err
			}
			r.mu.RUnlock()
			rlq.store.raftTruncator.addPendingTruncation(ctx, r, roachpb.RaftTruncatedState{
				Index: r.mu.lastIndex,
				Term:  term,
			}, decision.Input.FirstIndex, -r.mu.raftLogSize, true /* deltaIncludesSideloaded */)
			return nil
		}()
		if err != nil {
			return false, err
		}
		return true, nil
	}

	if n := decision.NumNewRaftSnapshots(); log.V(1) || n > 0 && rlq.logSnapshots.ShouldProcess(timeutil.Now()) {
		log.Infof(ctx, "%v", log.Safe(decision.String()))
	} else {
		log.VEventf(ctx, 1, "%v", log.Safe(decision.String()))
	}
	b := &kv.Batch{}
	truncRequest := &roachpb.TruncateLogRequest{
		RequestHeader: roachpb.RequestHeader{Key: r.Desc().StartKey.AsRawKey()},
		Index:         decision.NewFirstIndex,
		RangeID:       r.RangeID,
	}
	if rlq.store.ClusterSettings().Version.IsActive(
		ctx, clusterversion.LooselyCoupledRaftLogTruncation) {
		truncRequest.ExpectedFirstIndex = decision.Input.FirstIndex
	}
	b.AddRawRequest(truncRequest)
	if err := rlq.db.Run(ctx, b); err != nil {
		return false, err
	}
	r.store.metrics.RaftLogTruncated.Inc(int64(decision.NumTruncatableIndexes()))
	return true, nil
}

// timer returns interval between processing successive queued truncations.
func (*raftLogQueue) timer(_ time.Duration) time.Duration {
	return raftLogQueueTimerDuration
}

// purgatoryChan returns nil.
func (*raftLogQueue) purgatoryChan() <-chan time.Time {
	return nil
}

// pendingTruncations tracks proposed truncations for a replica that have not
// yet been enacted due to the corresponding RaftAppliedIndex not yet being
// durable.
type pendingLogTruncations struct {
	// We only track the oldest and latest pending truncation. We cannot track
	// only the latest since it may always be ahead of the durable
	// RaftAppliedIndex and so we may never be able to truncate. We assume
	// liveness of durability advancement, which means that if no new pending
	// truncations are added, the latest one will eventually be enacted.
	//
	// Note that this liveness assumption is not completely true -- if there are
	// no writes happening to the store, the durability (due to memtable
	// flushes) may not advance. We deem this (a) an uninteresting case, since
	// if there are no writes we possibly don't care about aggressively
	// truncating the log, (b) fixing the liveness assumption is not within
	// scope of the truncator (it has to work with what it is given).
	truncs [2]pendingTruncation
}

func (p *pendingLogTruncations) computePostTruncLogSize(raftLogSize int64) int64 {
	p.iterate(func(trunc pendingTruncation) {
		raftLogSize += trunc.logDeltaBytes
	})
	if raftLogSize < 0 {
		raftLogSize = 0
	}
	return raftLogSize
}

func (p *pendingLogTruncations) computePostTruncFirstIndex(firstIndex uint64) uint64 {
	p.iterate(func(trunc pendingTruncation) {
		if firstIndex < trunc.Index+1 {
			firstIndex = trunc.Index + 1
		}
	})
	return firstIndex
}

func (p *pendingLogTruncations) empty() bool {
	return p.truncs[0] == (pendingTruncation{})
}

func (p *pendingLogTruncations) front() pendingTruncation {
	return p.truncs[0]
}

func (p *pendingLogTruncations) pop() {
	p.truncs[0] = pendingTruncation{}
	if !(p.truncs[1] == (pendingTruncation{})) {
		p.truncs[0] = p.truncs[1]
		p.truncs[1] = pendingTruncation{}
	}
}

func (p *pendingLogTruncations) iterate(f func(trunc pendingTruncation)) {
	for _, trunc := range p.truncs {
		if !(trunc == (pendingTruncation{})) {
			f(trunc)
		}
	}
}

type pendingTruncation struct {
	// The pending truncation will truncate entries up to
	// RaftTruncatedState.Index, inclusive.
	roachpb.RaftTruncatedState

	// The raftLogDeltaBytes are computed under the assumption that the
	// truncation is deleting [expectedFirstIndex,RaftTruncatedState.Index]. It
	// originates in ReplicatedEvalResult, where it is accurate.
	// There are two reasons isDeltaTrusted could be considered false here:
	// - The original "accurate" delta does not account for sideloaded files. It
	//   is adjusted on this replica using
	//   SideloadStorage.BytesIfTruncatedFromTo, but it is possible that the
	//   truncated state of this replica is already > expectedFirstIndex. Note
	//   here that problem is that the original "accurate" delta will result in
	//   inaccuracy when added to this replica's raft log size. We don't
	//   actually set isDeltaTrusted=false for this case since we will change
	//   Replica.raftLogSizeTrusted to false after enacting this truncation.
	// - We merge pendingTruncation entries in the pendingTruncations struct. We
	//   are making an effort to have consecutive TruncateLogRequest provide us
	//   stats for index intervals that are adjacent and non-overlapping, but
	//   that behavior is best-effort.
	expectedFirstIndex uint64
	// logDeltaBytes includes the bytes from sideloaded files.
	logDeltaBytes  int64
	isDeltaTrusted bool
}

type rangeAndReplicaID struct {
	rangeID   roachpb.RangeID
	replicaID roachpb.ReplicaID
}

// truncatorForReplicasInStore is responsible for actually enacting
// truncations.
// Mutex ordering: Replica mutexes > truncatorForReplicasInStore.mu
type truncatorForReplicasInStore struct {
	store *Store
	mu    struct {
		syncutil.Mutex
		ranges map[rangeAndReplicaID]struct{}
	}
}

func makeTruncatorForReplicasInStore(store *Store) truncatorForReplicasInStore {
	t := truncatorForReplicasInStore{
		store: store,
	}
	t.mu.ranges = make(map[rangeAndReplicaID]struct{})
	return t
}

// addPendingTruncation assumes r.raftMu is held and r.mu is not held.
// raftExpectedFirstIndex and raftLogDelta have the same meaning as in
// ReplicatedEvalResult. Never called before cluster is at
// LooselyCoupledRaftLogTruncation. If deltaIncludesSideloaded is true, the
// raftLogDelta already includes the contribution of sideloaded files.
func (t *truncatorForReplicasInStore) addPendingTruncation(
	ctx context.Context,
	r *Replica,
	trunc roachpb.RaftTruncatedState,
	raftExpectedFirstIndex uint64,
	raftLogDelta int64,
	deltaIncludesSideloaded bool,
) {
	pendingTrunc := pendingTruncation{
		RaftTruncatedState: trunc,
		expectedFirstIndex: raftExpectedFirstIndex,
		logDeltaBytes:      raftLogDelta,
		isDeltaTrusted:     true,
	}
	pos, mergePending, alreadyTruncIndex :=
		func() (pos int, mergePending bool, alreadyTruncIndex uint64) {
			r.mu.RLock()
			defer r.mu.RUnlock()
			// truncState is guaranteed to be non-nil
			truncState := r.mu.state.TruncatedState
			alreadyTruncIndex = truncState.Index
			pos = 0
			mergePending = false
			for i, n := 0, len(r.mu.pendingLogTruncations.truncs); i < n; i++ {
				if !(r.mu.pendingLogTruncations.truncs[i] == (pendingTruncation{})) {
					if r.mu.pendingLogTruncations.truncs[i].Index > alreadyTruncIndex {
						alreadyTruncIndex = r.mu.pendingLogTruncations.truncs[i].Index
					}
					if i == n-1 {
						mergePending = true
						pos = i
					}
				} else {
					pos = i
					break
				}
			}
			return
		}()
	if alreadyTruncIndex >= pendingTrunc.Index {
		// Noop
		return
	}
	if !deltaIncludesSideloaded {
		// It is possible that alreadyTruncIndex + 1 > raftExpectedFirstIndex. When
		// we merge or enact we will see this problem and set the trusted bit to
		// false. But we can at least avoid double counting sideloaded entries,
		// which can be large, since we do the computation here. That will reduce
		// the undercounting of the bytes in the raft log.
		sideloadedFreed, _, err := r.raftMu.sideloaded.BytesIfTruncatedFromTo(
			ctx, alreadyTruncIndex+1, pendingTrunc.Index+1)
		// Log a loud error since we need to continue enqueuing the truncation.
		if err != nil {
			log.Errorf(ctx, "while computing size of sideloaded files to truncate: %+v", err)
			pendingTrunc.isDeltaTrusted = false
		}
		pendingTrunc.logDeltaBytes -= sideloadedFreed
	}
	if mergePending {
		pendingTrunc.isDeltaTrusted = pendingTrunc.isDeltaTrusted ||
			r.mu.pendingLogTruncations.truncs[pos].isDeltaTrusted
		if r.mu.pendingLogTruncations.truncs[pos].Index+1 != pendingTrunc.expectedFirstIndex {
			pendingTrunc.isDeltaTrusted = false
		}
		pendingTrunc.logDeltaBytes += r.mu.pendingLogTruncations.truncs[pos].logDeltaBytes
		pendingTrunc.expectedFirstIndex = r.mu.pendingLogTruncations.truncs[pos].expectedFirstIndex
	}
	r.mu.Lock()
	r.mu.pendingLogTruncations.truncs[pos] = pendingTrunc
	r.mu.Unlock()

	if pos == 0 {
		if mergePending {
			panic("should never be merging pending truncations at pos 0")
		}
		// First entry in queue of pending truncations for this replica.
		t.mu.Lock()
		t.mu.ranges[rangeAndReplicaID{rangeID: r.RangeID, replicaID: r.mu.replicaID}] = struct{}{}
		t.mu.Unlock()
	}
}

// Invoked whenever the durability of the store advances. We assume that this
// is coarse in that the advancement of durability will apply to all ranges in
// this store, and most of the preceding pending truncations have their goal
// truncated index become durable in RangeAppliedState.RaftAppliedIndex. This
// coarseness assumption is important for not wasting much work being done in
// this method.
// TODO(sumeer): hook this up to the callback that will be invoked on the
// Store by the Engine (Pebble).
func (t *truncatorForReplicasInStore) durabilityAdvanced(ctx context.Context) {
	var ranges []rangeAndReplicaID
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		n := len(t.mu.ranges)
		if n == 0 {
			return
		}
		ranges = make([]rangeAndReplicaID, n)
		i := 0
		for k := range t.mu.ranges {
			ranges[i] = k
			// If another pendingTruncation is added to this Replica, it will not be
			// added back to the map since the Replica already has pending
			// truncations. That is ok: we will try to enact all pending truncations
			// for that Replica below, since there typically will only be one
			// pending, and if there are any remaining we will add it back to the
			// map.
			delete(t.mu.ranges, k)
			i++
		}
	}()
	if len(ranges) == 0 {
		return
	}

	// Create an engine Reader to provide a safe lower bound on what is durable.
	//
	// TODO(sumeer): This is incorrect -- change this reader to only read
	// durable state after merging
	// https://github.com/cockroachdb/pebble/pull/1490 and incorporating into
	// CockroachDB.
	reader := t.store.Engine().NewReadOnly()

	for _, repl := range ranges {
		r, err := t.store.GetReplica(repl.rangeID)
		if err != nil || r == nil || r.mu.replicaID != repl.replicaID {
			// Not found or not the same replica.
			continue
		}
		r.raftMu.Lock()
		r.mu.Lock()
		if r.mu.destroyStatus.Removed() {
			r.mu.Unlock()
			r.raftMu.Unlock()
			continue
		}
		// Not destroyed. We can release Replica.mu below and be sure it will not
		// be replaced by another replica with the same RangeID, since we continue
		// to hold Replica.raftMu.

		truncState := *r.mu.state.TruncatedState
		for !r.mu.pendingLogTruncations.empty() {
			pendingTrunc := r.mu.pendingLogTruncations.front()
			if pendingTrunc.Index <= truncState.Index {
				r.mu.pendingLogTruncations.pop()
			}
		}
		if r.mu.pendingLogTruncations.empty() {
			r.mu.Unlock()
			r.raftMu.Unlock()
			continue
		}
		// Have some pending truncations.
		r.mu.Unlock()
		// NB: we can read r.mu.pendingTruncations since we still hold r.raftMu.
		popAll := func() {
			r.mu.Lock()
			for !r.mu.pendingLogTruncations.empty() {
				r.mu.pendingLogTruncations.pop()
			}
			r.mu.Unlock()
			r.raftMu.Unlock()
		}
		// Use the reader to decide what is durable.
		as, err := r.raftMu.stateLoader.LoadRangeAppliedState(ctx, reader)
		if err != nil {
			log.Errorf(ctx, "while loading RangeAppliedState for log truncation: %+v", err)
			// Pop all the truncations.
			popAll()
			continue
		}
		enactIndex := -1
		r.mu.pendingLogTruncations.iterate(func(trunc pendingTruncation) {
			if trunc.Index > as.RaftAppliedIndex {
				return
			}
			enactIndex++
		})
		if enactIndex > 0 {
			// Do the truncation.
			batch := t.store.Engine().NewUnindexedBatch(false)
			apply, err := handleTruncatedStateBelowRaftPreApply(ctx, &truncState,
				&r.mu.pendingLogTruncations.truncs[enactIndex].RaftTruncatedState, r.raftMu.stateLoader,
				batch)
			if err != nil {
				popAll()
				batch.Close()
				continue
			}
			if !apply {
				panic("unexpected !apply returned from handleTruncatedStateBelowRaftPreApply")
			}
			if err := batch.Commit(false); err != nil {
				popAll()
				continue
			}
			// Truncation done. Need to update the Replica state.
			r.mu.Lock()
			for i := 0; i <= enactIndex; i++ {
				pendingTrunc := r.mu.pendingLogTruncations.front()
				if r.mu.state.TruncatedState.Index+1 != pendingTrunc.expectedFirstIndex ||
					!pendingTrunc.isDeltaTrusted {
					r.mu.raftLogSizeTrusted = false
				}
				r.mu.raftLogSize += pendingTrunc.logDeltaBytes
				r.mu.raftLogLastCheckSize += pendingTrunc.logDeltaBytes
				*r.mu.state.TruncatedState = pendingTrunc.RaftTruncatedState
				r.mu.pendingLogTruncations.pop()
			}
			if r.mu.raftLogSize < 0 {
				r.mu.raftLogSize = 0
			}
			if r.mu.raftLogLastCheckSize < 0 {
				r.mu.raftLogLastCheckSize = 0
			}
			if !r.mu.pendingLogTruncations.empty() {
				t.mu.Lock()
				t.mu.ranges[rangeAndReplicaID{rangeID: r.RangeID, replicaID: r.mu.replicaID}] = struct{}{}
				t.mu.Unlock()
			}
			r.mu.Unlock()
		} else {
			t.mu.Lock()
			t.mu.ranges[rangeAndReplicaID{rangeID: r.RangeID, replicaID: r.mu.replicaID}] = struct{}{}
			t.mu.Unlock()
		}
		r.raftMu.Unlock()
	}
}

func isLooselyCoupledRaftLogTruncationEnabled(
	ctx context.Context, settings *cluster.Settings,
) bool {
	return settings.Version.IsActive(
		ctx, clusterversion.LooselyCoupledRaftLogTruncation) &&
		enableLooselyCoupledTruncation.Get(&settings.SV)
}
