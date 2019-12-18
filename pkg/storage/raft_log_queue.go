// Copyright 2015 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/tracker"
)

// Overview of Raft log truncation:
// Our desired state is local truncation decision and action at both the leaseholder and followers.
// Leaseholders need to be less aggressive with truncation since they may need to provide log
// entries to slow followers to catch up. The safety requirement for truncation is that the
// entries being truncated are already durably applied to the state machine. For performance
// reasons truncation should typically do work when there are "significant" bytes or number of
// entries to truncate. However, if the replica is quiescent we would like to truncate the whole
// log when it becomes possible.
//
// The raftLogQueue handles the process of truncation. An attempt is made to add a replica to the
// queue under two situations:
// - Event occurs that indicates that there are significant bytes/entries that can be truncated.
//   Until the truncation actually happens, these events can keep firing. The queue dedups the
//   additions until the replica has been processed. Note that there is insufficient information
//   at the time of addition to predict that the truncation will actually happen. Only the
//   processing will finally decide whether truncation should happen, hence the deduping cannot
//   happen outside the queue (say by changing the firing condition). If nothing is done when
//   processing the replica, the continued firing of the events will cause the replica to again be
//   added to the queue. In the current code, these events can only trigger after application to the
//   state machine.
// - Periodic addition via the replicaScanner: this is helpful in two ways (a) the events in the
//   previous bullet can under-fire if the size estimates are wrong, (b) if the replica becomes
//   quiescent, those events can stop firing but truncation may not have been done due to other
//   constraints (lagging followers, latest applied state not yet durable). The periodic
//   addition (polling) takes care of ensuring that when those other constraints are removed, the
//   truncation happens.
//
// We have to consider legacy behavior in our desired state of local truncation.
// - Legacy19.1 is replicated TruncatedState (TruncatedStateLegacyReplicated): this was the case
//   when the raft log was sent in snapshots. The truncation decision is made in the leaseholder and
//   replicated via Raft and acted on everywhere.
// - Legacy19.2 is unreplicated TruncatedState (TruncatedStateUnreplicated): the raft log is not
//   sent in snapshots. So the leaseholder may have more entries than a follower that applied a
//   snapshot. The truncation decision is still made in the leaseholder and replicated via Raft
//   amd acted on everywhere. The followers who have already truncated past that state ignore this
//   decision.
// - 20.1 is unreplicated TruncatedState with local truncation: This will come in two variants (a)
//   the state machine has a WAL, so applied state is immediately durable, (b) the state machine
//   does not have a WAL so Replica.mu.state.RaftAppliedIndex can be ahead of the durable index.
//   The difference between (a) and (b) is local to a node and other nodes do not need to care.

// To allow for these different behaviors to co-exist, we make the leaseholder continue to make
// truncation decisions that it will replicate via Raft. However a 20.1 follower node will ignore
// these truncation commands and truncate locally. The truncation decision
// making (at leaseholder and follower) and local action (follower) and (for the leaseholder) the
// replication all happen via this raftLogQueue (for the benefits that the queue brings, as stated
// earlier). Once we have no Legacy* nodes, the replication logic can be removed. Note that since
// in the followers the truncation decision is locally made one can only safely truncate up to
// RaftAppliedIndex. The leaseholder continues to truncate itself through Raft since we don't want
// to run the risk of it truncating without the replication of the truncate being successful -- say
// the leaseholder submits the proposal and truncates locally but by the time the proposal is
// replicated it is no longer the leaseholder so the followers ignore it (TODO: do truncation
// commands also get ingored if the ProposerLeaseSequence is not the same as LeaseSequence?).
//
// TODO: the aggressive truncation at followers means that if a follower becomes the leaseholder
// in the near future it may not have enough of a log to catchup lagging followers and needs to
// send them a snapshot. Should we instead use consensus to inform the followers on an upper bound
// on what is allowable to truncate according to the leaseholder. The actual truncation will
// locally but be informed by the latest upper bound delivered by the leaseholder. This would
// require us to persist the upper bound in the replicated state.
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
	// While a snapshot is in flight, we won't truncate past the snapshot's log
	// index. This behavior is extended to a grace period after the snapshot is
	// marked as completed as it is applied at the receiver only a little later,
	// leaving a window for a truncation that requires another snapshot.
	raftLogQueuePendingSnapshotGracePeriod = 3 * time.Second
)

// raftLogQueue manages a queue of replicas slated to have their raft logs
// truncated by removing unneeded entries.
type raftLogQueue struct {
	*baseQueue
	db *client.DB

	logSnapshots util.EveryN
}

// newRaftLogQueue returns a new instance of raftLogQueue. Replicas are passed
// to the queue both proactively (triggered by write load) and periodically
// (via the scanner). When processing a replica, the queue decides whether the
// Raft log can be truncated, which is a tradeoff between wanting to keep the
// log short overall and allowing slower followers to catch up before they get
// cut off by a truncation and need a snapshot. See newTruncateDecision for
// details on this decision making process.
func newRaftLogQueue(store *Store, db *client.DB, gossip *gossip.Gossip) *raftLogQueue {
	rlq := &raftLogQueue{
		db:           db,
		logSnapshots: util.Every(10 * time.Second),
	}
	rlq.baseQueue = newBaseQueue(
		"raftlog", rlq, store, gossip,
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
// eventually truncated: when the raft log size exceeds a limit (4mb at time of
// writing), truncations become willing and able to cut off followers as long as
// a quorum has acked the truncation index. The quota pool ensures that the delta
// between "acked by quorum" and "acked by all" is bounded, while Raft limits the
// size of the uncommitted, i.e. not "acked by quorum", part of the log; thus
// the "quorum" truncation strategy bounds the absolute size of the log on all
// followers.
//
// Exceptions are made for replicas for which information is missing ("probing
// state") as long as they are known to have been online recently, and for
// in-flight snapshots (in particular preemptive snapshots) which are not
// adequately reflected in the Raft status and would otherwise be cut off with
// regularity. Probing live followers should only remain in this state for a
// short moment and so we deny a log truncation outright (as there's no safe
// index to truncate to); for snapshots, we can still truncate, but not past
// the snapshot's index.
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
// nodes with sufficient disk space. Unfortunately, at the time of writing, the
// Raft log is included in Raft snapshots. On the other hand, IMPORT/RESTORE's
// split/scatter phase interacts poorly with overly aggressive truncations and
// can DDOS the Raft snapshot queue.
func newTruncateDecision(ctx context.Context, r *Replica) (truncateDecision, error) {
	rangeID := r.RangeID
	now := timeutil.Now()

	// NB: we need an exclusive lock due to grabbing the first index.
	r.mu.Lock()
	raftLogSize := r.mu.raftLogSize
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
	if targetSize > *r.mu.zone.RangeMaxBytes {
		targetSize = *r.mu.zone.RangeMaxBytes
	}
	raftStatus := r.raftStatusRLocked()

	var leaseholder bool
	// TODO: how do we know whether leaseholder?

	firstIndex, err := r.raftFirstIndexLocked()
	const anyRecipientStore roachpb.StoreID = 0
	pendingSnapshotIndex := r.getAndGCSnapshotLogTruncationConstraintsLocked(now, anyRecipientStore)
	// TODO: since leaseholder is still applying below Raft should we continue to use r.mu.lastIndex
	// for leaseholder and use RaftAppliedIndex only for follower.
	lastIndex := r.mu.state.RaftAppliedIndex
	logSizeTrusted := r.mu.raftLogSizeTrusted
	quiescent := r.mu.quiescent

	r.mu.Unlock()

	if err != nil {
		return truncateDecision{}, errors.Errorf("error retrieving first index for r%d: %s", rangeID, err)
	}

	// TODO: so nothing to truncate?
	if raftStatus == nil {
		if log.V(6) {
			log.Infof(ctx, "the raft group doesn't exist for r%d", rangeID)
		}
		return truncateDecision{}, nil
	}

	// Is this a leaseholder that is not the raft leader? We do not perform log truncation in this
	// case since the leaseholder does not have up to date info on followers.
	// TODO: presumably this is a transient state and the raft leader will give up its leadership to
	// the leaseholder?
	if leaseholder && raftStatus.RaftState != raft.StateLeader {
		return truncateDecision{}, nil
	}
	if raftStatus.RaftState == raft.StateLeader {
		// For all our followers, overwrite the RecentActive field (which is always
		// true since we don't use CheckQuorum) with our own activity check.
		r.mu.RLock()
		log.Eventf(ctx, "raft status before lastUpdateTimes check: %+v", raftStatus.Progress)
		log.Eventf(ctx, "lastUpdateTimes: %+v", r.mu.lastUpdateTimes)
		updateRaftProgressFromActivity(
			ctx, raftStatus.Progress, r.descRLocked().Replicas().All(), r.mu.lastUpdateTimes, now,
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
	input := truncateDecisionInput{
		RaftStatus:           *raftStatus,
		LogSize:              raftLogSize,
		MaxLogSize:           targetSize,
		LogSizeTrusted:       logSizeTrusted,
		FirstIndex:           firstIndex,
		LastIndex:            lastIndex,
		PendingSnapshotIndex: pendingSnapshotIndex,
		Quiescent:            quiescent,
		Leaseholder:          leaseholder,
	}

	decision := computeTruncateDecision(input)
	return decision, nil
}

func updateRaftProgressFromActivity(
	ctx context.Context,
	prs map[uint64]tracker.Progress,
	replicas []roachpb.ReplicaDescriptor,
	lastUpdate lastUpdateTimesMap,
	now time.Time,
) {
	for _, replDesc := range replicas {
		replicaID := replDesc.ReplicaID
		pr, ok := prs[uint64(replicaID)]
		if !ok {
			continue
		}
		pr.RecentActive = lastUpdate.isFollowerActive(ctx, replicaID, now)
		// Override this field for safety since we don't use it. Instead, we use
		// pendingSnapshotIndex from above which is also populated for preemptive
		// snapshots.
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
	truncatableIndexChosenViaQuorumIndex     = "quorum"
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
	Quiescent             bool
	Leaseholder           bool
}

func (input truncateDecisionInput) LogTooLarge() bool {
	return input.LogSize > input.MaxLogSize
}

type truncateDecision struct {
	Input       truncateDecisionInput
	QuorumIndex uint64 // largest index known to be present on quorum. Only when this is leaseholder.

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
		(n > 0 && td.Input.LogSize >= RaftLogQueueStaleSize) ||
		// If quiescent and not a leaseholder we can truncate everything, so truncate even if n is
		// small. At the leaseholder this truncation is going to propose to raft, and will cause it
		// not be quiescent, so only do so if there are enough entries for this to be worthwhile.
		// Additionally, for the leaseholder we want to avoid the pathological situation where
		// the raft entries after this truncation are too many and after becoming quiescent again we
		// again truncate and become not quiescent and so on.
		// TODO: is 3 the right lower bound here -- does queiscence happen
		// through raft. Ideally we would truncate before becoming quiescent but that seems complicated
		// to arrange due to the background queue behavior -- should we not bother with quiescence
		// based truncation at the leaseholder until we are done handling legacy cases?
		//
		// Note that in the quiescent case this code path is not being triggered by raft applies so
		// there isn't the danger that we will do many small truncation for this replica.
		(td.Input.Quiescent && (n > 3 || (n > 0 && !td.Input.Leaseholder)))
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

	// The last index is most aggressive possible truncation that we could do.
	// Everything else in this method makes the truncation less aggressive.
	decision.NewFirstIndex = decision.Input.LastIndex
	decision.ChosenVia = truncatableIndexChosenViaLastIndex

	if input.Leaseholder {
		// Start by protecting quorum index. Typically the quorum index will not be
		// smaller than the lastIndex (since lastIndex represents what has applied at this replica).
		// It can be substantially higher since RaftStatus.Progress.Match is updated on the leader when
		// a command is proposed and in a single replica Raft group this also means that
		// RaftStatus.Commit is updated at propose time.
		decision.QuorumIndex = getQuorumIndex(&input.RaftStatus)
		decision.ProtectIndex(decision.QuorumIndex, truncatableIndexChosenViaQuorumIndex)

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
					decision.ProtectIndex(decision.Input.FirstIndex, truncatableIndexChosenViaProbingFollower)
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

			// Otherwise, we let it truncate to the quorum index.
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
		if decision.NewFirstIndex < decision.Input.FirstIndex {
			decision.NewFirstIndex = decision.Input.FirstIndex
			decision.ChosenVia = truncatableIndexChosenViaFirstIndex
		}
	}
	// Invariants: NewFirstIndex >= FirstIndex
	//             NewFirstIndex <= LastIndex (if != 10)
	//             NewFirstIndex <= QuorumIndex (if != 0)
	//
	// For uninit'ed replicas we can have input.FirstIndex > input.LastIndex, more
	// specifically input.FirstIndex = input.LastIndex + 1. FirstIndex is set to
	// TruncatedState.Index + 1, and for an unit'ed replica, LastIndex is simply
	// 10. This is what informs the `input.LastIndex == 10` conditional below.
	valid := (decision.NewFirstIndex >= input.FirstIndex) &&
		(decision.NewFirstIndex <= input.LastIndex || input.LastIndex == 10) &&
		(decision.NewFirstIndex <= decision.QuorumIndex || decision.QuorumIndex == 0)
	if !valid {
		err := fmt.Sprintf("invalid truncation decision; output = %d, input: [%d, %d], quorum idx = %d",
			decision.NewFirstIndex, input.FirstIndex, input.LastIndex, decision.QuorumIndex)
		panic(err)
	}

	return decision
}

// getQuorumIndex returns the index which a quorum of the nodes have committed.
// Note that getQuorumIndex may return 0 if the progress map doesn't contain
// information for a sufficient number of followers (e.g. the local replica has
// only recently become the leader). In general, the value returned by
// getQuorumIndex may be smaller than raftStatus.Commit which is the log index
// that has been committed by a quorum of replicas where that quorum was
// determined at the time the index was written. If you're thinking of using
// getQuorumIndex for some purpose, consider that raftStatus.Commit might be
// more appropriate (e.g. determining if a replica is up to date).
func getQuorumIndex(raftStatus *raft.Status) uint64 {
	match := make([]uint64, 0, len(raftStatus.Progress))
	for _, progress := range raftStatus.Progress {
		if progress.State == tracker.StateReplicate {
			match = append(match, progress.Match)
		} else {
			match = append(match, 0)
		}
	}
	sort.Sort(uint64Slice(match))
	quorum := computeQuorum(len(match))
	return match[len(match)-quorum]
}

// shouldQueue determines whether a range should be queued for truncating. This
// is true only if the replica is the raft leader and if the total number of
// the range's raft log's stale entries exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, r *Replica, _ *config.SystemConfig,
) (shouldQ bool, priority float64) {
	decision, err := newTruncateDecision(ctx, r)
	if err != nil {
		log.Warning(ctx, err)
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
	return true, true, 1.0 + float64(decision.Input.MaxLogSize)/2.0
}

// process truncates the raft log of the range if the replica is the raft
// leader and if the total number of the range's raft log's stale entries
// exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) process(ctx context.Context, r *Replica, _ *config.SystemConfig) error {
	decision, err := newTruncateDecision(ctx, r)
	if err != nil {
		return err
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
			return errors.Wrap(err, "recomputing raft log size")
		}

		log.VEventf(ctx, 2, "recomputed raft log size to %s", humanizeutil.IBytes(n))

		// Override the decision, now that an accurate log size is available.
		decision, err = newTruncateDecision(ctx, r)
		if err != nil {
			return err
		}
	}

	// Can and should the raft logs be truncated?
	if decision.ShouldTruncate() {
		if n := decision.NumNewRaftSnapshots(); log.V(1) || n > 0 && rlq.logSnapshots.ShouldProcess(timeutil.Now()) {
			log.Info(ctx, decision.String())
		} else {
			log.VEvent(ctx, 1, decision.String())
		}
		if decision.Input.Leaseholder {
			b := &client.Batch{}
			b.AddRawRequest(&roachpb.TruncateLogRequest{
				RequestHeader: roachpb.RequestHeader{Key: r.Desc().StartKey.AsRawKey()},
				Index:         decision.NewFirstIndex,
				RangeID:       r.RangeID,
			})
			if err := rlq.db.Run(ctx, b); err != nil {
				return err
			}
		} else {
			if err := rlq.doLocalTruncation(ctx, r, decision.NewFirstIndex); err != nil {
				return err
			}
		}
		r.store.metrics.RaftLogTruncated.Inc(int64(decision.NumTruncatableIndexes()))
	} else {
		log.VEventf(ctx, 3, decision.String())
	}
	return nil
}

// timer returns interval between processing successive queued truncations.
func (*raftLogQueue) timer(_ time.Duration) time.Duration {
	return raftLogQueueTimerDuration
}

// purgatoryChan returns nil.
func (*raftLogQueue) purgatoryChan() <-chan time.Time {
	return nil
}

// TODO: deal with a concurrent snapshot being applied in applySnapshot()
func (rlq *raftLogQueue) doLocalTruncation(ctx context.Context, r *Replica, index uint64) error {
	// TODO: Can we assume not in Legacy19.1 and so don't need to delete the legacy key? Do we require
	// upgrades to go through all the major versions?
	// TODO: wrap all error returns with more info.

	// Compute which entries we need to delete and the term for the new truncated state.
	firstIndex, err := r.GetFirstIndex()
	if err != nil {
		return err
	}
	if firstIndex >= index {
		// nothing to do
		return nil
	}
	term, err := r.GetTerm(index)
	if err != nil {
		return err
	}

	// Compute the number of bytes freed by this truncation.
	start := keys.RaftLogKey(r.RangeID, firstIndex)
	end := keys.RaftLogKey(r.RangeID, index)
	batch := r.store.Engine().NewBatch()
	iter := batch.NewIterator(engine.IterOptions{UpperBound: end})
	defer iter.Close()
	// We can pass zero as nowNanos because we're only interested in SysBytes.
	ms, err := iter.ComputeStats(start, end, 0 /* nowNanos */)
	if err != nil {
		return err
	}
	bytes := -ms.SysBytes

	truncState, legacy, err := r.mu.stateLoader.LoadRaftTruncatedState(ctx, batch)
	if err != nil {
		return err
	}
	if legacy {
		// TODO: error?
		return fmt.Errorf("?")
	}

	// Add deletions for entries.
	prefixBuf := &r.mu.stateLoader.RangeIDPrefixBuf
	for idx := firstIndex; idx < index; idx++ {
		// NB: RangeIDPrefixBufs have sufficient capacity (32 bytes) to
		// avoid allocating when constructing Raft log keys (16 bytes).
		unsafeKey := prefixBuf.RaftLogKey(idx)
		if err := batch.Clear(engine.MakeMVCCMetadataKey(unsafeKey)); err != nil {
			return errors.Wrapf(err, "unable to clear truncated Raft entries")
		}
	}
	// Add put for new truncated state.
	truncState.Index = index
	truncState.Term = term
	if err := engine.MVCCPutProto(
		ctx, batch, nil /* ms */, prefixBuf.RaftTruncatedStateKey(),
		hlc.Timestamp{}, nil /* txn */, &truncState,
	); err != nil {
		return errors.Wrap(err, "unable to migrate RaftTruncatedState")
	}

	// Commit the batch.
	if err := batch.Commit(true); err != nil {
		return wrapWithNonDeterministicFailure(err, "unable to commit Raft entry batch")
	}
	batch.Close()

	// Make in-memory state consistent with this truncation, remove side-loaded entries and return
	// the size of the removed side-loaded entries.
	bytes += r.handleTruncatedStateResult(ctx, &truncState)
	// Update the size tracking.
	r.handleRaftLogDeltaResult(ctx, bytes)
	return nil
}

var _ sort.Interface = uint64Slice(nil)

// uint64Slice implements sort.Interface
type uint64Slice []uint64

// Len implements sort.Interface
func (a uint64Slice) Len() int { return len(a) }

// Swap implements sort.Interface
func (a uint64Slice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less implements sort.Interface
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
