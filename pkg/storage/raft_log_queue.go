// Copyright 2015 The Cockroach Authors.
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
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

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

// newRaftLogQueue returns a new instance of raftLogQueue.
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
// error occurs. If no truncation can be carried out, a zero decision is
// returned.
func newTruncateDecision(ctx context.Context, r *Replica) (*truncateDecision, error) {
	rangeID := r.RangeID
	now := timeutil.Now()

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

	firstIndex, err := r.raftFirstIndexLocked()
	pendingSnapshotIndex := r.getAndGCSnapshotLogTruncationConstraintsLocked(now)
	lastIndex := r.mu.lastIndex
	r.mu.Unlock()

	if err != nil {
		return nil, errors.Errorf("error retrieving first index for r%d: %s", rangeID, err)
	}

	if raftStatus == nil {
		if log.V(6) {
			log.Infof(ctx, "the raft group doesn't exist for r%d", rangeID)
		}
		return &truncateDecision{}, nil
	}

	// Is this the raft leader? We only perform log truncation on the raft leader
	// which has the up to date info on followers.
	if raftStatus.RaftState != raft.StateLeader {
		return &truncateDecision{}, nil
	}

	input := truncateDecisionInput{
		RaftStatus:                     raftStatus,
		LogSize:                        raftLogSize,
		MaxLogSize:                     targetSize,
		FirstIndex:                     firstIndex,
		LastIndex:                      lastIndex,
		PendingPreemptiveSnapshotIndex: pendingSnapshotIndex,
	}

	decision := computeTruncateDecision(input)
	return &decision, nil
}

const (
	truncatableIndexChosenViaQuorumIndex = "quorum"
	truncatableIndexChosenViaFollowers   = "followers"
	truncatableIndexChosenViaPendingSnap = "pending snapshot"
	truncatableIndexChosenViaFirstIndex  = "first index"
	truncatableIndexChosenViaLastIndex   = "last index"
)

type truncateDecisionInput struct {
	RaftStatus                     *raft.Status // never nil
	LogSize, MaxLogSize            int64
	FirstIndex, LastIndex          uint64
	PendingPreemptiveSnapshotIndex uint64
}

func (input truncateDecisionInput) LogTooLarge() bool {
	return input.LogSize > input.MaxLogSize
}

type truncateDecision struct {
	Input       truncateDecisionInput
	QuorumIndex uint64 // largest index known to be present on quorum

	NewFirstIndex uint64 // first index of the resulting log after truncation
	ChosenVia     string
}

func (td *truncateDecision) raftSnapshotsForIndex(index uint64) int {
	var n int
	for _, p := range td.Input.RaftStatus.Progress {
		if p.Match < index {
			n++
		}
	}
	if td.Input.PendingPreemptiveSnapshotIndex != 0 && td.Input.PendingPreemptiveSnapshotIndex < index {
		n++
	}

	return n
}

func (td *truncateDecision) NumNewRaftSnapshots() int {
	return td.raftSnapshotsForIndex(td.NewFirstIndex) - td.raftSnapshotsForIndex(td.Input.FirstIndex)
}

func (td *truncateDecision) String() string {
	var buf strings.Builder
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

	return buf.String()
}

func (td *truncateDecision) NumTruncatableIndexes() int {
	if td.NewFirstIndex < td.Input.FirstIndex {
		log.Fatalf(
			context.Background(),
			"invalid truncate decision: first index would move from %d to %d",
			td.Input.FirstIndex,
			td.NewFirstIndex,
		)
	}
	return int(td.NewFirstIndex - td.Input.FirstIndex)
}

func (td *truncateDecision) ShouldTruncate() bool {
	n := td.NumTruncatableIndexes()
	return n >= RaftLogQueueStaleThreshold ||
		(n > 0 && td.Input.LogSize >= RaftLogQueueStaleSize)
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
	decision.QuorumIndex = getQuorumIndex(input.RaftStatus)

	decision.NewFirstIndex = decision.QuorumIndex
	decision.ChosenVia = truncatableIndexChosenViaQuorumIndex

	if !input.LogTooLarge() {
		// Only truncate to one of the follower indexes if the raft log is less
		// than the target size. If the raft log is greater than the target size we
		// always truncate to the quorum commit index.
		for _, progress := range input.RaftStatus.Progress {
			index := progress.Match
			if decision.NewFirstIndex > index {
				decision.NewFirstIndex = index
				decision.ChosenVia = truncatableIndexChosenViaFollowers
			}
		}
	}

	// The pending snapshot index acts as a placeholder for a replica that is
	// about to be added to the range (or is in Raft recovery). We don't want to
	// truncate the log in a way that will require that new replica to be caught
	// up via yet another Raft snapshot.
	if input.PendingPreemptiveSnapshotIndex > 0 && decision.NewFirstIndex > input.PendingPreemptiveSnapshotIndex {
		decision.NewFirstIndex = input.PendingPreemptiveSnapshotIndex
		decision.ChosenVia = truncatableIndexChosenViaPendingSnap
	}

	// Advance to the first index, but never truncate past the quorum commit
	// index.
	if decision.NewFirstIndex < input.FirstIndex && input.FirstIndex <= decision.QuorumIndex {
		decision.NewFirstIndex = input.FirstIndex
		decision.ChosenVia = truncatableIndexChosenViaFirstIndex
	}
	// Never truncate past the last index. Naively, you would expect lastIndex to
	// never be smaller than quorumIndex, but RaftStatus.Progress.Match is
	// updated on the leader when a command is proposed and in a single replica
	// Raft group this also means that RaftStatus.Commit is updated at propose
	// time.
	if decision.NewFirstIndex > input.LastIndex {
		decision.NewFirstIndex = input.LastIndex
		decision.ChosenVia = truncatableIndexChosenViaLastIndex
	}

	// If new first index dropped below first index, make them equal (resulting
	// in a no-op).
	if decision.NewFirstIndex < decision.Input.FirstIndex {
		decision.NewFirstIndex = decision.Input.FirstIndex
		decision.ChosenVia = truncatableIndexChosenViaFirstIndex
	}
	return decision
}

// getQuorumIndex returns the index which a quorum of the nodes have
// committed. The snapshotLogTruncationConstraints indicates the index of a pending
// snapshot which is considered part of the Raft group even though it hasn't
// been added yet. Note that getQuorumIndex may return 0 if the progress map
// doesn't contain information for a sufficient number of followers (e.g. the
// local replica has only recently become the leader). In general, the value
// returned by getQuorumIndex may be smaller than raftStatus.Commit which is
// the log index that has been committed by a quorum of replicas where that
// quorum was determined at the time the index was written. If you're thinking
// of using getQuorumIndex for some purpose, consider that raftStatus.Commit
// might be more appropriate (e.g. determining if a replica is up to date).
func getQuorumIndex(raftStatus *raft.Status) uint64 {
	match := make([]uint64, 0, len(raftStatus.Progress))
	for _, progress := range raftStatus.Progress {
		match = append(match, progress.Match)
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
	return decision.ShouldTruncate(), float64(decision.Input.LogSize)
}

// process truncates the raft log of the range if the replica is the raft
// leader and if the total number of the range's raft log's stale entries
// exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) process(ctx context.Context, r *Replica, _ *config.SystemConfig) error {
	decision, err := newTruncateDecision(ctx, r)
	if err != nil {
		return err
	}

	// Can and should the raft logs be truncated?
	if decision.ShouldTruncate() {
		if n := decision.NumNewRaftSnapshots(); log.V(1) || n > 0 && rlq.logSnapshots.ShouldProcess(timeutil.Now()) {
			log.Info(ctx, decision)
		} else {
			log.VEvent(ctx, 1, decision.String())
		}
		b := &client.Batch{}
		b.AddRawRequest(&roachpb.TruncateLogRequest{
			RequestHeader: roachpb.RequestHeader{Key: r.Desc().StartKey.AsRawKey()},
			Index:         decision.NewFirstIndex,
			RangeID:       r.RangeID,
		})
		if err := rlq.db.Run(ctx, b); err != nil {
			return err
		}
		r.store.metrics.RaftLogTruncated.Inc(int64(decision.NumTruncatableIndexes()))
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

var _ sort.Interface = uint64Slice(nil)

// uint64Slice implements sort.Interface
type uint64Slice []uint64

// Len implements sort.Interface
func (a uint64Slice) Len() int { return len(a) }

// Swap implements sort.Interface
func (a uint64Slice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less implements sort.Interface
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
