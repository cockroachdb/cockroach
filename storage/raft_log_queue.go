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
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// raftLogQueueMaxSize is the max size of the queue.
	raftLogQueueMaxSize = 100
	// RaftLogQueueTimerDuration is the duration between truncations. This needs
	// to be relatively short so that truncations can keep up with raft log entry
	// creation.
	RaftLogQueueTimerDuration = 50 * time.Millisecond
	// RaftLogQueueStaleThreshold is the minimum threshold for stale raft log
	// entries. A stale entry is one which all replicas of the range have
	// progressed past and thus is no longer needed and can be truncated.
	RaftLogQueueStaleThreshold = 100
)

// raftLogQueue manages a queue of replicas slated to have their raft logs
// truncated by removing unneeded entries.
type raftLogQueue struct {
	baseQueue
	db *client.DB
}

// newRaftLogQueue returns a new instance of raftLogQueue.
func newRaftLogQueue(store *Store, db *client.DB, gossip *gossip.Gossip) *raftLogQueue {
	rlq := &raftLogQueue{
		db: db,
	}
	rlq.baseQueue = makeBaseQueue("raftlog", rlq, store, gossip, queueConfig{
		maxSize:              raftLogQueueMaxSize,
		needsLease:           false,
		acceptsUnsplitRanges: true,
	})
	return rlq
}

// getTruncatableIndexes returns the number of truncatable indexes and the
// oldest index that cannot be truncated for the replica.
// See computeTruncatableIndex.
func getTruncatableIndexes(r *Replica) (uint64, uint64, error) {
	rangeID := r.RangeID
	raftStatus := r.RaftStatus()
	if raftStatus == nil {
		if log.V(6) {
			log.Infof(context.TODO(), "the raft group doesn't exist for range %d", rangeID)
		}
		return 0, 0, nil
	}

	// Is this the raft leader? We only perform log truncation on the raft leader
	// which has the up to date info on followers.
	if raftStatus.RaftState != raft.StateLeader {
		return 0, 0, nil
	}

	r.mu.Lock()
	raftLogSize := r.mu.raftLogSize
	// We target the raft log size at the size of the replicated data. When
	// writing to a replica, it is common for the raft log to become larger than
	// the replicated data as the raft log contains the overhead of the
	// BatchRequest which includes the full transaction state as well as begin
	// and end transaction operations. If the estimated raft log size becomes
	// larger than the replica size, we're better off recovering the replica
	// using a snapshot.
	targetSize := r.mu.state.Stats.Total()
	if targetSize > r.mu.maxBytes {
		targetSize = r.mu.maxBytes
	}
	firstIndex, err := r.FirstIndex()
	pendingSnapshotIndex := r.mu.pendingSnapshotIndex
	r.mu.Unlock()
	if err != nil {
		return 0, 0, errors.Errorf("error retrieving first index for range %d: %s", rangeID, err)
	}

	truncatableIndex := computeTruncatableIndex(
		raftStatus, raftLogSize, targetSize, firstIndex, pendingSnapshotIndex)
	// Return the number of truncatable indexes.
	return truncatableIndex - firstIndex, truncatableIndex, nil
}

// computeTruncatableIndex returns the oldest index that cannot be
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
func computeTruncatableIndex(
	raftStatus *raft.Status,
	raftLogSize, targetSize int64,
	firstIndex, pendingSnapshotIndex uint64,
) uint64 {
	truncatableIndex := raftStatus.Commit
	if raftLogSize <= targetSize {
		// Only truncate to one of the behind indexes if the raft log is less than
		// the target size. If the raft log is greater than the target size we
		// always truncate to the quorum commit index.
		truncatableIndex = getBehindIndex(raftStatus)
		// The pending snapshot index acts as a placeholder for a replica that is
		// about to be added to the range. We don't want to truncate the log in a
		// way that will require that new replica to be caught up via a Raft
		// snapshot.
		if pendingSnapshotIndex > 0 && truncatableIndex > pendingSnapshotIndex {
			truncatableIndex = pendingSnapshotIndex
		}
		if truncatableIndex < firstIndex {
			truncatableIndex = firstIndex
		}
	}

	// Never truncate past the quorum committed index.
	if truncatableIndex > raftStatus.Commit {
		truncatableIndex = raftStatus.Commit
	}
	return truncatableIndex
}

// getBehindIndex returns the raft log index of the oldest node or the quorum
// commit index if all nodes are caught up.
func getBehindIndex(raftStatus *raft.Status) uint64 {
	behind := raftStatus.Commit
	for _, progress := range raftStatus.Progress {
		index := progress.Match
		if behind > index {
			behind = index
		}
	}
	return behind
}

// shouldQueue determines whether a range should be queued for truncating. This
// is true only if the replica is the raft leader and if the total number of
// the range's raft log's stale entries exceeds RaftLogQueueStaleThreshold.
func (*raftLogQueue) shouldQueue(
	now hlc.Timestamp, r *Replica, _ config.SystemConfig,
) (shouldQ bool, priority float64) {
	truncatableIndexes, _, err := getTruncatableIndexes(r)
	if err != nil {
		log.Warning(context.TODO(), err)
		return false, 0
	}

	return truncatableIndexes >= RaftLogQueueStaleThreshold, float64(truncatableIndexes)
}

// process truncates the raft log of the range if the replica is the raft
// leader and if the total number of the range's raft log's stale entries
// exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) process(
	ctx context.Context,
	now hlc.Timestamp,
	r *Replica,
	_ config.SystemConfig,
) error {
	truncatableIndexes, oldestIndex, err := getTruncatableIndexes(r)
	if err != nil {
		return err
	}

	// Can and should the raft logs be truncated?
	if truncatableIndexes >= RaftLogQueueStaleThreshold {
		if log.V(1) {
			log.Infof(ctx, "%s: truncating raft log %d-%d",
				r, oldestIndex-truncatableIndexes, oldestIndex)
		}
		b := &client.Batch{}
		b.AddRawRequest(&roachpb.TruncateLogRequest{
			Span:    roachpb.Span{Key: r.Desc().StartKey.AsRawKey()},
			Index:   oldestIndex,
			RangeID: r.RangeID,
		})
		return rlq.db.Run(b)
	}
	return nil
}

// timer returns interval between processing successive queued truncations.
func (*raftLogQueue) timer() time.Duration {
	return RaftLogQueueTimerDuration
}

// purgatoryChan returns nil.
func (*raftLogQueue) purgatoryChan() <-chan struct{} {
	return nil
}
