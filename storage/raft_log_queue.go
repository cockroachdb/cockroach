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
	"math"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
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
	// progressed past and thus is no longer needed and can be pruned.
	RaftLogQueueStaleThreshold = 100
)

// raftLogQueue manages a queue of replicas slated to have their raft logs
// truncated by removing unneeded entries.
type raftLogQueue struct {
	baseQueue
	db *client.DB
}

// newRaftLogQueue returns a new instance of raftLogQueue.
func newRaftLogQueue(db *client.DB, gossip *gossip.Gossip) *raftLogQueue {
	rlq := &raftLogQueue{
		db: db,
	}
	rlq.baseQueue = makeBaseQueue("raftlog", rlq, gossip, queueConfig{
		maxSize:              raftLogQueueMaxSize,
		needsLeaderLease:     false,
		acceptsUnsplitRanges: true,
	})
	return rlq
}

// getTruncatableIndexes returns the total number of stale raft log entries that
// can be truncated and the oldest index that cannot be pruned. If estimate is
// true, it returns a resource cheap estimate that can be used for scheduling
// purposes.
func getTruncatableIndexes(r *Replica, estimate bool) (uint64, uint64, error) {
	rangeID := r.RangeID
	// TODO(bram): r.store.RaftStatus(rangeID) differs from r.RaftStatus() in
	// tests, causing TestGetTruncatableIndexes to fail. Figure out why and fix.
	raftStatus := r.store.RaftStatus(rangeID)
	if raftStatus == nil {
		if log.V(1) {
			log.Infof("the raft group doesn't exist for range %d", rangeID)
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
	targetSize := r.mu.maxBytes
	r.mu.Unlock()

	// Always truncate the oldest raft log entry which has been committed by all replicas.
	oldestIndex := getMaximumMatchedIndex(raftStatus)

	// Truncate raft logs if the log is too big.
	if raftLogSize > targetSize {
		if estimate {
			// When estimating assume that the truncate threshold has been reached.
			oldestIndex += RaftLogQueueStaleThreshold
		} else {
			oldestSizeIndex, err := getLogIndexForSize(r.store.Engine(), r.RangeID, raftLogSize, targetSize)
			if err != nil {
				return 0, 0, err
			}
			if oldestSizeIndex > oldestIndex {
				oldestIndex = oldestSizeIndex
			}
		}
	}

	// Never truncate uncommitted logs.
	if oldestIndex > raftStatus.Commit {
		oldestIndex = raftStatus.Commit
	}

	firstIndex, err := r.GetFirstIndex()
	if err != nil {
		return 0, 0, util.Errorf("error retrieving first index for range %d: %s", rangeID, err)
	}

	if oldestIndex < firstIndex {
		return 0, 0, util.Errorf("raft log's oldest index (%d) is less than the first index (%d) for range %d",
			oldestIndex, firstIndex, rangeID)
	}

	// Return the number of truncatable indexes.
	return oldestIndex - firstIndex, oldestIndex, nil
}

// shouldQueue determines whether a range should be queued for truncating. This
// is true only if the replica is the raft leader and if the total number of
// the range's raft log's stale entries exceeds RaftLogQueueStaleThreshold.
func (*raftLogQueue) shouldQueue(
	now hlc.Timestamp, r *Replica, _ config.SystemConfig,
) (shouldQ bool, priority float64) {
	truncatableIndexes, _, err := getTruncatableIndexes(r, true /* estimate */)
	if err != nil {
		log.Warning(err)
		return false, 0
	}

	return truncatableIndexes >= RaftLogQueueStaleThreshold, float64(truncatableIndexes)
}

// process truncates the raft log of the range if the replica is the raft
// leader and if the total number of the range's raft log's stale entries
// exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) process(now hlc.Timestamp, r *Replica, _ config.SystemConfig) error {
	truncatableIndexes, oldestIndex, err := getTruncatableIndexes(r, false /* estimate */)
	if err != nil {
		return err
	}

	// Can and should the raft logs be truncated?
	if truncatableIndexes >= RaftLogQueueStaleThreshold {
		if log.V(1) {
			log.Infof("truncating the raft log of range %d to %d", r.RangeID, oldestIndex)
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

// getMaximumMatchedIndex returns the index that has been committed by every
// node in the raft group.
func getMaximumMatchedIndex(raftStatus *raft.Status) uint64 {
	smallestMatch := uint64(math.MaxUint64)
	for _, progress := range raftStatus.Progress {
		if smallestMatch > progress.Match {
			smallestMatch = progress.Match
		}
	}
	return smallestMatch
}

// getLogIndexForSize returns the oldest raft log index that keeps the log size
// under the target size. If there are no log entries 0 is returned.
func getLogIndexForSize(eng engine.Engine, rangeID roachpb.RangeID, currentSize, targetSize int64) (uint64, error) {
	var entry raftpb.Entry
	err := iterateRaftLog(eng, rangeID, func(kv roachpb.KeyValue) (bool, error) {
		currentSize -= int64(kv.Size())
		if currentSize < targetSize {
			return true, kv.Value.GetProto(&entry)
		}
		return false, nil
	})
	return entry.Index, err
}
