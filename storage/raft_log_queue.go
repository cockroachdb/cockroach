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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft"
)

const (
	// raftLogQueueMaxSize is the max size of the queue.
	raftLogQueueMaxSize = 100
	// RaftLogQueueTimerDuration is the duration between checking the
	// raft logs.
	RaftLogQueueTimerDuration = time.Second
	// RaftLogQueueStaleThreshold is the minimum threshold for stale raft log
	// entries. A stale entry is one which all replicas of the range have
	// progressed past and thus is no longer needed and can be pruned.
	RaftLogQueueStaleThreshold = 1
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
	rlq.baseQueue = makeBaseQueue("raftlog", rlq, gossip, raftLogQueueMaxSize)
	return rlq
}

func (*raftLogQueue) needsLeaderLease() bool {
	return false
}

func (*raftLogQueue) acceptsUnsplitRanges() bool {
	return true
}

// getTruncatableIndexes returns the total number of stale raft log entries that
// can be truncated and the oldest index that cannot be pruned.
func getTruncatableIndexes(r *Replica) (uint64, uint64, error) {
	rangeID := r.RangeID
	raftStatus := r.store.RaftStatus(rangeID)
	if raftStatus == nil {
		if log.V(1) {
			log.Infof("the raft group doesn't exist for range %d", rangeID)
		}
		return 0, 0, nil
	}

	// Is this the raft leader?
	if raftStatus.RaftState != raft.StateLeader {
		return 0, 0, nil
	}

	// Find the oldest index still in use by the range.
	oldestIndex := raftStatus.Applied
	for _, progress := range raftStatus.Progress {
		if progress.Match < oldestIndex {
			oldestIndex = progress.Match
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	firstIndex, err := r.FirstIndex()
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
func (*raftLogQueue) shouldQueue(now roachpb.Timestamp, r *Replica, _ config.SystemConfig) (shouldQ bool,
	priority float64) {

	truncatableIndexes, _, err := getTruncatableIndexes(r)
	if err != nil {
		log.Warning(err)
		return false, 0
	}

	return truncatableIndexes > RaftLogQueueStaleThreshold, float64(truncatableIndexes)
}

// process truncates the raft log of the range if the replica is the raft
// leader and if the total number of the range's raft log's stale entries
// exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) process(now roachpb.Timestamp, r *Replica, _ config.SystemConfig) error {

	truncatableIndexes, oldestIndex, err := getTruncatableIndexes(r)
	if err != nil {
		return err
	}

	// Can and should the raft logs be truncated?
	if truncatableIndexes > RaftLogQueueStaleThreshold {
		if log.V(1) {
			log.Infof("truncating the raft log of range %d to %d", r.RangeID, oldestIndex)
		}
		b := &client.Batch{}
		b.InternalAddRequest(&roachpb.TruncateLogRequest{
			Span:    roachpb.Span{Key: r.Desc().StartKey.AsRawKey()},
			Index:   oldestIndex,
			RangeID: r.RangeID,
		})
		return rlq.db.Run(b).GoError()
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
