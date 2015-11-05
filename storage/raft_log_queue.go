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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
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
	// RaftLogQueueLogSizeThreshold is the number of raft log entries after
	// which truncation is required.
	RaftLogQueueLogSizeThreshold = 1
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

// getOldestIndex returns the the oldest index still in use for the passed in
// raft status.
func getOldestIndex(raftStatus raft.Status) uint64 {
	// Find the oldest index still in use by the range.
	oldestIndex := raftStatus.Applied
	for _, progress := range raftStatus.Progress {
		if progress.Match < oldestIndex {
			oldestIndex = progress.Match
		}
	}

	return oldestIndex
}

// shouldQueue determines whether a range should be queued for truncating. This
// is true only if the replica is the raft leader and if the range's raft log's
// old entries exceeds the size threshold.
func (*raftLogQueue) shouldQueue(now roachpb.Timestamp, r *Replica, _ *config.SystemConfig) (shouldQ bool,
	priority float64) {
	raftLeader, raftStatus, err := r.isRaftLeader()
	if !raftLeader {
		if err != nil {
			log.Warning(err)
		}
		return false, 0
	}

	firstIndex, err := r.FirstIndex()
	if err != nil {
		log.Warningf("error retrieving first index for range %s: %s", r.Desc(), err)
		return false, 0
	}

	oldestIndex := getOldestIndex(raftStatus)

	// Can and should the raft logs be truncated?
	truncatableIndexes := oldestIndex - firstIndex
	return truncatableIndexes > RaftLogQueueLogSizeThreshold, float64(truncatableIndexes)
}

// process synchronously invokes for each replica that requires a raft log
// truncation.
func (rlq *raftLogQueue) process(now roachpb.Timestamp, r *Replica, _ *config.SystemConfig) error {
	raftLeader, raftStatus, err := r.isRaftLeader()
	if !raftLeader {
		if err != nil {
			return err
		}
		return nil
	}

	firstIndex, err := r.FirstIndex()
	if err != nil {
		return util.Errorf("error retrieving first index for range %s: %s", r.Desc(), err)
	}

	oldestIndex := getOldestIndex(raftStatus)

	// Can and should the raft logs be truncated?
	truncatableIndexes := oldestIndex - firstIndex
	if truncatableIndexes > RaftLogQueueLogSizeThreshold {
		if log.V(1) {
			log.Infof("truncating the raft log of range %d from %d to %d", r.Desc().RangeID, firstIndex, oldestIndex)
		}
		b := &client.Batch{}
		b.InternalAddRequest(&roachpb.TruncateLogRequest{Index: oldestIndex})
		return rlq.db.Run(b)
	}
	return nil
}

// timer returns interval between processing successive queued truncations.
func (*raftLogQueue) timer() time.Duration {
	return RaftLogQueueTimerDuration
}
