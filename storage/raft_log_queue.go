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
	"sort"
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
		if log.V(1) {
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
	targetSize := r.mu.maxBytes
	firstIndex, err := r.FirstIndex()
	r.mu.Unlock()
	if err != nil {
		return 0, 0, errors.Errorf("error retrieving first index for range %d: %s", rangeID, err)
	}

	truncatableIndex := computeTruncatableIndex(raftStatus, raftLogSize, targetSize, firstIndex)
	// Return the number of truncatable indexes.
	return truncatableIndex - firstIndex, truncatableIndex, nil
}

// computeTruncatableIndex returns the oldest index that cannot be truncated. If
// there is a behind node, we want to keep old raft logs so it can catch up
// without having to send a full snapshot. However, if a node down is down long
// enough, sending a snapshot is more efficient and we should truncate the log
// to the next behind node or the quorum committed index. We currently truncate
// when the raft log size is bigger than the range max bytes value (typically
// 64MB). When there are no nodes behind, or we can't catch any of them up via
// the raft log (due to a previous truncation) this returns the quorum committed
// index.
func computeTruncatableIndex(raftStatus *raft.Status, raftLogSize, targetSize int64, firstIndex uint64) uint64 {
	truncatableIndex := raftStatus.Commit
	behindIndexes := getBehindIndexes(raftStatus)
	for _, behindIndex := range behindIndexes {
		// If the behind index is beyond the first index, that means some node has
		// caught up (or one is too far behind) and we should truncate the log to
		// the behind index.
		// If the behind index equals the first index and the raft log is within the
		// target size, don't truncate it any further. However, if it's bigger than
		// the target size, truncate to the next behind index. This allows for
		// multiple nodes to be behind and not give up on the more recently behind
		// nodes.
		if behindIndex > firstIndex || (behindIndex == firstIndex && raftLogSize <= targetSize) {
			truncatableIndex = behindIndex
			break
		}
	}

	// Never truncate past the quorum committed index.
	if truncatableIndex > raftStatus.Commit {
		truncatableIndex = raftStatus.Commit
	}

	return truncatableIndex
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
			log.Infof(ctx, "truncating the raft log of range %d to %d", r.RangeID, oldestIndex)
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

// getBehindIndexes returns the indexes of nodes that are behind the quorum
// commit index without duplicates and sorted from most behind to most recent.
func getBehindIndexes(raftStatus *raft.Status) []uint64 {
	var behind []uint64
	behindUniq := make(map[uint64]struct{})
	for _, progress := range raftStatus.Progress {
		index := progress.Match
		if index < raftStatus.Commit {
			if _, ok := behindUniq[index]; ok {
				continue
			}
			behindUniq[index] = struct{}{}
			behind = append(behind, index)
		}
	}
	sort.Sort(uint64Slice(behind))
	return behind
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
