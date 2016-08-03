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
// Author: Ben Darnell

package storage

import (
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// replicaGCQueueMaxSize is the max size of the gc queue.
	replicaGCQueueMaxSize = 100

	// replicaGCQueueTimerDuration is the duration between GCs of queued replicas.
	replicaGCQueueTimerDuration = 50 * time.Millisecond

	// ReplicaGCQueueInactivityThreshold is the inactivity duration after which
	// a range will be considered for garbage collection. Exported for testing.
	ReplicaGCQueueInactivityThreshold = 10 * 24 * time.Hour // 10 days
	// ReplicaGCQueueCandidateTimeout is the duration after which a range in
	// candidate Raft state (which is a typical sign of having been removed
	// from the group) will be considered for garbage collection.
	ReplicaGCQueueCandidateTimeout = 1 * time.Second
)

// replicaGCQueue manages a queue of replicas to be considered for garbage
// collections. The GC process asynchronously removes local data for
// ranges that have been rebalanced away from this store.
type replicaGCQueue struct {
	baseQueue
	db *client.DB
}

// newReplicaGCQueue returns a new instance of replicaGCQueue.
func newReplicaGCQueue(store *Store, db *client.DB, gossip *gossip.Gossip) *replicaGCQueue {
	q := &replicaGCQueue{
		db: db,
	}
	q.baseQueue = makeBaseQueue("replicaGC", q, store, gossip, queueConfig{
		maxSize:              replicaGCQueueMaxSize,
		needsLease:           false,
		acceptsUnsplitRanges: true,
	})
	return q
}

// shouldQueue determines whether a replica should be queued for GC,
// and if so at what priority. To be considered for possible GC, a
// replica's range lease must not have been active for longer than
// ReplicaGCQueueInactivityThreshold. Further, the last replica GC
// check must have occurred more than ReplicaGCQueueInactivityThreshold
// in the past.
func (*replicaGCQueue) shouldQueue(now hlc.Timestamp, rng *Replica, _ config.SystemConfig) (bool, float64) {
	lastCheck, err := rng.getLastReplicaGCTimestamp()
	if err != nil {
		log.Errorf(context.TODO(), "could not read last replica GC timestamp: %s", err)
		return false, 0
	}

	lastActivity := hlc.ZeroTimestamp.Add(rng.store.startedAt, 0)

	lease, nextLease := rng.getLease()
	if lease != nil {
		lastActivity.Forward(lease.Expiration)
	}
	if nextLease != nil {
		lastActivity.Forward(nextLease.Expiration)
	}

	var isCandidate bool
	if raftStatus := rng.RaftStatus(); raftStatus != nil {
		isCandidate = (raftStatus.SoftState.RaftState == raft.StateCandidate)
	}
	return replicaGCShouldQueueImpl(now, lastCheck, lastActivity, isCandidate)
}

func replicaGCShouldQueueImpl(
	now, lastCheck, lastActivity hlc.Timestamp, isCandidate bool,
) (bool, float64) {
	timeout := ReplicaGCQueueInactivityThreshold
	var priority float64

	if isCandidate {
		// If the range is a candidate (which happens if its former replica set
		// ignores it), let it expire much earlier.
		timeout = ReplicaGCQueueCandidateTimeout
		priority++
	} else if now.Less(lastCheck.Add(ReplicaGCQueueInactivityThreshold.Nanoseconds(), 0)) {
		// Return false immediately if the previous check was less than the
		// check interval in the past. Note that we don't do this is the
		// replica is in candidate state, in which case we want to be more
		// aggressive - a failed rebalance attempt could have checked this
		// range, and candidate state suggests that a retry succeeded. See
		// #7489.
		return false, 0
	}

	shouldQ := lastActivity.Add(timeout.Nanoseconds(), 0).Less(now)

	if !shouldQ {
		return false, 0
	}

	return shouldQ, priority
}

// process performs a consistent lookup on the range descriptor to see if we are
// still a member of the range.
func (q *replicaGCQueue) process(
	ctx context.Context,
	now hlc.Timestamp,
	rng *Replica,
	_ config.SystemConfig,
) error {
	// Note that the Replicas field of desc is probably out of date, so
	// we should only use `desc` for its static fields like RangeID and
	// StartKey (and avoid rng.GetReplica() for the same reason).
	desc := rng.Desc()

	// Calls to RangeLookup typically use inconsistent reads, but we
	// want to do a consistent read here. This is important when we are
	// considering one of the metadata ranges: we must not do an
	// inconsistent lookup in our own copy of the range.
	b := &client.Batch{}
	b.AddRawRequest(&roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			Key: keys.RangeMetaKey(desc.StartKey),
		},
		MaxRanges: 1,
	})
	if err := q.db.Run(b); err != nil {
		return err
	}
	br := b.RawResponse()
	reply := br.Responses[0].GetInner().(*roachpb.RangeLookupResponse)

	if len(reply.Ranges) != 1 {
		return errors.Errorf("expected 1 range descriptor, got %d", len(reply.Ranges))
	}

	replyDesc := reply.Ranges[0]
	if _, currentMember := replyDesc.GetReplicaDescriptor(rng.store.StoreID()); !currentMember {
		// We are no longer a member of this range; clean up our local data.
		if log.V(1) {
			log.Infof(ctx, "destroying local data from range %d", desc.RangeID)
		}
		log.Trace(ctx, "destroying local data")
		if err := rng.store.RemoveReplica(rng, replyDesc, true); err != nil {
			return err
		}
	} else if desc.RangeID != replyDesc.RangeID {
		// If we get a different range ID back, then the range has been merged
		// away. But currentMember is true, so we are still a member of the
		// subsuming range. Shut down raft processing for the former range
		// and delete any remaining metadata, but do not delete the data.
		if log.V(1) {
			log.Infof(ctx, "removing merged range %d", desc.RangeID)
		}
		log.Trace(ctx, "removing merged range")
		if err := rng.store.RemoveReplica(rng, replyDesc, false); err != nil {
			return err
		}

		// TODO(bdarnell): remove raft logs and other metadata (while leaving a
		// tombstone). Add tests for GC of merged ranges.
	} else {
		// This replica is a current member of the raft group. Set the last replica
		// GC check time to avoid re-processing for another check interval.
		if err := rng.setLastReplicaGCTimestamp(now); err != nil {
			return err
		}
	}

	return nil
}

func (*replicaGCQueue) timer() time.Duration {
	return replicaGCQueueTimerDuration
}

// purgatoryChan returns nil.
func (*replicaGCQueue) purgatoryChan() <-chan struct{} {
	return nil
}
