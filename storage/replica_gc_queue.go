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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// replicaGCQueueMaxSize is the max size of the gc queue.
	replicaGCQueueMaxSize = 100

	// replicaGCQueueTimerDuration is the duration between GCs of queued replicas.
	replicaGCQueueTimerDuration = 10 * time.Second

	// ReplicaGCQueueInactivityThreshold is the inactivity duration after which
	// a range will be considered for garbage collection. Exported for testing.
	ReplicaGCQueueInactivityThreshold = 10 * 24 * time.Hour // 10 days
)

// replicaGCQueue manages a queue of replicas to be considered for garbage
// collections. The GC process asynchronously removes local data for
// ranges that have been rebalanced away from this store.
type replicaGCQueue struct {
	baseQueue
	db *client.DB
}

// newReplicaGCQueue returns a new instance of replicaGCQueue.
func newReplicaGCQueue(db *client.DB, gossip *gossip.Gossip) *replicaGCQueue {
	q := &replicaGCQueue{
		db: db,
	}
	q.baseQueue = makeBaseQueue("replicaGC", q, gossip, replicaGCQueueMaxSize)
	return q
}

func (*replicaGCQueue) needsLeaderLease() bool {
	return false
}

func (*replicaGCQueue) acceptsUnsplitRanges() bool {
	return true
}

// shouldQueue determines whether a replica should be queued for GC,
// and if so at what priority. To be considered for possible GC, a
// replica's leader lease must not have been active for longer than
// ReplicaGCQueueInactivityThreshold. Further, the last replica GC
// check must have occurred more than ReplicaGCQueueInactivityThreshold
// in the past.
func (*replicaGCQueue) shouldQueue(now roachpb.Timestamp, rng *Replica, _ config.SystemConfig) (bool, float64) {
	lastCheck, err := rng.getLastReplicaGCTimestamp()
	if err != nil {
		log.Errorf("could not read last replica GC timestamp: %s", err)
		return false, 0
	}
	// Return false immediately if the previous check was less than the
	// check interval in the past.
	if now.Less(lastCheck.Add(ReplicaGCQueueInactivityThreshold.Nanoseconds(), 0)) {
		return false, 0
	}
	// Return whether or not lease activity occurred within the inactivity threshold.
	lease, _ := rng.getLeaderLease()
	return lease.Expiration.Add(ReplicaGCQueueInactivityThreshold.Nanoseconds(), 0).Less(now), 0
}

// process performs a consistent lookup on the range descriptor to see if we are
// still a member of the range.
func (q *replicaGCQueue) process(now roachpb.Timestamp, rng *Replica, _ config.SystemConfig) error {
	// Note that the Replicas field of desc is probably out of date, so
	// we should only use `desc` for its static fields like RangeID and
	// StartKey (and avoid rng.GetReplica() for the same reason).
	desc := rng.Desc()

	// Calls to RangeLookup typically use inconsistent reads, but we
	// want to do a consistent read here. This is important when we are
	// considering one of the metadata ranges: we must not do an
	// inconsistent lookup in our own copy of the range.
	b := &client.Batch{}
	b.InternalAddRequest(&roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			Key: keys.RangeMetaKey(desc.StartKey),
		},
		MaxRanges: 1,
	})
	br, pErr := q.db.RunWithResponse(b)
	if pErr != nil {
		return pErr.GoError()
	}
	reply := br.Responses[0].GetInner().(*roachpb.RangeLookupResponse)

	if len(reply.Ranges) != 1 {
		return util.Errorf("expected 1 range descriptor, got %d", len(reply.Ranges))
	}

	replyDesc := reply.Ranges[0]
	currentMember := false
	storeID := rng.store.StoreID()
	for _, rep := range replyDesc.Replicas {
		if rep.StoreID == storeID {
			currentMember = true
			break
		}
	}

	if !currentMember {
		// We are no longer a member of this range; clean up our local data.
		if log.V(1) {
			log.Infof("destroying local data from range %d", desc.RangeID)
		}
		if err := rng.store.RemoveReplica(rng, replyDesc, true); err != nil {
			return err
		}
	} else if desc.RangeID != replyDesc.RangeID {
		// If we get a different  range ID back, then the range has been merged
		// away. But currentMember is true, so we are still a member of the
		// subsuming range. Shut down raft processing for the former range
		// and delete any remaining metadata, but do not delete the data.
		if log.V(1) {
			log.Infof("removing merged range %d", desc.RangeID)
		}
		if err := rng.store.RemoveReplica(rng, replyDesc, false); err != nil {
			return err
		}

		// TODO(bdarnell): remove raft logs and other metadata (while leaving a
		// tombstone). Add tests for GC of merged ranges.
	} else {
		// This range is a current member of the raft group. Set the last replica
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
