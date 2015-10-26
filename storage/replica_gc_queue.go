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
// Author: Ben Darnell

package storage

import (
	"sync"
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
	db     *client.DB
	locker sync.Locker
}

// newReplicaGCQueue returns a new instance of replicaGCQueue.
func newReplicaGCQueue(db *client.DB, gossip *gossip.Gossip, locker sync.Locker) *replicaGCQueue {
	q := &replicaGCQueue{
		db:     db,
		locker: locker,
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

// shouldQueue determines whether a replica should be queued for GC, and
// if so at what priority. Replicas which have been inactive for longer
// than ReplicaGCQueueInactivityThreshold are considered for possible GC
// at equal priority.
func (*replicaGCQueue) shouldQueue(now roachpb.Timestamp, rng *Replica,
	_ *config.SystemConfig) (bool, float64) {

	if l := rng.getLease(); l.Expiration.Add(ReplicaGCQueueInactivityThreshold.Nanoseconds(), 0).Less(now) {
		return true, 0
	}
	return false, 0
}

// process performs a consistent lookup on the range descriptor to see if we are
// still a member of the range.
func (q *replicaGCQueue) process(now roachpb.Timestamp, rng *Replica, _ *config.SystemConfig) error {
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
	br, err := q.db.RunWithResponse(b)
	if err != nil {
		return err
	}
	reply := br.Responses[0].GetInner().(*roachpb.RangeLookupResponse)

	if len(reply.Ranges) != 1 {
		return util.Errorf("expected 1 range descriptor, got %d", len(reply.Ranges))
	}

	replyDesc := reply.Ranges[0]
	currentMember := false
	if me := rng.GetReplica(); me != nil {
		for _, rep := range replyDesc.Replicas {
			if rep.StoreID == me.StoreID {
				currentMember = true
				break
			}
		}
	}

	if !currentMember {
		// We are no longer a member of this range; clean up our local data.
		if log.V(1) {
			log.Infof("destroying local data from range %d", desc.RangeID)
		}
		if err := rng.store.RemoveReplica(rng); err != nil {
			return err
		}

		// Lock the store to prevent a new replica of the range from being
		// added while we're deleting the previous one. We'd really like
		// to do this before calling RemoveReplica, but this could
		// deadlock with other work on the Store.processRaft goroutine.
		// Instead, we check after acquiring the lock to make sure the
		// range is still absent.
		q.locker.Lock()
		defer q.locker.Unlock()

		if _, err := rng.store.GetReplica(desc.RangeID); err == nil {
			log.Infof("replica recreated during deletion; aborting deletion")
			return nil
		}

		if err := rng.Destroy(); err != nil {
			return err
		}
	} else if desc.RangeID != desc.RangeID {
		// If we get a different  range ID back, then the range has been merged
		// away. But currentMember is true, so we are still a member of the
		// subsuming range. Shut down raft processing for the former range
		// and delete any remaining metadata, but do not delete the data.
		if log.V(1) {
			log.Infof("removing merged range %d", desc.RangeID)
		}
		if err := rng.store.RemoveReplica(rng); err != nil {
			return err
		}

		// TODO(bdarnell): remove raft logs and other metadata (while leaving a
		// tombstone). Add tests for GC of merged ranges.
	} else {
		// This range is a current member of the raft group. Acquire the lease
		// to avoid processing this range again before the next inactivity threshold.
		if err := rng.requestLeaderLease(now); err != nil {
			if _, ok := err.(*roachpb.LeaseRejectedError); !ok {
				if log.V(1) {
					log.Infof("unable to acquire lease from valid range %s: %s", rng, err)
				}
			}
		}
	}

	return nil
}

func (*replicaGCQueue) timer() time.Duration {
	return replicaGCQueueTimerDuration
}
