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
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// rangeGCQueueMaxSize is the max size of the gc queue.
	rangeGCQueueMaxSize = 100

	// rangeGCQueueTimerDuration is the duration between GCs of queued ranges.
	rangeGCQueueTimerDuration = 10 * time.Second
)

// rangeGCQueue manages a queue of ranges to be considered for garbage
// collections. The GC process asynchronously removes local data for
// ranges that have been rebalanced away from this store.
type rangeGCQueue struct {
	*baseQueue
	db *client.KV
}

// newRangeGCQueue returns a new instance of rangeGCQueue.
func newRangeGCQueue(db *client.KV) *rangeGCQueue {
	q := &rangeGCQueue{
		db: db,
	}
	q.baseQueue = newBaseQueue("rangeGC", q, rangeGCQueueMaxSize)
	return q
}

func (q *rangeGCQueue) needsLeaderLease() bool {
	return false
}

// shouldQueue determins whether a range should be queued for GC,
// and if so at what priority. Currently all inactive ranges are
// considered for possible GC at equal priority.
func (q *rangeGCQueue) shouldQueue(now proto.Timestamp, rng *Range) (bool, float64) {
	if _, expired := rng.HasLeaderLease(now); !expired {
		// If anyone holds a non-expired lease and we know about it, we
		// have recently been an active member of the range.  This is not
		// a guarantee that the range is not garbage (we could have just
		// been removed), but leader leases are short enough that it
		// doesn't hurt to wait for them to expire, and this gives us a
		// cheap heuristic to avoid unnecessary queries.
		return false, 0
	}

	// Otherwise, continue to processing.
	return true, 0
}

// process performs a consistent lookup on the range descriptor to see if we are
// still a member of the range.
func (q *rangeGCQueue) process(now proto.Timestamp, rng *Range) error {
	// Calls to InternalRangeLookup typically use inconsistent reads, but we
	// want to do a consistent read here. This is important when we are
	// considering one of the metadata ranges: we must not do an inconsistent
	// lookup in our own copy of the range.
	reply := proto.InternalRangeLookupResponse{}
	err := q.db.Run(client.Call{
		Args: &proto.InternalRangeLookupRequest{
			RequestHeader: proto.RequestHeader{
				Key: engine.RangeMetaKey(rng.Desc().StartKey),
			},
			MaxRanges: 1,
		},
		Reply: &reply,
	})
	if err != nil {
		return err
	}

	if len(reply.Ranges) != 1 {
		return util.Errorf("expected 1 range descriptor, got %d", len(reply.Ranges))
	}
	desc := reply.Ranges[0]

	currentMember := false
	me := rng.GetReplica()
	for _, rep := range desc.Replicas {
		if rep.StoreID == me.StoreID {
			currentMember = true
			break
		}
	}

	if !currentMember {
		// We are no longer a member of this range; clean up our local data.
		log.V(1).Infof("destroying local data from range %d", rng.Desc().RaftID)
		if err := rng.rm.RemoveRange(rng); err != nil {
			return err
		}
		// TODO(bdarnell): update Destroy to leave tombstones for removed ranges (#768)
		// TODO(bdarnell): add some sort of locking to prevent the range
		// from being recreated while the underlying data is being destroyed.
		if err := rng.Destroy(); err != nil {
			return err
		}
	} else if desc.RaftID != rng.Desc().RaftID {
		// If we get a different raft ID back, then the range has been merged
		// away. But currentMember is true, so we are still a member of the
		// subsuming range. Shut down raft processing for the former range
		// and delete any remaining metadata, but do not delete the data.
		log.V(1).Infof("removing merged range %d", rng.Desc().RaftID)
		if err := rng.rm.RemoveRange(rng); err != nil {
			return err
		}

		// TODO(bdarnell): remove raft logs and other metadata (while leaving a
		// tombstone). Add tests for GC of merged ranges.
	}

	return nil
}

func (q *rangeGCQueue) timer() time.Duration {
	return rangeGCQueueTimerDuration
}
