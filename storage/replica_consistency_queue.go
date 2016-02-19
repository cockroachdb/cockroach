// Copyright 2016 The Cockroach Authors.
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
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	replicaConsistencyQueueSize = 100
)

type replicaConsistencyQueue struct {
	baseQueue
}

// newReplicaConsistencyQueue returns a new instance of replicaConsistencyQueue.
func newReplicaConsistencyQueue(gossip *gossip.Gossip) *replicaConsistencyQueue {
	rcq := &replicaConsistencyQueue{}
	rcq.baseQueue = makeBaseQueue("replica consistency checker", rcq, gossip, replicaConsistencyQueueSize)
	return rcq
}

func (*replicaConsistencyQueue) needsLeaderLease() bool {
	return true
}

func (*replicaConsistencyQueue) acceptsUnsplitRanges() bool {
	return true
}

func (*replicaConsistencyQueue) shouldQueue(now roachpb.Timestamp, rng *Replica,
	_ *config.SystemConfig) (bool, float64) {
	return true, 1.0
}

// process performs a consistent lookup on the range descriptor to see if we are
// still a member of the range.
func (q *replicaConsistencyQueue) process(_ roachpb.Timestamp, rng *Replica, _ *config.SystemConfig) error {
	log.Info("run consistency check on a replica")
	id := uuid.MakeV4()
	desc := rng.Desc()
	key := desc.StartKey.AsRawKey()
	endKey := desc.EndKey.AsRawKey()

	// Send a ComputeChecksum to all the replicas of the leader replica.
	{
		var ba roachpb.BatchRequest
		ba.RangeID = rng.Desc().RangeID
		checkArgs := &roachpb.ComputeChecksumRequest{
			Span: roachpb.Span{
				Key:    key,
				EndKey: endKey,
			},
			ChecksumID: id,
		}
		ba.Add(checkArgs)
		if _, pErr := rng.Send(rng.context(), ba); pErr != nil {
			return pErr.GoError()
		}
	}
	// wait for checksum and collect it.
	// TODO: Change to notification mechanism in this PR.
	var checksum []byte
	for i := 0; i < 3; i++ {
		time.Sleep(20 * time.Millisecond)
		sha, hasComputed := rng.getChecksum(id)
		if hasComputed {
			checksum = sha
			break
		}
	}

	if checksum == nil {
		// Don't bother verifying the checksum.
		return nil
	}

	// Send a VerifyChecksum to all the replicas.
	{
		var ba roachpb.BatchRequest
		ba.RangeID = rng.Desc().RangeID
		checkArgs := &roachpb.VerifyChecksumRequest{
			Span: roachpb.Span{
				Key:    key,
				EndKey: endKey,
			},
			ChecksumID: id,
			Checksum:   checksum,
		}
		ba.Add(checkArgs)
		if _, pErr := rng.Send(rng.context(), ba); pErr != nil {
			return pErr.GoError()
		}
	}
	return nil
}

func (*replicaConsistencyQueue) timer() time.Duration {
	return 0
}

// purgatoryChan returns nil.
func (*replicaConsistencyQueue) purgatoryChan() <-chan struct{} {
	return nil
}
