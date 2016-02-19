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
// still a member of the range. processi() is called in a stopper.RunTask() so it is
// illegal to call ShouldStop() in here.
func (q *replicaConsistencyQueue) process(_ roachpb.Timestamp, rng *Replica, _ *config.SystemConfig) error {
	log.Infof("run consistency check on a replica %s", rng)
	id := roachpb.ChecksumID{UUID: uuid.MakeV4()}
	desc := rng.Desc()
	key := desc.StartKey.AsRawKey()
	endKey := desc.EndKey.AsRawKey()
	notifyChan := make(chan []byte)
	if err := rng.setChecksumNotify(id, notifyChan); err != nil {
		return err
	}
	defer func() {
		rng.mu.Lock()
		defer rng.mu.Unlock()
		delete(rng.mu.checksumNotify, id)
	}()
	// Send a ComputeChecksum to all the replicas of the range.
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
	var checksum []byte
	select {
	case checksum = <-notifyChan:
	case <-rng.store.stopper.ShouldDrain():
		return nil
	}
	if checksum == nil {
		// Don't bother verifying the checksum.
		return nil
	}

	// Send a VerifyChecksum to all the replicas of the range.
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
	log.Info("done with consistency check for replica")
	return nil
}

func (*replicaConsistencyQueue) timer() time.Duration {
	return 10 * time.Second
}

// purgatoryChan returns nil.
func (*replicaConsistencyQueue) purgatoryChan() <-chan struct{} {
	return nil
}
