// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// verifyQueueMaxSize is the max size of the verification queue.
	verifyQueueMaxSize = 100
	// verificationInterval is the target duration for verifying on-disk
	// checksums via full scan.
	verificationInterval = 60 * 24 * time.Hour // 60 days
)

// rangeCountFn should return the total number of ranges on the store providing
// ranges to this queue.
type rangeCountFn func() int

// verifyQueue periodically verifies on-disk checksums to identify
// bit-rot in read-only data sets. See
// http://en.wikipedia.org/wiki/Data_degradation.
type verifyQueue struct {
	baseQueue
	countFn rangeCountFn
}

// newVerifyQueue returns a new instance of verifyQueue.
func newVerifyQueue(gossip *gossip.Gossip, countFn rangeCountFn) *verifyQueue {
	vq := &verifyQueue{countFn: countFn}
	vq.baseQueue = makeBaseQueue("verify", vq, gossip, verifyQueueMaxSize)
	return vq
}

func (*verifyQueue) needsLeaderLease() bool {
	return false
}

func (*verifyQueue) acceptsUnsplitRanges() bool {
	return true
}

// shouldQueue determines whether a range should be queued for
// verification scanning, and if so, at what priority. Returns true
// for shouldQ in the event that it's been longer since the last scan
// than the verification interval.
func (*verifyQueue) shouldQueue(now roachpb.Timestamp, rng *Replica,
	_ config.SystemConfig) (shouldQ bool, priority float64) {

	// Get last verification timestamp.
	lastVerify, err := rng.getLastVerificationTimestamp()
	if err != nil {
		log.Errorf("unable to fetch last verification timestamp: %s", err)
		return
	}
	verifyScore := float64(now.WallTime-lastVerify.WallTime) / float64(verificationInterval.Nanoseconds())
	if verifyScore > 1 {
		priority = verifyScore
		shouldQ = true
	}
	return
}

// process iterates through all keys and values in a range. The very
// act of scanning keys verifies on-disk checksums, as each block
// checksum is checked on load.
func (*verifyQueue) process(now roachpb.Timestamp, rng *Replica,
	_ config.SystemConfig) error {

	snap := rng.store.Engine().NewSnapshot()
	iter := newReplicaDataIterator(rng.Desc(), snap, false /* !replicatedOnly */)
	defer iter.Close()
	defer snap.Close()

	// Iterate through all keys & values.
	for ; iter.Valid(); iter.Next() {
	}
	// An error during iteration is presumed to mean a checksum failure
	// while iterating over the underlying key/value data.
	if iter.Error() != nil {
		// TODO(spencer): do something other than fatal error here. We
		// want to quarantine this range, make it a non-participating raft
		// follower until it can be replaced and then destroyed.
		log.Fatalf("unhandled failure when scanning range %s; probable data corruption: %s", rng, iter.Error())
	}

	// Store current timestamp as last verification for this range.
	return rng.setLastVerificationTimestamp(now)
}

// timer returns the duration of intervals between successive range
// verification scans. The durations are sized so that the full
// complement of ranges can be scanned within verificationInterval.
func (vq *verifyQueue) timer() time.Duration {
	return time.Duration(verificationInterval.Nanoseconds() / int64((vq.countFn() + 1)))
}

// purgatoryChan returns nil.
func (*verifyQueue) purgatoryChan() <-chan struct{} {
	return nil
}
