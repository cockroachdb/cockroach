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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// verifyQueueMaxSize is the max size of the verification queue.
	verifyQueueMaxSize = 100
	// verificationInterval is the target duration for verifying on-disk
	// checksums via full scan.
	verificationInterval = 60 * 24 * time.Hour // 60 days
)

// storeStatsFn returns the store stats for the store which owns this
// verification queue.
type storeStatsFn func() storeStats

// verifyQueue periodically verifies on-disk checksums to identify
// bit-rot in read-only data sets. See
// http://en.wikipedia.org/wiki/Data_degradation.
type verifyQueue struct {
	stats storeStatsFn
	*baseQueue
}

// newVerifyQueue returns a new instance of verifyQueue.
func newVerifyQueue(stats storeStatsFn) *verifyQueue {
	vq := &verifyQueue{stats: stats}
	vq.baseQueue = newBaseQueue("verify", vq, verifyQueueMaxSize)
	return vq
}

func (vq *verifyQueue) needsLeaderLease() bool {
	return false
}

// shouldQueue determines whether a range should be queued for
// verification scanning, and if so, at what priority. Returns true
// for shouldQ in the event that it's been longer since the last scan
// than the verification interval.
func (vq *verifyQueue) shouldQueue(now proto.Timestamp, rng *Range) (shouldQ bool, priority float64) {
	// Get last verification timestamp.
	lastVerify, err := rng.GetLastVerificationTimestamp()
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
func (vq *verifyQueue) process(now proto.Timestamp, rng *Range) error {
	snap := rng.rm.Engine().NewSnapshot()
	iter := newRangeDataIterator(rng.Desc(), snap)
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
	return rng.SetLastVerificationTimestamp(now)
}

// timer returns the duration of intervals between successive range
// verification scans. The durations are sized so that the full
// complement of ranges can be scanned within verificationInterval.
func (vq *verifyQueue) timer() time.Duration {
	return time.Duration(verificationInterval.Nanoseconds() / int64((vq.stats().RangeCount + 1)))
}
