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

	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// scanQueueMaxSize is the max size of the scan queue.
	scanQueueMaxSize = 100
	// gcByteCountNormalization is the count of GC'able bytes which
	// amount to a score of "1" added to total range priority.
	gcByteCountNormalization = 1 << 20 // 1 MB
	// intentSweepInterval is the target duration for resolving extant
	// write intents. If this much time has passed since the last scan
	// and write intents are present, the range should be queued. Cleaning
	// up intents allows transaction records to be GC'd.
	intentSweepInterval = 10 * 24 * time.Hour // 10 days
	// intentAgeThreshold is the threshold after which an extant intent
	// will be resolved.
	intentAgeThreshold = 1 * time.Hour // 1 hour
	// verificationInterval is the target duration for verifying on-disk
	// checksums via full scan.
	verificationInterval = 30 * 24 * time.Hour // 30 days
)

// scanQueue manages a queue of ranges slated to be scanned in their
// entirety using the MVCC versions iterator. Currently, range scans
// manage the following tasks:
//
//  - GC of version data via TTL expiration (and more complex schemes
//    as implemented going forward).
//  - Resolve extant write intents and determine oldest non-resolvable
//    intent.
//  - Periodic verification of on-disk checksums to identify bit-rot
//    in read-only data sets. See http://en.wikipedia.org/wiki/Data_degradation.
//
// The shouldQueue function combines the need for all three tasks into
// a single priority. If any task is overdue, shouldQueue returns true.
type scanQueue struct {
	*baseQueue
}

// newScanQueue returns a new instance of scanQueue.
func newScanQueue() *scanQueue {
	sq := &scanQueue{}
	sq.baseQueue = newBaseQueue("scan", sq.shouldQueue, sq.process, scanQueueMaxSize)
	return sq
}

// shouldQueue determines whether a range should be queued for
// scanning, and if so, at what priority. Returns true for shouldQ in
// the event that there are GC'able bytes, or it's been longer since
// the last scan than the intent sweep or verification
// intervals. Priority is derived from the addition of priority from
// GC'able bytes and how many multiples of intent or verification
// intervals have elapsed since the last scan.
func (sq *scanQueue) shouldQueue(now time.Time, rng *Range) (shouldQ bool, priority float64) {
	scanMeta, err := rng.GetScanMetadata()
	if err != nil {
		log.Errorf("unable to fetch scan metadata: %s", err)
		return
	}
	elapsedNanos := now.UnixNano() - scanMeta.LastScanNanos

	// Compute non-live bytes.
	nonLiveBytes := rng.stats.GetSize() - rng.stats.LiveBytes

	// GC score.
	estGCBytes := scanMeta.GC.EstimatedBytes(elapsedNanos, nonLiveBytes)
	gcScore := float64(estGCBytes) / float64(gcByteCountNormalization)

	// Intent sweep score. First check for intents. We only compute an
	// intent score if there are any outstanding intents.
	intentScore := float64(0)
	if rng.stats.IntentCount > 0 {
		intentScore = float64(elapsedNanos) / float64(intentSweepInterval.Nanoseconds())
	}

	// Verify score.
	verifyScore := float64(elapsedNanos) / float64(verificationInterval.Nanoseconds())

	// Compute priority.
	if gcScore > 0 {
		priority += gcScore
	}
	if intentScore > 1 {
		priority += (intentScore - 1)
	}
	if verifyScore > 1 {
		priority += (verifyScore - 1)
	}
	shouldQ = priority > 0
	return
}

// process iterates through all keys in a range, calling the garbage
// collector for each key and associated set of values. GC'd keys are
// batched into InternalGC calls. Extant intents are resolved if
// intents are older than intentAgeThreshold. The very act of scanning
// keys verifies on-disk checksums, as each block checksum is checked
// on load.
func (sq *scanQueue) process(now time.Time, rng *Range) error {
	snap := rng.rm.Engine().NewSnapshot()
	iter := newRangeDataIterator(rng, snap)
	defer iter.Close()
	defer snap.Stop()

	for ; iter.Valid(); iter.Next() {
		// TODO(spencer): implement processing.
	}

	return nil
}
