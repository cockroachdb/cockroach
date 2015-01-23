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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// makeTS creates a new hybrid logical timestamp.
func makeTS(nanos int64, logical int32) proto.Timestamp {
	return proto.Timestamp{
		WallTime: nanos,
		Logical:  logical,
	}
}

// TestScanQueueShouldQueue verifies conditions which inform priority
// and whether or not the range should be queued into the scan queue.
// Ranges are queued for scanning based on three conditions. The bytes
// available to be GC'd, and the time since last GC, the time since
// last scan for unresolved intents (if there are any active intent
// bytes), and the time since last scan for verification of checksum
// data.
func TestScanQueueShouldQueue(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	iaN := intentAgeNormalization.Nanoseconds()
	ia := iaN / 1E9
	bc := int64(gcByteCountNormalization)
	ttl := int64(24 * 60 * 60)
	scanMeta := &proto.ScanMetadata{}

	testCases := []struct {
		gcBytes     int64
		gcBytesAge  int64
		intentCount int64
		intentAge   int64
		now         proto.Timestamp
		shouldQ     bool
		priority    float64
	}{
		// No GC'able bytes, no time elapsed.
		{0, 0, 0, 0, makeTS(0, 0), false, 0},
		// No GC'able bytes, with intent age, 1/2 intent normalization period elapsed.
		{0, 0, 1, ia / 2, makeTS(0, 0), false, 0},
		// No GC'able bytes, with intent age=1/2 period, and other 1/2 period elapsed.
		{0, 0, 1, ia / 2, makeTS(iaN/2, 0), false, 0},
		// No GC'able bytes, with intent age=2*intent normalization.
		{0, 0, 1, 3 * ia / 2, makeTS(iaN/2, 0), true, 2},
		// No GC'able bytes, 2 intents, with avg intent age=4x intent normalization.
		{0, 0, 2, 7 * ia, makeTS(iaN, 0), true, 4.5},
		// No GC'able bytes, no intent bytes, verification interval elapsed.
		{0, 0, 0, 0, makeTS(verificationInterval.Nanoseconds(), 0), false, 0},
		// No GC'able bytes, no intent bytes, verification interval * 2 elapsed.
		{0, 0, 0, 0, makeTS(verificationInterval.Nanoseconds()*2, 0), true, 2},
		// No GC'able bytes, with combination of intent bytes and verification interval * 2 elapsed.
		{0, 0, 1, 0, makeTS(verificationInterval.Nanoseconds()*2, 0), true, 62},
		// GC'able bytes, no time elapsed.
		{bc, 0, 0, 0, makeTS(0, 0), false, 0},
		// GC'able bytes, avg age = TTLSeconds.
		{bc, bc * ttl, 0, 0, makeTS(0, 0), false, 0},
		// GC'able bytes, avg age = 2*TTLSeconds.
		{bc, 2 * bc * ttl, 0, 0, makeTS(0, 0), true, 2},
		// x2 GC'able bytes, avg age = TTLSeconds.
		{2 * bc, 2 * bc * ttl, 0, 0, makeTS(0, 0), true, 2},
		// GC'able bytes, intent bytes, and intent normalization * 2 elapsed.
		{bc, bc * ttl, 1, 0, makeTS(iaN*2, 0), true, 5},
	}

	scanQ := newScanQueue()

	for i, test := range testCases {
		// Write scan metadata.
		if err := tc.rng.PutScanMetadata(scanMeta); err != nil {
			t.Fatal(err)
		}
		// Write gc'able bytes as key bytes; since "live" bytes will be
		// zero, this will translate into non live bytes.  Also write
		// intent count. Note: the actual accounting on bytes is fictional
		// in this test.
		stats := engine.MVCCStats{
			KeyBytes:    test.gcBytes,
			IntentCount: test.intentCount,
			IntentAge:   test.intentAge,
			GCBytesAge:  test.gcBytesAge,
		}
		tc.rng.stats.SetMVCCStats(tc.rng.rm.Engine(), stats)

		shouldQ, priority := scanQ.shouldQueue(test.now, tc.rng)
		if shouldQ != test.shouldQ {
			t.Errorf("%d: should queue expected %t; got %t", i, test.shouldQ, shouldQ)
		}
		if math.Abs(priority-test.priority) > 0.00001 {
			t.Errorf("%d: priority expected %f; got %f", i, test.priority, priority)
		}
	}
}
