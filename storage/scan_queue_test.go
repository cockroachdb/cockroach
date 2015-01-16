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
	"log"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
)

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

	day := 1 * 24 * time.Hour // 1 day
	mb := int64(1 << 20)      // 1 MB
	scanMeta0 := &proto.ScanMetadata{
		LastScanNanos: 0,
		GC: proto.GCMetadata{
			TTLSeconds: int32(day.Nanoseconds() / 1000000000),
			ByteCounts: []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}
	scanMeta1 := &proto.ScanMetadata{
		LastScanNanos: 0,
		GC: proto.GCMetadata{
			TTLSeconds: int32(day.Nanoseconds() / 1000000000),
			ByteCounts: []int64{
				10 * mb, 9 * mb, 8 * mb, 7 * mb, 6 * mb, 5 * mb, 4 * mb, 3 * mb, 2 * mb, 1 * mb / 2,
			},
		},
	}

	testCases := []struct {
		scanMeta     *proto.ScanMetadata
		nonLiveBytes int64
		intentBytes  int64
		now          time.Time
		shouldQ      bool
		priority     float64
	}{
		// No GC'able bytes, no time elapsed.
		{scanMeta0, 0, 0, time.Unix(0, 0), false, 0},
		// No GC'able bytes, no intent bytes, but intent interval elapsed.
		{scanMeta0, 0, 0, time.Unix(intentSweepInterval.Nanoseconds()/1000000000, 0), false, 0},
		// No GC'able bytes, with intent bytes, intent interval elapsed.
		{scanMeta0, 0, 1, time.Unix(intentSweepInterval.Nanoseconds()/1000000000, 0), false, 0},
		// No GC'able bytes, with intent bytes, intent interval * 2 elapsed.
		{scanMeta0, 0, 1, time.Unix((intentSweepInterval.Nanoseconds()*2)/1000000000, 0), true, 1},
		// No GC'able bytes, no intent bytes, verification interval elapsed.
		{scanMeta0, 0, 0, time.Unix(verificationInterval.Nanoseconds()/1000000000, 0), false, 0},
		// No GC'able bytes, no intent bytes, verification interval * 2 elapsed.
		{scanMeta0, 0, 0, time.Unix((verificationInterval.Nanoseconds()*2)/1000000000, 0), true, 1},
		// No GC'able bytes, with combination of intent bytes and verification interval * 2 elapsed.
		{scanMeta0, 0, 1, time.Unix((verificationInterval.Nanoseconds()*2)/1000000000, 0), true, 6},
		// GC'able bytes, no time elapsed.
		{scanMeta1, 0, 0, time.Unix(0, 0), false, 0},
		// GC'able bytes, TTLSeconds / 10 elapsed (note that 1st 1/10 of bytes is 0.5M).
		{scanMeta1, 0, 0, time.Unix(24*360, 0), true, 0.5},
		// GC'able bytes, TTLSeconds / 2 elapsed.
		{scanMeta1, 0, 0, time.Unix(12*3600, 0), true, 5},
		// GC'able bytes, TTLSeconds elapsed.
		{scanMeta1, 0, 0, time.Unix(24*3600, 0), true, 10},
		// GC'able bytes with non-live bytes, TTLSeconds elapsed.
		{scanMeta1, 1 * mb, 0, time.Unix(24*3600, 0), true, 10},
		// GC'able bytes with non-live bytes, TTLSeconds * 2 elapsed.
		{scanMeta1, 1 * mb, 0, time.Unix(48*3600, 0), true, 10.5},
		// GC'able bytes with non-live bytes, TTLSeconds * 4 elapsed.
		{scanMeta1, 1 * mb, 0, time.Unix(96*3600, 0), true, 10.75},
		// GC'able bytes with non-live bytes, intent bytes, and intent interval * 2 elapsed.
		{scanMeta1, 1 * mb, 1, time.Unix((intentSweepInterval.Nanoseconds()*2)/1000000000, 0), true, 11.95},
	}

	scanQ := newScanQueue()

	for i, test := range testCases {
		// Write scan metadata.
		if err := tc.rng.PutScanMetadata(test.scanMeta); err != nil {
			t.Fatal(err)
		}
		// Write non live bytes as key bytes; since "live" bytes will be zero, this will translate into non live bytes.
		nonLiveBytes := test.scanMeta.GC.ByteCounts[0] + test.nonLiveBytes
		if err := engine.SetStat(tc.rng.rm.Engine(), tc.rng.Desc.RaftID, 0,
			engine.StatKeyBytes, nonLiveBytes); err != nil {
			log.Fatal(err)
		}
		// Write intent bytes. Note: the actual accounting on bytes is fictional in this test.
		if err := engine.SetStat(tc.rng.rm.Engine(), tc.rng.Desc.RaftID, 0,
			engine.StatIntentBytes, test.intentBytes); err != nil {
			log.Fatal(err)
		}

		shouldQ, priority := scanQ.shouldQueue(test.now, tc.rng)
		if shouldQ != test.shouldQ {
			t.Errorf("%d: should queue expected %t; got %t", i, test.shouldQ, shouldQ)
		}
		if math.Abs(priority-test.priority) > 0.00001 {
			t.Errorf("%d: priority expected %f; got %f", i, test.priority, priority)
		}
	}
}
