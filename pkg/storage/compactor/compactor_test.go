// Copyright 2017 The Cockroach Authors.
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

package compactor

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const testCompactionLatency = 1 * time.Millisecond

type wrappedEngine struct {
	*engine.RocksDB
	mu struct {
		syncutil.Mutex
		compactions []roachpb.Span
	}
}

func newWrappedEngine() *wrappedEngine {
	inMem := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	return &wrappedEngine{
		RocksDB: inMem.RocksDB,
	}
}

func (we *wrappedEngine) GetSSTables() engine.SSTableInfos {
	key := func(s string) engine.MVCCKey {
		return engine.MakeMVCCMetadataKey([]byte(s))
	}
	ssti := engine.SSTableInfos{
		// Level 0.
		{Level: 0, Size: 20, Start: key("a"), End: key("z")},
		{Level: 0, Size: 15, Start: key("a"), End: key("k")},
		// Level 2.
		{Level: 2, Size: 200, Start: key("a"), End: key("j")},
		{Level: 2, Size: 100, Start: key("k"), End: key("o")},
		{Level: 2, Size: 100, Start: key("r"), End: key("t")},
		// Level 6.
		{Level: 6, Size: 201, Start: key("a"), End: key("c")},
		{Level: 6, Size: 200, Start: key("d"), End: key("f")},
		{Level: 6, Size: 300, Start: key("h"), End: key("r")},
		{Level: 6, Size: 405, Start: key("s"), End: key("z")},
	}
	sort.Sort(ssti)
	return ssti
}

func (we *wrappedEngine) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	we.mu.Lock()
	defer we.mu.Unlock()
	time.Sleep(testCompactionLatency)
	we.mu.compactions = append(we.mu.compactions, roachpb.Span{Key: start, EndKey: end})
	return nil
}

func (we *wrappedEngine) GetCompactions() []roachpb.Span {
	we.mu.Lock()
	defer we.mu.Unlock()
	return append([]roachpb.Span(nil), we.mu.compactions...)
}

func testSetup(capFn storeCapacityFunc) (*Compactor, *wrappedEngine, *int32, func()) {
	stopper := stop.NewStopper()
	eng := newWrappedEngine()
	stopper.AddCloser(eng)
	compactionCount := new(int32)
	doneFn := func(_ context.Context) { atomic.AddInt32(compactionCount, 1) }
	compactor := NewCompactor(eng, capFn, doneFn)
	compactor.Start(context.Background(), tracing.NewTracer(), stopper)
	return compactor, eng, compactionCount, func() {
		stopper.Stop(context.Background())
	}
}

func key(s string) roachpb.Key {
	return roachpb.Key([]byte(s))
}

// TestCompactorThresholds verifies the thresholding logic for the compactor.
func TestCompactorThresholds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fractionThresh := defaultThresholdBytesFraction*defaultThresholdBytes + 1
	fractionAvailableThresh := defaultThresholdBytesAvailableFraction*defaultThresholdBytes + 1
	nowNanos := timeutil.Now().UnixNano()
	testCases := []struct {
		name              string
		suggestions       []storagebase.SuggestedCompaction
		logicalBytes      int64 // logical byte count to return with store capacity
		availableBytes    int64 // available byte count to return with store capacity
		expBytesCompacted int64
		expCompactions    []roachpb.Span
		expUncompacted    []roachpb.Span
	}{
		// Single suggestion under all thresholds.
		{
			name: "single suggestion under all thresholds",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - 1,
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: 0,
			expCompactions:    nil,
			expUncompacted: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Single suggestion which is over absolute bytes threshold should compact.
		{
			name: "single suggestion over absolute threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes,
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: defaultThresholdBytes,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Single suggestion over the fractional threshold.
		{
			name: "single suggestion over fractional threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            int64(fractionThresh),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes,
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: int64(fractionThresh),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Single suggestion over the fractional bytes available threshold.
		{
			name: "single suggestion over fractional bytes available threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            int64(fractionAvailableThresh),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes,
			expBytesCompacted: int64(fractionAvailableThresh),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Double suggestion which in aggregate exceed absolute bytes threshold.
		{
			name: "double suggestion over absolute threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("b"), EndKey: key("c"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - (defaultThresholdBytes / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: defaultThresholdBytes,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("c")},
			},
		},
		// Double suggestion to same span.
		{
			name: "double suggestion to same span over absolute threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - (defaultThresholdBytes / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: defaultThresholdBytes,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Double suggestion overlapping.
		{
			name: "double suggestion overlapping over absolute threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("c"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("b"), EndKey: key("d"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - (defaultThresholdBytes / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: defaultThresholdBytes,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("d")},
			},
		},
		// Double suggestion which in aggregate exceeds fractional bytes threshold.
		{
			name: "double suggestion over fractional threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            int64(fractionThresh / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("b"), EndKey: key("c"),
					Compaction: storagebase.Compaction{
						Bytes:            int64(fractionThresh) - int64(fractionThresh/2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes,
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: int64(fractionThresh),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("c")},
			},
		},
		// Double suggestion without excessive gap.
		{
			name: "double suggestion without excessive gap",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are only two sstables between ("b", "e") at the max level.
				{
					StartKey: key("e"), EndKey: key("f"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - (defaultThresholdBytes / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: defaultThresholdBytes,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("f")},
			},
		},
		// Double suggestion with excessive gap.
		{
			name: "double suggestion with excessive gap",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are three sstables between ("b", "h0") at the max level.
				{
					StartKey: key("h0"), EndKey: key("i"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - (defaultThresholdBytes / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: 0,
			expCompactions:    nil,
			expUncompacted: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
				{Key: key("h0"), EndKey: key("i")},
			},
		},
		// Double suggestion with excessive gap, but both over absolute threshold.
		{
			name: "double suggestion with excessive gap but both over threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes,
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are three sstables between ("b", "h0") at the max level.
				{
					StartKey: key("h0"), EndKey: key("i"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes,
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: defaultThresholdBytes * 2,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
				{Key: key("h0"), EndKey: key("i")},
			},
		},
		// Double suggestion with excessive gap, with just one over absolute threshold.
		{
			name: "double suggestion with excessive gap but one over threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes,
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are three sstables between ("b", "h0") at the max level.
				{
					StartKey: key("h0"), EndKey: key("i"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - 1,
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: defaultThresholdBytes,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
			expUncompacted: []roachpb.Span{
				{Key: key("h0"), EndKey: key("i")},
			},
		},
		// Quadruple suggestion which can be aggregated into a single compaction.
		{
			name: "quadruple suggestion which aggregates",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 4,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("e"), EndKey: key("f0"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 4,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("g"), EndKey: key("q"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 4,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("y"), EndKey: key("zzz"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - 3*(defaultThresholdBytes/4),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			availableBytes:    defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: defaultThresholdBytes,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("zzz")},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			capacityFn := func() (roachpb.StoreCapacity, error) {
				return roachpb.StoreCapacity{
					LogicalBytes: test.logicalBytes,
					Available:    test.availableBytes,
				}, nil
			}
			compactor, we, compactionCount, cleanup := testSetup(capacityFn)
			defer cleanup()
			// Shorten wait times for compactor processing.
			compactor.opts.CompactionMinInterval = time.Millisecond

			for _, sc := range test.suggestions {
				compactor.Suggest(context.Background(), sc)
			}

			// If we expect no compaction, pause to ensure test will fail if
			// a compaction happens. Note that 10ms is not enough to make
			// this fail all the time, but it will surely trigger a failure
			// most of the time.
			if len(test.expCompactions) == 0 {
				time.Sleep(10 * time.Millisecond)
			}
			testutils.SucceedsSoon(t, func() error {
				comps := we.GetCompactions()
				if !reflect.DeepEqual(test.expCompactions, comps) {
					return fmt.Errorf("expected %+v; got %+v", test.expCompactions, comps)
				}
				if a, e := compactor.Metrics.BytesCompacted.Count(), test.expBytesCompacted; a != e {
					return fmt.Errorf("expected bytes compacted %d; got %d", e, a)
				}
				if a, e := compactor.Metrics.CompactionSuccesses.Count(), int64(len(test.expCompactions)); a != e {
					return fmt.Errorf("expected compactions %d; got %d", e, a)
				}
				if a, e := atomic.LoadInt32(compactionCount), int32(len(test.expCompactions)); a != e {
					return fmt.Errorf("expected compactions %d; got %d", e, a)
				}
				if len(test.expCompactions) == 0 {
					if cn := compactor.Metrics.CompactingNanos.Count(); cn > 0 {
						return fmt.Errorf("expected compaction time to be 0; got %d", cn)
					}
				} else {
					expNanos := int64(len(test.expCompactions)) * int64(testCompactionLatency)
					if a, e := compactor.Metrics.CompactingNanos.Count(), expNanos; a < e {
						return fmt.Errorf("expected compacting nanos > %d; got %d", e, a)
					}
				}
				// Read the remaining suggestions in the queue; verify compacted
				// spans have been cleared and uncompacted spans remain.
				var idx int
				return we.Iterate(
					engine.MVCCKey{Key: keys.LocalStoreSuggestedCompactionsMin},
					engine.MVCCKey{Key: keys.LocalStoreSuggestedCompactionsMax},
					func(kv engine.MVCCKeyValue) (bool, error) {
						start, end, err := keys.DecodeStoreSuggestedCompactionKey(kv.Key.Key)
						if err != nil {
							t.Fatalf("failed to decode suggested compaction key: %s", err)
						}
						if idx >= len(test.expUncompacted) {
							return true, fmt.Errorf("found unexpected uncompacted span %s-%s", start, end)
						}
						if !start.Equal(test.expUncompacted[idx].Key) || !end.Equal(test.expUncompacted[idx].EndKey) {
							return true, fmt.Errorf("found unexpected uncompacted span %s-%s; expected %s-%s",
								start, end, test.expUncompacted[idx].Key, test.expUncompacted[idx].EndKey)
						}
						idx++
						return false, nil // continue iteration
					},
				)
			})
		})
	}
}

// TestCompactorProcessingInitialization verifies that a compactor gets
// started with processing if the queue is non-empty.
func TestCompactorProcessingInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()

	capacityFn := func() (roachpb.StoreCapacity, error) {
		return roachpb.StoreCapacity{LogicalBytes: 100 * defaultThresholdBytes}, nil
	}
	compactor, we, compactionCount, cleanup := testSetup(capacityFn)
	defer cleanup()

	// Add a suggested compaction -- this won't get processed by this
	// compactor for an hour.
	compactor.opts.CompactionMinInterval = time.Hour
	compactor.Suggest(context.Background(), storagebase.SuggestedCompaction{
		StartKey: key("a"), EndKey: key("b"),
		Compaction: storagebase.Compaction{
			Bytes:            defaultThresholdBytes,
			SuggestedAtNanos: timeutil.Now().UnixNano(),
		},
	})

	// Create a new fast compactor with a short wait time for processing,
	// using the same engine so that it sees a non-empty queue.
	stopper := stop.NewStopper()
	doneFn := func(_ context.Context) { atomic.AddInt32(compactionCount, 1) }
	fastCompactor := NewCompactor(we, capacityFn, doneFn)
	fastCompactor.opts.CompactionMinInterval = time.Millisecond
	fastCompactor.Start(context.Background(), tracing.NewTracer(), stopper)
	defer stopper.Stop(context.Background())

	testutils.SucceedsSoon(t, func() error {
		comps := we.GetCompactions()
		expComps := []roachpb.Span{{Key: key("a"), EndKey: key("b")}}
		if !reflect.DeepEqual(expComps, comps) {
			return fmt.Errorf("expected %+v; got %+v", expComps, comps)
		}
		if a, e := atomic.LoadInt32(compactionCount), int32(1); a != e {
			return fmt.Errorf("expected %d; got %d", e, a)
		}
		return nil
	})
}

// TestCompactorCleansUpOldRecords verifies that records which exceed
// the maximum age are deleted if they cannot be compacted.
func TestCompactorCleansUpOldRecords(t *testing.T) {
	defer leaktest.AfterTest(t)()

	capacityFn := func() (roachpb.StoreCapacity, error) {
		return roachpb.StoreCapacity{
			LogicalBytes: 100 * defaultThresholdBytes,
			Available:    100 * defaultThresholdBytes,
		}, nil
	}
	compactor, we, compactionCount, cleanup := testSetup(capacityFn)
	compactor.opts.CompactionMinInterval = time.Millisecond
	compactor.opts.MaxSuggestedCompactionRecordAge = 1 * time.Millisecond
	defer cleanup()

	// Add a suggested compaction that won't get processed because it's
	// not over any of the thresholds.
	compactor.Suggest(context.Background(), storagebase.SuggestedCompaction{
		StartKey: key("a"), EndKey: key("b"),
		Compaction: storagebase.Compaction{
			Bytes:            defaultThresholdBytes - 1,
			SuggestedAtNanos: timeutil.Now().UnixNano(),
		},
	})

	// Verify that the record is deleted without a compaction and that the
	// bytes are recorded as having been skipped.
	testutils.SucceedsSoon(t, func() error {
		comps := we.GetCompactions()
		if !reflect.DeepEqual([]roachpb.Span(nil), comps) {
			return fmt.Errorf("expected nil compactions; got %+v", comps)
		}
		if a, e := compactor.Metrics.BytesSkipped.Count(), compactor.opts.ThresholdBytes-1; a != e {
			return fmt.Errorf("expected skipped bytes %d; got %d", e, a)
		}
		if a, e := atomic.LoadInt32(compactionCount), int32(0); a != e {
			return fmt.Errorf("expected compactions processed %d; got %d", e, a)
		}
		// Verify compaction queue is empty.
		if bytesQueued, err := compactor.examineQueue(context.Background()); err != nil || bytesQueued > 0 {
			return fmt.Errorf("compaction queue not empty (%d bytes) or err %v", bytesQueued, err)
		}
		return nil
	})
}
