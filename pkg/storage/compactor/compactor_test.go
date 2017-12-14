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
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"golang.org/x/net/context"

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
	"github.com/pkg/errors"
)

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
		// Level 1.
		{Level: 1, Size: 200, Start: key("a"), End: key("j")},
		{Level: 1, Size: 100, Start: key("k"), End: key("o")},
		{Level: 1, Size: 100, Start: key("r"), End: key("t")},
		// Level 2.
		{Level: 2, Size: 201, Start: key("a"), End: key("c")},
		{Level: 2, Size: 200, Start: key("d"), End: key("f")},
		{Level: 2, Size: 300, Start: key("h"), End: key("r")},
		{Level: 2, Size: 405, Start: key("s"), End: key("z")},
	}
	sort.Sort(ssti)
	return ssti
}

func (we *wrappedEngine) CompactRange(start, end engine.MVCCKey) error {
	we.mu.Lock()
	defer we.mu.Unlock()
	we.mu.compactions = append(we.mu.compactions, roachpb.Span{Key: start.Key, EndKey: end.Key})
	return nil
}

func (we *wrappedEngine) GetCompactions() []roachpb.Span {
	we.mu.Lock()
	defer we.mu.Unlock()
	return append([]roachpb.Span(nil), we.mu.compactions...)
}

func testSetup(capFn storeCapacityFunc) (*Compactor, *wrappedEngine, func()) {
	stopper := stop.NewStopper()
	eng := newWrappedEngine()
	stopper.AddCloser(eng)
	compactor := NewCompactor(eng, capFn)
	compactor.Start(context.Background(), tracing.NewTracer(), stopper)
	return compactor, eng, func() {
		stopper.Stop(context.Background())
	}
}

func rkey(s string) roachpb.RKey {
	return roachpb.RKey([]byte(s))
}

func key(s string) roachpb.Key {
	return roachpb.Key([]byte(s))
}

// TestCompactorThresholds verifies the thresholding logic for the compactor.
func TestCompactorThresholds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fractionThresh := defaultThresholdBytesFraction*defaultThresholdBytes + 1
	nowNanos := timeutil.Now().UnixNano()
	testCases := []struct {
		name              string
		suggestions       []storagebase.SuggestedCompaction
		logicalBytes      int64 // logical byte count to return with store capacity
		expBytesCompacted int64
		expCompactions    []roachpb.Span
		expUncompacted    []roachpb.Span
	}{
		// Single suggestion under all thresholds.
		{
			name: "single suggestion under all thresholds",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - 1,
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
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
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes,
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
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
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            int64(fractionThresh),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes,
			expBytesCompacted: int64(fractionThresh),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Double suggestion which in aggregate exceed absolute bytes threshold.
		{
			name: "double suggestion over absolute threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: rkey("b"), EndKey: rkey("c"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - (defaultThresholdBytes / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
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
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - (defaultThresholdBytes / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
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
					StartKey: rkey("a"), EndKey: rkey("c"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: rkey("b"), EndKey: rkey("d"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - (defaultThresholdBytes / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
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
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            int64(fractionThresh / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: rkey("b"), EndKey: rkey("c"),
					Compaction: storagebase.Compaction{
						Bytes:            int64(fractionThresh) - int64(fractionThresh/2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes,
			expBytesCompacted: int64(fractionThresh),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("c")},
			},
		},
		// Double suggestion without too much space in between.
		{
			name: "double suggestion without too much space",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are only two sstables between ("b", "e") at the max level.
				{
					StartKey: rkey("e"), EndKey: rkey("f"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - (defaultThresholdBytes / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: defaultThresholdBytes,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("f")},
			},
		},
		// Double suggestion with too much space in between.
		{
			name: "double suggestion with too much space",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are three sstables between ("b", "h0") at the max level.
				{
					StartKey: rkey("h0"), EndKey: rkey("i"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - (defaultThresholdBytes / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: 0,
			expCompactions:    nil,
			expUncompacted: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
				{Key: key("h0"), EndKey: key("i")},
			},
		},
		// Double suggestion with too much space in between, but both over absolute threshold.
		{
			name: "double suggestion with too much space but both over threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes,
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are three sstables between ("b", "h0") at the max level.
				{
					StartKey: rkey("h0"), EndKey: rkey("i"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes,
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
			expBytesCompacted: defaultThresholdBytes * 2,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
				{Key: key("h0"), EndKey: key("i")},
			},
		},
		// Double suggestion with too much space in between, with just one over absolute threshold.
		{
			name: "double suggestion with too much space but one over threshold",
			suggestions: []storagebase.SuggestedCompaction{
				{
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes,
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are three sstables between ("b", "h0") at the max level.
				{
					StartKey: rkey("h0"), EndKey: rkey("i"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - 1,
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
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
					StartKey: rkey("a"), EndKey: rkey("b"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 4,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: rkey("e"), EndKey: rkey("f0"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 4,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: rkey("g"), EndKey: rkey("q"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes / 4,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: rkey("y"), EndKey: rkey("zzz"),
					Compaction: storagebase.Compaction{
						Bytes:            defaultThresholdBytes - 3*(defaultThresholdBytes/4),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      defaultThresholdBytes * 100, // not going to trigger fractional threshold
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
				}, nil
			}
			compactor, we, cleanup := testSetup(capacityFn)
			defer cleanup()
			// Shorten wait times for compactor processing.
			compactor.opts.CompactionMinInterval = time.Millisecond

			for _, sc := range test.suggestions {
				compactor.SuggestCompaction(context.Background(), sc)
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
					t.Errorf("expected bytes compacted %d; got %d", e, a)
				}
				if a, e := compactor.Metrics.Compactions.Count(), int64(len(test.expCompactions)); a != e {
					t.Errorf("expected compactions %d; got %d", e, a)
				}
				if a, e := compactor.Metrics.CompactingNanos.Count(), (len(test.expCompactions) > 0); (a > 0) != e {
					t.Errorf("expected compaction time > 0? %t; got %d", e, a)
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
							return false, errors.Wrapf(err, "failed to decode suggested compaction key")
						}
						if idx >= len(test.expUncompacted) {
							fmt.Printf("found unexpected uncompacted span %s-%s\n", start, end)
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
	compactor, we, cleanup := testSetup(capacityFn)
	defer cleanup()

	// Add a suggested compaction -- this won't get processed by this
	// compactor for two minutes.
	compactor.SuggestCompaction(context.Background(), storagebase.SuggestedCompaction{
		StartKey: rkey("a"), EndKey: rkey("b"),
		Compaction: storagebase.Compaction{
			Bytes:            defaultThresholdBytes,
			SuggestedAtNanos: timeutil.Now().UnixNano(),
		},
	})

	// Create a new fast compactor with a short wait time for processing,
	// using the same engine so that it see a non-empty queue.
	stopper := stop.NewStopper()
	fastCompactor := NewCompactor(we, capacityFn)
	fastCompactor.Start(context.Background(), tracing.NewTracer(), stopper)
	defer func() {
		stopper.Stop(context.Background())
	}()
	fastCompactor.opts.CompactionMinInterval = time.Millisecond

	testutils.SucceedsSoon(t, func() error {
		comps := we.GetCompactions()
		expComps := []roachpb.Span{{Key: key("a"), EndKey: key("b")}}
		if !reflect.DeepEqual(expComps, comps) {
			return fmt.Errorf("expected %+v; got %+v", expComps, comps)
		}
		return nil
	})
}

// TestCompactorCleansUpOldRecords verifies that records which exceed
// the maximum age are deleted if they cannot be compacted.
func TestCompactorCleansUpOldRecords(t *testing.T) {
	defer leaktest.AfterTest(t)()

	capacityFn := func() (roachpb.StoreCapacity, error) {
		return roachpb.StoreCapacity{LogicalBytes: 100 * defaultThresholdBytes}, nil
	}
	compactor, we, cleanup := testSetup(capacityFn)
	compactor.opts.CompactionMinInterval = time.Millisecond
	compactor.opts.MaxSuggestedCompactionRecordAge = 1 * time.Millisecond
	defer cleanup()

	// Add a suggested compaction that won't get processed because it's
	// not over any of the thresholds.
	compactor.SuggestCompaction(context.Background(), storagebase.SuggestedCompaction{
		StartKey: rkey("a"), EndKey: rkey("b"),
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
		// Verify compaction queue is empty.
		if empty, err := compactor.isSpanEmpty(
			context.Background(), keys.LocalStoreSuggestedCompactionsMin, keys.LocalStoreSuggestedCompactionsMax,
		); err != nil || !empty {
			return fmt.Errorf("compaction queue not empty or err: %t, %s", empty, err)
		}
		return nil
	})
}
