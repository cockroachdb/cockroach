// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const testCompactionLatency = 1 * time.Millisecond

type wrappedEngine struct {
	storage.Engine
	mu struct {
		syncutil.Mutex
		compactions []roachpb.Span
	}
}

func newWrappedEngine() *wrappedEngine {
	return &wrappedEngine{
		Engine: storage.NewDefaultInMem(),
	}
}

func (we *wrappedEngine) GetSSTables() storage.SSTableInfos {
	key := func(s string) storage.MVCCKey {
		return storage.MakeMVCCMetadataKey([]byte(s))
	}
	ssti := storage.SSTableInfos{
		// Level 0.
		{Level: 0, Size: 20, Start: key("a"), End: key("z")},
		{Level: 0, Size: 15, Start: key("a"), End: key("k")},
		// Level 2.
		{Level: 2, Size: 200, Start: key("a"), End: key("j")},
		{Level: 2, Size: 100, Start: key("k"), End: key("o")},
		{Level: 2, Size: 100, Start: key("r"), End: key("t")},
		// Level 6.
		{Level: 6, Size: 200, Start: key("0"), End: key("9")},
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
	st := cluster.MakeTestingClusterSettings()
	compactor := NewCompactor(st, eng, capFn, doneFn)
	compactor.Start(context.Background(), stopper)
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

	// This test relies on concurrently waiting for a value to change in the
	// underlying engine(s). Since the teeing engine does not respond well to
	// value mismatches, whether transient or permanent, skip this test if the
	// teeing engine is being used. See
	// https://github.com/cockroachdb/cockroach/issues/42656 for more context.
	if storage.DefaultStorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		t.Skip("disabled on teeing engine")
	}

	fractionUsedThresh := thresholdBytesUsedFraction.Default()*float64(thresholdBytes.Default()) + 1
	fractionAvailableThresh := thresholdBytesAvailableFraction.Default()*float64(thresholdBytes.Default()) + 1
	nowNanos := timeutil.Now().UnixNano()
	testCases := []struct {
		name              string
		suggestions       []kvserverpb.SuggestedCompaction
		logicalBytes      int64 // logical byte count to return with store capacity
		availableBytes    int64 // available byte count to return with store capacity
		expBytesCompacted int64
		expCompactions    []roachpb.Span
		expUncompacted    []roachpb.Span
	}{
		// Single suggestion under all thresholds.
		{
			name: "single suggestion under all thresholds",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() - 1,
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: 0,
			expCompactions:    nil,
			expUncompacted: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Single suggestion which is over absolute bytes threshold should compact.
		{
			name: "single suggestion over absolute threshold",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default(),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: thresholdBytes.Default(),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Single suggestion which is over absolute bytes threshold should not
		// trigger a compaction if the span contains keys.
		{
			name: "outdated single suggestion over absolute threshold",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("0"), EndKey: key("9"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default(),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: 0,
			expCompactions:    nil,
			expUncompacted: []roachpb.Span{
				{Key: key("0"), EndKey: key("9")},
			},
		},
		// Single suggestion over the fractional threshold.
		{
			name: "single suggestion over fractional threshold",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            int64(fractionUsedThresh),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default(),
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: int64(fractionUsedThresh),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Single suggestion over the fractional bytes available threshold.
		{
			name: "single suggestion over fractional bytes available threshold",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            int64(fractionAvailableThresh),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default(),
			expBytesCompacted: int64(fractionAvailableThresh),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Double suggestion which in aggregate exceed absolute bytes threshold.
		{
			name: "double suggestion over absolute threshold",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("b"), EndKey: key("c"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() - (thresholdBytes.Default() / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: thresholdBytes.Default(),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("c")},
			},
		},
		// Double suggestion to same span.
		{
			name: "double suggestion to same span over absolute threshold",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() - (thresholdBytes.Default() / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: thresholdBytes.Default(),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
			},
		},
		// Double suggestion overlapping.
		{
			name: "double suggestion overlapping over absolute threshold",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("c"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("b"), EndKey: key("d"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() - (thresholdBytes.Default() / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: thresholdBytes.Default(),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("d")},
			},
		},
		// Double suggestion which in aggregate exceeds fractional bytes threshold.
		{
			name: "double suggestion over fractional threshold",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            int64(fractionUsedThresh / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("b"), EndKey: key("c"),
					Compaction: kvserverpb.Compaction{
						Bytes:            int64(fractionUsedThresh) - int64(fractionUsedThresh/2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default(),
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: int64(fractionUsedThresh),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("c")},
			},
		},
		// Double suggestion without excessive gap.
		{
			name: "double suggestion without excessive gap",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are only two sstables between ("b", "e") at the max level.
				{
					StartKey: key("e"), EndKey: key("f"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() - (thresholdBytes.Default() / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: thresholdBytes.Default(),
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("f")},
			},
		},
		// Double suggestion with non-excessive gap, but there are live keys in the
		// gap.
		//
		// NOTE: when a suggestion itself contains live keys, we skip the compaction
		// because amounts of data may have been added to the span since the
		// compaction was proposed. When only the gap contains live keys, however,
		// it's still desirable to compact: the individual suggestions are empty, so
		// we can assume there's lots of data to reclaim by compacting, and the
		// aggregator is very careful not to jump gaps that span too many SSTs.
		{
			name: "double suggestion over gap with live keys",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("0"), EndKey: key("4"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("6"), EndKey: key("9"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() - (thresholdBytes.Default() / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: thresholdBytes.Default(),
			expCompactions: []roachpb.Span{
				{Key: key("0"), EndKey: key("9")},
			},
		},
		// Double suggestion with excessive gap.
		{
			name: "double suggestion with excessive gap",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() / 2,
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are three sstables between ("b", "h0") at the max level.
				{
					StartKey: key("h0"), EndKey: key("i"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() - (thresholdBytes.Default() / 2),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
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
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default(),
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are three sstables between ("b", "h0") at the max level.
				{
					StartKey: key("h0"), EndKey: key("i"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default(),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: thresholdBytes.Default() * 2,
			expCompactions: []roachpb.Span{
				{Key: key("a"), EndKey: key("b")},
				{Key: key("h0"), EndKey: key("i")},
			},
		},
		// Double suggestion with excessive gap, with just one over absolute threshold.
		{
			name: "double suggestion with excessive gap but one over threshold",
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default(),
						SuggestedAtNanos: nowNanos,
					},
				},
				// There are three sstables between ("b", "h0") at the max level.
				{
					StartKey: key("h0"), EndKey: key("i"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() - 1,
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: thresholdBytes.Default(),
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
			suggestions: []kvserverpb.SuggestedCompaction{
				{
					StartKey: key("a"), EndKey: key("b"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() / 4,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("e"), EndKey: key("f0"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() / 4,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("g"), EndKey: key("q"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() / 4,
						SuggestedAtNanos: nowNanos,
					},
				},
				{
					StartKey: key("y"), EndKey: key("zzz"),
					Compaction: kvserverpb.Compaction{
						Bytes:            thresholdBytes.Default() - 3*(thresholdBytes.Default()/4),
						SuggestedAtNanos: nowNanos,
					},
				},
			},
			logicalBytes:      thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			availableBytes:    thresholdBytes.Default() * 100, // not going to trigger fractional threshold
			expBytesCompacted: thresholdBytes.Default(),
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
			minInterval.Override(&compactor.st.SV, time.Millisecond)

			// Add a key so we can test that suggestions that span live data are
			// ignored.
			if err := we.Put(storage.MakeMVCCMetadataKey(key("5")), nil); err != nil {
				t.Fatal(err)
			}

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
					keys.LocalStoreSuggestedCompactionsMin,
					keys.LocalStoreSuggestedCompactionsMax,
					func(kv storage.MVCCKeyValue) (bool, error) {
						start, end, err := keys.DecodeStoreSuggestedCompactionKey(kv.Key.Key)
						if err != nil {
							t.Fatalf("failed to decode suggested compaction key: %+v", err)
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

// TestCompactorDeadlockOnStart prevents regression of an issue that
// could cause nodes to lock up during the boot sequence. The
// compactor may receive suggestions before starting the goroutine,
// yet starting the goroutine could block on the suggestions channel,
// deadlocking the call to (Compactor).Start and thus the main node
// boot goroutine. This was observed in practice.
func TestCompactorDeadlockOnStart(t *testing.T) {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	eng := newWrappedEngine()
	stopper.AddCloser(eng)
	capFn := func() (roachpb.StoreCapacity, error) {
		return roachpb.StoreCapacity{}, errors.New("never called")
	}
	doneFn := func(_ context.Context) {}
	st := cluster.MakeTestingClusterSettings()
	compactor := NewCompactor(st, eng, capFn, doneFn)

	compactor.ch <- struct{}{}

	compactor.Start(context.Background(), stopper)
}

// TestCompactorProcessingInitialization verifies that a compactor gets
// started with processing if the queue is non-empty.
func TestCompactorProcessingInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()

	capacityFn := func() (roachpb.StoreCapacity, error) {
		return roachpb.StoreCapacity{LogicalBytes: 100 * thresholdBytes.Default()}, nil
	}
	compactor, we, compactionCount, cleanup := testSetup(capacityFn)
	defer cleanup()

	// Add a suggested compaction -- this won't get processed by this
	// compactor for an hour.
	minInterval.Override(&compactor.st.SV, time.Hour)
	compactor.Suggest(context.Background(), kvserverpb.SuggestedCompaction{
		StartKey: key("a"), EndKey: key("b"),
		Compaction: kvserverpb.Compaction{
			Bytes:            thresholdBytes.Default(),
			SuggestedAtNanos: timeutil.Now().UnixNano(),
		},
	})

	// Create a new fast compactor with a short wait time for processing,
	// using the same engine so that it sees a non-empty queue.
	stopper := stop.NewStopper()
	doneFn := func(_ context.Context) { atomic.AddInt32(compactionCount, 1) }
	st := cluster.MakeTestingClusterSettings()
	fastCompactor := NewCompactor(st, we, capacityFn, doneFn)
	minInterval.Override(&fastCompactor.st.SV, time.Millisecond)
	fastCompactor.Start(context.Background(), stopper)
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
			LogicalBytes: 100 * thresholdBytes.Default(),
			Available:    100 * thresholdBytes.Default(),
		}, nil
	}
	compactor, we, compactionCount, cleanup := testSetup(capacityFn)
	minInterval.Override(&compactor.st.SV, time.Millisecond)
	// NB: The compactor had a bug where it would never revisit skipped compactions
	// alone when there wasn't also a new suggestion. Making the max age larger
	// than the min interval exercises that code path (flakily).
	maxSuggestedCompactionRecordAge.Override(&compactor.st.SV, 5*time.Millisecond)
	defer cleanup()

	// Add a suggested compaction that won't get processed because it's
	// not over any of the thresholds.
	compactor.Suggest(context.Background(), kvserverpb.SuggestedCompaction{
		StartKey: key("a"), EndKey: key("b"),
		Compaction: kvserverpb.Compaction{
			Bytes:            thresholdBytes.Default() - 1,
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
		if a, e := compactor.Metrics.BytesSkipped.Count(), thresholdBytes.Get(&compactor.st.SV)-1; a != e {
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

// TestCompactorDisabled that a disabled compactor throws away past and future
// suggestions.
func TestCompactorDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()

	threshold := int64(10)

	capacityFn := func() (roachpb.StoreCapacity, error) {
		return roachpb.StoreCapacity{
			LogicalBytes: 100 * threshold,
			Available:    100 * threshold,
		}, nil
	}
	compactor, we, compactionCount, cleanup := testSetup(capacityFn)
	minInterval.Override(&compactor.st.SV, time.Millisecond)
	maxSuggestedCompactionRecordAge.Override(&compactor.st.SV, 24*time.Hour) // large
	thresholdBytesAvailableFraction.Override(&compactor.st.SV, 0.0)          // disable
	thresholdBytesUsedFraction.Override(&compactor.st.SV, 0.0)               // disable
	defer cleanup()

	compactor.Suggest(context.Background(), kvserverpb.SuggestedCompaction{
		StartKey: key("a"), EndKey: key("b"),
		Compaction: kvserverpb.Compaction{
			// Suggest so little that this suggestion plus the one below stays below
			// the threshold. Otherwise this test gets racy and difficult to fix
			// without remodeling the compactor.
			Bytes:            threshold / 3,
			SuggestedAtNanos: timeutil.Now().UnixNano(),
		},
	})

	enabled.Override(&compactor.st.SV, false)

	compactor.Suggest(context.Background(), kvserverpb.SuggestedCompaction{
		// Note that we don't reuse the same interval above or we hit another race,
		// in which the compactor discards the first suggestion and wipes out the
		// second one with it, without incrementing the discarded metric.
		StartKey: key("b"), EndKey: key("c"),
		Compaction: kvserverpb.Compaction{
			Bytes:            threshold / 3,
			SuggestedAtNanos: timeutil.Now().UnixNano(),
		},
	})

	// Verify that the record is deleted without a compaction and that the
	// bytes are recorded as having been skipped.
	testutils.SucceedsSoon(t, func() error {
		if a, e := atomic.LoadInt32(compactionCount), int32(0); a != e {
			t.Fatalf("expected compactions processed %d; got %d", e, a)
		}

		comps := we.GetCompactions()
		if !reflect.DeepEqual([]roachpb.Span(nil), comps) {
			return fmt.Errorf("expected nil compactions; got %+v", comps)
		}
		if a, e := compactor.Metrics.BytesSkipped.Count(), 2*(threshold/3); a != e {
			return fmt.Errorf("expected skipped bytes %d; got %d", e, a)
		}

		// Verify compaction queue is empty.
		if bytesQueued, err := compactor.examineQueue(context.Background()); err != nil || bytesQueued > 0 {
			return fmt.Errorf("compaction queue not empty (%d bytes) or err %v", bytesQueued, err)
		}
		return nil
	})
}
