// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gc

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// randomRunGCTestSpec specifies a distribution to create random data for
// running randomized GC tests as well as benchmarking new code vs preserved
// legacy GC.
type randomRunGCTestSpec struct {
	ds           distSpec
	now          hlc.Timestamp
	ttlSec       int32
	intentAgeSec int32
}

var (
	fewVersionsTinyRows = uniformDistSpec{
		tsSecFrom: 1, tsSecTo: 100,
		keySuffixMin: 2, keySuffixMax: 6,
		valueLenMin: 1, valueLenMax: 1,
		deleteFrac:      0,
		keysPerValueMin: 1, keysPerValueMax: 2,
		intentFrac: .1,
	}
	someVersionsMidSizeRows = uniformDistSpec{
		tsSecFrom: 1, tsSecTo: 100,
		keySuffixMin: 8, keySuffixMax: 8,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:      .1,
		keysPerValueMin: 1, keysPerValueMax: 100,
		intentFrac: .1,
	}
	lotsOfVersionsMidSizeRows = uniformDistSpec{
		tsSecFrom: 1, tsSecTo: 100,
		keySuffixMin: 8, keySuffixMax: 8,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:      .1,
		keysPerValueMin: 1000, keysPerValueMax: 1000000,
		intentFrac: .1,
	}
	// This spec is identical to someVersionsMidSizeRows except for number of
	// intents.
	someVersionsMidSizeRowsLotsOfIntents = uniformDistSpec{
		tsSecFrom: 1, tsSecTo: 100,
		keySuffixMin: 8, keySuffixMax: 8,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:      .1,
		keysPerValueMin: 1, keysPerValueMax: 100,
		intentFrac: 1,
	}
	someVersionsWithSomeRangeKeys = uniformDistSpec{
		tsSecFrom: 1, tsSecTo: 100,
		tsSecMinIntent: 70, tsSecOldIntentTo: 85,
		keySuffixMin: 8, keySuffixMax: 8,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:      .1,
		keysPerValueMin: 1,
		keysPerValueMax: 100,
		intentFrac:      .1,
		oldIntentFrac:   .1,
		rangeKeyFrac:    .1,
	}
)

const intentAgeThreshold = 2 * time.Hour

// TestRunNewVsOld exercises the behavior of Run relative to the old
// implementation. It runs both the new and old implementation and ensures
// that they produce exactly the same results on the same set of keys.
func TestRunNewVsOld(t *testing.T) {
	ctx := context.Background()
	const N = 100000

	for _, tc := range []randomRunGCTestSpec{
		{
			ds: someVersionsMidSizeRowsLotsOfIntents,
			// Current time in the future enough for intents to get resolved
			now: hlc.Timestamp{
				WallTime: (intentAgeThreshold + 100*time.Second).Nanoseconds(),
			},
			// GC everything beyond intent resolution threshold
			ttlSec: int32(intentAgeThreshold.Seconds()),
		},
		{
			ds: someVersionsMidSizeRows,
			now: hlc.Timestamp{
				WallTime: 100 * time.Second.Nanoseconds(),
			},
			ttlSec: 1,
		},
	} {
		t.Run(fmt.Sprintf("%v@%v,ttlSec=%v", tc.ds, tc.now, tc.ttlSec), func(t *testing.T) {
			rng, seed := randutil.NewTestRand()
			t.Logf("Using subtest seed: %d", seed)

			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()

			tc.ds.dist(N, rng).setupTest(t, eng, *tc.ds.desc())
			snap := eng.NewSnapshot()

			oldGCer := makeFakeGCer()
			ttl := time.Duration(tc.ttlSec) * time.Second
			newThreshold := CalculateThreshold(tc.now, ttl)
			gcInfoOld, err := runGCOld(ctx, tc.ds.desc(), snap, tc.now,
				newThreshold, RunOptions{IntentAgeThreshold: intentAgeThreshold}, ttl,
				&oldGCer,
				oldGCer.resolveIntents,
				oldGCer.resolveIntentsAsync)
			require.NoError(t, err)

			newGCer := makeFakeGCer()
			gcInfoNew, err := Run(ctx, tc.ds.desc(), snap, tc.now,
				newThreshold, RunOptions{IntentAgeThreshold: intentAgeThreshold}, ttl,
				&newGCer,
				newGCer.resolveIntents,
				newGCer.resolveIntentsAsync)
			require.NoError(t, err)

			oldGCer.normalize()
			newGCer.normalize()
			require.EqualValues(t, gcInfoOld, gcInfoNew)
			require.EqualValues(t, oldGCer, newGCer)
		})
	}
}

// BenchmarkRun benchmarks the old and implementations of Run with different
// data distributions.
func BenchmarkRun(b *testing.B) {
	ctx := context.Background()
	runGC := func(eng storage.Engine, old bool, spec randomRunGCTestSpec) (Info, error) {
		runGCFunc := Run
		if old {
			runGCFunc = runGCOld
		}
		snap := eng.NewSnapshot()
		ttl := time.Duration(spec.ttlSec) * time.Second
		intentThreshold := intentAgeThreshold
		if spec.intentAgeSec > 0 {
			intentThreshold = time.Duration(spec.intentAgeSec) * time.Second
		}
		return runGCFunc(ctx, spec.ds.desc(), snap, spec.now,
			CalculateThreshold(spec.now, ttl), RunOptions{IntentAgeThreshold: intentThreshold},
			ttl,
			NoopGCer{},
			func(ctx context.Context, intents []roachpb.Intent) error {
				return nil
			},
			func(ctx context.Context, txn *roachpb.Transaction) error {
				return nil
			})
	}
	makeTest := func(old bool, spec randomRunGCTestSpec, rng *rand.Rand) func(b *testing.B) {
		return func(b *testing.B) {
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()
			ms := spec.ds.dist(b.N, rng).setupTest(b, eng, *spec.ds.desc())
			b.SetBytes(int64(float64(ms.Total()) / float64(b.N)))
			b.ResetTimer()
			_, err := runGC(eng, old, spec)
			b.StopTimer()
			require.NoError(b, err)
		}
	}
	specsWithTTLs := func(
		ds distSpec, now hlc.Timestamp, ttls []int32,
	) (specs []randomRunGCTestSpec) {
		for _, ttl := range ttls {
			specs = append(specs, randomRunGCTestSpec{
				ds:     ds,
				now:    now,
				ttlSec: ttl,
			})
		}
		return specs
	}
	ts100 := hlc.Timestamp{WallTime: (100 * time.Second).Nanoseconds()}
	ttls := []int32{0, 25, 50, 75, 100}
	specs := specsWithTTLs(fewVersionsTinyRows, ts100, ttls)
	specs = append(specs, specsWithTTLs(someVersionsMidSizeRows, ts100, ttls)...)
	specs = append(specs, specsWithTTLs(lotsOfVersionsMidSizeRows, ts100, ttls)...)
	for _, old := range []bool{true, false} {
		b.Run(fmt.Sprintf("old=%v", old), func(b *testing.B) {
			rng, seed := randutil.NewTestRand()
			b.Logf("Using benchmark seed: %d", seed)

			for _, spec := range specs {
				b.Run(fmt.Sprint(spec.ds), makeTest(old, spec, rng))
			}
		})
	}
}

func TestNewVsInvariants(t *testing.T) {
	ctx := context.Background()
	const N = 100000

	for _, tc := range []randomRunGCTestSpec{
		{
			ds: someVersionsMidSizeRowsLotsOfIntents,
			// Current time in the future enough for intents to get resolved
			now: hlc.Timestamp{
				WallTime: (intentAgeThreshold + 100*time.Second).Nanoseconds(),
			},
			// GC everything beyond intent resolution threshold
			ttlSec: int32(intentAgeThreshold.Seconds()),
		},
		{
			ds: someVersionsMidSizeRows,
			now: hlc.Timestamp{
				WallTime: 100 * time.Second.Nanoseconds(),
			},
			ttlSec: 1,
		},
		{
			ds: someVersionsWithSomeRangeKeys,
			now: hlc.Timestamp{
				WallTime: 100 * time.Second.Nanoseconds(),
			},
			ttlSec:       10,
			intentAgeSec: 15,
		},
		{
			ds: someVersionsWithSomeRangeKeys,
			now: hlc.Timestamp{
				WallTime: 100 * time.Second.Nanoseconds(),
			},
			// Higher TTL means range tombstones between 70 sec and 50 sec are
			// not removed.
			ttlSec: 50,
		},
	} {
		t.Run(fmt.Sprintf("%v@%v,ttl=%vsec", tc.ds, tc.now, tc.ttlSec), func(t *testing.T) {
			rng, seed := randutil.NewTestRand()
			t.Logf("Using subtest seed: %d", seed)

			desc := tc.ds.desc()
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()

			sortedDistribution(tc.ds.dist(N, rng)).setupTest(t, eng, *desc)
			beforeGC := eng.NewSnapshot()

			// Run GCer over snapshot.
			ttl := time.Duration(tc.ttlSec) * time.Second
			gcThreshold := CalculateThreshold(tc.now, ttl)
			intentThreshold := tc.now.Add(-intentAgeThreshold.Nanoseconds(), 0)

			gcer := makeFakeGCer()
			gcInfoNew, err := Run(ctx, desc, beforeGC, tc.now,
				gcThreshold, RunOptions{IntentAgeThreshold: intentAgeThreshold}, ttl,
				&gcer,
				gcer.resolveIntents,
				gcer.resolveIntentsAsync)
			require.NoError(t, err)

			// Handle GC + resolve intents.
			var stats enginepb.MVCCStats
			require.NoError(t,
				storage.MVCCGarbageCollect(ctx, eng, &stats, gcer.pointKeys(), gcThreshold))
			for _, i := range gcer.intents {
				l := roachpb.LockUpdate{
					Span:   roachpb.Span{Key: i.Key},
					Txn:    i.Txn,
					Status: roachpb.ABORTED,
				}
				_, err := storage.MVCCResolveWriteIntent(ctx, eng, &stats, l)
				require.NoError(t, err, "failed to resolve intent")
			}
			for _, batch := range gcer.rangeKeyBatches() {
				rangeKeys := makeCollectableGCRangesFromGCRequests(desc.StartKey.AsRawKey(),
					desc.EndKey.AsRawKey(), batch)
				require.NoError(t,
					storage.MVCCGarbageCollectRangeKeys(ctx, eng, &stats, rangeKeys))
			}

			assertLiveData(t, eng, beforeGC, *desc, tc.now, gcThreshold, intentThreshold, ttl,
				gcInfoNew)
		})
	}
}

type historyItem struct {
	storage.MVCCKeyValue
	isRangeDel bool
}

// assertLiveData will create a stream of expected values based on full data
// set contained in provided "before" reader and compare it with the "after"
// reader that contains data after applying GC request.
// Same process is then repeated with range tombstones.
// For range tombstones, we merge all the fragments before asserting to avoid
// any dependency on how key splitting is done inside pebble.
// Generated expected values are produces by simulating GC in a naive way where
// each value is considered live if:
// - it is a value or tombstone and its timestamp is higher than gc threshold
// - it is a range tombstone and its timestamp is higher than gc threshold
// - it is a first value at or below gc threshold and there are no deletions
//   between gc threshold and the value
func assertLiveData(
	t *testing.T,
	after, before storage.Reader,
	desc roachpb.RangeDescriptor,
	now, gcThreshold, intentThreshold hlc.Timestamp,
	gcTTL time.Duration,
	gcInfo Info,
) {
	failureDetails := func(key storage.MVCCKey) string {
		return fmt.Sprintf("key=%s, GC time=%s, intent time=%s, key history=%s", key.String(),
			gcThreshold.String(), intentThreshold.String(), getKeyHistory(t, before, key.Key))
	}

	// Validation works on original data applying simple GC eligibility to key
	// history and only returning remaining elements and accounting for remaining
	// ones as gc.Info.
	expInfo := Info{
		Now:       now,
		GCTTL:     gcTTL,
		Threshold: gcThreshold,
	}
	pointIt := before.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind,
		storage.IterOptions{
			LowerBound: desc.StartKey.AsRawKey(),
			UpperBound: desc.EndKey.AsRawKey(),
			KeyTypes:   storage.IterKeyTypePointsAndRanges,
		})
	defer pointIt.Close()
	pointIt.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	pointExpectationsGenerator := getExpectationsGenerator(t, pointIt, gcThreshold, intentThreshold,
		&expInfo)

	rangeIt := before.NewMVCCIterator(storage.MVCCKeyIterKind,
		storage.IterOptions{
			LowerBound: desc.StartKey.AsRawKey(),
			UpperBound: desc.EndKey.AsRawKey(),
			KeyTypes:   storage.IterKeyTypeRangesOnly,
		})
	defer rangeIt.Close()
	rangeIt.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	expectedRanges := mergeRanges(filterRangeFragments(rangeFragmentsFromIt(t, rangeIt), gcThreshold,
		&expInfo))

	// Loop over engine data after applying GCer requests and compare with
	// expected point keys.
	itAfter := after.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound: desc.StartKey.AsRawKey(),
		UpperBound: desc.EndKey.AsRawKey(),
		KeyTypes:   storage.IterKeyTypePointsOnly,
	})
	defer itAfter.Close()

	itAfter.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	eKV, dataOk := pointExpectationsGenerator()
	for {
		ok, err := itAfter.Valid()
		require.NoError(t, err, "failed to iterate engine after GC")
		if !ok && !dataOk {
			break
		}
		if !ok {
			require.Failf(t, "reached end of GC'd engine data, but expect more", "missing key: %s",
				failureDetails(eKV.Key))
		}
		if !dataOk {
			require.Failf(t, "reached end of expected data bug engine contains more", "ungc'd key: %s",
				failureDetails(itAfter.UnsafeKey()))
		}

		switch eKV.Key.Compare(itAfter.UnsafeKey()) {
		case 1:
			assert.Failf(t, "key was not collected", failureDetails(itAfter.UnsafeKey()))
			itAfter.Next()
		case -1:
			assert.Failf(t, "key was collected by mistake", failureDetails(eKV.Key))
			eKV, dataOk = pointExpectationsGenerator()
		default:
			itAfter.Next()
			eKV, dataOk = pointExpectationsGenerator()
		}
	}

	rangeItAfter := after.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypeRangesOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	defer rangeItAfter.Close()
	rangeItAfter.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	actualRanges := mergeRanges(rangeFragmentsFromIt(t, rangeItAfter))

	// Be careful when enabling logging on tests with default large N,
	// 1000 elements is ok, but 10k or 100k entries might become unreadable.
	if log.V(1) {
		ctx := context.Background()
		log.Info(ctx, "Expected data:")
		for _, l := range formatTable(engineData(t, before, desc), desc.StartKey.AsRawKey()) {
			log.Infof(ctx, "%s", l)
		}
	}

	require.EqualValues(t, expectedRanges, actualRanges, "GC'd range tombstones")

	require.EqualValues(t, expInfo, gcInfo, "collected gc info mismatch")
}

func getExpectationsGenerator(
	t *testing.T, it storage.MVCCIterator, gcThreshold, intentThreshold hlc.Timestamp, expInfo *Info,
) func() (storage.MVCCKeyValue, bool) {
	var pending []storage.MVCCKeyValue
	return func() (storage.MVCCKeyValue, bool) {
		for {
			// First return all pending history for the previous key.
			if len(pending) > 0 {
				defer func() {
					pending = pending[1:]
				}()
				return pending[0], true
			}

			// For new key, collect intent and all versions from highest to lowest
			// to make a decision.
			var baseKey roachpb.Key
			var history []historyItem
			for {
				ok, err := it.Valid()
				require.NoError(t, err, "failed to read data from unmodified engine")
				if !ok {
					break
				}
				p, r := it.HasPointAndRange()
				if p {
					k := it.Key()
					v := it.Value()
					if len(baseKey) == 0 {
						baseKey = k.Key
						// We are only interested in range tombstones covering current point,
						// so we will add them to history of current key for analysis.
						// Bare range tombstones are ignored.
						if r {
							for _, r := range it.RangeKeys().AsRangeKeys() {
								history = append(history, historyItem{
									MVCCKeyValue: storage.MVCCKeyValue{
										Key: storage.MVCCKey{
											Key:       r.StartKey,
											Timestamp: r.Timestamp,
										},
										Value: nil,
									},
									isRangeDel: true,
								})
							}
						}
					} else if !baseKey.Equal(k.Key) {
						break
					}
					history = append(history, historyItem{
						MVCCKeyValue: storage.MVCCKeyValue{Key: k, Value: v},
						isRangeDel:   false,
					})
				}
				it.Next()
			}
			if len(history) == 0 {
				return storage.MVCCKeyValue{}, false
			}
			// Sort with zero timestamp at the beginning of the list. This could only
			// happen if we have a meta record. There could only be single record,
			// and it is always at the top of the history so zero timestamp is
			// prioritized.
			sort.Slice(history, func(i, j int) bool {
				if history[i].Key.Timestamp.IsEmpty() {
					return true
				}
				if history[j].Key.Timestamp.IsEmpty() {
					return false
				}
				return history[j].Key.Timestamp.Less(history[i].Key.Timestamp)
			})

			// Process key history slice by first filtering intents as needed and then
			// applying invariant that values on or above gc threshold should remain,
			// deletions above threshold should remain.
			// All eligible elements are copied to pending for emitting.
			stop := false
			i := 0
			for i < len(history) && !stop {
				if history[i].Key.Timestamp.IsEmpty() {
					// Intent, need to see if its TS is too old or not.
					// We need to emit intents if they are above threshold as they would be ignored by
					// resolver.
					var meta enginepb.MVCCMetadata
					require.NoError(t, protoutil.Unmarshal(history[i].Value, &meta),
						"failed to unmarshal txn metadata")
					if meta.Timestamp.ToTimestamp().Less(intentThreshold) {
						// This is an old intent. Skip intent with proposed value and continue.
						expInfo.IntentsConsidered++
						// We always use a new transaction for each intent and consider
						// operations successful in testGCer.
						expInfo.IntentTxns++
						expInfo.PushTxn++
						expInfo.ResolveTotal++
					} else {
						// Intent is not considered as a part of GC removal cycle so we keep
						// it intact if it doesn't satisfy push age check.
						pending = append(pending, history[i].MVCCKeyValue)
						pending = append(pending, history[i+1].MVCCKeyValue)
					}
					i += 2
					continue
				}

				// Apply GC checks to produce expected state.
				switch {
				case gcThreshold.Less(history[i].Key.Timestamp):
					// Any value above threshold including intents that have no timestamp.
					if !history[i].isRangeDel {
						pending = append(pending, history[i].MVCCKeyValue)
					}
					i++
				case history[i].Key.Timestamp.LessEq(gcThreshold) && len(history[i].Value) > 0:
					// First value on or under threshold should be preserved, but the rest
					// of history should be skipped.
					pending = append(pending, history[i].MVCCKeyValue)
					i++
					stop = true
				default:
					// This is ts <= threshold and v == nil
					stop = true
				}
			}

			// Remaining part of the history is removed, so accumulate it as gc stats.
			if i < len(history) {
				removedKeys := false
				for ; i < len(history); i++ {
					if !history[i].isRangeDel {
						expInfo.AffectedVersionsKeyBytes += int64(history[i].Key.EncodedSize())
						expInfo.AffectedVersionsValBytes += int64(len(history[i].Value))
						removedKeys = true
					}
				}
				if removedKeys {
					expInfo.NumKeysAffected++
				}
			}
		}
	}
}

func getKeyHistory(t *testing.T, r storage.Reader, key roachpb.Key) string {
	var result []string

	it := r.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound:           key,
		UpperBound:           key.Next(),
		KeyTypes:             storage.IterKeyTypePointsAndRanges,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	defer it.Close()

	it.SeekGE(storage.MVCCKey{Key: key})
	for {
		ok, err := it.Valid()
		require.NoError(t, err, "failed to read engine iterator")
		if !ok {
			break
		}
		p, r := it.HasPointAndRange()
		if !p {
			it.Next()
			continue
		}
		if !it.UnsafeKey().Key.Equal(key) {
			break
		}
		if r && len(result) == 0 {
			for _, rk := range it.RangeKeys().AsRangeKeyValues() {
				result = append(result, fmt.Sprintf("R:%s", rk.RangeKey.String()))
			}
		}
		result = append(result, fmt.Sprintf("P:%s(%d)", it.UnsafeKey().String(), len(it.UnsafeValue())))
		it.Next()
	}

	return strings.Join(result, ", ")
}

func rangeFragmentsFromIt(t *testing.T, it storage.MVCCIterator) [][]storage.MVCCRangeKeyValue {
	var result [][]storage.MVCCRangeKeyValue
	for {
		ok, err := it.Valid()
		require.NoError(t, err, "failed to read range tombstones from iterator")
		if !ok {
			break
		}
		if _, hasRange := it.HasPointAndRange(); hasRange {
			result = append(result, it.RangeKeys().Clone().AsRangeKeyValues())
		}
		it.NextKey()
	}
	return result
}

// Filter all fragments that match GC criteria and update gcinfo accordingly.
func filterRangeFragments(
	fragments [][]storage.MVCCRangeKeyValue, gcThreshold hlc.Timestamp, expInfo *Info,
) [][]storage.MVCCRangeKeyValue {
	var result [][]storage.MVCCRangeKeyValue
	for _, stack := range fragments {
		var newStack []storage.MVCCRangeKeyValue
		for i, r := range stack {
			if r.RangeKey.Timestamp.LessEq(gcThreshold) {
				// Update expectations:
				// On lowest range timestamp bump range counter.
				if i == len(stack)-1 {
					expInfo.NumRangeKeysAffected++
				}
				// If all fragments are deleted then keys bytes are accounted for.
				if i == 0 {
					expInfo.AffectedVersionsRangeKeyBytes += int64(len(r.RangeKey.StartKey) + len(r.RangeKey.EndKey))
				}
				// Count timestamps for all versions of range keys.
				expInfo.AffectedVersionsRangeKeyBytes += storage.MVCCVersionTimestampSize
				expInfo.AffectedVersionsRangeValBytes += int64(len(r.Value))
			} else {
				newStack = append(newStack, r)
			}
		}
		if len(newStack) > 0 {
			result = append(result, newStack)
		}
	}
	return result
}

func mergeRanges(fragments [][]storage.MVCCRangeKeyValue) []storage.MVCCRangeKeyValue {
	var result []storage.MVCCRangeKeyValue
	var partialRangeKeys []storage.MVCCRangeKeyValue
	var lastEnd roachpb.Key
	for _, stack := range fragments {
		start, end := stack[0].RangeKey.StartKey, stack[0].RangeKey.EndKey
		if lastEnd.Equal(start) {
			// Try merging keys by timestamp.
			var newPartial []storage.MVCCRangeKeyValue
			i, j := 0, 0
			for i < len(stack) && j < len(partialRangeKeys) {
				switch stack[i].RangeKey.Timestamp.Compare(partialRangeKeys[j].RangeKey.Timestamp) {
				case 1:
					newPartial = append(newPartial, stack[i])
					i++
				case 0:
					// We don't compare range values here as it would complicate things
					// too much and not worth for this test as we don't expect mixed
					// tombstone types.
					newPartial = append(newPartial, storage.MVCCRangeKeyValue{
						RangeKey: storage.MVCCRangeKey{
							StartKey:  partialRangeKeys[j].RangeKey.StartKey,
							EndKey:    stack[i].RangeKey.EndKey,
							Timestamp: partialRangeKeys[j].RangeKey.Timestamp,
						},
						Value: partialRangeKeys[j].Value,
					})
					i++
					j++
				case -1:
					newPartial = append(newPartial, partialRangeKeys[j])
					j++
				}
			}
			for ; i < len(stack); i++ {
				newPartial = append(newPartial, stack[i])
			}
			for ; j < len(partialRangeKeys); j++ {
				newPartial = append(newPartial, partialRangeKeys[j])
			}
			partialRangeKeys = newPartial
		} else {
			result = append(result, partialRangeKeys...)
			partialRangeKeys = make([]storage.MVCCRangeKeyValue, 0, len(stack))
			partialRangeKeys = append(partialRangeKeys, stack...)
		}
		lastEnd = end
	}
	result = append(result, partialRangeKeys...)
	return result
}

type fakeGCer struct {
	gcKeys map[string]roachpb.GCRequest_GCKey
	// fake GCer stores range key batches as it since we need to be able to
	// feed them into MVCCGarbageCollectRangeKeys and ranges argument should be
	// non-overlapping.
	gcRangeKeyBatches [][]roachpb.GCRequest_GCRangeKey
	gcRangeDeleteKeys []roachpb.GCRequest_GCClearRangeKey
	threshold         Threshold
	intents           []roachpb.Intent
	batches           [][]roachpb.Intent
	txnIntents        []txnIntents
}

func makeFakeGCer() fakeGCer {
	return fakeGCer{
		gcKeys: make(map[string]roachpb.GCRequest_GCKey),
	}
}

var _ GCer = (*fakeGCer)(nil)

func (f *fakeGCer) SetGCThreshold(ctx context.Context, t Threshold) error {
	f.threshold = t
	return nil
}

func (f *fakeGCer) GC(ctx context.Context, keys []roachpb.GCRequest_GCKey,
	rangeKeys []roachpb.GCRequest_GCRangeKey, clearRangeKey *roachpb.GCRequest_GCClearRangeKey,
) error {
	for _, k := range keys {
		f.gcKeys[k.Key.String()] = k
	}
	f.gcRangeKeyBatches = append(f.gcRangeKeyBatches, rangeKeys)
	if clearRangeKey != nil {
		f.gcRangeDeleteKeys = append(f.gcRangeDeleteKeys, *clearRangeKey)
	}
	return nil
}

func (f *fakeGCer) resolveIntentsAsync(_ context.Context, txn *roachpb.Transaction) error {
	f.txnIntents = append(f.txnIntents, txnIntents{txn: txn, intents: txn.LocksAsLockUpdates()})
	return nil
}

func (f *fakeGCer) resolveIntents(_ context.Context, intents []roachpb.Intent) error {
	f.intents = append(f.intents, intents...)
	f.batches = append(f.batches, intents)
	return nil
}

func (f *fakeGCer) normalize() {
	sortIntents := func(i, j int) bool {
		return intentLess(&f.intents[i], &f.intents[j])
	}
	sort.Slice(f.intents, sortIntents)
	for i := range f.txnIntents {
		sort.Slice(f.txnIntents[i].intents, sortIntents)
	}
	sort.Slice(f.txnIntents, func(i, j int) bool {
		return f.txnIntents[i].txn.ID.String() < f.txnIntents[j].txn.ID.String()
	})
	f.batches = nil
}

func (f *fakeGCer) pointKeys() []roachpb.GCRequest_GCKey {
	var reqs []roachpb.GCRequest_GCKey
	for _, r := range f.gcKeys {
		reqs = append(reqs, r)
	}
	return reqs
}

func (f *fakeGCer) rangeKeyBatches() [][]roachpb.GCRequest_GCRangeKey {
	return f.gcRangeKeyBatches
}

func (f *fakeGCer) rangeKeys() []roachpb.GCRequest_GCRangeKey {
	var reqs []roachpb.GCRequest_GCRangeKey
	for _, r := range f.gcRangeKeyBatches {
		reqs = append(reqs, r...)
	}
	return reqs
}

func intentLess(a, b *roachpb.Intent) bool {
	cmp := a.Key.Compare(b.Key)
	switch {
	case cmp < 0:
		return true
	case cmp > 0:
		return false
	default:
		return a.Txn.ID.String() < b.Txn.ID.String()
	}
}

type txnIntents struct {
	txn     *roachpb.Transaction
	intents []roachpb.LockUpdate
}

// makeCollectableGCRangesFromGCRequests mirrors
// MakeCollectableGCRangesFromGCRequests to break cyclic dependecies.
func makeCollectableGCRangesFromGCRequests(
	rangeStart, rangeEnd roachpb.Key, rangeKeys []roachpb.GCRequest_GCRangeKey,
) []storage.CollectableGCRangeKey {
	collectableKeys := make([]storage.CollectableGCRangeKey, len(rangeKeys))
	for i, rk := range rangeKeys {
		leftPeekBound := rk.StartKey.Prevish(roachpb.PrevishKeyLength)
		if len(rangeStart) > 0 && leftPeekBound.Compare(rangeStart) <= 0 {
			leftPeekBound = rangeStart
		}
		rightPeekBound := rk.EndKey.Next()
		if len(rangeEnd) > 0 && rightPeekBound.Compare(rangeEnd) >= 0 {
			rightPeekBound = rangeEnd
		}
		collectableKeys[i] = storage.CollectableGCRangeKey{
			MVCCRangeKey: storage.MVCCRangeKey{
				StartKey:  rk.StartKey,
				EndKey:    rk.EndKey,
				Timestamp: rk.Timestamp,
			},
			LatchSpan: roachpb.Span{
				Key:    leftPeekBound,
				EndKey: rightPeekBound,
			},
		}
	}
	return collectableKeys
}
