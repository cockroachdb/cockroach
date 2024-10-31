// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gc

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// randomRunGCTestSpec specifies a distribution to create random data for
// running randomized GC tests as well as benchmarking new code vs preserved
// legacy GC.
type randomRunGCTestSpec struct {
	ds                distSpec
	now               hlc.Timestamp
	ttlSec            int32
	intentAgeSec      int32
	clearRangeMinKeys int64 // Set to 0 for test default value or -1 to disable.
}

var (
	fewVersionsTinyRows = uniformDistSpec{
		tsSecFrom: 1, tsSecTo: 100,
		keySuffixMin: 2, keySuffixMax: 6,
		valueLenMin: 1, valueLenMax: 1,
		deleteFrac:        0,
		versionsPerKeyMin: 1, versionsPerKeyMax: 2,
		intentFrac: .1,
	}
	someVersionsMidSizeRows = uniformDistSpec{
		tsSecFrom: 1, tsSecTo: 100,
		keySuffixMin: 8, keySuffixMax: 8,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:        .1,
		versionsPerKeyMin: 1, versionsPerKeyMax: 100,
		intentFrac: .1,
	}
	lotsOfVersionsMidSizeRows = uniformDistSpec{
		tsSecFrom: 1, tsSecTo: 100,
		keySuffixMin: 7, keySuffixMax: 9,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:        .1,
		versionsPerKeyMin: 1000, versionsPerKeyMax: 1000000,
		intentFrac: .1,
	}
	// This spec is identical to someVersionsMidSizeRows except for number of
	// intents.
	someVersionsMidSizeRowsLotsOfIntents = uniformDistSpec{
		tsSecFrom: 1, tsSecTo: 100,
		keySuffixMin: 7, keySuffixMax: 9,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:        .1,
		versionsPerKeyMin: 1, versionsPerKeyMax: 100,
		intentFrac: 1,
	}
	someVersionsWithSomeRangeKeys = uniformDistSpec{
		tsSecFrom: 1, tsSecTo: 100,
		tsSecMinIntent: 70, tsSecOldIntentTo: 85,
		keySuffixMin: 7, keySuffixMax: 9,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:        .1,
		versionsPerKeyMin: 1,
		versionsPerKeyMax: 100,
		intentFrac:        .1,
		oldIntentFrac:     .1,
		rangeKeyFrac:      .1,
	}

	// smallEngineBlocks configures Pebble with a block size of 1 byte, to provoke
	// bugs in time-bound iterators.
	smallEngineBlocks = metamorphic.ConstantWithTestBool("small-engine-blocks", false)
)

const lockAgeThreshold = 2 * time.Hour
const txnCleanupThreshold = time.Hour

// BenchmarkRun benchmarks Run with different
// data distributions.
func BenchmarkRun(b *testing.B) {
	ctx := context.Background()
	runGC := func(eng storage.Engine, spec randomRunGCTestSpec) (Info, error) {
		snap := eng.NewSnapshot()
		defer snap.Close()
		ttl := time.Duration(spec.ttlSec) * time.Second
		intentThreshold := lockAgeThreshold
		if spec.intentAgeSec > 0 {
			intentThreshold = time.Duration(spec.intentAgeSec) * time.Second
		}
		return Run(ctx, spec.ds.desc(), snap, spec.now,
			CalculateThreshold(spec.now, ttl), RunOptions{
				LockAgeThreshold:    intentThreshold,
				TxnCleanupThreshold: txnCleanupThreshold,
				ClearRangeMinKeys:   defaultClearRangeMinKeys,
			},
			ttl,
			NoopGCer{},
			func(ctx context.Context, locks []roachpb.Lock) error {
				return nil
			},
			func(ctx context.Context, txn *roachpb.Transaction) error {
				return nil
			})
	}
	makeTest := func(spec randomRunGCTestSpec, rng *rand.Rand) func(b *testing.B) {
		return func(b *testing.B) {
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()
			ms := spec.ds.dist(b.N, rng).setupTest(b, eng, *spec.ds.desc())
			b.SetBytes(int64(float64(ms.Total()) / float64(b.N)))
			b.ResetTimer()
			_, err := runGC(eng, spec)
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
	b.Run("old=false", func(b *testing.B) {
		rng, seed := randutil.NewTestRand()
		b.Logf("Using benchmark seed: %d", seed)

		for _, spec := range specs {
			b.Run(fmt.Sprint(spec.ds), makeTest(spec, rng))
		}
	})
}

func TestNewVsInvariants(t *testing.T) {
	ctx := context.Background()
	N := 100000
	if util.RaceEnabled {
		// Reduce the row count under race. Otherwise, the test takes >5m.
		N /= 100
	}

	for _, tc := range []randomRunGCTestSpec{
		{
			ds: someVersionsMidSizeRowsLotsOfIntents,
			// Current time in the future enough for intents to get resolved
			now: hlc.Timestamp{
				WallTime: (lockAgeThreshold + 100*time.Second).Nanoseconds(),
			},
			// GC everything beyond intent resolution threshold
			ttlSec: int32(lockAgeThreshold.Seconds()),
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
			ttlSec:       10,
			intentAgeSec: 15,
			// Reduced clear range sequence to allow key batches to be split at
			// various boundaries.
			clearRangeMinKeys: 49,
		},
		{
			ds: someVersionsWithSomeRangeKeys,
			now: hlc.Timestamp{
				WallTime: 100 * time.Second.Nanoseconds(),
			},
			ttlSec:       10,
			intentAgeSec: 15,
			// Disable clear range to reliably engage specific code paths that handle
			// simplified no-clear-range batching.
			clearRangeMinKeys: -1,
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
		{
			ds: someVersionsWithSomeRangeKeys,
			now: hlc.Timestamp{
				WallTime: 100 * time.Second.Nanoseconds(),
			},
			// Higher TTL means range tombstones between 70 sec and 50 sec are
			// not removed.
			ttlSec: 50,
			// Reduced clear range sequence to allow key batches to be split at
			// various boundaries.
			clearRangeMinKeys: 10,
		},
	} {
		clearRangeMinKeys := int64(0)
		switch tc.clearRangeMinKeys {
		case -1:
		case 0:
			// Default value for test.
			clearRangeMinKeys = 100
		default:
			clearRangeMinKeys = tc.clearRangeMinKeys
		}
		name := fmt.Sprintf("%v@%v,ttl=%vsec,clearRangeMinKeys=%d", tc.ds, tc.now, tc.ttlSec,
			clearRangeMinKeys)
		t.Run(name, func(t *testing.T) {
			rng, seed := randutil.NewTestRand()
			t.Logf("Using subtest seed: %d", seed)

			desc := tc.ds.desc()
			eng := storage.NewDefaultInMemForTesting(storage.If(smallEngineBlocks, storage.BlockSize(1)))
			defer eng.Close()

			sortedDistribution(tc.ds.dist(N, rng)).setupTest(t, eng, *desc)
			beforeGC := eng.NewSnapshot()
			defer beforeGC.Close()

			// Run GCer over snapshot.
			ttl := time.Duration(tc.ttlSec) * time.Second
			gcThreshold := CalculateThreshold(tc.now, ttl)
			intentThreshold := tc.now.Add(-lockAgeThreshold.Nanoseconds(), 0)

			gcer := makeFakeGCer()
			gcInfoNew, err := Run(ctx, desc, beforeGC, tc.now,
				gcThreshold, RunOptions{
					LockAgeThreshold:    lockAgeThreshold,
					TxnCleanupThreshold: txnCleanupThreshold,
					ClearRangeMinKeys:   clearRangeMinKeys,
				}, ttl,
				&gcer,
				gcer.resolveIntents,
				gcer.resolveIntentsAsync)
			require.NoError(t, err)

			// Handle GC + resolve intents.
			var stats enginepb.MVCCStats
			require.NoError(t,
				storage.MVCCGarbageCollect(ctx, eng, &stats, gcer.pointKeys(), gcThreshold))
			for _, i := range gcer.locks {
				l := roachpb.LockUpdate{
					Span:   roachpb.Span{Key: i.Key},
					Txn:    i.Txn,
					Status: roachpb.ABORTED,
				}
				_, _, _, _, err := storage.MVCCResolveWriteIntent(
					ctx, eng, &stats, l, storage.MVCCResolveWriteIntentOptions{},
				)
				require.NoError(t, err, "failed to resolve intent")
			}
			for _, cr := range gcer.clearRanges() {
				require.False(t, cr.StartKeyTimestamp.IsEmpty(), "unexpected full range delete")
				if cr.EndKey == nil {
					cr.EndKey = cr.StartKey.Next()
				}
				require.NoError(t,
					storage.MVCCGarbageCollectPointsWithClearRange(ctx, eng, &stats, cr.StartKey, cr.EndKey,
						cr.StartKeyTimestamp, gcThreshold))
			}
			for _, batch := range gcer.rangeKeyBatches() {
				rangeKeys := makeCollectableGCRangesFromGCRequests(desc.StartKey.AsRawKey(),
					desc.EndKey.AsRawKey(), batch)
				require.NoError(t,
					storage.MVCCGarbageCollectRangeKeys(ctx, eng, &stats, rangeKeys))
			}

			// For the sake of assertion we need to reset this counter as it signals
			// counter for specific feature rather than processed data. Data and number
			// of cleared keys and versions should be the same regardless of operations
			// used to clear it.
			gcInfoNew.ClearRangeSpanOperations = 0
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
//   - it is a value or tombstone and its timestamp is higher than gc threshold
//   - it is a range tombstone and its timestamp is higher than gc threshold
//   - it is a first value at or below gc threshold and there are no deletions
//     between gc threshold and the value
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
	pointIt, err := before.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound: desc.StartKey.AsRawKey(),
		UpperBound: desc.EndKey.AsRawKey(),
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
	})
	require.NoError(t, err)
	defer pointIt.Close()
	pointIt.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	pointExpectationsGenerator := getExpectationsGenerator(t, pointIt, gcThreshold, intentThreshold,
		&expInfo)

	rangeIt, err := before.NewMVCCIterator(context.Background(), storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound: desc.StartKey.AsRawKey(),
		UpperBound: desc.EndKey.AsRawKey(),
		KeyTypes:   storage.IterKeyTypeRangesOnly,
	})
	require.NoError(t, err)
	defer rangeIt.Close()
	rangeIt.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	expectedRanges := mergeRanges(filterRangeFragments(rangeFragmentsFromIt(t, rangeIt), gcThreshold,
		&expInfo))

	// Loop over engine data after applying GCer requests and compare with
	// expected point keys.
	itAfter, err := after.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound: desc.StartKey.AsRawKey(),
		UpperBound: desc.EndKey.AsRawKey(),
		KeyTypes:   storage.IterKeyTypePointsOnly,
	})
	require.NoError(t, err)
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

	rangeItAfter, err := after.NewMVCCIterator(context.Background(), storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypeRangesOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	require.NoError(t, err)
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
					k := it.UnsafeKey().Clone()
					v, err := it.Value()
					require.NoError(t, err)
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
						expInfo.LocksConsidered++
						// We always use a new transaction for each intent and consider
						// operations successful in testGCer.
						expInfo.LockTxns++
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
				v, err := storage.DecodeMVCCValue(history[i].Value)
				require.NoError(t, err)
				switch {
				case gcThreshold.Less(history[i].Key.Timestamp):
					// Any value above threshold including intents that have no timestamp.
					if !history[i].isRangeDel {
						pending = append(pending, history[i].MVCCKeyValue)
					}
					i++
				case history[i].Key.Timestamp.LessEq(gcThreshold) && !v.IsTombstone():
					// First value on or under threshold should be preserved, but the rest
					// of history should be skipped.
					pending = append(pending, history[i].MVCCKeyValue)
					i++
					stop = true
				default:
					// This is ts <= threshold and v.IsTombstone().
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

	it, err := r.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound:           key,
		UpperBound:           key.Next(),
		KeyTypes:             storage.IterKeyTypePointsAndRanges,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	require.NoError(t, err)
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
		v, err := it.UnsafeValue()
		require.NoError(t, err)
		result = append(result, fmt.Sprintf("P:%s(%d)", it.UnsafeKey().String(), len(v)))
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
							StartKey:               partialRangeKeys[j].RangeKey.StartKey,
							EndKey:                 stack[i].RangeKey.EndKey,
							Timestamp:              partialRangeKeys[j].RangeKey.Timestamp,
							EncodedTimestampSuffix: partialRangeKeys[j].RangeKey.EncodedTimestampSuffix,
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
	gcKeys          map[string]kvpb.GCRequest_GCKey
	gcPointsBatches [][]kvpb.GCRequest_GCKey
	// fake GCer stores range key batches as it since we need to be able to
	// feed them into MVCCGarbageCollectRangeKeys and ranges argument should be
	// non-overlapping.
	gcRangeKeyBatches [][]kvpb.GCRequest_GCRangeKey
	gcClearRanges     []kvpb.GCRequest_GCClearRange
	threshold         Threshold
	locks             []roachpb.Lock
	batches           [][]roachpb.Lock
	txnIntents        []txnIntents
}

func makeFakeGCer() fakeGCer {
	return fakeGCer{
		gcKeys: make(map[string]kvpb.GCRequest_GCKey),
	}
}

var _ GCer = (*fakeGCer)(nil)

func (f *fakeGCer) SetGCThreshold(ctx context.Context, t Threshold) error {
	f.threshold = t
	return nil
}

func (f *fakeGCer) GC(
	ctx context.Context,
	keys []kvpb.GCRequest_GCKey,
	rangeKeys []kvpb.GCRequest_GCRangeKey,
	clearRange *kvpb.GCRequest_GCClearRange,
) error {
	for _, k := range keys {
		f.gcKeys[k.Key.String()] = k
	}
	if keys != nil {
		f.gcPointsBatches = append(f.gcPointsBatches, keys)
	}
	if rangeKeys != nil {
		f.gcRangeKeyBatches = append(f.gcRangeKeyBatches, rangeKeys)
	}
	if clearRange != nil {
		f.gcClearRanges = append(f.gcClearRanges, kvpb.GCRequest_GCClearRange{
			StartKey:          clearRange.StartKey.Clone(),
			StartKeyTimestamp: clearRange.StartKeyTimestamp,
			EndKey:            clearRange.EndKey.Clone(),
		})
	}
	return nil
}

func (f *fakeGCer) resolveIntentsAsync(_ context.Context, txn *roachpb.Transaction) error {
	f.txnIntents = append(f.txnIntents, txnIntents{txn: txn, intents: txn.LocksAsLockUpdates()})
	return nil
}

func (f *fakeGCer) resolveIntents(_ context.Context, locks []roachpb.Lock) error {
	f.locks = append(f.locks, locks...)
	f.batches = append(f.batches, locks)
	return nil
}

// normalize will converge GC request history between old and new
// implementations and drop info that is not produced by old GC.
// It will however preserve info like clear range which covers functionality
// not relevant for old gc as it shouldn't be compared between such invocations.
func (f *fakeGCer) normalize() {
	sortIntents := func(i, j int) bool {
		return lockLess(&f.locks[i], &f.locks[j])
	}
	sort.Slice(f.locks, sortIntents)
	for i := range f.txnIntents {
		sort.Slice(f.txnIntents[i].intents, sortIntents)
	}
	sort.Slice(f.txnIntents, func(i, j int) bool {
		return f.txnIntents[i].txn.ID.String() < f.txnIntents[j].txn.ID.String()
	})
	f.batches = nil
	f.gcPointsBatches = nil
}

func (f *fakeGCer) pointKeys() []kvpb.GCRequest_GCKey {
	var reqs []kvpb.GCRequest_GCKey
	for _, r := range f.gcKeys {
		reqs = append(reqs, r)
	}
	return reqs
}

func (f *fakeGCer) rangeKeyBatches() [][]kvpb.GCRequest_GCRangeKey {
	return f.gcRangeKeyBatches
}

func (f *fakeGCer) rangeKeys() []kvpb.GCRequest_GCRangeKey {
	var reqs []kvpb.GCRequest_GCRangeKey
	for _, r := range f.gcRangeKeyBatches {
		reqs = append(reqs, r...)
	}
	return reqs
}

func (f *fakeGCer) clearRanges() []kvpb.GCRequest_GCClearRange {
	return f.gcClearRanges
}

func lockLess(a, b *roachpb.Lock) bool {
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
	rangeStart, rangeEnd roachpb.Key, rangeKeys []kvpb.GCRequest_GCRangeKey,
) []storage.CollectableGCRangeKey {
	collectableKeys := make([]storage.CollectableGCRangeKey, len(rangeKeys))
	for i, rk := range rangeKeys {
		peekBounds := expandRangeSpan(roachpb.Span{
			Key:    rk.StartKey,
			EndKey: rk.EndKey,
		}, roachpb.Span{
			Key:    rangeStart,
			EndKey: rangeEnd,
		})
		collectableKeys[i] = storage.CollectableGCRangeKey{
			MVCCRangeKey: storage.MVCCRangeKey{
				StartKey:  rk.StartKey,
				EndKey:    rk.EndKey,
				Timestamp: rk.Timestamp,
			},
			LatchSpan: peekBounds,
		}
	}
	return collectableKeys
}

func expandRangeSpan(rangeKey, limits roachpb.Span) roachpb.Span {
	leftPeekBound := rangeKey.Key.Prevish(roachpb.PrevishKeyLength)
	if len(limits.Key) > 0 && leftPeekBound.Compare(limits.Key) <= 0 {
		leftPeekBound = limits.Key
	}
	rightPeekBound := rangeKey.EndKey.Next()
	if len(limits.EndKey) > 0 && rightPeekBound.Compare(limits.EndKey) >= 0 {
		rightPeekBound = limits.EndKey
	}
	return roachpb.Span{
		Key:    leftPeekBound,
		EndKey: rightPeekBound,
	}
}
