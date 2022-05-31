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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// randomRunGCTestSpec specifies a distribution for to create random data for
// testing Run
type randomRunGCTestSpec struct {
	ds  distSpec
	now hlc.Timestamp
	ttl int32 // seconds
}

var (
	fewVersionsTinyRows = uniformDistSpec{
		tsSecFrom: 0, tsSecTo: 100,
		keySuffixMin: 2, keySuffixMax: 3,
		valueLenMin: 1, valueLenMax: 1,
		deleteFrac:      0,
		keysPerValueMin: 1, keysPerValueMax: 2,
		intentFrac: .1,
	}
	someVersionsMidSizeRows = uniformDistSpec{
		tsSecFrom: 0, tsSecTo: 100,
		keySuffixMin: 8, keySuffixMax: 8,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:      .1,
		keysPerValueMin: 1, keysPerValueMax: 100,
		intentFrac: .1,
	}
	lotsOfVersionsMidSizeRows = uniformDistSpec{
		tsSecFrom: 0, tsSecTo: 100,
		keySuffixMin: 8, keySuffixMax: 8,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:      .1,
		keysPerValueMin: 1000, keysPerValueMax: 1000000,
		intentFrac: .1,
	}
)

const intentAgeThreshold = 2 * time.Hour

// TestRunNewVsOld exercises the behavior of Run relative to the old
// implementation. It runs both the new and old implementation and ensures
// that they produce exactly the same results on the same set of keys.
func TestRunNewVsOld(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	ctx := context.Background()
	const N = 100000

	someVersionsMidSizeRowsLotsOfIntents := someVersionsMidSizeRows
	someVersionsMidSizeRowsLotsOfIntents.intentFrac = 1
	for _, tc := range []randomRunGCTestSpec{
		{
			ds: someVersionsMidSizeRowsLotsOfIntents,
			// Current time in the future enough for intents to get resolved
			now: hlc.Timestamp{
				WallTime: (intentAgeThreshold + 100*time.Second).Nanoseconds(),
			},
			// GC everything beyond intent resolution threshold
			ttl: int32(intentAgeThreshold.Seconds()),
		},
		{
			ds: someVersionsMidSizeRows,
			now: hlc.Timestamp{
				WallTime: 100 * time.Second.Nanoseconds(),
			},
			ttl: 1,
		},
	} {
		t.Run(fmt.Sprintf("%v@%v,ttl=%v", tc.ds, tc.now, tc.ttl), func(t *testing.T) {
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()

			tc.ds.dist(N, rng).setupTest(t, eng, *tc.ds.desc())
			snap := eng.NewSnapshot()

			oldGCer := makeFakeGCer()
			ttl := time.Duration(tc.ttl) * time.Second
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
	rng := rand.New(rand.NewSource(1))
	ctx := context.Background()
	runGC := func(eng storage.Engine, old bool, spec randomRunGCTestSpec) (Info, error) {
		runGCFunc := Run
		if old {
			runGCFunc = runGCOld
		}
		snap := eng.NewSnapshot()
		ttl := time.Duration(spec.ttl) * time.Second
		return runGCFunc(ctx, spec.ds.desc(), snap, spec.now,
			CalculateThreshold(spec.now, ttl), RunOptions{IntentAgeThreshold: intentAgeThreshold},
			ttl,
			NoopGCer{},
			func(ctx context.Context, intents []roachpb.Intent) error {
				return nil
			},
			func(ctx context.Context, txn *roachpb.Transaction) error {
				return nil
			})
	}
	makeTest := func(old bool, spec randomRunGCTestSpec) func(b *testing.B) {
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
				ds:  ds,
				now: now,
				ttl: ttl,
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
			for _, spec := range specs {
				b.Run(fmt.Sprint(spec.ds), makeTest(old, spec))
			}
		})
	}
}

// Test:
//  - generate random data
//  - run gc
//  - check what was collected (compare with invariants)
//  - add computation of gc info and compare with GC
//  - find appropriate gc/intent thresholds for generated data
// Phase 1a:
//  - add range deletions by having blacklists for point and key timestamps
// Phase 2:
//  - change data generation to produce history bottom up
func TestNewVsInvariants(t *testing.T) {
	ctx := context.Background()
	const N = 100000

	someVersionsMidSizeRowsLotsOfIntents := someVersionsMidSizeRows
	someVersionsMidSizeRowsLotsOfIntents.intentFrac = 1
	for _, tc := range []randomRunGCTestSpec{
		{
			ds: someVersionsMidSizeRowsLotsOfIntents,
			// Current time in the future enough for intents to get resolved
			now: hlc.Timestamp{
				WallTime: (intentAgeThreshold + 100*time.Second).Nanoseconds(),
			},
			// GC everything beyond intent resolution threshold
			ttl: int32(intentAgeThreshold.Seconds()),
		},
		{
			ds: someVersionsMidSizeRows,
			now: hlc.Timestamp{
				WallTime: 100 * time.Second.Nanoseconds(),
			},
			ttl: 1,
		},
	} {
		t.Run(fmt.Sprintf("%v@%v,ttl=%v", tc.ds, tc.now, tc.ttl), func(t *testing.T) {
			rng := rand.New(rand.NewSource(1))
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()

			tc.ds.dist(N, rng).setupTest(t, eng, *tc.ds.desc())
			snap := eng.NewSnapshot()

			// Run GCer over snapshot.
			ttl := time.Duration(tc.ttl) * time.Second
			gcThreshold := CalculateThreshold(tc.now, ttl)
			intentThreshold := tc.now.Add(-intentAgeThreshold.Nanoseconds(), 0)

			gcer := makeFakeGCer()
			gcInfoNew, err := Run(ctx, tc.ds.desc(), snap, tc.now,
				gcThreshold, RunOptions{IntentAgeThreshold: intentAgeThreshold}, ttl,
				&gcer,
				gcer.resolveIntents,
				gcer.resolveIntentsAsync)
			require.NoError(t, err)

			// Handle GC + resolve intents.
			var stats enginepb.MVCCStats
			require.NoError(t,
				storage.MVCCGarbageCollect(ctx, eng, &stats, gcer.requests(), gcThreshold))
			for _, i := range gcer.intents {
				l := roachpb.LockUpdate{
					Span:   roachpb.Span{Key: i.Key},
					Txn:    i.Txn,
					Status: roachpb.ABORTED,
				}
				_, err := storage.MVCCResolveWriteIntent(ctx, eng, &stats, l)
				require.NoError(t, err, "failed to resolve intent")
			}

			assertGCInvariants(t, eng, snap, *tc.ds.desc(), tc.now, gcThreshold, intentThreshold, ttl,
				gcInfoNew)
		})
	}
}

// assertGCInvariants will create a stream of expected values based on full data
// set contained in provided original reader and compare it with the after
// reader that contains data after applying GC request.
func assertGCInvariants(
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
	itBefore := before.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind,
		storage.IterOptions{
			LowerBound: desc.StartKey.AsRawKey(),
			UpperBound: desc.EndKey.AsRawKey(),
			KeyTypes:   storage.IterKeyTypePointsOnly,
		})
	defer itBefore.Close()
	itBefore.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})

	expInfo := Info{
		Now:       now,
		GCTTL:     gcTTL,
		Threshold: gcThreshold,
	}
	var pending []storage.MVCCKeyValue
	expectations := func() (storage.MVCCKeyValue, bool) {
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
			var history []storage.MVCCKeyValue
			for {
				ok, err := itBefore.Valid()
				require.NoError(t, err, "failed to read data from unmodified engine")
				if !ok {
					break
				}
				k := itBefore.Key()
				v := itBefore.Value()
				if len(baseKey) == 0 {
					baseKey = k.Key
				} else if !baseKey.Equal(k.Key) {
					break
				}
				history = append(history, storage.MVCCKeyValue{Key: k, Value: v})
				itBefore.Next()
			}
			if len(history) == 0 {
				return storage.MVCCKeyValue{}, false
			}

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
						// This is an old intent. Skip intent and proposed value and continue.
						expInfo.IntentsConsidered++
						// We always use a new transaction for each intent and consider
						// operations successful in testGCer.
						expInfo.IntentTxns++
						expInfo.PushTxn++
						expInfo.ResolveTotal++
					} else {
						// Return intent as is and bypass history checks.
						pending = append(pending, history[i])
						pending = append(pending, history[i+1])
					}
					i += 2
					continue
				}

				// Apply GC checks to produce expected state.
				switch {
				case gcThreshold.Less(history[i].Key.Timestamp):
					// Any value above threshold including intents that have no timestamp.
					pending = append(pending, history[i])
					i++
				case history[i].Key.Timestamp.LessEq(gcThreshold) && len(history[i].Value) > 0:
					// First value on or under threshold should be preserved, but the rest
					// of history should be skipped.
					pending = append(pending, history[i])
					i++
					stop = true
				default:
					// This is ts <= threshold and v == nil
					stop = true
				}
			}

			// Remaining part of the history is removed, so accumulate it as gc stats.
			if i < len(history) {
				expInfo.NumKeysAffected++
				for ; i < len(history); i++ {
					expInfo.AffectedVersionsKeyBytes += int64(history[i].Key.EncodedSize())
					expInfo.AffectedVersionsValBytes += int64(len(history[i].Value))
				}
			}
		}
	}

	itAfter := after.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypePointsOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	defer itAfter.Close()

	itAfter.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	eKV, dataOk := expectations()
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
			eKV, dataOk = expectations()
		default:
			itAfter.Next()
			eKV, dataOk = expectations()
		}
	}

	require.EqualValues(t, expInfo, gcInfo, "collected gc info mismatch")
}

func getKeyHistory(t *testing.T, r storage.Reader, key roachpb.Key) string {
	var result []string

	it := r.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound:           key,
		UpperBound:           key.Next(),
		KeyTypes:             storage.IterKeyTypePointsOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	defer it.Close()

	it.SeekGE(storage.MVCCKey{Key: key})
	for {
		ok, err := it.Valid()
		require.NoError(t, err, "failed to read engine iterator")
		if !ok || !it.UnsafeKey().Key.Equal(key) {
			break
		}
		result = append(result, it.UnsafeKey().String())
		it.Next()
	}

	return strings.Join(result, ", ")
}

type fakeGCer struct {
	gcKeys     map[string]roachpb.GCRequest_GCKey
	threshold  Threshold
	intents    []roachpb.Intent
	batches    [][]roachpb.Intent
	txnIntents []txnIntents
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

func (f *fakeGCer) GC(ctx context.Context, keys []roachpb.GCRequest_GCKey) error {
	for _, k := range keys {
		f.gcKeys[k.Key.String()] = k
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

func (f *fakeGCer) requests() []roachpb.GCRequest_GCKey {
	var reqs []roachpb.GCRequest_GCKey
	for _, r := range f.gcKeys {
		reqs = append(reqs, r)
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
