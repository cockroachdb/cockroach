// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/require"
)

// Note: most benchmarks in this package have an engine-specific Benchmark
// function (see bench_rocksdb_test.go and bench_pebble_test.go). The newer
// Benchmarks with a unified implementation are here at the top of this file
// with the business logic for the implementation of the other tests following.

func BenchmarkMVCCGarbageCollect(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	// NB: To debug #16068, test only 128-128-15000-6.
	keySizes := []int{128}
	valSizes := []int{128}
	numKeys := []int{1, 1024}
	versionConfigs := []struct {
		total    int
		toDelete []int
	}{
		{2, []int{1}},
		{1024, []int{1, 16, 32, 512, 1015, 1023}},
	}
	engineMakers := []struct {
		name   string
		create engineMaker
	}{
		{"pebble", setupMVCCInMemPebble},
	}

	ctx := context.Background()
	for _, engineImpl := range engineMakers {
		b.Run(engineImpl.name, func(b *testing.B) {
			for _, keySize := range keySizes {
				b.Run(fmt.Sprintf("keySize=%d", keySize), func(b *testing.B) {
					for _, valSize := range valSizes {
						b.Run(fmt.Sprintf("valSize=%d", valSize), func(b *testing.B) {
							for _, numKeys := range numKeys {
								b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
									for _, versions := range versionConfigs {
										b.Run(fmt.Sprintf("numVersions=%d", versions.total), func(b *testing.B) {
											for _, toDelete := range versions.toDelete {
												b.Run(fmt.Sprintf("deleteVersions=%d", toDelete), func(b *testing.B) {
													runMVCCGarbageCollect(ctx, b, engineImpl.create,
														benchGarbageCollectOptions{
															benchDataOptions: benchDataOptions{
																numKeys:     numKeys,
																numVersions: versions.total,
																valueBytes:  valSize,
															},
															keyBytes:       keySize,
															deleteVersions: toDelete,
														})
												})
											}
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkExportToSst(b *testing.B) {
	defer log.Scope(b).Close(b)

	numKeys := []int{64, 512, 1024, 8192, 65536}
	numRevisions := []int{1, 10, 100}
	exportAllRevisions := []bool{false, true}
	useTBI := []bool{false, true}
	engineMakers := []struct {
		name   string
		create engineMaker
	}{
		{"pebble", setupMVCCPebble},
	}

	for _, engineImpl := range engineMakers {
		b.Run(engineImpl.name, func(b *testing.B) {
			for _, numKey := range numKeys {
				b.Run(fmt.Sprintf("numKeys=%d", numKey), func(b *testing.B) {
					for _, numRevision := range numRevisions {
						b.Run(fmt.Sprintf("numRevisions=%d", numRevision), func(b *testing.B) {
							for _, exportAllRevisionsVal := range exportAllRevisions {
								b.Run(fmt.Sprintf("exportAllRevisions=%t", exportAllRevisionsVal), func(b *testing.B) {
									for _, useTBIVal := range useTBI {
										b.Run(fmt.Sprintf("useTBI=%t", useTBIVal), func(b *testing.B) {
											runExportToSst(b, engineImpl.create, numKey, numRevision,
												exportAllRevisionsVal, useTBIVal)
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

const numIntentKeys = 1000

// setupKeysWithIntent writes keys using transactions to eng. The number of
// different keys is equal to numIntentKeys and each key has numVersions
// versions written to it. The number of versions that are resolved is either
// numVersions-1 or numVersions, which is controlled by the resolveAll
// parameter (when true, numVersions are resolved). The latest version for a
// key is generated using one of possibly two transactions. One of these is
// what is returned in the LockUpdate so that the caller can resolve intents
// for it (if any). This distinguished transaction writes the latest version
// with a stride equal to lockUpdateTxnHasLatestVersionStride. A stride of k
// means it writes every k-th latest version. Strides longer than 1 are for
// use in tests with ranged intent resolution for this distinguished
// transaction, where some of the intents encountered should not be resolved.
// The latest intents written by the non-distinguished transaction may also be
// resolved, if resolveIntentForLatestVersionWhenNonLockUpdateTxn is true.
// This causes ranged intent resolution to not encounter intents owned by
// other transactions, but may have to skip through keys that have no intents
// at all. numFlushedVersions is a parameter that controls how many versions
// are flushed out from the memtable to sstables -- when less is flushed out,
// and we are using separated intents, the SingleDeletes, Sets corresponding
// to resolved intents still exist in the memtable, and can result in more
// work during intent resolution. In a production setting numFlushedVersions
// should be close to all the versions.
func setupKeysWithIntent(
	b testing.TB,
	eng Engine,
	numVersions int,
	numFlushedVersions int,
	resolveAll bool,
	lockUpdateTxnHasLatestVersionStride int,
	resolveIntentForLatestVersionWhenNonLockUpdateTxn bool,
) roachpb.LockUpdate {
	txnIDCount := 2 * numVersions
	adjustTxnID := func(txnID int) int {
		// Assign txn IDs in a deterministic way that will mimic the end result of
		// random assignment -- the live intent is centered between dead intents,
		// when we have separated intents.
		if txnID%2 == 0 {
			txnID = txnIDCount - txnID
		}
		return txnID
	}
	txnIDWithLatestVersion := adjustTxnID(numVersions)
	otherTxnWithLatestVersion := txnIDCount + 2
	otherTxnUUID := uuid.FromUint128(uint128.FromInts(0, uint64(otherTxnWithLatestVersion)))
	val := []byte("value")
	var rvLockUpdate roachpb.LockUpdate
	for i := 1; i <= numVersions; i++ {
		// Assign txn IDs in a deterministic way that will mimic the end result of
		// random assignment -- the live intent is centered between dead intents,
		// when we have separated intents.
		txnID := adjustTxnID(i)
		txnUUID := uuid.FromUint128(uint128.FromInts(0, uint64(txnID)))
		ts := hlc.Timestamp{WallTime: int64(i)}
		txn := roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				ID:             txnUUID,
				Key:            []byte("foo"),
				WriteTimestamp: ts,
				MinTimestamp:   ts,
			},
			Status:                 roachpb.PENDING,
			ReadTimestamp:          ts,
			GlobalUncertaintyLimit: ts,
		}
		lockUpdate := roachpb.LockUpdate{
			Txn:    txn.TxnMeta,
			Status: roachpb.COMMITTED,
		}
		var otherTxn roachpb.Transaction
		var otherLockUpdate roachpb.LockUpdate
		if txnID == txnIDWithLatestVersion {
			rvLockUpdate = lockUpdate
			if lockUpdateTxnHasLatestVersionStride != 1 {
				otherTxn = roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{
						ID:             otherTxnUUID,
						Key:            []byte("foo"),
						WriteTimestamp: ts,
						MinTimestamp:   ts,
					},
					Status:                 roachpb.PENDING,
					ReadTimestamp:          ts,
					GlobalUncertaintyLimit: ts,
				}
				otherLockUpdate = roachpb.LockUpdate{
					Txn:    otherTxn.TxnMeta,
					Status: roachpb.COMMITTED,
				}
			}
		}
		value := roachpb.Value{RawBytes: val}
		batch := eng.NewBatch()
		for j := 0; j < numIntentKeys; j++ {
			putTxn := &txn
			if txnID == txnIDWithLatestVersion && j%lockUpdateTxnHasLatestVersionStride != 0 {
				putTxn = &otherTxn
			}
			key := makeKey(nil, j)
			require.NoError(b, MVCCPut(context.Background(), batch, nil, key, ts, value, putTxn))
		}
		require.NoError(b, batch.Commit(true))
		batch.Close()
		if i < numVersions || resolveAll || resolveIntentForLatestVersionWhenNonLockUpdateTxn {
			batch := eng.NewBatch()
			for j := 0; j < numIntentKeys; j++ {
				key := makeKey(nil, j)
				lu := lockUpdate
				latestVersionNonLockUpdateTxn := false
				if txnID == txnIDWithLatestVersion && j%lockUpdateTxnHasLatestVersionStride != 0 {
					lu = otherLockUpdate
					latestVersionNonLockUpdateTxn = true
				}
				lu.Key = key
				if i == numVersions && !resolveAll && !latestVersionNonLockUpdateTxn {
					// Only here because of
					// resolveIntentForLatestVersionWhenNonLockUpdateTxn, and this key
					// is not one that should be resolved.
					continue
				}
				found, err := MVCCResolveWriteIntent(context.Background(), batch, nil, lu)
				require.Equal(b, true, found)
				require.NoError(b, err)
			}
			require.NoError(b, batch.Commit(true))
			batch.Close()
		}
		if i == numFlushedVersions {
			require.NoError(b, eng.Flush())
		}
	}
	return rvLockUpdate
}

// BenchmarkIntentScan compares separated and interleaved intents, when
// reading the intent and latest version for a range of keys.
func BenchmarkIntentScan(b *testing.B) {
	skip.UnderShort(b, "setting up unflushed data takes too long")
	defer log.Scope(b).Close(b)

	for _, numVersions := range []int{10, 100, 200, 400} {
		b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
			for _, percentFlushed := range []int{0, 50, 80, 90, 100} {
				b.Run(fmt.Sprintf("percent-flushed=%d", percentFlushed), func(b *testing.B) {
					eng := setupMVCCInMemPebbleWithSeparatedIntents(b)
					numFlushedVersions := (percentFlushed * numVersions) / 100
					setupKeysWithIntent(b, eng, numVersions, numFlushedVersions, false, /* resolveAll */
						1, false /* resolveIntentForLatestVersionWhenNotLockUpdate */)
					lower := makeKey(nil, 0)
					iter := eng.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
						LowerBound: lower,
						UpperBound: makeKey(nil, numIntentKeys),
					})
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						valid, err := iter.Valid()
						if err != nil {
							b.Fatal(err)
						}
						if !valid {
							iter.SeekGE(MVCCKey{Key: lower})
						} else {
							// Read intent.
							k := iter.UnsafeKey()
							if k.IsValue() {
								b.Fatalf("expected intent %s", k.String())
							}
							// Read latest version.
							//
							// This Next dominates the cost of the benchmark when
							// percent-flushed is < 100, since the pebble.Iterator has
							// to iterate over all the Deletes/SingleDeletes/Sets
							// corresponding to resolved intents.
							iter.Next()
							valid, err = iter.Valid()
							if !valid || err != nil {
								b.Fatalf("valid: %t, err: %s", valid, err)
							}
							k = iter.UnsafeKey()
							if !k.IsValue() {
								b.Fatalf("expected value")
							}
							// Skip to next key. This dominates the cost of the benchmark,
							// when percent-flushed=100.
							iter.NextKey()
						}
					}
				})
			}
		})
	}
}

// BenchmarkScanAllIntentsResolved compares separated and interleaved intents,
// when reading the latest version for a range of keys, when all the intents
// have been resolved.
func BenchmarkScanAllIntentsResolved(b *testing.B) {
	skip.UnderShort(b, "setting up unflushed data takes too long")
	defer log.Scope(b).Close(b)

	for _, numVersions := range []int{200} {
		b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
			for _, percentFlushed := range []int{0, 50, 90, 100} {
				b.Run(fmt.Sprintf("percent-flushed=%d", percentFlushed), func(b *testing.B) {
					eng := setupMVCCInMemPebbleWithSeparatedIntents(b)
					numFlushedVersions := (percentFlushed * numVersions) / 100
					setupKeysWithIntent(b, eng, numVersions, numFlushedVersions, true, /* resolveAll */
						1, false /* resolveIntentForLatestVersionWhenNotLockUpdate */)
					lower := makeKey(nil, 0)
					var iter MVCCIterator
					var buf []byte
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var valid bool
						var err error
						if iter != nil {
							valid, err = iter.Valid()
						}
						if err != nil {
							b.Fatal(err)
						}
						if !valid {
							// Create a new MVCCIterator. Simply seeking to the earlier
							// key is not representative of a real workload where
							// iterator reuse always seeks to a later key (because of
							// the sorting in a BatchRequest). Seeking to an earlier key
							// allows for an optimization in lock table iteration when
							// not using the *WithLimit() operations on the underlying
							// pebble.Iterator, where the SeekGE can be turned into a
							// noop since the original SeekGE is what was remembered
							// (and this seek is to the same position as the original
							// seek). This optimization won't fire in this manner in
							// practice, so we don't want it to happen in this Benchmark
							// either.
							b.StopTimer()
							iter = eng.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
								LowerBound: lower,
								UpperBound: makeKey(nil, numIntentKeys),
							})
							b.StartTimer()
							iter.SeekGE(MVCCKey{Key: lower})
						} else {
							// Read latest version.
							k := iter.UnsafeKey()
							if !k.IsValue() {
								b.Fatalf("expected value %s", k.String())
							}
							// Skip to next key.
							buf = append(buf[:0], k.Key...)
							buf = roachpb.BytesNext(buf)
							iter.SeekGE(MVCCKey{Key: buf})
						}
					}
				})
			}
		})
	}
}

// BenchmarkScanOneAllIntentsResolved compares separated and interleaved
// intents, when reading the latest version for a range of keys, when all the
// intents have been resolved. Unlike the previous benchmark, each scan reads
// one key.
func BenchmarkScanOneAllIntentsResolved(b *testing.B) {
	skip.UnderShort(b, "setting up unflushed data takes too long")
	defer log.Scope(b).Close(b)

	for _, numVersions := range []int{200} {
		b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
			for _, percentFlushed := range []int{0, 50, 90, 100} {
				b.Run(fmt.Sprintf("percent-flushed=%d", percentFlushed), func(b *testing.B) {
					eng := setupMVCCInMemPebbleWithSeparatedIntents(b)
					numFlushedVersions := (percentFlushed * numVersions) / 100
					setupKeysWithIntent(b, eng, numVersions, numFlushedVersions, true, /* resolveAll */
						1, false /* resolveIntentForLatestVersionWhenNotLockUpdate */)
					lower := makeKey(nil, 0)
					upper := makeKey(nil, numIntentKeys)
					buf := append([]byte(nil), lower...)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						iter := eng.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
							LowerBound: buf,
							UpperBound: upper,
						})
						iter.SeekGE(MVCCKey{Key: buf})
						valid, err := iter.Valid()
						if err != nil {
							b.Fatal(err)
						}
						if !valid {
							buf = append(buf[:0], lower...)
						} else {
							// Read latest version.
							k := iter.UnsafeKey()
							if !k.IsValue() {
								b.Fatalf("expected value %s", k.String())
							}
							// Skip to next key.
							buf = append(buf[:0], k.Key...)
							buf = roachpb.BytesNext(buf)
							iter.Close()
						}
					}
				})
			}
		})
	}
}

// BenchmarkIntentResolution compares separated and interleaved intents, when
// doing intent resolution for individual intents.
func BenchmarkIntentResolution(b *testing.B) {
	skip.UnderShort(b, "setting up unflushed data takes too long")
	defer log.Scope(b).Close(b)

	for _, numVersions := range []int{10, 100, 200, 400} {
		b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
			for _, percentFlushed := range []int{0, 50, 80, 90, 100} {
				b.Run(fmt.Sprintf("percent-flushed=%d", percentFlushed), func(b *testing.B) {
					eng := setupMVCCInMemPebbleWithSeparatedIntents(b)
					numFlushedVersions := (percentFlushed * numVersions) / 100
					lockUpdate := setupKeysWithIntent(b, eng, numVersions, numFlushedVersions,
						false /* resolveAll */, 1,
						false /* resolveIntentForLatestVersionWhenNotLockUpdate */)
					keys := make([]roachpb.Key, numIntentKeys)
					for i := range keys {
						keys[i] = makeKey(nil, i)
					}
					batch := eng.NewBatch()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if i > 0 && i%numIntentKeys == 0 {
							// Wrapped around.
							b.StopTimer()
							batch.Close()
							batch = eng.NewBatch()
							b.StartTimer()
						}
						lockUpdate.Key = keys[i%numIntentKeys]
						found, err := MVCCResolveWriteIntent(context.Background(), batch, nil, lockUpdate)
						if !found || err != nil {
							b.Fatalf("intent not found or err %s", err)
						}
					}
				})
			}
		})
	}
}

// BenchmarkIntentRangeResolution compares separated and interleaved intents,
// when doing ranged intent resolution.
func BenchmarkIntentRangeResolution(b *testing.B) {
	skip.UnderShort(b, "setting up unflushed data takes too long")
	defer log.Scope(b).Close(b)

	for _, numVersions := range []int{10, 100, 400} {
		b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
			for _, sparseness := range []int{1, 100, 1000} {
				b.Run(fmt.Sprintf("sparseness=%d", sparseness), func(b *testing.B) {
					otherTxnUnresolvedIntentsCases := []bool{false, true}
					if sparseness == 1 {
						// Every intent is owned by the main txn.
						otherTxnUnresolvedIntentsCases = []bool{false}
					}
					for _, haveOtherTxnUnresolvedIntents := range otherTxnUnresolvedIntentsCases {
						b.Run(fmt.Sprintf("other-txn-intents=%t", haveOtherTxnUnresolvedIntents), func(b *testing.B) {
							for _, percentFlushed := range []int{0, 50, 100} {
								b.Run(fmt.Sprintf("percent-flushed=%d", percentFlushed), func(b *testing.B) {
									eng := setupMVCCInMemPebbleWithSeparatedIntents(b)
									numFlushedVersions := (percentFlushed * numVersions) / 100
									lockUpdate := setupKeysWithIntent(b, eng, numVersions, numFlushedVersions,
										false /* resolveAll */, sparseness, !haveOtherTxnUnresolvedIntents)
									keys := make([]roachpb.Key, numIntentKeys+1)
									for i := range keys {
										keys[i] = makeKey(nil, i)
									}
									batch := eng.NewBatch()
									numKeysPerRange := 100
									numRanges := numIntentKeys / numKeysPerRange
									var resolvedCount int64
									expectedResolvedCount := int64(numIntentKeys / sparseness)
									b.ResetTimer()
									for i := 0; i < b.N; i++ {
										if i > 0 && i%numRanges == 0 {
											// Wrapped around.
											b.StopTimer()
											if resolvedCount != expectedResolvedCount {
												b.Fatalf("expected to resolve %d, actual %d",
													expectedResolvedCount, resolvedCount)
											}
											resolvedCount = 0
											batch.Close()
											batch = eng.NewBatch()
											b.StartTimer()
										}
										rangeNum := i % numRanges
										lockUpdate.Key = keys[rangeNum*numKeysPerRange]
										lockUpdate.EndKey = keys[(rangeNum+1)*numKeysPerRange]
										resolved, span, err := MVCCResolveWriteIntentRange(
											context.Background(), batch, nil, lockUpdate, 1000 /* max */)
										if err != nil {
											b.Fatal(err)
										}
										resolvedCount += resolved
										if span != nil {
											b.Fatal("unexpected resume span")
										}
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

const overhead = 48 // Per key/value overhead (empirically determined)

type engineMaker func(testing.TB, string) Engine

type benchDataOptions struct {
	numVersions       int
	numKeys           int
	valueBytes        int
	numColumnFamilies int

	// In transactional mode, data is written by writing and later resolving
	// intents. In non-transactional mode, data is written directly, without
	// leaving intents. Transactional mode notably stresses RocksDB deletion
	// tombstones, as the metadata key is repeatedly written and deleted.
	//
	// Both modes are reflective of real workloads. Transactional mode simulates
	// data that has recently been INSERTed into a table, while non-transactional
	// mode simulates data that has been RESTOREd or is old enough to have been
	// fully compacted.
	transactional bool
}

// loadTestData writes numKeys keys in numBatches separate batches. Keys are
// written in order. Every key in a given batch has the same MVCC timestamp;
// batch timestamps start at batchTimeSpan and increase in intervals of
// batchTimeSpan.
//
// Importantly, writing keys in order convinces RocksDB to output one SST per
// batch, where each SST contains keys of only one timestamp. E.g., writing A,B
// at t0 and C at t1 will create two SSTs: one for A,B that only contains keys
// at t0, and one for C that only contains keys at t1. Conversely, writing A, C
// at t0 and B at t1 would create just one SST that contained A,B,C (due to an
// immediate compaction).
//
// The creation of the database is time consuming, so the caller can choose
// whether to use a temporary or permanent location.
func loadTestData(dir string, numKeys, numBatches, batchTimeSpan, valueBytes int) (Engine, error) {
	ctx := context.Background()

	exists := true
	if _, err := os.Stat(dir); oserror.IsNotExist(err) {
		exists = false
	}

	eng, err := Open(
		context.Background(),
		Filesystem(dir),
		Settings(cluster.MakeTestingClusterSettings()))
	if err != nil {
		return nil, err
	}

	if exists {
		testutils.ReadAllFiles(filepath.Join(dir, "*"))
		return eng, nil
	}

	log.Infof(context.Background(), "creating test data: %s", dir)

	// Generate the same data every time.
	rng := rand.New(rand.NewSource(1449168817))

	keys := make([]roachpb.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(i)))
	}

	sstTimestamps := make([]int64, numBatches)
	for i := 0; i < len(sstTimestamps); i++ {
		sstTimestamps[i] = int64((i + 1) * batchTimeSpan)
	}

	var batch Batch
	var minWallTime int64
	batchSize := len(keys) / numBatches
	for i, key := range keys {
		if (i % batchSize) == 0 {
			if i > 0 {
				log.Infof(ctx, "committing (%d/~%d)", i/batchSize, numBatches)
				if err := batch.Commit(false /* sync */); err != nil {
					return nil, err
				}
				batch.Close()
				if err := eng.Flush(); err != nil {
					return nil, err
				}
			}
			batch = eng.NewBatch()
			minWallTime = sstTimestamps[i/batchSize]
		}
		timestamp := hlc.Timestamp{WallTime: minWallTime + rand.Int63n(int64(batchTimeSpan))}
		value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueBytes))
		value.InitChecksum(key)
		if err := MVCCPut(ctx, batch, nil, key, timestamp, value, nil); err != nil {
			return nil, err
		}
	}
	if err := batch.Commit(false /* sync */); err != nil {
		return nil, err
	}
	batch.Close()
	if err := eng.Flush(); err != nil {
		return nil, err
	}

	return eng, nil
}

// setupMVCCData writes up to numVersions values at each of numKeys
// keys. The number of versions written for each key is chosen
// randomly according to a uniform distribution. Each successive
// version is written starting at 5ns and then in 5ns increments. This
// allows scans at various times, starting at t=5ns, and continuing to
// t=5ns*(numVersions+1). A version for each key will be read on every
// such scan, but the dynamics of the scan will change depending on
// the historical timestamp. Earlier timestamps mean scans which must
// skip more historical versions; later timestamps mean scans which
// skip fewer.
//
// The creation of the database is time consuming, especially for larger numbers
// of versions. The database is persisted between runs and stored in the current
// directory as "mvcc_scan_<versions>_<keys>_<columnFamilies>_<valueBytes>"
// (which is also returned).
func setupMVCCData(
	ctx context.Context, b *testing.B, emk engineMaker, opts benchDataOptions,
) (Engine, string) {
	loc := fmt.Sprintf("mvcc_data_%d_%d_%d_%d",
		opts.numVersions, opts.numKeys, opts.numColumnFamilies, opts.valueBytes)
	if opts.transactional {
		loc += "_txn"
	}

	exists := true
	if _, err := os.Stat(loc); oserror.IsNotExist(err) {
		exists = false
	} else {
		require.NoError(b, err)
	}

	eng := emk(b, loc)

	if exists {
		testutils.ReadAllFiles(filepath.Join(loc, "*"))
		return eng, loc
	}

	log.Infof(ctx, "creating mvcc data: %s", loc)

	// Generate the same data every time.
	rng := rand.New(rand.NewSource(1449168817))

	keySlice := make([]roachpb.Key, opts.numKeys)
	var order []int
	var cf uint32
	for i := 0; i < opts.numKeys; i++ {
		if opts.numColumnFamilies > 0 {
			keySlice[i] = makeBenchRowKey(b, nil, i/opts.numColumnFamilies, cf)
			cf = (cf + 1) % uint32(opts.numColumnFamilies)
		} else {
			keySlice[i] = roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(i)))
		}
		keyVersions := rng.Intn(opts.numVersions) + 1
		for j := 0; j < keyVersions; j++ {
			order = append(order, i)
		}
	}

	// Randomize the order in which the keys are written.
	for i, n := 0, len(order); i < n-1; i++ {
		j := i + rng.Intn(n-i)
		order[i], order[j] = order[j], order[i]
	}

	counts := make([]int, opts.numKeys)

	var txn *roachpb.Transaction
	if opts.transactional {
		txnCopy := *txn1Commit
		txn = &txnCopy
	}

	writeKey := func(batch Batch, idx int) {
		key := keySlice[idx]
		value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, opts.valueBytes))
		value.InitChecksum(key)
		counts[idx]++
		ts := hlc.Timestamp{WallTime: int64(counts[idx] * 5)}
		if txn != nil {
			txn.ReadTimestamp = ts
			txn.WriteTimestamp = ts
		}
		if err := MVCCPut(ctx, batch, nil /* ms */, key, ts, value, txn); err != nil {
			b.Fatal(err)
		}
	}

	resolveLastIntent := func(batch Batch, idx int) {
		key := keySlice[idx]
		txnMeta := txn.TxnMeta
		txnMeta.WriteTimestamp = hlc.Timestamp{WallTime: int64(counts[idx]) * 5}
		if _, err := MVCCResolveWriteIntent(ctx, batch, nil /* ms */, roachpb.LockUpdate{
			Span:   roachpb.Span{Key: key},
			Status: roachpb.COMMITTED,
			Txn:    txnMeta,
		}); err != nil {
			b.Fatal(err)
		}
	}

	batch := eng.NewBatch()
	for i, idx := range order {
		// Output the keys in ~20 batches. If we used a single batch to output all
		// of the keys rocksdb would create a single sstable. We want multiple
		// sstables in order to exercise filtering of which sstables are examined
		// during iterator seeking. We fix the number of batches we output so that
		// optimizations which change the data size result in the same number of
		// sstables.
		if scaled := len(order) / 20; i > 0 && (i%scaled) == 0 {
			log.Infof(ctx, "committing (%d/~%d)", i/scaled, 20)
			if err := batch.Commit(false /* sync */); err != nil {
				b.Fatal(err)
			}
			batch.Close()
			batch = eng.NewBatch()
			if err := eng.Flush(); err != nil {
				b.Fatal(err)
			}
		}

		if opts.transactional {
			// If we've previously written this key transactionally, we need to
			// resolve the intent we left. We don't do this immediately after writing
			// the key to introduce the possibility that the intent's resolution ends
			// up in a different batch than writing the intent itself. Note that the
			// first time through this loop for any given key we'll attempt to resolve
			// a non-existent intent, but that's OK.
			resolveLastIntent(batch, idx)
		}
		writeKey(batch, idx)
	}
	if opts.transactional {
		// If we were writing transactionally, we need to do one last round of
		// intent resolution. Just stuff it all into the last batch.
		for idx := range keySlice {
			resolveLastIntent(batch, idx)
		}
	}
	if err := batch.Commit(false /* sync */); err != nil {
		b.Fatal(err)
	}
	batch.Close()
	if err := eng.Flush(); err != nil {
		b.Fatal(err)
	}

	return eng, loc
}

type benchScanOptions struct {
	benchDataOptions
	numRows   int
	reverse   bool
	wholeRows bool
}

// runMVCCScan first creates test data (and resets the benchmarking
// timer). It then performs b.N MVCCScans in increments of numRows
// keys over all of the data in the Engine instance, restarting at
// the beginning of the keyspace, as many times as necessary.
func runMVCCScan(ctx context.Context, b *testing.B, emk engineMaker, opts benchScanOptions) {
	// Use the same number of keys for all of the mvcc scan
	// benchmarks. Using a different number of keys per test gives
	// preferential treatment to tests with fewer keys. Note that the
	// datasets all fit in cache and the cache is pre-warmed.
	if opts.numKeys != 0 {
		b.Fatal("test error: cannot call runMVCCScan with non-zero numKeys")
	}
	opts.numKeys = 100000
	if opts.wholeRows && opts.numColumnFamilies == 0 {
		b.Fatal("test error: wholeRows requires numColumnFamilies > 0")
	}

	eng, _ := setupMVCCData(ctx, b, emk, opts.benchDataOptions)
	defer eng.Close()

	{
		// Pull all of the sstables into the RocksDB cache in order to make the
		// timings more stable. Otherwise, the first run will be penalized pulling
		// data into the cache while later runs will not.
		iter := eng.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
		_, _ = iter.ComputeStats(keys.LocalMax, roachpb.KeyMax, 0)
		iter.Close()
	}

	var startKey, endKey roachpb.Key
	startKeyBuf := append(make([]byte, 0, 1024), []byte("key-")...)
	endKeyBuf := append(make([]byte, 0, 1024), []byte("key-")...)

	b.SetBytes(int64(opts.numRows * opts.valueBytes))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Choose a random key to start scan.
		if opts.numColumnFamilies == 0 {
			keyIdx := rand.Int31n(int32(opts.numKeys - opts.numRows))
			startKey = roachpb.Key(encoding.EncodeUvarintAscending(startKeyBuf[:4], uint64(keyIdx)))
			endKey = roachpb.Key(encoding.EncodeUvarintAscending(endKeyBuf[:4], uint64(keyIdx+int32(opts.numRows)-1))).Next()
		} else {
			startID := rand.Int63n(int64((opts.numKeys - opts.numRows) / opts.numColumnFamilies))
			endID := startID + int64(opts.numRows/opts.numColumnFamilies) + 1
			startKey = makeBenchRowKey(b, startKeyBuf[:0], int(startID), 0)
			endKey = makeBenchRowKey(b, endKeyBuf[:0], int(endID), 0)
		}
		walltime := int64(5 * (rand.Int31n(int32(opts.numVersions)) + 1))
		ts := hlc.Timestamp{WallTime: walltime}
		var wholeRowsOfSize int32
		if opts.wholeRows {
			wholeRowsOfSize = int32(opts.numColumnFamilies)
		}
		res, err := MVCCScan(ctx, eng, startKey, endKey, ts, MVCCScanOptions{
			MaxKeys:         int64(opts.numRows),
			WholeRowsOfSize: wholeRowsOfSize,
			AllowEmpty:      wholeRowsOfSize != 0,
			Reverse:         opts.reverse,
		})
		if err != nil {
			b.Fatalf("failed scan: %+v", err)
		}
		expectKVs := opts.numRows
		if opts.wholeRows {
			expectKVs -= opts.numRows % opts.numColumnFamilies
		}
		if len(res.KVs) != expectKVs {
			b.Fatalf("failed to scan: %d != %d", len(res.KVs), expectKVs)
		}
	}

	b.StopTimer()
}

// runMVCCGet first creates test data (and resets the benchmarking
// timer). It then performs b.N MVCCGets.
func runMVCCGet(ctx context.Context, b *testing.B, emk engineMaker, opts benchDataOptions) {
	// Use the same number of keys for all of the mvcc scan
	// benchmarks. Using a different number of keys per test gives
	// preferential treatment to tests with fewer keys. Note that the
	// datasets all fit in cache and the cache is pre-warmed.
	if opts.numKeys != 0 {
		b.Fatal("test error: cannot call runMVCCGet with non-zero numKeys")
	}
	opts.numKeys = 100000

	eng, _ := setupMVCCData(ctx, b, emk, opts)
	defer eng.Close()

	b.SetBytes(int64(opts.valueBytes))
	b.ResetTimer()

	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	for i := 0; i < b.N; i++ {
		// Choose a random key to retrieve.
		keyIdx := rand.Int31n(int32(opts.numKeys))
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(keyIdx)))
		walltime := int64(5 * (rand.Int31n(int32(opts.numVersions)) + 1))
		ts := hlc.Timestamp{WallTime: walltime}
		if v, _, err := MVCCGet(ctx, eng, key, ts, MVCCGetOptions{}); err != nil {
			b.Fatalf("failed get: %+v", err)
		} else if v == nil {
			b.Fatalf("failed get (key not found): %d@%d", keyIdx, walltime)
		} else if valueBytes, err := v.GetBytes(); err != nil {
			b.Fatal(err)
		} else if len(valueBytes) != opts.valueBytes {
			b.Fatalf("unexpected value size: %d", len(valueBytes))
		}
	}

	b.StopTimer()
}

func runMVCCPut(ctx context.Context, b *testing.B, emk engineMaker, valueSize int) {
	rng, _ := randutil.NewTestRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("put_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCPut(ctx, eng, nil, key, ts, value, nil); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
	}

	b.StopTimer()
}

func runMVCCBlindPut(ctx context.Context, b *testing.B, emk engineMaker, valueSize int) {
	rng, _ := randutil.NewTestRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("put_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCBlindPut(ctx, eng, nil, key, ts, value, nil); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
	}

	b.StopTimer()
}

func runMVCCConditionalPut(
	ctx context.Context, b *testing.B, emk engineMaker, valueSize int, createFirst bool,
) {
	rng, _ := randutil.NewTestRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("cput_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	var expected []byte
	if createFirst {
		for i := 0; i < b.N; i++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
			ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			if err := MVCCPut(ctx, eng, nil, key, ts, value, nil); err != nil {
				b.Fatalf("failed put: %+v", err)
			}
		}
		expected = value.TagAndDataBytes()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCConditionalPut(ctx, eng, nil, key, ts, value, expected, CPutFailIfMissing, nil); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
	}

	b.StopTimer()
}

func runMVCCBlindConditionalPut(ctx context.Context, b *testing.B, emk engineMaker, valueSize int) {
	rng, _ := randutil.NewTestRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("cput_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCBlindConditionalPut(ctx, eng, nil, key, ts, value, nil, CPutFailIfMissing, nil); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
	}

	b.StopTimer()
}

func runMVCCInitPut(ctx context.Context, b *testing.B, emk engineMaker, valueSize int) {
	rng, _ := randutil.NewTestRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("iput_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCInitPut(ctx, eng, nil, key, ts, value, false, nil); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
	}

	b.StopTimer()
}

func runMVCCBlindInitPut(ctx context.Context, b *testing.B, emk engineMaker, valueSize int) {
	rng, _ := randutil.NewTestRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("iput_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCBlindInitPut(ctx, eng, nil, key, ts, value, false, nil); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
	}

	b.StopTimer()
}

func runMVCCBatchPut(ctx context.Context, b *testing.B, emk engineMaker, valueSize, batchSize int) {
	rng, _ := randutil.NewTestRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("batch_put_%d_%d", valueSize, batchSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		batch := eng.NewBatch()

		for j := i; j < end; j++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(j)))
			ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			if err := MVCCPut(ctx, batch, nil, key, ts, value, nil); err != nil {
				b.Fatalf("failed put: %+v", err)
			}
		}

		if err := batch.Commit(false /* sync */); err != nil {
			b.Fatal(err)
		}

		batch.Close()
	}

	b.StopTimer()
}

// Benchmark batch time series merge operations. This benchmark does not
// perform any reads and is only used to measure the cost of the periodic time
// series updates.
func runMVCCBatchTimeSeries(ctx context.Context, b *testing.B, emk engineMaker, batchSize int) {
	// Precompute keys so we don't waste time formatting them at each iteration.
	numKeys := batchSize
	keys := make([]roachpb.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = roachpb.Key(fmt.Sprintf("key-%d", i))
	}

	// We always write the same time series data (containing a single unchanging
	// sample). This isn't realistic but is fine because we're never reading the
	// data.
	var value roachpb.Value
	if err := value.SetProto(&roachpb.InternalTimeSeriesData{
		StartTimestampNanos: 0,
		SampleDurationNanos: 1000,
		Samples: []roachpb.InternalTimeSeriesSample{
			{Offset: 0, Count: 1, Sum: 5.0},
		},
	}); err != nil {
		b.Fatal(err)
	}

	eng := emk(b, fmt.Sprintf("batch_merge_%d", batchSize))
	defer eng.Close()

	b.ResetTimer()

	var ts hlc.Timestamp
	for i := 0; i < b.N; i++ {
		batch := eng.NewBatch()

		for j := 0; j < batchSize; j++ {
			ts.Logical++
			if err := MVCCMerge(ctx, batch, nil, keys[j], ts, value); err != nil {
				b.Fatalf("failed put: %+v", err)
			}
		}

		if err := batch.Commit(false /* sync */); err != nil {
			b.Fatal(err)
		}
		batch.Close()
	}

	b.StopTimer()
}

// runMVCCGetMergedValue reads merged values for numKeys separate keys and mergesPerKey
// operands per key.
func runMVCCGetMergedValue(
	ctx context.Context, b *testing.B, emk engineMaker, numKeys, mergesPerKey int,
) {
	eng := emk(b, fmt.Sprintf("get_merged_%d_%d", numKeys, mergesPerKey))
	defer eng.Close()

	// Precompute keys so we don't waste time formatting them at each iteration.
	keys := make([]roachpb.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = roachpb.Key(fmt.Sprintf("key-%d", i))
	}

	timestamp := hlc.Timestamp{}
	for i := 0; i < numKeys; i++ {
		for j := 0; j < mergesPerKey; j++ {
			timeseries := &roachpb.InternalTimeSeriesData{
				StartTimestampNanos: 0,
				SampleDurationNanos: 1000,
				Samples: []roachpb.InternalTimeSeriesSample{
					{Offset: int32(j), Count: 1, Sum: 5.0},
				},
			}
			var value roachpb.Value
			if err := value.SetProto(timeseries); err != nil {
				b.Fatal(err)
			}
			ms := enginepb.MVCCStats{}
			timestamp.Logical++
			err := MVCCMerge(ctx, eng, &ms, keys[i], timestamp, value)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := MVCCGet(ctx, eng, keys[rand.Intn(numKeys)], timestamp, MVCCGetOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func runMVCCDeleteRange(ctx context.Context, b *testing.B, emk engineMaker, valueBytes int) {
	// 512 KB ranges so the benchmark doesn't take forever
	const rangeBytes = 512 * 1024
	numKeys := rangeBytes / (overhead + valueBytes)
	eng, dir := setupMVCCData(ctx, b, emk, benchDataOptions{
		numVersions: 1,
		numKeys:     numKeys,
		valueBytes:  valueBytes,
	})
	eng.Close()

	b.SetBytes(rangeBytes)
	b.StopTimer()
	b.ResetTimer()

	locDirty := dir + "_dirty"

	for i := 0; i < b.N; i++ {
		if err := os.RemoveAll(locDirty); err != nil {
			b.Fatal(err)
		}
		if err := fileutil.CopyDir(dir, locDirty); err != nil {
			b.Fatal(err)
		}
		func() {
			eng := emk(b, locDirty)
			defer eng.Close()

			b.StartTimer()
			if _, _, _, err := MVCCDeleteRange(
				ctx,
				eng,
				&enginepb.MVCCStats{},
				keys.LocalMax,
				roachpb.KeyMax,
				math.MaxInt64,
				hlc.MaxTimestamp,
				nil,
				false,
			); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
		}()
	}
}

func runClearRange(
	ctx context.Context,
	b *testing.B,
	emk engineMaker,
	clearRange func(e Engine, b Batch, start, end MVCCKey) error,
) {
	const rangeBytes = 64 << 20
	const valueBytes = 92
	numKeys := rangeBytes / (overhead + valueBytes)
	eng, _ := setupMVCCData(ctx, b, emk, benchDataOptions{
		numVersions: 1,
		numKeys:     numKeys,
		valueBytes:  valueBytes,
	})
	defer eng.Close()

	// It is not currently possible to ClearRange(NilKey, MVCCKeyMax) thanks to a
	// variety of hacks inside of ClearRange that explode if provided the NilKey.
	// So instead we start our ClearRange at the first key that actually exists.
	//
	// TODO(benesch): when those hacks are removed, don't bother computing the
	// first key and simply ClearRange(NilKey, MVCCKeyMax).
	//
	// TODO(sumeer): we are now seeking starting at LocalMax, so the
	// aforementioned issue is probably resolved. Clean this up.
	iter := eng.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
	defer iter.Close()
	iter.SeekGE(MVCCKey{Key: keys.LocalMax})
	if ok, err := iter.Valid(); !ok {
		b.Fatalf("unable to find first key (err: %v)", err)
	}
	firstKey := iter.Key()

	b.SetBytes(rangeBytes)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch := eng.NewUnindexedBatch(true /* writeOnly */)
		if err := clearRange(eng, batch, firstKey, MVCCKeyMax); err != nil {
			b.Fatal(err)
		}
		// NB: We don't actually commit the batch here as we don't want to delete
		// the data. Doing so would require repopulating on every iteration of the
		// loop which was ok when ClearRange was slow but now causes the benchmark
		// to take an exceptionally long time since ClearRange is very fast.
		batch.Close()
	}

	b.StopTimer()
}

// runMVCCComputeStats benchmarks computing MVCC stats on a 64MB range of data.
func runMVCCComputeStats(ctx context.Context, b *testing.B, emk engineMaker, valueBytes int) {
	const rangeBytes = 64 * 1024 * 1024
	numKeys := rangeBytes / (overhead + valueBytes)
	eng, _ := setupMVCCData(ctx, b, emk, benchDataOptions{
		numVersions: 1,
		numKeys:     numKeys,
		valueBytes:  valueBytes,
	})
	defer eng.Close()

	b.SetBytes(rangeBytes)
	b.ResetTimer()

	var stats enginepb.MVCCStats
	var err error
	for i := 0; i < b.N; i++ {
		iter := eng.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
		stats, err = iter.ComputeStats(keys.LocalMax, roachpb.KeyMax, 0)
		iter.Close()
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	log.Infof(ctx, "live_bytes: %d", stats.LiveBytes)
}

// runMVCCCFindSplitKey benchmarks MVCCFindSplitKey on a 64MB range of data.
func runMVCCFindSplitKey(ctx context.Context, b *testing.B, emk engineMaker, valueBytes int) {
	const rangeBytes = 64 * 1024 * 1024
	numKeys := rangeBytes / (overhead + valueBytes)
	eng, _ := setupMVCCData(ctx, b, emk, benchDataOptions{
		numVersions: 1,
		numKeys:     numKeys,
		valueBytes:  valueBytes,
	})
	defer eng.Close()

	b.SetBytes(rangeBytes)
	b.ResetTimer()

	var err error
	for i := 0; i < b.N; i++ {
		_, err = MVCCFindSplitKey(ctx, eng, roachpb.RKeyMin,
			roachpb.RKeyMax, rangeBytes/2)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

type benchGarbageCollectOptions struct {
	benchDataOptions
	keyBytes       int
	deleteVersions int
}

func runMVCCGarbageCollect(
	ctx context.Context, b *testing.B, emk engineMaker, opts benchGarbageCollectOptions,
) {
	rng, _ := randutil.NewTestRand()
	eng := emk(b, "mvcc_gc")
	defer eng.Close()

	ts := hlc.Timestamp{}.Add(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), 0)
	val := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, opts.valueBytes))

	// We write values at ts+(0,i), set now=ts+(1,0) so that we're ahead of all
	// the writes. This value doesn't matter in practice, as it's used only for
	// stats updates.
	now := ts.Add(1, 0)

	// Write 'numKeys' of the given 'keySize' and 'valSize' to the given engine.
	// For each key, write 'numVersions' versions, and add a GCRequest_GCKey to
	// the returned slice that affects the oldest 'deleteVersions' versions. The
	// first write for each key will be at `ts`, the second one at `ts+(0,1)`,
	// etc.
	//
	// NB: a real invocation of MVCCGarbageCollect typically has most of the keys
	// in sorted order. Here they will be ordered randomly.
	setup := func() (gcKeys []roachpb.GCRequest_GCKey) {
		batch := eng.NewBatch()
		for i := 0; i < opts.numKeys; i++ {
			key := randutil.RandBytes(rng, opts.keyBytes)
			if opts.deleteVersions > 0 {
				gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{
					Timestamp: ts.Add(0, int32(opts.deleteVersions-1)),
					Key:       key,
				})
			}
			for j := 0; j < opts.numVersions; j++ {
				if err := MVCCPut(ctx, batch, nil /* ms */, key, ts.Add(0, int32(j)), val, nil); err != nil {
					b.Fatal(err)
				}
			}
		}
		if err := batch.Commit(false); err != nil {
			b.Fatal(err)
		}
		batch.Close()
		return gcKeys
	}

	gcKeys := setup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := eng.NewBatch()
		if err := MVCCGarbageCollect(ctx, batch, nil /* ms */, gcKeys, now); err != nil {
			b.Fatal(err)
		}
		batch.Close()
	}
}

func runBatchApplyBatchRepr(
	ctx context.Context,
	b *testing.B,
	emk engineMaker,
	indexed, sequential bool,
	valueSize, batchSize int,
) {
	rng, _ := randutil.NewTestRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("batch_apply_batch_repr_%d_%d", valueSize, batchSize))
	defer eng.Close()

	var repr []byte
	{
		order := make([]int, batchSize)
		for i := range order {
			order[i] = i
		}
		if !sequential {
			rng.Shuffle(len(order), func(i, j int) {
				order[i], order[j] = order[j], order[i]
			})
		}

		batch := eng.NewUnindexedBatch(true /* writeOnly */)
		defer batch.Close() // NB: hold open so batch.Repr() doesn't get reused

		for i := 0; i < batchSize; i++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(order[i])))
			ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			if err := MVCCBlindPut(ctx, batch, nil, key, ts, value, nil); err != nil {
				b.Fatal(err)
			}
		}
		repr = batch.Repr()
	}

	b.SetBytes(int64(len(repr)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var batch Batch
		if !indexed {
			batch = eng.NewUnindexedBatch(true /* writeOnly */)
		} else {
			batch = eng.NewBatch()
		}
		if err := batch.ApplyBatchRepr(repr, false /* sync */); err != nil {
			b.Fatal(err)
		}
		batch.Close()
	}

	b.StopTimer()
}

func runExportToSst(
	b *testing.B,
	emk engineMaker,
	numKeys int,
	numRevisions int,
	exportAllRevisions bool,
	useTBI bool,
) {
	dir, cleanup := testutils.TempDir(b)
	defer cleanup()
	engine := emk(b, dir)
	defer engine.Close()

	batch := engine.NewUnindexedBatch(true /* writeOnly */)
	for i := 0; i < numKeys; i++ {
		key := make([]byte, 16)
		key = append(key, 'a', 'a', 'a')
		key = encoding.EncodeUint32Ascending(key, uint32(i))

		for j := 0; j < numRevisions; j++ {
			err := batch.PutMVCC(MVCCKey{Key: key, Timestamp: hlc.Timestamp{WallTime: int64(j + 1), Logical: 0}}, []byte("foobar"))
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := batch.Commit(true); err != nil {
		b.Fatal(err)
	}
	batch.Close()
	if err := engine.Flush(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startTS := hlc.Timestamp{WallTime: int64(numRevisions / 2)}
		endTS := hlc.Timestamp{WallTime: int64(numRevisions + 2)}
		_, _, _, err := engine.ExportMVCCToSst(context.Background(), ExportOptions{
			StartKey:           MVCCKey{Key: keys.LocalMax},
			EndKey:             roachpb.KeyMax,
			StartTS:            startTS,
			EndTS:              endTS,
			ExportAllRevisions: exportAllRevisions,
			TargetSize:         0,
			MaxSize:            0,
			StopMidKey:         false,
			UseTBI:             useTBI,
		}, noopWriter{})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

type noopWriter struct{}

func (noopWriter) Close() error                { return nil }
func (noopWriter) Write(p []byte) (int, error) { return len(p), nil }

func runCheckSSTConflicts(b *testing.B, numEngineKeys, numVersions, numSstKeys int, overlap bool) {
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	value := make([]byte, 128)
	for i := range value {
		value[i] = 'a'
	}

	eng := setupMVCCInMemPebble(b, "")
	defer eng.Close()

	for i := 0; i < numEngineKeys; i++ {
		batch := eng.NewBatch()
		for j := 0; j < numVersions; j++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
			ts := hlc.Timestamp{WallTime: int64(j + 1)}
			require.NoError(b, batch.PutMVCC(MVCCKey{key, ts}, value))
		}
		require.NoError(b, batch.Commit(false))
	}
	require.NoError(b, eng.Flush())

	// The engine contains keys numbered key-1, key-2, key-3, etc, while
	// the SST contains keys numbered key-11, key-21, etc., that fit in
	// between the engine keys without colliding.
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	sstFile := &MemFile{}
	sstWriter := MakeIngestionSSTWriter(ctx, st, sstFile)
	var sstStart, sstEnd MVCCKey
	for i := 0; i < numSstKeys; i++ {
		keyNum := int((float64(i) / float64(numSstKeys)) * float64(numEngineKeys))
		if !overlap {
			keyNum = i + numEngineKeys
		}
		key := roachpb.Key(encoding.EncodeUvarintAscending(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(keyNum)), 1))
		mvccKey := MVCCKey{Key: key, Timestamp: hlc.Timestamp{WallTime: int64(numVersions + 3)}}
		if i == 0 {
			sstStart.Key = append([]byte(nil), mvccKey.Key...)
			sstStart.Timestamp = mvccKey.Timestamp
		} else if i == numSstKeys-1 {
			sstEnd.Key = append([]byte(nil), mvccKey.Key...)
			sstEnd.Timestamp = mvccKey.Timestamp
		}
		require.NoError(b, sstWriter.Put(mvccKey, value))
	}
	sstWriter.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := CheckSSTConflicts(context.Background(), sstFile.Data(), eng, sstStart, sstEnd, false, hlc.Timestamp{}, math.MaxInt64)
		require.NoError(b, err)
	}
}

var benchRowColMap catalog.TableColMap
var benchRowPrefix roachpb.Key

func init() {
	benchRowColMap.Set(0, 0)
	benchRowPrefix = keys.SystemSQLCodec.IndexPrefix(1, 1)
}

// makeBenchRowKey makes a key for a SQL row for use in benchmarks,
// using the system tenant with table 1 index 1, and a single column.
func makeBenchRowKey(b *testing.B, buf []byte, id int, columnFamily uint32) roachpb.Key {
	var err error
	buf = append(buf, benchRowPrefix...)
	buf, _, err = rowenc.EncodeColumns(
		[]descpb.ColumnID{0}, nil /* directions */, benchRowColMap,
		[]tree.Datum{tree.NewDInt(tree.DInt(id))}, buf)
	if err != nil {
		// conditionally check this, for performance
		require.NoError(b, err)
	}
	return keys.MakeFamilyKey(buf, columnFamily)
}
