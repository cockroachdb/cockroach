// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

func BenchmarkMVCCComputeStats(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testCase struct {
		valueSize    int
		numRangeKeys int
	}
	var testCases []testCase
	for _, valueSize := range []int{8, 32, 256} {
		for _, numRangeKeys := range []int{0, 1, 100} {
			testCases = append(testCases, testCase{
				valueSize:    valueSize,
				numRangeKeys: numRangeKeys,
			})
		}
	}

	if testing.Short() {
		// Choose a configuration for the short version.
		testCases = []testCase{
			{valueSize: 8, numRangeKeys: 1},
		}
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"valueSize=%d/numRangeKeys=%d",
			tc.valueSize, tc.numRangeKeys,
		)

		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			runMVCCComputeStats(ctx, b, tc.valueSize, tc.numRangeKeys)
		})
	}
}

func BenchmarkMVCCGarbageCollect(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	// NB: To debug #16068, test only 128-128-15000-6.
	keySizes := []int{128}
	valSizes := []int{128}
	numKeys := []int{1, 1024}

	versionConfigs := []struct {
		total              int
		rangeTombstoneKeys []int
		toDelete           []int
	}{
		{
			total:              2,
			rangeTombstoneKeys: []int{0, 1, 2},
			toDelete:           []int{1},
		},
		{
			total:              1024,
			rangeTombstoneKeys: []int{0, 1, 100},
			toDelete:           []int{1, 16, 32, 512, 1015, 1023},
		},
	}
	updateStats := []bool{false, true}
	engineMakers := []struct {
		name   string
		create engineMaker
	}{
		{"pebble", setupPebbleInMemPebbleForLatestRelease},
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
													for _, rangeTombstones := range versions.rangeTombstoneKeys {
														b.Run(fmt.Sprintf("numRangeTs=%d", rangeTombstones), func(b *testing.B) {
															for _, stats := range updateStats {
																b.Run(fmt.Sprintf("updateStats=%t", stats), func(b *testing.B) {
																	runMVCCGarbageCollect(ctx, b, engineImpl.create,
																		benchGarbageCollectOptions{
																			mvccBenchData: mvccBenchData{
																				numKeys:      numKeys,
																				numVersions:  versions.total,
																				valueBytes:   valSize,
																				numRangeKeys: rangeTombstones,
																			},
																			keyBytes:       keySize,
																			deleteVersions: toDelete,
																			updateStats:    stats,
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
				})
			}
		})
	}
}

func BenchmarkMVCCGet(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testCase struct {
		batch        bool
		numVersions  int
		valueSize    int
		numRangeKeys int
	}
	var testCases []testCase
	for _, batch := range []bool{false, true} {
		for _, numVersions := range []int{1, 10, 100} {
			for _, valueSize := range []int{8} {
				for _, numRangeKeys := range []int{0, 1, 100} {
					testCases = append(testCases, testCase{
						batch:        batch,
						numVersions:  numVersions,
						valueSize:    valueSize,
						numRangeKeys: numRangeKeys,
					})
				}
			}
		}
	}

	if testing.Short() {
		// Choose a few configurations for the short version.
		testCases = []testCase{
			{batch: false, numVersions: 1, valueSize: 8, numRangeKeys: 0},
			{batch: true, numVersions: 10, valueSize: 8, numRangeKeys: 0},
			{batch: true, numVersions: 10, valueSize: 8, numRangeKeys: 10},
		}
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"batch=%t/versions=%d/valueSize=%d/numRangeKeys=%d",
			tc.batch, tc.numVersions, tc.valueSize, tc.numRangeKeys,
		)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			runMVCCGet(ctx, b, mvccBenchData{
				numVersions:  tc.numVersions,
				valueBytes:   tc.valueSize,
				numRangeKeys: tc.numRangeKeys,
			}, tc.batch)
		})
	}
}

func BenchmarkMVCCExportToSST(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	// To run and compare on range keys:
	//
	//     go test ./pkg/storage -run - -count 5 -bench BenchmarkMVCCExportToSST -timeout 500m 2>&1 | tee bench.txt
	//     for flavor in numRangeKeys=0 numRangeKeys=1 numRangeKeys=100; do grep -E "${flavor}[^0-9]+" bench.txt | sed -E "s/${flavor}+/X/" > $flavor.txt; done
	//     benchstat numRangeKeys\={0,1}.txt
	//     benchstat numRangeKeys\={0,100}.txt

	numKeys := []int{64, 512, 1024, 8192, 65536}
	numRevisions := []int{1, 10, 100}
	numRangeKeys := []int{0, 1, 100}
	exportAllRevisions := []bool{false, true}
	for _, numKey := range numKeys {
		b.Run(fmt.Sprintf("numKeys=%d", numKey), func(b *testing.B) {
			for _, numRevision := range numRevisions {
				b.Run(fmt.Sprintf("numRevisions=%d", numRevision), func(b *testing.B) {
					for _, numRangeKey := range numRangeKeys {
						b.Run(fmt.Sprintf("numRangeKeys=%d", numRangeKey), func(b *testing.B) {
							for _, perc := range []int{100, 50, 10} {
								if numRevision == 1 && perc != 100 {
									continue // no point in incremental exports with 1 revision
								}
								b.Run(fmt.Sprintf("perc=%d%%", perc), func(b *testing.B) {
									for _, exportAllRevisionsVal := range exportAllRevisions {
										b.Run(fmt.Sprintf("exportAllRevisions=%t", exportAllRevisionsVal), func(b *testing.B) {
											opts := mvccExportToSSTOpts{
												numKeys:            numKey,
												numRevisions:       numRevision,
												numRangeKeys:       numRangeKey,
												exportAllRevisions: exportAllRevisionsVal,
												percentage:         perc,
											}
											runMVCCExportToSST(b, opts)
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

	// Doubling the test variants because of this one parameter feels pretty
	// excessive (this benchmark is already very long), so handle it specially
	// and hard code the other parameters. To run and compare:
	//
	//     dev bench pkg/storage --filter BenchmarkMVCCExportToSST/useElasticCPUHandle --count 10 --timeout 20m -v --stream-output --ignore-cache 2>&1 | tee bench.txt
	//     for flavor in useElasticCPUHandle=true useElasticCPUHandle=false; do grep -E "${flavor}[^0-9]+" bench.txt | sed -E "s/${flavor}+/X/" > $flavor.txt; done
	//     benchstat useElasticCPUHandle\={false,true}.txt
	//
	// Results (08/29/2022):
	//
	//     goos: linux
	//     goarch: amd64
	//     cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
	//     ...
	//     $ benchstat useElasticCPUHandle\={false,true}.txt
	//     name                                                                         old time/op  new time/op  delta
	//     MVCCExportToSST/X/numKeys=65536/numRevisions=100/exportAllRevisions=true-24   2.54s ± 2%   2.53s ± 2%   ~     (p=0.549 n=10+9)
	//
	withElasticCPUHandle := []bool{false, true}
	for _, useElasticCPUHandle := range withElasticCPUHandle {
		numKey := numKeys[len(numKeys)-1]
		numRevision := numRevisions[len(numRevisions)-1]
		numRangeKey := numRangeKeys[len(numRangeKeys)-1]
		exportAllRevisionVal := exportAllRevisions[len(exportAllRevisions)-1]
		b.Run(fmt.Sprintf("useElasticCPUHandle=%t/numKeys=%d/numRevisions=%d/exportAllRevisions=%t",
			useElasticCPUHandle, numKey, numRevision, exportAllRevisionVal,
		), func(b *testing.B) {
			opts := mvccExportToSSTOpts{
				numKeys:             numKey,
				numRevisions:        numRevision,
				numRangeKeys:        numRangeKey,
				exportAllRevisions:  exportAllRevisionVal,
				useElasticCPUHandle: useElasticCPUHandle,
			}
			runMVCCExportToSST(b, opts)
		})
	}
	withImportEpochs := []bool{false, true}
	for _, ie := range withImportEpochs {
		numKey := numKeys[len(numKeys)-1]
		numRevision := numRevisions[len(numRevisions)-1]
		numRangeKey := numRangeKeys[len(numRangeKeys)-1]
		exportAllRevisionVal := exportAllRevisions[len(exportAllRevisions)-1]
		b.Run(fmt.Sprintf("importEpochs=%t/numKeys=%d/numRevisions=%d/exportAllRevisions=%t",
			ie, numKey, numRevision, exportAllRevisionVal,
		), func(b *testing.B) {
			opts := mvccExportToSSTOpts{
				numKeys:            numKey,
				numRevisions:       numRevision,
				numRangeKeys:       numRangeKey,
				exportAllRevisions: exportAllRevisionVal,
				importEpochs:       ie,
			}
			runMVCCExportToSST(b, opts)
		})
	}
}

func BenchmarkMVCCFindSplitKey(b *testing.B) {
	defer log.Scope(b).Close(b)
	for _, valueSize := range []int{32} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			ctx := context.Background()
			runMVCCFindSplitKey(ctx, b, valueSize)
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
		value := roachpb.MakeValueFromString("value")
		batch := eng.NewBatch()
		for j := 0; j < numIntentKeys; j++ {
			putTxn := &txn
			if txnID == txnIDWithLatestVersion && j%lockUpdateTxnHasLatestVersionStride != 0 {
				putTxn = &otherTxn
			}
			key := makeKey(nil, j)
			_, err := MVCCPut(context.Background(), batch, key, ts, value, MVCCWriteOptions{Txn: putTxn})
			require.NoError(b, err)
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
				found, _, _, _, err := MVCCResolveWriteIntent(context.Background(), batch, nil, lu, MVCCResolveWriteIntentOptions{})
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
					iter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{
						LowerBound: lower,
						UpperBound: makeKey(nil, numIntentKeys),
					})
					if err != nil {
						b.Fatal(err)
					}
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
							iter, err = eng.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{
								LowerBound: lower,
								UpperBound: makeKey(nil, numIntentKeys),
							})
							if err != nil {
								b.Fatal(err)
							}
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
							buf = encoding.BytesNext(buf)
							iter.SeekGE(MVCCKey{Key: buf})
						}
					}
				})
			}
		})
	}
}

func BenchmarkMVCCScan(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testCase struct {
		numRows       int
		numVersions   int
		valueSize     int
		numRangeKeys  int
		includeHeader bool
	}
	var testCases []testCase
	for _, numRows := range []int{1, 10, 100, 1000, 10000, 50000} {
		for _, numVersions := range []int{1, 2, 10, 100, 1000} {
			for _, valueSize := range []int{8, 64, 512} {
				for _, numRangeKeys := range []int{0, 1, 100} {
					testCases = append(testCases, testCase{
						numRows:      numRows,
						numVersions:  numVersions,
						valueSize:    valueSize,
						numRangeKeys: numRangeKeys,
					})
				}
			}
		}
	}

	if testing.Short() {
		// Choose a few configurations for the short version.
		testCases = []testCase{
			{numRows: 1, numVersions: 1, valueSize: 8, numRangeKeys: 0},
			{numRows: 100, numVersions: 2, valueSize: 64, numRangeKeys: 1},
			{numRows: 1000, numVersions: 10, valueSize: 64, numRangeKeys: 100},
		}
	}

	testCases = append(testCases, testCase{
		numRows:       1000,
		numVersions:   2,
		valueSize:     64,
		numRangeKeys:  0,
		includeHeader: true,
	})

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"rows=%d/versions=%d/valueSize=%d/numRangeKeys=%d/headers=%v",
			tc.numRows, tc.numVersions, tc.valueSize, tc.numRangeKeys, tc.includeHeader,
		)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			runMVCCScan(ctx, b, benchScanOptions{
				mvccBenchData: mvccBenchData{
					numVersions:   tc.numVersions,
					valueBytes:    tc.valueSize,
					numRangeKeys:  tc.numRangeKeys,
					includeHeader: tc.includeHeader,
				},
				numRows: tc.numRows,
				reverse: false,
			})
		})
	}
}

func BenchmarkMVCCScanGarbage(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testCase struct {
		numRows      int
		numVersions  int
		numRangeKeys int
		tombstones   bool
	}
	var testCases []testCase
	for _, numRows := range []int{1, 10, 100, 1000, 10000, 50000} {
		for _, numVersions := range []int{1, 2, 10, 100, 1000} {
			for _, numRangeKeys := range []int{0, 1, 100} {
				for _, tombstones := range []bool{false, true} {
					testCases = append(testCases, testCase{
						numRows:      numRows,
						numVersions:  numVersions,
						numRangeKeys: numRangeKeys,
						tombstones:   tombstones,
					})
				}
			}
		}
	}

	if testing.Short() {
		// Choose a few configurations for the short version.
		testCases = []testCase{
			{numRows: 1, numVersions: 1, numRangeKeys: 0, tombstones: false},
			{numRows: 10, numVersions: 2, numRangeKeys: 1, tombstones: true},
			{numRows: 1000, numVersions: 10, numRangeKeys: 100, tombstones: true},
		}
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"rows=%d/versions=%d/numRangeKeys=%d/tombstones=%t",
			tc.numRows, tc.numVersions, tc.numRangeKeys, tc.tombstones,
		)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			runMVCCScan(ctx, b, benchScanOptions{
				mvccBenchData: mvccBenchData{
					numVersions:  tc.numVersions,
					numRangeKeys: tc.numRangeKeys,
					garbage:      true,
				},
				numRows:    tc.numRows,
				tombstones: tc.tombstones,
				reverse:    false,
			})
		})
	}
}

func BenchmarkMVCCScanSQLRows(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testCase struct {
		numRows           int
		numColumnFamilies int
		numVersions       int
		valueSize         int
		wholeRows         bool
	}
	var testCases []testCase
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		for _, numColumnFamilies := range []int{1, 3, 10} {
			for _, numVersions := range []int{1} {
				for _, valueSize := range []int{8, 64, 512} {
					for _, wholeRows := range []bool{false, true} {
						testCases = append(testCases, testCase{
							numRows:           numRows,
							numColumnFamilies: numColumnFamilies,
							numVersions:       numVersions,
							valueSize:         valueSize,
							wholeRows:         wholeRows,
						})
					}
				}
			}
		}
	}

	if testing.Short() {
		// Choose a few configurations for the short version.
		testCases = []testCase{
			{numRows: 1, numColumnFamilies: 1, numVersions: 1, valueSize: 8, wholeRows: false},
			{numRows: 100, numColumnFamilies: 10, numVersions: 1, valueSize: 8, wholeRows: true},
			{numRows: 1000, numColumnFamilies: 3, numVersions: 1, valueSize: 64, wholeRows: true},
		}
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"rows=%d/columnFamillies=%d/versions=%d/valueSize=%d/wholeRows=%t",
			tc.numRows, tc.numColumnFamilies, tc.numVersions, tc.valueSize, tc.wholeRows,
		)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			runMVCCScan(ctx, b, benchScanOptions{
				mvccBenchData: mvccBenchData{
					numColumnFamilies: tc.numColumnFamilies,
					numVersions:       tc.numVersions,
					valueBytes:        tc.valueSize,
				},
				numRows:   tc.numRows,
				reverse:   false,
				wholeRows: tc.wholeRows,
			})
		})
	}
}

func BenchmarkMVCCReverseScan(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testCase struct {
		numRows      int
		numVersions  int
		valueSize    int
		numRangeKeys int
	}
	var testCases []testCase
	for _, numRows := range []int{1, 10, 100, 1000, 10000, 50000} {
		for _, numVersions := range []int{1, 2, 10, 100, 1000} {
			for _, valueSize := range []int{8, 64, 512} {
				for _, numRangeKeys := range []int{0, 1, 100} {
					testCases = append(testCases, testCase{
						numRows:      numRows,
						numVersions:  numVersions,
						valueSize:    valueSize,
						numRangeKeys: numRangeKeys,
					})
				}
			}
		}
	}

	if testing.Short() {
		// Choose a few configurations for the short version.
		testCases = []testCase{
			{numRows: 1, numVersions: 1, valueSize: 8, numRangeKeys: 0},
			{numRows: 100, numVersions: 1, valueSize: 8, numRangeKeys: 1},
			{numRows: 1000, numVersions: 2, valueSize: 64, numRangeKeys: 100},
		}
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"rows=%d/versions=%d/valueSize=%d/numRangeKeys=%d",
			tc.numRows, tc.numVersions, tc.valueSize, tc.numRangeKeys,
		)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			runMVCCScan(ctx, b, benchScanOptions{
				mvccBenchData: mvccBenchData{
					numVersions:  tc.numVersions,
					valueBytes:   tc.valueSize,
					numRangeKeys: tc.numRangeKeys,
				},
				numRows: tc.numRows,
				reverse: true,
			})
		})
	}
}

func BenchmarkMVCCScanTransactionalData(b *testing.B) {
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	runMVCCScan(ctx, b, benchScanOptions{
		numRows: 10000,
		mvccBenchData: mvccBenchData{
			numVersions:   2,
			valueBytes:    8,
			transactional: true,
		},
	})
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
						iter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{
							LowerBound: buf,
							UpperBound: upper,
						})
						if err != nil {
							b.Fatal(err)
						}
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
							buf = encoding.BytesNext(buf)
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
						found, _, _, _, err := MVCCResolveWriteIntent(context.Background(), batch, nil, lockUpdate, MVCCResolveWriteIntentOptions{})
						if !found || err != nil {
							b.Fatalf("intent not found or err %s", err)
						}
					}
				})
			}
		})
	}
}

// BenchmarkIntentRangeResolution benchmarks ranged intent resolution with
// various counts of mvcc versions and sparseness of intents.
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
										resolved, _, span, _, _, err := MVCCResolveWriteIntentRange(
											context.Background(), batch, nil, lockUpdate,
											MVCCResolveWriteIntentRangeOptions{MaxKeys: 1000})
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
		fs.MustInitPhysicalTestingEnv(dir),
		cluster.MakeTestingClusterSettings())
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
		timestamp := hlc.Timestamp{WallTime: minWallTime + rng.Int63n(int64(batchTimeSpan))}
		value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueBytes))
		value.InitChecksum(key)
		if _, err := MVCCPut(ctx, batch, key, timestamp, value, MVCCWriteOptions{}); err != nil {
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

type benchScanOptions struct {
	mvccBenchData
	numRows    int
	reverse    bool
	wholeRows  bool
	tombstones bool
}

// runMVCCScan first creates test data (and resets the benchmarking
// timer). It then performs b.N MVCCScans in increments of numRows
// keys over all of the data in the Engine instance, restarting at
// the beginning of the keyspace, as many times as necessary.
func runMVCCScan(ctx context.Context, b *testing.B, opts benchScanOptions) {
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

	eng := getInitialStateEngine(ctx, b, opts.mvccBenchData, false /* inMemory */)
	defer eng.Close()

	{
		// Pull all of the sstables into the RocksDB cache in order to make the
		// timings more stable. Otherwise, the first run will be penalized pulling
		// data into the cache while later runs will not.
		if _, err := ComputeStats(ctx, eng, keys.LocalMax, roachpb.KeyMax, 0); err != nil {
			b.Fatalf("stats failed: %s", err)
		}
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
		if opts.garbage {
			walltime = hlc.MaxTimestamp.WallTime
		}
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
			Tombstones:      opts.tombstones,
		})
		if err != nil {
			b.Fatalf("failed scan: %+v", err)
		}
		expectKVs := opts.numRows
		if opts.wholeRows {
			expectKVs -= opts.numRows % opts.numColumnFamilies
		}
		if !opts.garbage && len(res.KVs) != expectKVs {
			b.Fatalf("failed to scan: %d != %d", len(res.KVs), expectKVs)
		}
		if opts.garbage && !opts.tombstones && len(res.KVs) != 0 {
			b.Fatalf("failed to scan garbage: found %d keys", len(res.KVs))
		}
	}

	b.StopTimer()
}

// runMVCCGet first creates test data (and resets the benchmarking
// timer). It then performs b.N MVCCGets.
func runMVCCGet(ctx context.Context, b *testing.B, opts mvccBenchData, useBatch bool) {
	// Use the same number of keys for all of the mvcc scan
	// benchmarks. Using a different number of keys per test gives
	// preferential treatment to tests with fewer keys. Note that the
	// datasets all fit in cache and the cache is pre-warmed.
	if opts.numKeys != 0 {
		b.Fatal("test error: cannot call runMVCCGet with non-zero numKeys")
	}
	opts.numKeys = 100000

	eng := getInitialStateEngine(ctx, b, opts, false /* inMemory */)
	defer eng.Close()

	r := Reader(eng)
	if useBatch {
		batch := eng.NewBatch()
		defer batch.Close()
		r = batch
	}

	b.SetBytes(int64(opts.valueBytes))
	b.ResetTimer()

	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	for i := 0; i < b.N; i++ {
		// Choose a random key to retrieve.
		keyIdx := rand.Int31n(int32(opts.numKeys))
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(keyIdx)))
		walltime := int64(5 * (rand.Int31n(int32(opts.numVersions)) + 1))
		ts := hlc.Timestamp{WallTime: walltime}
		if valRes, err := MVCCGet(ctx, r, key, ts, MVCCGetOptions{}); err != nil {
			b.Fatalf("failed get: %+v", err)
		} else if valRes.Value == nil {
			b.Fatalf("failed get (key not found): %d@%d", keyIdx, walltime)
		} else if valueBytes, err := valRes.Value.GetBytes(); err != nil {
			b.Fatal(err)
		} else if len(valueBytes) != opts.valueBytes {
			b.Fatalf("unexpected value size: %d", len(valueBytes))
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
		batch := eng.NewWriteBatch()
		if _, err := MVCCBlindPut(ctx, batch, key, ts, value, MVCCWriteOptions{}); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
		if err := batch.Commit(true); err != nil {
			b.Fatalf("failed commit: %v", err)
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
			batch := eng.NewBatch()
			if _, err := MVCCPut(ctx, batch, key, ts, value, MVCCWriteOptions{}); err != nil {
				b.Fatalf("failed put: %+v", err)
			}
			if err := batch.Commit(true); err != nil {
				b.Fatalf("failed commit: %v", err)
			}
			batch.Close()
		}
		expected = value.TagAndDataBytes()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		batch := eng.NewBatch()
		if _, err := MVCCConditionalPut(ctx, batch, key, ts, value, expected, ConditionalPutWriteOptions{AllowIfDoesNotExist: CPutFailIfMissing}); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
		if err := batch.Commit(true); err != nil {
			b.Fatalf("failed commit: %v", err)
		}
		batch.Close()
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
		batch := eng.NewWriteBatch()
		if _, err := MVCCBlindConditionalPut(
			ctx, batch, key, ts, value, nil, ConditionalPutWriteOptions{AllowIfDoesNotExist: CPutFailIfMissing},
		); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
		if err := batch.Commit(true); err != nil {
			b.Fatalf("failed commit: %v", err)
		}
		batch.Close()
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
		batch := eng.NewBatch()
		if _, err := MVCCInitPut(ctx, batch, key, ts, value, false, MVCCWriteOptions{}); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
		if err := batch.Commit(true); err != nil {
			b.Fatalf("failed commit: %v", err)
		}
		batch.Close()
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
		wb := eng.NewWriteBatch()
		if _, err := MVCCBlindInitPut(ctx, wb, key, ts, value, false, MVCCWriteOptions{}); err != nil {
			b.Fatalf("failed put: %+v", err)
		}
		if err := wb.Commit(true); err != nil {
			b.Fatalf("failed commit: %v", err)
		}
		wb.Close()
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
			if _, err := MVCCPut(ctx, batch, key, ts, value, MVCCWriteOptions{}); err != nil {
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
		_, err := MVCCGet(ctx, eng, keys[rand.Intn(numKeys)], timestamp, MVCCGetOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func runMVCCDeleteRange(ctx context.Context, b *testing.B, valueBytes int) {
	// 512 KB ranges so the benchmark doesn't take forever
	const rangeBytes = 512 * 1024
	numKeys := rangeBytes / (overhead + valueBytes)
	b.SetBytes(rangeBytes)
	b.StopTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		func() {
			eng := getInitialStateEngine(ctx, b, mvccBenchData{
				numVersions: 1,
				numKeys:     numKeys,
				valueBytes:  valueBytes,
			}, false /* inMemory */)
			defer eng.Close()

			b.StartTimer()
			if _, _, _, _, err := MVCCDeleteRange(
				ctx,
				eng,
				keys.LocalMax,
				roachpb.KeyMax,
				math.MaxInt64,
				hlc.MaxTimestamp,
				MVCCWriteOptions{
					// NB: Though we don't capture the stats object, passing nil would
					// disable stats computations, which should be included in benchmarks.
					// This cost is not always negligible, e.g. in some cases such as
					// with MVCC range tombstones where additional seeks to find boundary
					// conditions are involved.
					Stats: &enginepb.MVCCStats{},
				},
				false,
			); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
		}()
	}
}

func runMVCCDeleteRangeUsingTombstone(
	ctx context.Context, b *testing.B, numKeys int, valueBytes int, entireRange bool,
) {
	opts := withCompactedDB(mvccBenchData{
		numVersions: 1,
		numKeys:     numKeys,
		valueBytes:  valueBytes,
	})

	var msCovered *enginepb.MVCCStats
	var leftPeekBound, rightPeekBound roachpb.Key
	if entireRange {
		func() {
			eng := getInitialStateEngine(ctx, b, opts, false /* inMemory */)
			defer eng.Close()

			ms, err := ComputeStats(ctx, eng, keys.LocalMax, keys.MaxKey, 0)
			if err != nil {
				b.Fatal(err)
			}

			leftPeekBound = keys.LocalMax
			rightPeekBound = keys.MaxKey
			msCovered = &ms
		}()
	}

	b.SetBytes(int64(numKeys) * int64(overhead+valueBytes))
	b.StopTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		func() {
			eng := getInitialStateEngine(ctx, b, opts, false /* inMemory */)
			defer eng.Close()

			b.StartTimer()
			if err := MVCCDeleteRangeUsingTombstone(
				ctx,
				eng,
				&enginepb.MVCCStats{},
				keys.LocalMax,
				roachpb.KeyMax,
				hlc.MaxTimestamp,
				hlc.ClockTimestamp{},
				leftPeekBound,
				rightPeekBound,
				false, // idempotent
				0,
				0,
				msCovered,
			); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
		}()
	}
}

// runMVCCDeleteRangeWithPredicate issues a predicate based delete range that
// deletes all keys created after the `deleteAfterLayer` (0 indexed).
func runMVCCDeleteRangeWithPredicate(
	ctx context.Context,
	b *testing.B,
	config mvccImportedData,
	deleteAfterLayer int64,
	rangeTombstoneThreshold int64,
) {
	b.SetBytes(int64(config.layers*config.keyCount) * int64(overhead+config.valueBytes))
	b.StopTimer()
	b.ResetTimer()

	// Since the db engine creates mvcc versions at 5 ns increments, multiply the
	// deleteAtVersion by 5 to compute the delete range timestamp predicate.
	predicates := kvpb.DeleteRangePredicates{
		StartTime: hlc.Timestamp{WallTime: (deleteAfterLayer+1)*5 + 1},
	}
	var leftPeekBound, rightPeekBound roachpb.Key
	for i := 0; i < b.N; i++ {
		func() {
			eng := getInitialStateEngine(ctx, b, config, false)
			defer eng.Close()
			b.StartTimer()
			resumeSpan, err := MVCCPredicateDeleteRange(
				ctx,
				eng,
				&enginepb.MVCCStats{},
				keys.LocalMax,
				roachpb.KeyMax,
				hlc.MaxTimestamp,
				hlc.ClockTimestamp{},
				leftPeekBound,
				rightPeekBound,
				predicates,
				0,
				math.MaxInt64,
				rangeTombstoneThreshold,
				0,
				0,
			)
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
			if resumeSpan != nil {
				b.Fatalf("unexpected resume span: %v", resumeSpan)
			}
		}()
	}
}

func runClearRange(
	ctx context.Context, b *testing.B, clearRange func(e Engine, b Batch, start, end MVCCKey) error,
) {
	const rangeBytes = 64 << 20
	const valueBytes = 92
	numKeys := rangeBytes / (overhead + valueBytes)
	eng := getInitialStateEngine(ctx, b, mvccBenchData{
		numVersions: 1,
		numKeys:     numKeys,
		valueBytes:  valueBytes,
	}, false /* inMemory */)
	defer eng.Close()

	b.SetBytes(rangeBytes)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch := eng.NewUnindexedBatch()
		if err := clearRange(eng, batch, MVCCKey{Key: keys.LocalMax}, MVCCKeyMax); err != nil {
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
func runMVCCComputeStats(ctx context.Context, b *testing.B, valueBytes int, numRangeKeys int) {
	const rangeBytes = 64 * 1024 * 1024
	numKeys := rangeBytes / (overhead + valueBytes)
	eng := getInitialStateEngine(ctx, b, mvccBenchData{
		numVersions:  1,
		numKeys:      numKeys,
		valueBytes:   valueBytes,
		numRangeKeys: numRangeKeys,
	}, false /* inMemory */)
	defer eng.Close()

	b.SetBytes(rangeBytes)
	b.ResetTimer()

	var stats enginepb.MVCCStats
	var err error
	for i := 0; i < b.N; i++ {
		stats, err = ComputeStats(ctx, eng, keys.LocalMax, keys.MaxKey, 0)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	log.Infof(ctx, "live_bytes: %d", stats.LiveBytes)
}

// runMVCCCFindSplitKey benchmarks MVCCFindSplitKey on a 64MB range of data.
func runMVCCFindSplitKey(ctx context.Context, b *testing.B, valueBytes int) {
	const rangeBytes = 64 * 1024 * 1024
	numKeys := rangeBytes / (overhead + valueBytes)

	eng := getInitialStateEngine(ctx, b, mvccBenchData{
		numVersions: 1,
		numKeys:     numKeys,
		valueBytes:  valueBytes,
	}, false /* inMemory */)
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
	mvccBenchData
	keyBytes       int
	deleteVersions int
	updateStats    bool
}

func runMVCCGarbageCollect(
	ctx context.Context, b *testing.B, emk engineMaker, opts benchGarbageCollectOptions,
) {
	rng, _ := randutil.NewTestRand()
	eng := emk(b, "mvcc_gc")
	defer eng.Close()

	ts := hlc.Timestamp{}.Add(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), 0)
	val := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, opts.valueBytes))

	// We write values at ts+(1,i), set now=ts+(2,0) so that we're ahead of all
	// the writes. This value doesn't matter in practice, as it's used only for
	// stats updates.
	now := ts.Add(2, 0)

	// Write 'numKeys' of the given 'keySize' and 'valSize' to the given engine.
	// For each key, write 'numVersions' versions, and add a GCRequest_GCKey to
	// the returned slice that affects the oldest 'deleteVersions' versions. The
	// first write for each key will be at `ts+(1,0)`, the second one
	// at `ts+(1,1)`, etc.
	//
	// Write 'numRangeKeys' covering random intervals of keys using set of keys
	// from the set described above. One range key is always below all point keys.
	// Range tombstones are spread evenly covering versions space inserted between
	// every m point versions.
	// Range tombstones use timestamps starting from 'ts+(0,1)` and increase wall
	// clock between version and if there are more range tombstones than
	// numVersions, then logical clock is used to distinguish between them.
	//
	// NB: a real invocation of MVCCGarbageCollect typically has most of the keys
	// in sorted order. Here they will be ordered randomly.

	pointKeyTs := func(index int) hlc.Timestamp {
		return ts.Add(int64(index+1), 0)
	}

	rangeKeyTs := func(index, numVersions, numRangeKeys int) hlc.Timestamp {
		wallTime := index * numVersions / numRangeKeys
		base := wallTime * numRangeKeys / numVersions
		logical := 1 + index - base
		return ts.Add(int64(wallTime), int32(logical))
	}

	setup := func() (gcKeys []kvpb.GCRequest_GCKey) {
		batch := eng.NewBatch()
		pointKeys := make([]roachpb.Key, opts.numKeys)
		for i := 0; i < opts.numKeys; i++ {
			pointKeys[i] = randutil.RandBytes(rng, opts.keyBytes)
			gcKeys = append(gcKeys, kvpb.GCRequest_GCKey{
				Timestamp: pointKeyTs(opts.deleteVersions - 1),
				Key:       pointKeys[i],
			})
		}
		rtsVersion := 0
		for version := 0; ; version++ {
			pts := pointKeyTs(version)
			// Insert range keys until we reach next point key or run out of range
			// key versions.
			for ; rtsVersion < opts.numRangeKeys; rtsVersion++ {
				rts := rangeKeyTs(rtsVersion, opts.numVersions, opts.numRangeKeys)
				if pts.LessEq(rts) {
					break
				}
				startKey := keys.LocalMax
				endKey := keys.MaxKey
				if rtsVersion > 0 && opts.numKeys > 1 {
					for {
						startKey = pointKeys[rng.Intn(opts.numKeys)]
						endKey = pointKeys[rng.Intn(opts.numKeys)]
						switch startKey.Compare(endKey) {
						case 0:
							continue
						case 1:
							startKey, endKey = endKey, startKey
						case -1:
						}
						break
					}
				}
				if err := MVCCDeleteRangeUsingTombstone(ctx, batch, nil, startKey, endKey,
					rts, hlc.ClockTimestamp{}, nil, nil, false, 0, 0, nil); err != nil {
					b.Fatal(err)
				}
			}
			if version == opts.numVersions {
				break
			}
			for _, key := range pointKeys {
				if _, err := MVCCPut(ctx, batch, key, pts, val, MVCCWriteOptions{}); err != nil {
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

	var ms *enginepb.MVCCStats
	if opts.updateStats {
		ms = &enginepb.MVCCStats{}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := eng.NewBatch()
		if err := MVCCGarbageCollect(ctx, batch, ms, gcKeys, now); err != nil {
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

		batch := eng.NewWriteBatch()
		defer batch.Close() // NB: hold open so batch.Repr() doesn't get reused

		for i := 0; i < batchSize; i++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(order[i])))
			ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			if _, err := MVCCBlindPut(ctx, batch, key, ts, value, MVCCWriteOptions{}); err != nil {
				b.Fatal(err)
			}
		}
		repr = batch.Repr()
	}

	b.SetBytes(int64(len(repr)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var batch WriteBatch
		if !indexed {
			batch = eng.NewWriteBatch()
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

func runMVCCCheckForAcquireLock(
	ctx context.Context,
	b *testing.B,
	emk engineMaker,
	useBatch bool,
	heldOtherTxn bool,
	heldSameTxn bool,
	strength lock.Strength,
) {
	runMVCCAcquireLockCommon(ctx, b, emk, useBatch, heldOtherTxn, heldSameTxn, strength, true /* checkFor */)
}

func runMVCCAcquireLock(
	ctx context.Context,
	b *testing.B,
	emk engineMaker,
	useBatch bool,
	heldOtherTxn bool,
	heldSameTxn bool,
	strength lock.Strength,
) {
	runMVCCAcquireLockCommon(ctx, b, emk, useBatch, heldOtherTxn, heldSameTxn, strength, false /* checkFor */)
}

func runMVCCAcquireLockCommon(
	ctx context.Context,
	b *testing.B,
	emk engineMaker,
	useBatch bool,
	heldOtherTxn bool,
	heldSameTxn bool,
	strength lock.Strength,
	checkFor bool,
) {
	if heldOtherTxn && heldSameTxn {
		b.Fatalf("heldOtherTxn and heldSameTxn cannot both be true")
	}

	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	makeKey := func(i int) roachpb.Key {
		// NOTE: we're appending to a buffer with sufficient capacity, so this does
		// not allocate, but as a result, we need to watch out for aliasing bugs.
		return encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i))
	}
	makeTxn := func(name string) roachpb.Transaction {
		return roachpb.MakeTransaction(name, keyBuf, 0, 0, hlc.Timestamp{WallTime: 1}, 0, 0, 0, false /* omitInRangefeeds */)
	}
	txn1 := makeTxn("txn1")
	txn2 := makeTxn("txn2")

	loc := "acquire_lock"
	if checkFor {
		loc = "check_for_acquire_lock"
	}
	eng := emk(b, loc)
	defer eng.Close()

	for i := 0; i < b.N; i++ {
		key := makeKey(i)
		if heldOtherTxn || heldSameTxn {
			txn := &txn1
			if heldOtherTxn {
				txn = &txn2
			}
			// Acquire a shared and an exclusive lock on the key.
			err := MVCCAcquireLock(ctx, eng, &txn.TxnMeta, txn.IgnoredSeqNums, lock.Shared, key, nil, 0, 0)
			if err != nil {
				b.Fatal(err)
			}
			err = MVCCAcquireLock(ctx, eng, &txn.TxnMeta, txn.IgnoredSeqNums, lock.Exclusive, key, nil, 0, 0)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	rw := ReadWriter(eng)
	if useBatch {
		batch := eng.NewBatch()
		defer batch.Close()
		rw = batch
	}
	ms := &enginepb.MVCCStats{}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := makeKey(i)
		txn := &txn1
		var err error
		if checkFor {
			err = MVCCCheckForAcquireLock(ctx, rw, txn, strength, key, 0, 0)
		} else {
			err = MVCCAcquireLock(ctx, rw, &txn.TxnMeta, txn.IgnoredSeqNums, strength, key, ms, 0, 0)
		}
		if heldOtherTxn {
			if err == nil {
				b.Fatalf("expected error but got %s", err)
			}
		} else if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

type mvccExportToSSTOpts struct {
	numKeys, numRevisions, numRangeKeys                   int
	importEpochs, exportAllRevisions, useElasticCPUHandle bool

	// percentage specifies the share of the dataset to export. 100 will be a full
	// export, disabling the TBI optimization. <100 will be an incremental export
	// with a non-zero StartTS, using a TBI. 0 defaults to 100.
	percentage int
}

func runMVCCExportToSST(b *testing.B, opts mvccExportToSSTOpts) {
	dir, cleanup := testutils.TempDir(b)
	defer cleanup()
	engine := setupMVCCPebble(b, dir)
	defer engine.Close()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	mkKey := func(i int) roachpb.Key {
		var key []byte
		key = append(key, keys.LocalMax...)
		key = append(key, bytes.Repeat([]byte{'a'}, 19)...)
		key = encoding.EncodeUint32Ascending(key, uint32(i))
		return key
	}

	mkWall := func(j int) int64 {
		wt := int64(j + 1)
		return wt
	}

	// Write range keys.
	func() {
		rng := rand.New(rand.NewSource(12345))
		batch := engine.NewBatch()
		defer batch.Close()
		for i := 0; i < opts.numRangeKeys; i++ {
			// NB: regular keys are written at ts 1+, so this is below any of the
			// regular writes and thus won't delete anything.
			ts := hlc.Timestamp{WallTime: 0, Logical: int32(i + 1)}
			start := rng.Intn(opts.numKeys)
			end := start + rng.Intn(opts.numKeys-start) + 1
			// As a special case, if we're only writing one range key, write it across
			// the entire span.
			if opts.numRangeKeys == 1 {
				start = 0
				end = opts.numKeys + 1
			}
			startKey := mkKey(start)
			endKey := mkKey(end)
			err := MVCCDeleteRangeUsingTombstone(
				ctx, batch, nil, startKey, endKey, ts, hlc.ClockTimestamp{}, nil, nil, false, 0, 0, nil)
			if err != nil {
				b.Fatal(err)
			}

		}
		err := batch.Commit(false /* sync */)
		if err != nil {
			b.Fatal(err)
		}
	}()

	batch := engine.NewBatch()
	for i := 0; i < opts.numKeys; i++ {
		key := mkKey(i)

		for j := 0; j < opts.numRevisions; j++ {
			mvccKey := MVCCKey{Key: key, Timestamp: hlc.Timestamp{WallTime: mkWall(j), Logical: 0}}
			mvccValue := MVCCValue{Value: roachpb.MakeValueFromString("foobar")}
			if opts.importEpochs {
				mvccValue.ImportEpoch = 1
			}
			err := batch.PutMVCC(mvccKey, mvccValue)
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

	var startWall int64
	if opts.percentage > 0 && opts.percentage < 100 {
		startWall = mkWall(opts.numRevisions*(100-opts.percentage)/100 - 1) // exclusive
	}
	endWall := mkWall(opts.numRevisions + 1) // see latest revision for every key
	var expKVsInSST int
	if opts.exportAllRevisions {
		// First, compute how many revisions are visible for each key.
		// Could probably use a closed formula for this but this works.
		for i := 0; i < opts.numRevisions; i++ {
			wall := mkWall(i)
			if wall > startWall && wall <= endWall {
				expKVsInSST++
			}
		}
		// Then compute the total.
		expKVsInSST *= opts.numKeys
	} else {
		// See one revision per key.
		expKVsInSST = opts.numKeys
	}

	if opts.useElasticCPUHandle {
		ctx = admission.ContextWithElasticCPUWorkHandle(ctx, admission.TestingNewElasticCPUHandle())
	}

	var buf bytes.Buffer
	buf.Grow(1 << 20)
	b.ResetTimer()
	b.StopTimer()
	var assertLen int // buf.Len shouldn't change between runs
	for i := 0; i < b.N; i++ {
		buf.Reset()
		b.StartTimer()
		startTS := hlc.Timestamp{WallTime: startWall}
		endTS := hlc.Timestamp{WallTime: endWall}
		_, _, err := MVCCExportToSST(ctx, st, engine, MVCCExportOptions{
			StartKey:               MVCCKey{Key: keys.LocalMax},
			EndKey:                 roachpb.KeyMax,
			StartTS:                startTS,
			EndTS:                  endTS,
			ExportAllRevisions:     opts.exportAllRevisions,
			TargetSize:             0,
			MaxSize:                0,
			StopMidKey:             false,
			IncludeMVCCValueHeader: opts.importEpochs,
		}, &buf)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		if i == 0 {
			if buf.Len() == 0 {
				b.Fatalf("empty SST")
			}
			assertLen = buf.Len()
		}

		if buf.Len() != assertLen {
			b.Fatalf("unexpected SST size: %d, expected %d", buf.Len(), assertLen)
		}
	}

	// Run sanity checks on last produced SST.
	it, err := NewMemSSTIterator(
		buf.Bytes(), true /* verify */, IterOptions{
			LowerBound: keys.LocalMax,
			UpperBound: roachpb.KeyMax,
			KeyTypes:   IterKeyTypePointsAndRanges,
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	it.SeekGE(MakeMVCCMetadataKey(roachpb.LocalMax))
	var n int // points
	var r int // range keys (within stacks)
	for {
		ok, err := it.Valid()
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			break
		}
		hasPoint, hasRange := it.HasPointAndRange()
		if hasPoint {
			n++
		}
		if hasRange && it.RangeKeyChanged() {
			r += it.RangeKeys().Len()
		}
		it.Next()
	}
	if expKVsInSST != n {
		b.Fatalf("unexpected number of keys in SST: %d, expected %d", n, expKVsInSST)
	}
	// Should not see any rangedel stacks if startTS is set.
	if opts.numRangeKeys > 0 && startWall == 0 && opts.exportAllRevisions {
		if r < opts.numRangeKeys {
			b.Fatalf("unexpected number of range keys in SST: %d, expected at least %d", r, opts.numRangeKeys)
		}
	} else if r != 0 {
		b.Fatalf("unexpected number of range keys in SST: %d, expected 0", r)
	}
}

func runCheckSSTConflicts(
	b *testing.B, numEngineKeys, numVersions, numSstKeys int, overlap, usePrefixSeek bool,
) {
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	valueBuf := make([]byte, 128)
	for i := range valueBuf {
		valueBuf[i] = 'a'
	}
	value := MVCCValue{Value: roachpb.MakeValueFromBytes(valueBuf)}

	eng := setupMVCCInMemPebble(b, "")
	defer eng.Close()

	for i := 0; i < numEngineKeys; i++ {
		batch := eng.NewBatch()
		for j := 0; j < numVersions; j++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
			ts := hlc.Timestamp{WallTime: int64(j + 1)}
			require.NoError(b, batch.PutMVCC(MVCCKey{Key: key, Timestamp: ts}, value))
		}
		require.NoError(b, batch.Commit(false))
	}
	require.NoError(b, eng.Flush())

	// The engine contains keys numbered key-1, key-2, key-3, etc, while
	// the SST contains keys numbered key-11, key-21, etc., that fit in
	// between the engine keys without colliding.
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	sstFile := &MemObject{}
	sstWriter := MakeIngestionSSTWriter(ctx, st, sstFile)
	var sstStart, sstEnd MVCCKey
	lastKeyNum := -1
	lastKeyCounter := 0
	for i := 0; i < numSstKeys; i++ {
		keyNum := int((float64(i) / float64(numSstKeys)) * float64(numEngineKeys))
		if !overlap {
			keyNum = i + numEngineKeys
		} else if lastKeyNum == keyNum {
			lastKeyCounter++
		} else {
			lastKeyCounter = 0
		}
		key := roachpb.Key(encoding.EncodeUvarintAscending(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(keyNum)), uint64(1+lastKeyCounter)))
		mvccKey := MVCCKey{Key: key, Timestamp: hlc.Timestamp{WallTime: int64(numVersions + 3)}}
		if i == 0 {
			sstStart.Key = append([]byte(nil), mvccKey.Key...)
			sstStart.Timestamp = mvccKey.Timestamp
		} else if i == numSstKeys-1 {
			sstEnd.Key = append([]byte(nil), mvccKey.Key...)
			sstEnd.Timestamp = mvccKey.Timestamp
		}
		require.NoError(b, sstWriter.PutMVCC(mvccKey, value))
		lastKeyNum = keyNum
	}
	sstWriter.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := CheckSSTConflicts(context.Background(), sstFile.Data(), eng, sstStart, sstEnd, sstStart.Key, sstEnd.Key.Next(), hlc.Timestamp{}, hlc.Timestamp{}, math.MaxInt64, 0, usePrefixSeek)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func runSSTIterator(b *testing.B, numKeys int, verify bool) {
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	value := MVCCValue{Value: roachpb.MakeValueFromBytes(bytes.Repeat([]byte("a"), 128))}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	sstFile := &MemObject{}
	sstWriter := MakeIngestionSSTWriter(ctx, st, sstFile)

	for i := 0; i < numKeys; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		mvccKey := MVCCKey{Key: key, Timestamp: hlc.Timestamp{WallTime: 1}}
		require.NoError(b, sstWriter.PutMVCC(mvccKey, value))
	}
	sstWriter.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := NewMemSSTIterator(sstFile.Bytes(), verify, IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: keys.MinKey,
			UpperBound: keys.MaxKey,
		})
		if err != nil {
			b.Fatal(err)
		}
		for iter.SeekGE(NilKey); ; iter.Next() {
			if ok, err := iter.Valid(); err != nil {
				b.Fatal(err)
			} else if !ok {
				iter.Close()
				break
			}
			_ = iter.UnsafeKey()
			_, _ = iter.UnsafeValue()
		}
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
	buf, err = rowenc.EncodeColumns(
		[]descpb.ColumnID{0}, nil /* directions */, benchRowColMap,
		[]tree.Datum{tree.NewDInt(tree.DInt(id))}, buf)
	if err != nil {
		// conditionally check this, for performance
		require.NoError(b, err)
	}
	return keys.MakeFamilyKey(buf, columnFamily)
}

// Benchmark with 7 levels (these are L0 sub-levels, but are similar to
// normal levels in using levelIter inside Pebble) each with one file. 1000 roachpb.Keys
// with the first 10 keys having 6 versions, and the remaining with 1 version.
// Each key is Put using a transactional write, so an intent is written too.
// The lowest level has only the intents and corresponding provisional value.
// The next higher level has the intent resolution of the next lower level,
// and its own Puts (i.e., intents and provisional value). This means all
// SingleDelete, Set pairs for the intents are separated into 2 files (in 2
// levels). And the first 10 keys with 6 versions have each of their versions
// in separate files (in separate levels). Each iteration is a MVCCScan over
// all these keys reading at a timestamp higher than the latest version.
//
// This benchmark is intended to behave akin to a real LSM with many levels,
// where the intent have been deleted but the deletes have not been compacted
// away. See #96361 for more motivation.
func BenchmarkMVCCScannerWithIntentsAndVersions(b *testing.B) {
	skip.UnderShort(b, "setting up takes too long")
	defer log.Scope(b).Close(b)

	st := cluster.MakeTestingClusterSettings()
	ctx := context.Background()
	eng, err := Open(ctx, InMemory(), st, CacheSize(testCacheSize),
		func(cfg *engineConfig) error {
			cfg.opts.DisableAutomaticCompactions = true
			return nil
		})
	require.NoError(b, err)
	defer eng.Close()
	value := roachpb.MakeValueFromString("value")
	numVersions := 6
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
	const totalNumKeys = 1000
	var prevTxn roachpb.Transaction
	var numPrevKeys int
	for i := 1; i <= numVersions+1; i++ {
		lockUpdate := roachpb.LockUpdate{
			Txn:    prevTxn.TxnMeta,
			Status: roachpb.COMMITTED,
		}
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
		prevTxn = txn
		batch := eng.NewBatch()
		// Resolve the previous intents.
		for j := 0; j < numPrevKeys; j++ {
			key := makeKey(nil, j)
			lu := lockUpdate
			lu.Key = key
			found, _, _, _, err := MVCCResolveWriteIntent(
				ctx, batch, nil, lu, MVCCResolveWriteIntentOptions{})
			require.Equal(b, true, found)
			require.NoError(b, err)
		}
		numKeys := totalNumKeys
		if i == numVersions+1 {
			numKeys = 0
		} else if i != 1 {
			numKeys = 10
		}
		// Put the keys for this iteration.
		for j := 0; j < numKeys; j++ {
			key := makeKey(nil, j)
			_, err := MVCCPut(ctx, batch, key, ts, value, MVCCWriteOptions{Txn: &txn})
			require.NoError(b, err)
		}
		numPrevKeys = numKeys
		// Read the keys from the Batch and write them to a sstable to ingest.
		reader := batch.(*pebbleBatch).batch.Reader()
		kind, key, value, ok, err := reader.Next()
		if err != nil {
			b.Fatal(err)
		}
		type kvPair struct {
			key   []byte
			kind  pebble.InternalKeyKind
			value []byte
		}
		var kvPairs []kvPair
		for ; ok; kind, key, value, ok, err = reader.Next() {
			kvPairs = append(kvPairs, kvPair{key: key, kind: kind, value: value})
		}
		if err != nil {
			b.Fatal(err)
		}
		sort.Slice(kvPairs, func(i, j int) bool {
			cmp := EngineComparer.Compare(kvPairs[i].key, kvPairs[j].key)
			if cmp == 0 {
				// Should not happen since we resolve in a different batch from the
				// one where we wrote the intent.
				b.Fatalf("found equal user keys in same batch")
			}
			return cmp < 0
		})
		sstFileName := fmt.Sprintf("tmp-ingest-%d", i)
		sstFile, err := eng.Env().Create(sstFileName, fs.UnspecifiedWriteCategory)
		require.NoError(b, err)
		// No improvement with v3 since the multiple versions are in different
		// files.
		format := sstable.TableFormatPebblev2
		opts := DefaultPebbleOptions().MakeWriterOptions(0, format)
		writer := sstable.NewWriter(objstorageprovider.NewFileWritable(sstFile), opts)
		for _, kv := range kvPairs {
			require.NoError(b, writer.Raw().AddWithForceObsolete(
				pebble.MakeInternalKey(kv.key, 0 /* seqNum */, kv.kind), kv.value, false /* forceObsolete */))
		}
		require.NoError(b, writer.Close())
		batch.Close()
		require.NoError(b, eng.IngestLocalFiles(ctx, []string{sstFileName}))
	}
	for i := 0; i < b.N; i++ {
		ro := eng.NewReader(StandardDurability)
		ts := hlc.Timestamp{WallTime: int64(numVersions) + 5}
		startKey := makeKey(nil, 0)
		endKey := makeKey(nil, totalNumKeys+1)
		iter, err := newMVCCIterator(
			ctx, ro, ts, false, false, IterOptions{
				KeyTypes:   IterKeyTypePointsAndRanges,
				LowerBound: startKey,
				UpperBound: endKey,
			},
		)
		if err != nil {
			b.Fatal(err)
		}
		res, err := mvccScanToKvs(ctx, iter, startKey, endKey,
			hlc.Timestamp{WallTime: int64(numVersions) + 5}, MVCCScanOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if res.NumKeys != totalNumKeys {
			b.Fatalf("expected %d keys, and found %d", totalNumKeys, res.NumKeys)
		}
		if i == 0 {
			// This is to understand the results.
			stats := iter.Stats()
			fmt.Printf("stats: %s\n", stats.Stats.String())
		}
		iter.Close()
		ro.Close()
	}
}

func BenchmarkMVCCBlindPut(b *testing.B) {
	defer log.Scope(b).Close(b)

	valueSizes := []int{10, 100, 1000, 10000}
	if testing.Short() {
		valueSizes = []int{10, 10000}
	}

	for _, valueSize := range valueSizes {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			ctx := context.Background()
			runMVCCBlindPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCConditionalPut(b *testing.B) {
	defer log.Scope(b).Close(b)

	valueSizes := []int{10, 100, 1000, 10000}
	if testing.Short() {
		valueSizes = []int{10, 10000}
	}

	for _, createFirst := range []bool{false, true} {
		prefix := "Create"
		if createFirst {
			prefix = "Replace"
		}
		b.Run(prefix, func(b *testing.B) {
			for _, valueSize := range valueSizes {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					ctx := context.Background()
					runMVCCConditionalPut(ctx, b, setupMVCCInMemPebble, valueSize, createFirst)
				})
			}
		})
	}
}

func BenchmarkMVCCBlindConditionalPut(b *testing.B) {
	defer log.Scope(b).Close(b)

	valueSizes := []int{10, 100, 1000, 10000}
	if testing.Short() {
		valueSizes = []int{10, 10000}
	}

	for _, valueSize := range valueSizes {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			ctx := context.Background()
			runMVCCBlindConditionalPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCInitPut(b *testing.B) {
	defer log.Scope(b).Close(b)

	valueSizes := []int{10, 100, 1000, 10000}
	if testing.Short() {
		valueSizes = []int{10, 10000}
	}

	for _, valueSize := range valueSizes {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			ctx := context.Background()
			runMVCCInitPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCBlindInitPut(b *testing.B) {
	defer log.Scope(b).Close(b)

	valueSizes := []int{10, 100, 1000, 10000}
	if testing.Short() {
		valueSizes = []int{10, 10000}
	}

	for _, valueSize := range valueSizes {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			ctx := context.Background()
			runMVCCBlindInitPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCPutDelete(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	db := setupMVCCInMemPebble(b, "put_delete")
	defer db.Close()

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(r, 10))
	var blockNum int64

	for i := 0; i < b.N; i++ {
		blockID := r.Int63()
		blockNum++
		key := encoding.EncodeVarintAscending(nil, blockID)
		key = encoding.EncodeVarintAscending(key, blockNum)

		if _, err := MVCCPut(ctx, db, key, hlc.Timestamp{}, value, MVCCWriteOptions{}); err != nil {
			b.Fatal(err)
		}
		if _, _, err := MVCCDelete(ctx, db, key, hlc.Timestamp{}, MVCCWriteOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMVCCBatchPut(b *testing.B) {
	defer log.Scope(b).Close(b)

	batchSizes := []int{10, 100, 1000, 10000}
	if testing.Short() {
		batchSizes = []int{10, 10000}
	}

	for _, valueSize := range []int{10} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			for _, batchSize := range batchSizes {
				b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
					ctx := context.Background()
					runMVCCBatchPut(ctx, b, setupMVCCInMemPebble, valueSize, batchSize)
				})
			}
		})
	}
}

func BenchmarkMVCCBatchTimeSeries(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, batchSize := range []int{282} {
		b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
			runMVCCBatchTimeSeries(ctx, b, setupMVCCInMemPebble, batchSize)
		})
	}
}

// BenchmarkMVCCGetMergedTimeSeries computes performance of reading merged
// time series data using `MVCCGet()`. Uses an in-memory engine.
func BenchmarkMVCCGetMergedTimeSeries(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testCase struct {
		numKeys      int
		mergesPerKey int
	}
	var testCases []testCase
	for _, numKeys := range []int{1, 16, 256} {
		for _, mergesPerKey := range []int{1, 16, 256} {
			testCases = append(testCases, testCase{
				numKeys:      numKeys,
				mergesPerKey: mergesPerKey,
			})
		}
	}

	if testing.Short() {
		// Choose a configuration for the short version.
		testCases = []testCase{
			{numKeys: 16, mergesPerKey: 16},
		}
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("numKeys=%d/mergesPerKey=%d", tc.numKeys, tc.mergesPerKey)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			runMVCCGetMergedValue(ctx, b, setupMVCCInMemPebble, tc.numKeys, tc.mergesPerKey)
		})
	}
}

// DeleteRange benchmarks below (using on-disk data).
//
// TODO(peter): Benchmark{MVCCDeleteRange,ClearRange,ClearIterRange}
// give nonsensical results (DeleteRange is absurdly slow and ClearRange
// reports a processing speed of 481 million MB/s!). We need to take a look at
// what these benchmarks are trying to measure, and fix them.

func BenchmarkMVCCDeleteRange(b *testing.B) {
	// TODO(radu): run one configuration under Short once the above TODO is
	// resolved.
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCDeleteRange(ctx, b, valueSize)
		})
	}
}

func BenchmarkMVCCDeleteRangeUsingTombstone(b *testing.B) {
	// TODO(radu): run one configuration under Short once the above TODO is
	// resolved.
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, numKeys := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
			for _, valueSize := range []int{64} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					for _, entireRange := range []bool{false, true} {
						b.Run(fmt.Sprintf("entireRange=%t", entireRange), func(b *testing.B) {
							runMVCCDeleteRangeUsingTombstone(ctx, b, numKeys, valueSize, entireRange)
						})
					}
				})
			}
		})
	}
}

// BenchmarkMVCCDeleteRangeWithPredicate benchmarks predicate based
// delete range under certain configs. A lower streak bound simulates sequential
// imports with more interspersed keys, leading to fewer range tombstones and
// more point tombstones.
func BenchmarkMVCCDeleteRangeWithPredicate(b *testing.B) {
	// TODO(radu): run one configuration under Short once the above TODO is
	// resolved.
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, streakBound := range []int{10, 100, 200, 500} {
		b.Run(fmt.Sprintf("streakBound=%d", streakBound), func(b *testing.B) {
			for _, rangeKeyThreshold := range []int64{64} {
				b.Run(fmt.Sprintf("rangeKeyThreshold=%d", rangeKeyThreshold), func(b *testing.B) {
					config := mvccImportedData{
						streakBound: streakBound,
						keyCount:    2000,
						valueBytes:  64,
						layers:      2,
					}
					runMVCCDeleteRangeWithPredicate(ctx, b, config, 0, rangeKeyThreshold)
				})
			}
		})
	}
}

func BenchmarkClearMVCCVersions(b *testing.B) {
	// TODO(radu): run one configuration under Short once the above TODO is
	// resolved.
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	runClearRange(ctx, b, func(eng Engine, batch Batch, start, end MVCCKey) error {
		return batch.ClearMVCCVersions(start, end)
	})
}

func BenchmarkClearMVCCIteratorRange(b *testing.B) {
	ctx := context.Background()
	defer log.Scope(b).Close(b)
	runClearRange(ctx, b, func(eng Engine, batch Batch, start, end MVCCKey) error {
		return batch.ClearMVCCIteratorRange(start.Key, end.Key, true, true)
	})
}

func BenchmarkBatchApplyBatchRepr(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testCase struct {
		indexed    bool
		sequential bool
		valueSize  int
		batchSize  int
	}
	var testCases []testCase

	for _, indexed := range []bool{false, true} {
		for _, sequential := range []bool{false, true} {
			for _, valueSize := range []int{10} {
				for _, batchSize := range []int{10000} {
					testCases = append(testCases, testCase{
						indexed:    indexed,
						sequential: sequential,
						valueSize:  valueSize,
						batchSize:  batchSize,
					})
				}
			}
		}
	}

	if testing.Short() {
		// Choose a configuration for the short version.
		testCases = []testCase{
			{indexed: true, sequential: false, valueSize: 10, batchSize: 8},
		}
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"indexed=%t/seq=%t/valueSize=%d/batchSize=%d",
			tc.indexed, tc.sequential, tc.valueSize, tc.batchSize,
		)

		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			runBatchApplyBatchRepr(ctx, b, setupMVCCInMemPebble,
				tc.indexed, tc.sequential, tc.valueSize, tc.batchSize)
		})
	}
}

type acquireLockTestCase struct {
	batch        bool
	heldOtherTxn bool
	heldSameTxn  bool
	strength     lock.Strength
}

func (tc acquireLockTestCase) name() string {
	return fmt.Sprintf(
		"batch=%t/heldOtherTxn=%t/heldSameTxn=%t/strength=%s",
		tc.batch, tc.heldOtherTxn, tc.heldSameTxn, tc.strength,
	)
}

func acquireLockTestCases() []acquireLockTestCase {
	var res []acquireLockTestCase
	for _, batch := range []bool{false, true} {
		for _, heldOtherTxn := range []bool{false, true} {
			for _, heldSameTxn := range []bool{false, true} {
				if heldOtherTxn && heldSameTxn {
					continue // not possible
				}
				for _, strength := range []lock.Strength{lock.Shared, lock.Exclusive} {
					res = append(res, acquireLockTestCase{
						batch:        batch,
						heldOtherTxn: heldOtherTxn,
						heldSameTxn:  heldSameTxn,
						strength:     strength,
					})
				}
			}
		}
	}
	return res
}

func BenchmarkMVCCCheckForAcquireLock(b *testing.B) {
	defer log.Scope(b).Close(b)

	for _, tc := range acquireLockTestCases() {
		b.Run(tc.name(), func(b *testing.B) {
			ctx := context.Background()
			runMVCCCheckForAcquireLock(ctx, b, setupMVCCInMemPebble, tc.batch, tc.heldOtherTxn, tc.heldSameTxn, tc.strength)
		})
	}
}

func BenchmarkMVCCAcquireLock(b *testing.B) {
	defer log.Scope(b).Close(b)

	for _, tc := range acquireLockTestCases() {
		b.Run(tc.name(), func(b *testing.B) {
			ctx := context.Background()
			runMVCCAcquireLock(ctx, b, setupMVCCInMemPebble, tc.batch, tc.heldOtherTxn, tc.heldSameTxn, tc.strength)
		})
	}
}

func BenchmarkBatchBuilderPut(b *testing.B) {
	defer log.Scope(b).Close(b)
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := setupMVCCInMemPebble(b, "")
	defer eng.Close()
	batch := eng.NewBatch()

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(j)))
			ts := hlc.Timestamp{WallTime: int64(j + 1)} // j+1 to avoid zero timestamp
			err := batch.PutMVCC(MVCCKey{key, ts}, MVCCValue{Value: roachpb.MakeValueFromBytes(value)})
			if err != nil {
				b.Fatal(err)
			}
		}
		batch.Close()
		batch = eng.NewBatch()
	}

	b.StopTimer()
}

func BenchmarkCheckSSTConflicts(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testCase struct {
		numKeys       int
		numSSTKeys    int
		overlap       bool
		usePrefixSeek bool
	}
	var testCases []testCase

	for _, numKeys := range []int{1000, 10000, 100000} {
		for _, numSSTKeys := range []int{10, 100, 1000, 10000, 100000} {
			for _, overlap := range []bool{false, true} {
				for _, usePrefixSeek := range []bool{false, true} {
					testCases = append(testCases, testCase{
						numKeys:       numKeys,
						numSSTKeys:    numSSTKeys,
						overlap:       overlap,
						usePrefixSeek: usePrefixSeek,
					})
				}
			}
		}
	}

	if testing.Short() {
		// Choose a few configurations for the short version.
		testCases = []testCase{
			{numKeys: 10000, numSSTKeys: 100, overlap: false, usePrefixSeek: false},
			{numKeys: 10000, numSSTKeys: 1000, overlap: true, usePrefixSeek: true},
		}
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"keys=%d/sstKeys=%d/overlap=%t/usePrefixSeek=%v",
			tc.numKeys, tc.numSSTKeys, tc.overlap, tc.usePrefixSeek,
		)
		b.Run(name, func(b *testing.B) {
			runCheckSSTConflicts(b, tc.numKeys, 1 /* numVersions */, tc.numSSTKeys, tc.overlap, tc.usePrefixSeek)
		})
	}
}

func BenchmarkSSTIterator(b *testing.B) {
	defer log.Scope(b).Close(b)

	for _, numKeys := range []int{1, 100, 10000} {
		b.Run(fmt.Sprintf("keys=%d", numKeys), func(b *testing.B) {
			for _, verify := range []bool{false, true} {
				b.Run(fmt.Sprintf("verify=%t", verify), func(b *testing.B) {
					runSSTIterator(b, numKeys, verify)
				})
			}
		})
	}
}

const testCacheSize = 1 << 30 // 1 GB

func setupMVCCPebble(b testing.TB, dir string) Engine {
	peb, err := Open(
		context.Background(),
		fs.MustInitPhysicalTestingEnv(dir),
		cluster.MakeTestingClusterSettings(),
		CacheSize(testCacheSize))
	if err != nil {
		b.Fatalf("could not create new pebble instance at %s: %+v", dir, err)
	}
	return peb
}

func setupMVCCInMemPebble(b testing.TB, loc string) Engine {
	return setupMVCCInMemPebbleWithSeparatedIntents(b)
}

func setupMVCCInMemPebbleWithSeparatedIntents(b testing.TB) Engine {
	peb, err := Open(
		context.Background(),
		InMemory(),
		cluster.MakeClusterSettings(),
		CacheSize(testCacheSize))
	if err != nil {
		b.Fatalf("could not create new in-mem pebble instance: %+v", err)
	}
	return peb
}

func setupPebbleInMemPebbleForLatestRelease(b testing.TB, _ string) Engine {
	ctx := context.Background()
	s := cluster.MakeClusterSettings()
	if err := clusterversion.Initialize(ctx, clusterversion.Latest.Version(),
		&s.SV); err != nil {
		b.Fatalf("failed to set current cluster version: %+v", err)
	}

	peb, err := Open(ctx, InMemory(), s, CacheSize(testCacheSize))
	if err != nil {
		b.Fatalf("could not create new in-mem pebble instance: %+v", err)
	}
	return peb
}
