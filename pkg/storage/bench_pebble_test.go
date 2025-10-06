// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

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

func BenchmarkMVCCScan_Pebble(b *testing.B) {
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

func BenchmarkMVCCScanGarbage_Pebble(b *testing.B) {
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

func BenchmarkMVCCScanSQLRows_Pebble(b *testing.B) {
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

func BenchmarkMVCCReverseScan_Pebble(b *testing.B) {
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

func BenchmarkMVCCScanTransactionalData_Pebble(b *testing.B) {
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

func BenchmarkMVCCGet_Pebble(b *testing.B) {
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

func BenchmarkMVCCComputeStats_Pebble(b *testing.B) {
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

func BenchmarkMVCCFindSplitKey_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	for _, valueSize := range []int{32} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			ctx := context.Background()
			runMVCCFindSplitKey(ctx, b, valueSize)
		})
	}
}

func BenchmarkMVCCPut_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testCase struct {
		valueSize int
		versions  int
	}
	var testCases []testCase

	for _, valueSize := range []int{10, 100, 1000, 10000} {
		for _, versions := range []int{1, 10} {
			testCases = append(testCases, testCase{
				valueSize: valueSize,
				versions:  versions,
			})
		}
	}

	if testing.Short() {
		// Choose a few configurations for the short version.
		testCases = []testCase{
			{valueSize: 10, versions: 1},
			{valueSize: 1000, versions: 10},
		}
	}

	for _, tc := range testCases {
		// We use "batch=false" so that we can compare with corresponding benchmarks in older branches.
		name := fmt.Sprintf("batch=false/valueSize=%d/versions=%d", tc.valueSize, tc.versions)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			runMVCCPut(ctx, b, setupMVCCInMemPebble, tc.valueSize, tc.versions)
		})
	}
}

func BenchmarkMVCCBlindPut_Pebble(b *testing.B) {
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

func BenchmarkMVCCConditionalPut_Pebble(b *testing.B) {
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

func BenchmarkMVCCBlindConditionalPut_Pebble(b *testing.B) {
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

func BenchmarkMVCCInitPut_Pebble(b *testing.B) {
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

func BenchmarkMVCCBlindInitPut_Pebble(b *testing.B) {
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

func BenchmarkMVCCPutDelete_Pebble(b *testing.B) {
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

func BenchmarkMVCCBatchPut_Pebble(b *testing.B) {
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

func BenchmarkMVCCBatchTimeSeries_Pebble(b *testing.B) {
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
func BenchmarkMVCCGetMergedTimeSeries_Pebble(b *testing.B) {
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
// TODO(peter): Benchmark{MVCCDeleteRange,ClearRange,ClearIterRange}_Pebble
// give nonsensical results (DeleteRange is absurdly slow and ClearRange
// reports a processing speed of 481 million MB/s!). We need to take a look at
// what these benchmarks are trying to measure, and fix them.

func BenchmarkMVCCDeleteRange_Pebble(b *testing.B) {
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

func BenchmarkMVCCDeleteRangeUsingTombstone_Pebble(b *testing.B) {
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

// BenchmarkMVCCDeleteRangeWithPredicate_Pebble benchmarks predicate based
// delete range under certain configs. A lower streak bound simulates sequential
// imports with more interspersed keys, leading to fewer range tombstones and
// more point tombstones.
func BenchmarkMVCCDeleteRangeWithPredicate_Pebble(b *testing.B) {
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

func BenchmarkClearMVCCVersions_Pebble(b *testing.B) {
	// TODO(radu): run one configuration under Short once the above TODO is
	// resolved.
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	runClearRange(ctx, b, func(eng Engine, batch Batch, start, end MVCCKey) error {
		return batch.ClearMVCCVersions(start, end)
	})
}

func BenchmarkClearMVCCIteratorRange_Pebble(b *testing.B) {
	ctx := context.Background()
	defer log.Scope(b).Close(b)
	runClearRange(ctx, b, func(eng Engine, batch Batch, start, end MVCCKey) error {
		return batch.ClearMVCCIteratorRange(start.Key, end.Key, true, true)
	})
}

func BenchmarkBatchApplyBatchRepr_Pebble(b *testing.B) {
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

func BenchmarkMVCCCheckForAcquireLock_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)

	for _, tc := range acquireLockTestCases() {
		b.Run(tc.name(), func(b *testing.B) {
			ctx := context.Background()
			runMVCCCheckForAcquireLock(ctx, b, setupMVCCInMemPebble, tc.batch, tc.heldOtherTxn, tc.heldSameTxn, tc.strength)
		})
	}
}

func BenchmarkMVCCAcquireLock_Pebble(b *testing.B) {
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

	b.ResetTimer()

	const batchSize = 1000
	var batch pebble.Batch
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(j)))
			ts := hlc.Timestamp{WallTime: int64(j)}
			require.NoError(b, batch.Set(EncodeMVCCKey(MVCCKey{key, ts}), value, nil /* WriteOptions */))
		}
		batch.Reset()
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
