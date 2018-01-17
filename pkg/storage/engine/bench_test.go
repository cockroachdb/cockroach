// Copyright 2014 The Cockroach Authors.
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

package engine

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const overhead = 48 // Per key/value overhead (empirically determined)

type engineMaker func(testing.TB, string) Engine

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
// The creation of the database is time consuming, especially for larger
// numbers of versions. The database is persisted between runs and stored in
// the current directory as "mvcc_scan_<versions>_<keys>_<valueBytes>" (which
// is also returned).
func setupMVCCData(
	emk engineMaker, numVersions, numKeys, valueBytes int, b *testing.B,
) (Engine, string) {
	loc := fmt.Sprintf("mvcc_data_%d_%d_%d", numVersions, numKeys, valueBytes)

	exists := true
	if _, err := os.Stat(loc); os.IsNotExist(err) {
		exists = false
	} else if err != nil {
		b.Fatal(err)
	}

	eng := emk(b, loc)

	if exists {
		testutils.ReadAllFiles(filepath.Join(loc, "*"))
		return eng, loc
	}

	log.Infof(context.Background(), "creating mvcc data: %s", loc)

	// Generate the same data every time.
	rng := rand.New(rand.NewSource(1449168817))

	keys := make([]roachpb.Key, numKeys)
	var order []int
	for i := 0; i < numKeys; i++ {
		keys[i] = roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(i)))
		keyVersions := rng.Intn(numVersions) + 1
		for j := 0; j < keyVersions; j++ {
			order = append(order, i)
		}
	}

	// Randomize the order in which the keys are written.
	for i, n := 0, len(order); i < n-1; i++ {
		j := i + rng.Intn(n-i)
		order[i], order[j] = order[j], order[i]
	}

	counts := make([]int, numKeys)
	batch := eng.NewBatch()
	for i, idx := range order {
		// Output the keys in ~20 batches. If we used a single batch to output all
		// of the keys rocksdb would create a single sstable. We want multiple
		// sstables in order to exercise filtering of which sstables are examined
		// during iterator seeking. We fix the number of batches we output so that
		// optimizations which change the data size result in the same number of
		// sstables.
		if scaled := len(order) / 20; i > 0 && (i%scaled) == 0 {
			log.Infof(context.Background(), "committing (%d/~%d)", i/scaled, 20)
			if err := batch.Commit(false /* sync */); err != nil {
				b.Fatal(err)
			}
			batch.Close()
			batch = eng.NewBatch()
			if err := eng.Flush(); err != nil {
				b.Fatal(err)
			}
		}

		key := keys[idx]
		ts := hlc.Timestamp{WallTime: int64(counts[idx]+1) * 5}
		counts[idx]++
		value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueBytes))
		value.InitChecksum(key)
		if err := MVCCPut(context.Background(), batch, nil, key, ts, value, nil); err != nil {
			b.Fatal(err)
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

// runMVCCScan first creates test data (and resets the benchmarking
// timer). It then performs b.N MVCCScans in increments of numRows
// keys over all of the data in the Engine instance, restarting at
// the beginning of the keyspace, as many times as necessary.
func runMVCCScan(emk engineMaker, numRows, numVersions, valueSize int, reverse bool, b *testing.B) {
	// Use the same number of keys for all of the mvcc scan
	// benchmarks. Using a different number of keys per test gives
	// preferential treatment to tests with fewer keys. Note that the
	// datasets all fit in cache and the cache is pre-warmed.
	const numKeys = 100000

	eng, _ := setupMVCCData(emk, numVersions, numKeys, valueSize, b)
	defer eng.Close()

	{
		// Pull all of the sstables into the RocksDB cache in order to make the
		// timings more stable. Otherwise, the first run will be penalized pulling
		// data into the cache while later runs will not.
		iter := eng.NewIterator(false)
		_, _ = iter.ComputeStats(MakeMVCCMetadataKey(roachpb.KeyMin), MakeMVCCMetadataKey(roachpb.KeyMax), 0)
		iter.Close()
	}

	scan := MVCCScan
	if reverse {
		scan = MVCCReverseScan
	}
	b.SetBytes(int64(numRows * valueSize))
	b.ResetTimer()

	startKeyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	endKeyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	for i := 0; i < b.N; i++ {
		// Choose a random key to start scan.
		keyIdx := rand.Int31n(int32(numKeys - numRows))
		startKey := roachpb.Key(encoding.EncodeUvarintAscending(startKeyBuf[:4], uint64(keyIdx)))
		endKey := roachpb.Key(encoding.EncodeUvarintAscending(endKeyBuf[:4], uint64(keyIdx+int32(numRows)-1)))
		endKey = endKey.Next()
		walltime := int64(5 * (rand.Int31n(int32(numVersions)) + 1))
		ts := hlc.Timestamp{WallTime: walltime}
		kvs, _, _, err := scan(context.Background(), eng, startKey, endKey, int64(numRows), ts, true, nil)
		if err != nil {
			b.Fatalf("failed scan: %s", err)
		}
		if len(kvs) != numRows {
			b.Fatalf("failed to scan: %d != %d", len(kvs), numRows)
		}
	}

	b.StopTimer()
}

// runMVCCGet first creates test data (and resets the benchmarking
// timer). It then performs b.N MVCCGets.
func runMVCCGet(emk engineMaker, numVersions, valueSize int, b *testing.B) {
	// Use the same number of keys for all of the mvcc scan
	// benchmarks. Using a different number of keys per test gives
	// preferential treatment to tests with fewer keys. Note that the
	// datasets all fit in cache and the cache is pre-warmed.
	const numKeys = 100000

	eng, _ := setupMVCCData(emk, numVersions, numKeys, valueSize, b)
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	for i := 0; i < b.N; i++ {
		// Choose a random key to retrieve.
		keyIdx := rand.Int31n(int32(numKeys))
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(keyIdx)))
		walltime := int64(5 * (rand.Int31n(int32(numVersions)) + 1))
		ts := hlc.Timestamp{WallTime: walltime}
		if v, _, err := MVCCGet(context.Background(), eng, key, ts, true, nil); err != nil {
			b.Fatalf("failed get: %s", err)
		} else if v == nil {
			b.Fatalf("failed get (key not found): %d@%d", keyIdx, walltime)
		} else if valueBytes, err := v.GetBytes(); err != nil {
			b.Fatal(err)
		} else if len(valueBytes) != valueSize {
			b.Fatalf("unexpected value size: %d", len(valueBytes))
		}
	}

	b.StopTimer()
}

func runMVCCPut(emk engineMaker, valueSize int, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("put_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCPut(context.Background(), eng, nil, key, ts, value, nil); err != nil {
			b.Fatalf("failed put: %s", err)
		}
	}

	b.StopTimer()
}

func runMVCCBlindPut(emk engineMaker, valueSize int, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("put_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCBlindPut(context.Background(), eng, nil, key, ts, value, nil); err != nil {
			b.Fatalf("failed put: %s", err)
		}
	}

	b.StopTimer()
}

func runMVCCConditionalPut(emk engineMaker, valueSize int, createFirst bool, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("cput_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	var expected *roachpb.Value
	if createFirst {
		for i := 0; i < b.N; i++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
			ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			if err := MVCCPut(context.Background(), eng, nil, key, ts, value, nil); err != nil {
				b.Fatalf("failed put: %s", err)
			}
		}
		expected = &value
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCConditionalPut(context.Background(), eng, nil, key, ts, value, expected, nil); err != nil {
			b.Fatalf("failed put: %s", err)
		}
	}

	b.StopTimer()
}

func runMVCCBlindConditionalPut(emk engineMaker, valueSize int, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("cput_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCBlindConditionalPut(context.Background(), eng, nil, key, ts, value, nil, nil); err != nil {
			b.Fatalf("failed put: %s", err)
		}
	}

	b.StopTimer()
}

func runMVCCInitPut(emk engineMaker, valueSize int, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("iput_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCInitPut(context.Background(), eng, nil, key, ts, value, false, nil); err != nil {
			b.Fatalf("failed put: %s", err)
		}
	}

	b.StopTimer()
}

func runMVCCBlindInitPut(emk engineMaker, valueSize int, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("iput_%d", valueSize))
	defer eng.Close()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if err := MVCCBlindInitPut(context.Background(), eng, nil, key, ts, value, false, nil); err != nil {
			b.Fatalf("failed put: %s", err)
		}
	}

	b.StopTimer()
}

func runMVCCBatchPut(emk engineMaker, valueSize, batchSize int, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
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
			if err := MVCCPut(context.Background(), batch, nil, key, ts, value, nil); err != nil {
				b.Fatalf("failed put: %s", err)
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
func runMVCCBatchTimeSeries(emk engineMaker, batchSize int, b *testing.B) {
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
			if err := MVCCMerge(context.Background(), batch, nil, keys[j], ts, value); err != nil {
				b.Fatalf("failed put: %s", err)
			}
		}

		if err := batch.Commit(false /* sync */); err != nil {
			b.Fatal(err)
		}
		batch.Close()
	}

	b.StopTimer()
}

// runMVCCMerge merges value into numKeys separate keys.
func runMVCCMerge(emk engineMaker, value *roachpb.Value, numKeys int, b *testing.B) {
	eng := emk(b, fmt.Sprintf("merge_%d", numKeys))
	defer eng.Close()

	// Precompute keys so we don't waste time formatting them at each iteration.
	keys := make([]roachpb.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = roachpb.Key(fmt.Sprintf("key-%d", i))
	}

	b.ResetTimer()

	ts := hlc.Timestamp{}
	// Use parallelism if specified when test is run.
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ms := enginepb.MVCCStats{}
			ts.Logical++
			err := MVCCMerge(context.Background(), eng, &ms, keys[rand.Intn(numKeys)], ts, *value)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Read values out to force merge.
	for _, key := range keys {
		val, _, err := MVCCGet(context.Background(), eng, key, hlc.Timestamp{}, true, nil)
		if err != nil {
			b.Fatal(err)
		} else if val == nil {
			continue
		}
	}

	b.StopTimer()
}

// BenchmarkMVCCMergeTimeSeries computes performance of merging time series data.
// Uses an in-memory engine.
func BenchmarkMVCCMergeTimeSeries_RocksDB(b *testing.B) {
	ts := &roachpb.InternalTimeSeriesData{
		StartTimestampNanos: 0,
		SampleDurationNanos: 1000,
		Samples: []roachpb.InternalTimeSeriesSample{
			{Offset: 0, Count: 1, Sum: 5.0},
		},
	}
	var value roachpb.Value
	if err := value.SetProto(ts); err != nil {
		b.Fatal(err)
	}
	runMVCCMerge(setupMVCCInMemRocksDB, &value, 1024, b)
}

func copyDir(from, to string) error {
	return filepath.Walk(from, func(srcPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		destPath := strings.Replace(srcPath, from, to, 1)
		if info.IsDir() {
			return os.MkdirAll(destPath, info.Mode())
		}
		src, err := os.Open(srcPath)
		if err != nil {
			return err
		}
		defer src.Close()
		dest, err := os.Create(destPath)
		if err != nil {
			return err
		}
		defer dest.Close()
		if _, err := io.Copy(dest, src); err != nil {
			return err
		}
		return dest.Sync()
	})
}

func runMVCCDeleteRange(emk engineMaker, valueBytes int, b *testing.B) {
	// 512 KB ranges so the benchmark doesn't take forever
	const rangeBytes = 512 * 1024
	numKeys := rangeBytes / (overhead + valueBytes)
	eng, dir := setupMVCCData(emk, 1, numKeys, valueBytes, b)
	eng.Close()

	b.SetBytes(rangeBytes)
	b.StopTimer()
	b.ResetTimer()

	locDirty := dir + "_dirty"

	for i := 0; i < b.N; i++ {
		if err := os.RemoveAll(locDirty); err != nil {
			b.Fatal(err)
		}
		if err := copyDir(dir, locDirty); err != nil {
			b.Fatal(err)
		}
		func() {
			eng := emk(b, locDirty)
			defer eng.Close()

			b.StartTimer()
			if _, _, _, err := MVCCDeleteRange(
				context.Background(),
				eng,
				&enginepb.MVCCStats{},
				roachpb.KeyMin,
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

// runMVCCComputeStats benchmarks computing MVCC stats on a 64MB range of data.
func runMVCCComputeStats(emk engineMaker, valueBytes int, b *testing.B) {
	const rangeBytes = 64 * 1024 * 1024
	numKeys := rangeBytes / (overhead + valueBytes)
	eng, _ := setupMVCCData(emk, 1, numKeys, valueBytes, b)
	defer eng.Close()

	b.SetBytes(rangeBytes)
	b.ResetTimer()

	var stats enginepb.MVCCStats
	var err error
	for i := 0; i < b.N; i++ {
		iter := eng.NewIterator(false)
		stats, err = iter.ComputeStats(mvccKey(roachpb.KeyMin), mvccKey(roachpb.KeyMax), 0)
		iter.Close()
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	log.Infof(context.Background(), "live_bytes: %d", stats.LiveBytes)
}

// runMVCCCFindSplitKey benchmarks MVCCFindSplitKey on a 64MB range of data.
func runMVCCFindSplitKey(emk engineMaker, valueBytes int, b *testing.B) {
	const rangeBytes = 64 * 1024 * 1024
	numKeys := rangeBytes / (overhead + valueBytes)
	eng, _ := setupMVCCData(emk, 1, numKeys, valueBytes, b)
	defer eng.Close()

	b.SetBytes(rangeBytes)
	b.ResetTimer()

	var err error
	for i := 0; i < b.N; i++ {
		_, err = MVCCFindSplitKey(context.Background(), eng, roachpb.RKeyMin,
			roachpb.RKeyMax, rangeBytes/2, true /* allowMeta2Splits */)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func runBatchApplyBatchRepr(
	emk engineMaker, writeOnly bool, valueSize, batchSize int, b *testing.B,
) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	eng := emk(b, fmt.Sprintf("batch_apply_batch_repr_%d_%d", valueSize, batchSize))
	defer eng.Close()

	var repr []byte
	{
		batch := eng.NewBatch()
		for i := 0; i < batchSize; i++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
			ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			if err := MVCCPut(context.Background(), batch, nil, key, ts, value, nil); err != nil {
				b.Fatal(err)
			}
		}
		repr = batch.Repr()
		batch.Close()
	}

	b.SetBytes(int64(len(repr)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var batch Batch
		if writeOnly {
			batch = eng.NewWriteOnlyBatch()
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

func BenchmarkMVCCPutDelete_RocksDB(b *testing.B) {
	rocksdb := setupMVCCInMemRocksDB(b, "put_delete")
	defer rocksdb.Close()

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(r, 10))
	var blockNum int64

	for i := 0; i < b.N; i++ {
		blockID := r.Int63()
		blockNum++
		key := encoding.EncodeVarintAscending(nil, blockID)
		key = encoding.EncodeVarintAscending(key, blockNum)

		if err := MVCCPut(context.Background(), rocksdb, nil, key, hlc.Timestamp{}, value, nil /* txn */); err != nil {
			b.Fatal(err)
		}
		if err := MVCCDelete(context.Background(), rocksdb, nil, key, hlc.Timestamp{}, nil /* txn */); err != nil {
			b.Fatal(err)
		}
	}
}

func runBenchmarkClearRange(
	b *testing.B, clearRange func(e Engine, b Batch, start, end MVCCKey) error,
) {
	const rangeBytes = 64 << 20
	const valueBytes = 92
	numKeys := rangeBytes / (overhead + valueBytes)
	eng, _ := setupMVCCData(setupMVCCRocksDB, 1, numKeys, valueBytes, b)
	defer eng.Close()

	b.SetBytes(rangeBytes)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch := eng.NewWriteOnlyBatch()
		if err := clearRange(eng, batch, NilKey, MVCCKeyMax); err != nil {
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

func BenchmarkClearRange_RocksDB(b *testing.B) {
	runBenchmarkClearRange(b, func(eng Engine, batch Batch, start, end MVCCKey) error {
		return batch.ClearRange(start, end)
	})
}

func BenchmarkClearIterRange_RocksDB(b *testing.B) {
	runBenchmarkClearRange(b, func(eng Engine, batch Batch, start, end MVCCKey) error {
		iter := eng.NewIterator(false)
		defer iter.Close()
		return batch.ClearIterRange(iter, start, end)
	})
}

func BenchmarkMVCCGarbageCollect(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.Background()
	ts := hlc.Timestamp{}.Add(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), 0)

	// Write 'numKeys' of the given 'keySize' and 'valSize' to the given engine.
	// For each key, write 'numVersions' versions, and add a GCRequest_GCKey to
	// the returned slice that affects the oldest 'deleteVersions' versions. The
	// first write for each key will be at `ts`, the second one at `ts+(0,1)`,
	// etc.
	//
	// NB: a real invocation of MVCCGarbageCollect typically has most of the keys
	// in sorted order. Here they will be ordered randomly.
	setup := func(
		engine Engine, keySize, valSize, numKeys, numVersions, deleteVersions int,
	) []roachpb.GCRequest_GCKey {
		var ms enginepb.MVCCStats
		var gcKeys []roachpb.GCRequest_GCKey

		mkKey := func() []byte {
			k := make([]byte, keySize)
			if n, err := rand.Read(k); err != nil || n != keySize {
				b.Fatalf("error or read too little: n=%d err=%v", n, err)
			}
			return k
		}
		valBytes := make([]byte, valSize)
		if n, err := rand.Read(valBytes); err != nil || n != valSize {
			b.Fatalf("error or read too little: n=%d err=%v", n, err)
		}
		v := roachpb.MakeValueFromBytesAndTimestamp(valBytes, hlc.Timestamp{})

		for j := 0; j < numKeys; j++ {
			curKey := mkKey()
			if deleteVersions > 0 {
				gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{
					Timestamp: ts.Add(0, int32(deleteVersions-1)),
					Key:       curKey,
				})
			}

			for i := 0; i < numVersions; i++ {
				if err := MVCCPut(
					ctx, engine, &ms, curKey, ts.Add(0, int32(i)), v, nil); err != nil {
					b.Fatal(err)
				}
			}
		}
		return gcKeys
	}

	// We write values at ts+(0,i), set now=ts+(1,0) so that we're ahead of all
	// the writes. This value doesn't matter in practice, it's used only for
	// stats updates.
	now := ts.Add(1, 0)

	run := func(
		b *testing.B, keySize, valSize, numKeys, numVersions, deleteVersions int,
	) {
		engine := createTestEngine()
		defer engine.Close()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			gcKeys := setup(engine, keySize, valSize, numKeys, numVersions, deleteVersions)

			b.StartTimer()
			if err := MVCCGarbageCollect(
				ctx, engine, &enginepb.MVCCStats{}, gcKeys, now,
			); err != nil {
				b.Fatal(err)
			}
		}
	}

	// NB: To debug #16068, test only 128-128-15000-6.
	for _, keySize := range []int{128} {
		b.Run(fmt.Sprintf("keySize=%d", keySize), func(b *testing.B) {
			for _, valSize := range []int{128} {
				b.Run(fmt.Sprintf("valSize=%d", valSize), func(b *testing.B) {
					for _, numKeys := range []int{1, 1024} {
						b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
							for _, numVersions := range []int{2, 1024} {
								b.Run(fmt.Sprintf("numVersions=%d", numVersions), func(b *testing.B) {
									deleteVersions := numVersions - 1
									run(b, keySize, valSize, numKeys, numVersions, deleteVersions)
								})
							}
						})
					}
				})
			}
		})
	}
}
