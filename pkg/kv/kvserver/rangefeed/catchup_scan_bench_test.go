// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func runCatchUpBenchmark(b *testing.B, emk engineMaker, opts benchOptions) (numEvents int) {
	eng, _ := setupData(context.Background(), b, emk, opts.dataOpts)
	defer eng.Close()
	startKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(0)))
	endKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(opts.dataOpts.numKeys)))
	span := roachpb.Span{
		Key:    startKey,
		EndKey: endKey,
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			iter, err := rangefeed.NewCatchUpIterator(ctx, eng, span, opts.ts, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer iter.Close()
			counter := 0
			err = iter.CatchUpScan(ctx, func(*kvpb.RangeFeedEvent) error {
				counter++
				return nil
			}, opts.withDiff, false /* withFiltering */, false /* withOmitRemote */)
			if err != nil {
				b.Fatalf("failed catchUp scan: %+v", err)
			}
			if counter < 1 {
				b.Fatalf("didn't emit any events!")
			}
			if numEvents == 0 {
				// Preserve number of events so that caller can compare it between
				// different invocations that it knows should not affect number of
				// events.
				numEvents = counter
			}
			// Number of events can't change between iterations.
			require.Equal(b, numEvents, counter)
		}()
	}
	return numEvents
}

func BenchmarkCatchUpScan(b *testing.B) {
	defer log.Scope(b).Close(b)
	skip.UnderShort(b)

	numKeys := 1_000_000
	valueBytes := 64

	dataOpts := map[string]benchDataOptions{
		// linear-keys is one of our best-case scenarios. In
		// this case, each newly written row is at a key
		// following the previously written row and at a later
		// timestamp. Further, once compacted, all of the SSTs
		// should be in L5 and L6. As a result, the time-based
		// optimization can exclude SSTs fairly easily.
		"linear-keys": {
			numKeys:    numKeys,
			valueBytes: valueBytes,
			rwMode:     fs.ReadWrite,
		},
		// random-keys is our worst case. We write keys in
		// random order but with timestamps that keep marching
		// forward. Once compacted, most of the data is in L5
		// and L6. So, we have very few overlapping SSTs and
		// most SSTs in our lower level will have at least 1
		// key that needs to be included in our scan, despite
		// the time based optimization.
		"random-keys": {
			randomKeyOrder: true,
			numKeys:        numKeys,
			valueBytes:     valueBytes,
			rwMode:         fs.ReadWrite,
		},
		// mixed-case is a middling case.
		//
		// This case is trying to simulate a larger store, but
		// with fewer bytes. If we did not reduce
		// LBaseMaxBytes, almost all data would be in Lbase or
		// L6, and TBI would be ineffective. By reducing
		// LBaseMaxBytes, the data should spread out over more
		// levels, like in a real store. The LSM state
		// depicted below shows that this was only partially
		// successful.
		//
		// We return a read only engine to prevent read-based
		// compactions after the initial data generation.
		//
		// As of 2021-08-18 data generated using these
		// settings looked like:
		//
		//__level_____count____size___score______in__ingest(sz_cnt)____move(sz_cnt)___write(sz_cnt)____read___r-amp___w-amp
		//   WAL         1     0 B       -     0 B       -       -       -       -     0 B       -       -       -     0.0
		//     0         0     0 B    0.00     0 B     0 B       0     0 B       0     0 B       0     0 B       0     0.0
		//     1         2   4.4 M 1819285.94     0 B     0 B       0     0 B       0     0 B       0     0 B       0     0.0
		//     2         0     0 B    0.00     0 B     0 B       0     0 B       0     0 B       0     0 B       0     0.0
		//     3         0     0 B    0.00     0 B     0 B       0     0 B       0     0 B       0     0 B       0     0.0
		//     4         1   397 K    0.79     0 B     0 B       0     0 B       0     0 B       0     0 B       0     0.0
		//     5         0     0 B    0.00     0 B     0 B       0     0 B       0     0 B       0     0 B       0     0.0
		//     6         1    83 M       -     0 B     0 B       0     0 B       0     0 B       0     0 B       0     0.0
		// total         4    88 M       -     0 B     0 B       0     0 B       0     0 B       0     0 B       0     0.0
		"mixed-case": {
			randomKeyOrder: true,
			numKeys:        numKeys,
			valueBytes:     valueBytes,
			rwMode:         fs.ReadOnly,
			lBaseMaxBytes:  256,
		},
	}

	for name, do := range dataOpts {
		b.Run(name, func(b *testing.B) {
			for _, withDiff := range []bool{true, false} {
				b.Run(fmt.Sprintf("withDiff=%v", withDiff), func(b *testing.B) {
					for _, tsExcludePercent := range []float64{0.0, 0.50, 0.75, 0.95, 0.99} {
						wallTime := int64((5 * (float64(numKeys)*tsExcludePercent + 1)))
						ts := hlc.Timestamp{WallTime: wallTime}
						b.Run(fmt.Sprintf("perc=%2.2f", tsExcludePercent*100), func(b *testing.B) {
							for _, numRangeKeys := range []int{0, 1, 100} {
								b.Run(fmt.Sprintf("numRangeKeys=%d", numRangeKeys), func(b *testing.B) {
									do := do
									do.numRangeKeys = numRangeKeys
									n := runCatchUpBenchmark(b, setupMVCCPebble, benchOptions{
										dataOpts: do,
										ts:       ts,
										withDiff: withDiff,
									})
									// We shouldn't be seeing the range deletions returned in this
									// benchmark since they are at timestamp 1 and we catch up at
									// a timestamp >= 5 (which corresponds to tsExcludePercent ==
									// 0). Note that the oldest key is always excluded, since the
									// floor for wallTime is 5 and that's the oldest key's
									// timestamp but the start timestamp is exclusive.
									require.EqualValues(b, int64(numKeys)-wallTime/5, n)
								})
							}
						})
					}
				})
			}
		})
	}
}

type benchDataOptions struct {
	numKeys        int
	valueBytes     int
	randomKeyOrder bool
	rwMode         fs.RWMode
	lBaseMaxBytes  int64
	numRangeKeys   int
}

type benchOptions struct {
	ts       hlc.Timestamp
	withDiff bool
	dataOpts benchDataOptions
}

//
// The following code was copied and then modified from the testing
// code in pkg/storage.
//

type engineMaker func(testing.TB, string, int64, fs.RWMode) storage.Engine

func setupMVCCPebble(b testing.TB, dir string, lBaseMaxBytes int64, rw fs.RWMode) storage.Engine {
	env, err := fs.InitEnv(context.Background(), vfs.Default, dir, fs.EnvConfig{RW: rw}, nil /* statsCollector */)
	if err != nil {
		b.Fatalf("could not initialize new fs env at %s: %+v", dir, err)
	}
	eng, err := storage.Open(
		context.Background(),
		env,
		cluster.MakeTestingClusterSettings(),
		storage.LBaseMaxBytes(lBaseMaxBytes))
	if err != nil {
		b.Fatalf("could not create new pebble instance at %s: %+v", dir, err)
	}
	return eng
}

// setupData data writes numKeys keys. One version of each key
// is written. The write timestamp starts at 5ns and then in 5ns
// increments. This allows scans at various times, starting at t=5ns,
// and continuing to t=5ns*(numKeys+1). The goal of this is to
// approximate an append-only type workload.
//
// A read-only engine is returned if opts.rwMode is set to fs.ReadOnly. The goal
// of this is to prevent read-triggered compactions that might change the
// distribution of data across levels.
//
// The creation of the database is time consuming, especially for
// larger numbers of versions. The database is persisted between runs
// and stored in the current directory.
func setupData(
	ctx context.Context, b *testing.B, emk engineMaker, opts benchDataOptions,
) (storage.Engine, string) {
	verStr := fmt.Sprintf("v%s", clusterversion.Latest.String())
	orderStr := "linear"
	if opts.randomKeyOrder {
		orderStr = "random"
	}
	readOnlyStr := ""
	if opts.rwMode == fs.ReadOnly {
		readOnlyStr = "_readonly"
	}
	loc := fmt.Sprintf("rangefeed_bench_data_%s_%s%s_%d_%d_%d_%d",
		verStr, orderStr, readOnlyStr, opts.numKeys, opts.valueBytes, opts.lBaseMaxBytes, opts.numRangeKeys)
	exists := true
	if _, err := os.Stat(loc); oserror.IsNotExist(err) {
		exists = false
	} else if err != nil {
		b.Fatal(err)
	}

	absPath, err := filepath.Abs(loc)
	if err != nil {
		absPath = loc
	}
	if exists {
		log.Infof(ctx, "using existing refresh range benchmark data: %s", absPath)
		testutils.ReadAllFiles(filepath.Join(loc, "*"))
		return emk(b, loc, opts.lBaseMaxBytes, opts.rwMode), loc
	}

	eng := emk(b, loc, opts.lBaseMaxBytes, fs.ReadWrite)
	log.Infof(ctx, "creating rangefeed benchmark data: %s", absPath)

	// Generate the same data every time.
	rng := rand.New(rand.NewSource(1449168817))

	keys := make([]roachpb.Key, opts.numKeys)
	order := make([]int, 0, opts.numKeys)
	for i := 0; i < opts.numKeys; i++ {
		keys[i] = roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(i)))
		order = append(order, i)
	}

	if opts.randomKeyOrder {
		rng.Shuffle(len(order), func(i, j int) {
			order[i], order[j] = order[j], order[i]
		})
	}

	writeRangeKeys := func(b testing.TB, wallTime int) {
		batch := eng.NewBatch()
		defer batch.Close()
		for i := 0; i < opts.numRangeKeys; i++ {
			// NB: regular keys are written at ts 5+, so this is below any of the
			// regular writes and thus won't delete anything.
			ts := hlc.Timestamp{WallTime: int64(wallTime), Logical: int32(i + 1)}
			start := rng.Intn(opts.numKeys)
			end := start + rng.Intn(opts.numKeys-start) + 1
			// As a special case, if we're only writing one range key, write it across
			// the entire span.
			if opts.numRangeKeys == 1 {
				start = 0
				end = opts.numKeys + 1
			}
			startKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(start)))
			endKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(end)))
			require.NoError(b, storage.MVCCDeleteRangeUsingTombstone(
				ctx, batch, nil, startKey, endKey, ts, hlc.ClockTimestamp{}, nil, nil, false, 0, 0, nil))
		}
		require.NoError(b, batch.Commit(false /* sync */))
	}
	writeRangeKeys(b, 1 /* wallTime */)

	writeKey := func(batch storage.Batch, idx int, pos int) {
		key := keys[idx]
		value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, opts.valueBytes))
		value.InitChecksum(key)
		ts := hlc.Timestamp{WallTime: int64((pos + 1) * 5)}
		if _, err := storage.MVCCPut(ctx, batch, key, ts, value, storage.MVCCWriteOptions{}); err != nil {
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
			log.Infof(ctx, "committing (%d/~%d) (%d/%d)", i/scaled, 20, i, len(order))
			if err := batch.Commit(false /* sync */); err != nil {
				b.Fatal(err)
			}
			batch.Close()
			batch = eng.NewBatch()
			if err := eng.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		writeKey(batch, idx, i)
	}
	if err := batch.Commit(false /* sync */); err != nil {
		b.Fatal(err)
	}
	batch.Close()
	if err := eng.Flush(); err != nil {
		b.Fatal(err)
	}

	if opts.rwMode == fs.ReadOnly {
		eng.Close()
		eng = emk(b, loc, opts.lBaseMaxBytes, opts.rwMode)
	}
	return eng, loc
}
