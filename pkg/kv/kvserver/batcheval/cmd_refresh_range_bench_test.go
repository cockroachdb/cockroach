// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testfixtures"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// BenchmarkRefreshRange benchmarks ranged refresh requests with different LSM
// shapes and refresh windows. It was heavily adapted from BenchmarkCatchUpScan,
// which was itself heavily adapted from code in pkg/storage.
func BenchmarkRefreshRange(b *testing.B) {
	defer log.Scope(b).Close(b)

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
			tsPercents := []float64{0.0, 0.50, 0.75, 0.95, 0.99}
			for _, refreshFrom := range tsPercents {
				for _, refreshTo := range tsPercents {
					if refreshTo < refreshFrom {
						continue
					}
					name := fmt.Sprintf("refresh_window=[%2.2f,%2.2f]", refreshFrom*100, refreshTo*100)
					b.Run(name, func(b *testing.B) {
						tsForPercent := func(p float64) hlc.Timestamp {
							walltime := int64(5 * (float64(numKeys)*p + 1)) // see setupData
							return hlc.Timestamp{WallTime: walltime}
						}
						runRefreshRangeBenchmark(b, setupMVCCPebble, benchOptions{
							refreshFrom: tsForPercent(refreshFrom),      // exclusive
							refreshTo:   tsForPercent(refreshTo).Next(), // inclusive
							dataOpts:    do,
						})
					})
				}
			}
		})
	}
}

func runRefreshRangeBenchmark(b *testing.B, emk engineMaker, opts benchOptions) {
	ctx := context.Background()
	eng, _ := setupData(ctx, b, emk, opts.dataOpts)
	defer eng.Close()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := (&batcheval.MockEvalCtx{ClusterSettings: st}).EvalContext()
	startKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(0)))
	endKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(opts.dataOpts.numKeys)))

	var refreshErr *kvpb.RefreshFailedError

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			var resp kvpb.RefreshRangeResponse
			_, err := batcheval.RefreshRange(ctx, eng, batcheval.CommandArgs{
				EvalCtx: evalCtx,
				Args: &kvpb.RefreshRangeRequest{
					RequestHeader: kvpb.RequestHeader{
						Key:    startKey,
						EndKey: endKey,
					},
					RefreshFrom: opts.refreshFrom,
				},
				Header: kvpb.Header{
					Txn: &roachpb.Transaction{
						TxnMeta: enginepb.TxnMeta{
							WriteTimestamp: opts.refreshTo,
						},
						ReadTimestamp: opts.refreshTo,
					},
					Timestamp: opts.refreshTo,
				},
			}, &resp)

			// If the refresh window was empty, we expect the refresh to scan the
			// entire span and succeed. Otherwise, it will short-circuit as soon
			// as it hits a conflict and return an error.
			emptyWindow := opts.refreshTo.Equal(opts.refreshFrom.Next())
			if emptyWindow {
				require.NoError(b, err)
			} else {
				require.Error(b, err)
				require.ErrorAs(b, err, &refreshErr)
			}
		}()
	}
}

type benchDataOptions struct {
	numKeys        int
	valueBytes     int
	randomKeyOrder bool
	rwMode         fs.RWMode
	lBaseMaxBytes  int64
}

type benchOptions struct {
	refreshFrom hlc.Timestamp
	refreshTo   hlc.Timestamp
	dataOpts    benchDataOptions
}

type engineMaker func(testing.TB, string, int64, fs.RWMode) storage.Engine

func setupMVCCPebble(b testing.TB, dir string, lBaseMaxBytes int64, rw fs.RWMode) storage.Engine {
	env, err := fs.InitEnv(context.Background(), vfs.Default, dir, fs.EnvConfig{RW: rw}, nil /* statsCollector */)
	if err != nil {
		b.Fatalf("could not initialize fs env at %s: %+v", dir, err)
	}
	eng, err := storage.Open(context.Background(), env, cluster.MakeTestingClusterSettings(), storage.LBaseMaxBytes(lBaseMaxBytes))
	if err != nil {
		env.Close()
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
// The creation of the database is time-consuming, especially for larger numbers
// of versions. The database is persisted between runs using
// testfixtures.ReuseOrGenerate.
func setupData(
	ctx context.Context, b *testing.B, emk engineMaker, opts benchDataOptions,
) (storage.Engine, string) {
	// Include the current version in the fixture name, or we may inadvertently
	// run against a left-over fixture that is no longer supported.
	verStr := fmt.Sprintf("v%s", clusterversion.Latest.String())
	orderStr := "linear"
	if opts.randomKeyOrder {
		orderStr = "random"
	}
	readOnlyStr := ""
	if opts.rwMode == fs.ReadOnly {
		readOnlyStr = "_readonly"
	}
	name := fmt.Sprintf("refresh_range_bench_data_%s_%s%s_%d_%d_%d_v2",
		verStr, orderStr, readOnlyStr, opts.numKeys, opts.valueBytes, opts.lBaseMaxBytes)

	dir := testfixtures.ReuseOrGenerate(b, name, func(dir string) {
		eng := emk(b, dir, opts.lBaseMaxBytes, fs.ReadWrite)
		log.Infof(ctx, "creating refresh range benchmark data: %s", dir)

		// Generate the same data every time.
		rng := rand.New(rand.NewSource(1449168817))

		keys := make([]roachpb.Key, opts.numKeys)
		order := make([]int, 0, opts.numKeys)
		for i := 0; i < opts.numKeys; i++ {
			keys[i] = encoding.EncodeUvarintAscending([]byte("key-"), uint64(i))
			order = append(order, i)
		}

		if opts.randomKeyOrder {
			rng.Shuffle(len(order), func(i, j int) {
				order[i], order[j] = order[j], order[i]
			})
		}

		writeKey := func(batch storage.Batch, idx int, pos int) {
			key := keys[idx]
			value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, opts.valueBytes))
			value.InitChecksum(key)
			ts := hlc.Timestamp{WallTime: int64((pos + 1) * 5)}
			if _, err := storage.MVCCPut(
				ctx, batch, key, ts, value, storage.MVCCWriteOptions{},
			); err != nil {
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
		eng.Close()
	})

	testutils.ReadAllFiles(filepath.Join(dir, "*"))
	return emk(b, dir, opts.lBaseMaxBytes, opts.rwMode), dir
}
