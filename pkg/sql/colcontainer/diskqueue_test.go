// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package colcontainer_test

import (
	"context"
	"flag"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestDiskQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	rng, _ := randutil.NewPseudoRand()
	for _, rewindable := range []bool{false, true} {
		for _, bufferSizeBytes := range []int{0, 16<<10 + rng.Intn(1<<20) /* 16 KiB up to 1 MiB */} {
			for _, maxFileSizeBytes := range []int{10 << 10 /* 10 KiB */, 1<<20 + rng.Intn(64<<20) /* 1 MiB up to 64 MiB */} {
				alwaysCompress := rng.Float64() < 0.5
				diskQueueCacheMode := colcontainer.DiskQueueCacheModeDefault
				// testReuseCache will test the reuse cache modes.
				testReuseCache := rng.Float64() < 0.5
				dequeuedProbabilityBeforeAllEnqueuesAreDone := 0.5
				if testReuseCache {
					dequeuedProbabilityBeforeAllEnqueuesAreDone = 0
					if rng.Float64() < 0.5 {
						diskQueueCacheMode = colcontainer.DiskQueueCacheModeReuseCache
					} else {
						diskQueueCacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
					}
				}
				prefix, suffix := "", fmt.Sprintf("/BufferSizeBytes=%s/MaxFileSizeBytes=%s",
					humanizeutil.IBytes(int64(bufferSizeBytes)),
					humanizeutil.IBytes(int64(maxFileSizeBytes)))
				if rewindable {
					dequeuedProbabilityBeforeAllEnqueuesAreDone = 0
					prefix, suffix = "Rewindable/", ""
				}
				numBatches := 1 + rng.Intn(1024)
				t.Run(fmt.Sprintf("%sDiskQueueCacheMode=%d/AlwaysCompress=%t%s/NumBatches=%d",
					prefix, diskQueueCacheMode, alwaysCompress, suffix, numBatches), func(t *testing.T) {
					// Create random input.
					batches := make([]coldata.Batch, 0, numBatches)
					op := coldatatestutils.NewRandomDataOp(testAllocator, rng, coldatatestutils.RandomDataOpArgs{
						NumBatches: cap(batches),
						BatchSize:  1 + rng.Intn(coldata.BatchSize()),
						Nulls:      true,
						BatchAccumulator: func(b coldata.Batch, typs []*types.T) {
							batches = append(batches, coldatatestutils.CopyBatch(b, typs, testColumnFactory))
						},
					})
					typs := op.Typs()

					queueCfg.CacheMode = diskQueueCacheMode
					queueCfg.SetDefaultBufferSizeBytesForCacheMode()
					if !rewindable {
						if !testReuseCache {
							queueCfg.BufferSizeBytes = bufferSizeBytes
						}
						queueCfg.MaxFileSizeBytes = maxFileSizeBytes
					}
					queueCfg.TestingKnobs.AlwaysCompress = alwaysCompress

					// Create queue.
					var (
						q   colcontainer.Queue
						err error
					)
					if rewindable {
						q, err = colcontainer.NewRewindableDiskQueue(ctx, typs, queueCfg, testDiskAcc)
					} else {
						q, err = colcontainer.NewDiskQueue(ctx, typs, queueCfg, testDiskAcc)
					}
					require.NoError(t, err)

					// Verify that a directory was created.
					directories, err := queueCfg.FS.List(queueCfg.Path)
					require.NoError(t, err)
					require.Equal(t, 1, len(directories))

					// Run verification.
					ctx := context.Background()
					for {
						b := op.Next(ctx)
						require.NoError(t, q.Enqueue(ctx, b))
						if b.Length() == 0 {
							break
						}
						if rng.Float64() < dequeuedProbabilityBeforeAllEnqueuesAreDone {
							if ok, err := q.Dequeue(ctx, b); !ok {
								t.Fatal("queue incorrectly considered empty")
							} else if err != nil {
								t.Fatal(err)
							}
							coldata.AssertEquivalentBatches(t, batches[0], b)
							batches = batches[1:]
						}
					}
					numReadIterations := 1
					if rewindable {
						numReadIterations = 2
					}
					for i := 0; i < numReadIterations; i++ {
						batchIdx := 0
						b := coldata.NewMemBatch(typs, testColumnFactory)
						for batchIdx < len(batches) {
							if ok, err := q.Dequeue(ctx, b); !ok {
								t.Fatal("queue incorrectly considered empty")
							} else if err != nil {
								t.Fatal(err)
							}
							coldata.AssertEquivalentBatches(t, batches[batchIdx], b)
							batchIdx++
						}

						if testReuseCache {
							// Trying to Enqueue after a Dequeue should return an error in these
							// CacheModes.
							require.Error(t, q.Enqueue(ctx, b))
						}

						if ok, err := q.Dequeue(ctx, b); ok {
							if b.Length() != 0 {
								t.Fatal("queue should be empty")
							}
						} else if err != nil {
							t.Fatal(err)
						}

						if rewindable {
							require.NoError(t, q.(colcontainer.RewindableQueue).Rewind())
						}
					}

					// Close queue.
					require.NoError(t, q.Close(ctx))

					// Verify no directories are left over.
					directories, err = queueCfg.FS.List(queueCfg.Path)
					require.NoError(t, err)
					require.Equal(t, 0, len(directories))
				})
			}
		}
	}
}

// Flags for BenchmarkQueue.
var (
	bufferSizeBytes = flag.String("bufsize", "128KiB", "number of bytes to buffer in memory before flushing")
	blockSizeBytes  = flag.String("blocksize", "32MiB", "block size for the number of bytes stored in a block. In pebble, this is the value size, with the flat implementation, this is the file size")
	dataSizeBytes   = flag.String("datasize", "512MiB", "size of data in bytes to sort")
)

// BenchmarkDiskQueue benchmarks a queue with parameters provided through flags.
func BenchmarkDiskQueue(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}

	bufSize, err := humanizeutil.ParseBytes(*bufferSizeBytes)
	if err != nil {
		b.Fatalf("could not parse -bufsize: %s", err)
	}
	blockSize, err := humanizeutil.ParseBytes(*blockSizeBytes)
	if err != nil {
		b.Fatalf("could not parse -blocksize: %s", err)
	}
	dataSize, err := humanizeutil.ParseBytes(*dataSizeBytes)
	if err != nil {
		b.Fatalf("could not pase -datasize: %s", err)
	}
	numBatches := int(dataSize / (8 * int64(coldata.BatchSize())))

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()
	queueCfg.BufferSizeBytes = int(bufSize)
	queueCfg.MaxFileSizeBytes = int(blockSize)

	rng, _ := randutil.NewPseudoRand()
	typs := []*types.T{types.Int}
	batch := coldatatestutils.RandomBatch(testAllocator, rng, typs, coldata.BatchSize(), 0, 0)
	op := colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		op.ResetBatchesToReturn(numBatches)
		q, err := colcontainer.NewDiskQueue(ctx, typs, queueCfg, testDiskAcc)
		require.NoError(b, err)
		for {
			batchToEnqueue := op.Next(ctx)
			if err := q.Enqueue(ctx, batchToEnqueue); err != nil {
				b.Fatal(err)
			}
			if batchToEnqueue.Length() == 0 {
				break
			}
		}
		dequeuedBatch := coldata.NewMemBatch(typs, testColumnFactory)
		for dequeuedBatch.Length() != 0 {
			if _, err := q.Dequeue(ctx, dequeuedBatch); err != nil {
				b.Fatal(err)
			}
		}
		if err := q.Close(ctx); err != nil {
			b.Fatal(err)
		}
	}
	// When running this benchmark multiple times, disk throttling might kick in
	// and result in unfair numbers. Uncomment this code to run the benchmark
	// multiple times.
	/*
		b.StopTimer()
		time.Sleep(10 * time.Second)
	*/
}
