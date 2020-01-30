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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestDiskQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	queueCfg, cleanup := colcontainer.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	rng, _ := randutil.NewPseudoRand()
	for _, bufferSizeBytes := range []int{0, 16<<10 + rng.Intn(1<<20) /* 16 KiB up to 1 MiB */} {
		for _, maxFileSizeBytes := range []int{10 << 10 /* 10 KiB */, 1<<20 + rng.Intn(64<<20) /* 1 MiB up to 64 MiB */} {
			alwaysCompress := rng.Float64() < 0.5
			t.Run(fmt.Sprintf("AlwaysCompress=%t/BufferSizeBytes=%s/MaxFileSizeBytes=%s", alwaysCompress, humanizeutil.IBytes(int64(bufferSizeBytes)), humanizeutil.IBytes(int64(maxFileSizeBytes))), func(t *testing.T) {
				// Create random input.
				batches := make([]coldata.Batch, 0, 1+rng.Intn(2048))
				op := colexec.NewRandomDataOp(testAllocator, rng, colexec.RandomDataOpArgs{
					AvailableTyps: coltypes.AllTypes,
					NumBatches:    cap(batches),
					BatchSize:     1 + rng.Intn(int(coldata.BatchSize())),
					Nulls:         true,
					BatchAccumulator: func(b coldata.Batch) {
						batches = append(batches, colexec.CopyBatch(testAllocator, b))
					},
				})
				typs := op.Typs()

				// Create queue.
				queueCfg.TestingKnobs.AlwaysCompress = alwaysCompress
				q, err := colcontainer.NewDiskQueue(typs, queueCfg)
				require.NoError(t, err)

				// Run verification.
				ctx := context.Background()
				for {
					b := op.Next(ctx)
					require.NoError(t, q.Enqueue(b))
					if b.Length() == 0 {
						break
					}
					if rng.Float64() < 0.5 {
						if ok, err := q.Dequeue(b); !ok {
							t.Fatal("queue incorrectly considered empty")
						} else if err != nil {
							t.Fatal(err)
						}
						coldata.AssertEquivalentBatches(t, batches[0], b)
						batches = batches[1:]
					}
				}
				b := coldata.NewMemBatch(typs)
				i := 0
				for len(batches) > 0 {
					if ok, err := q.Dequeue(b); !ok {
						t.Fatal("queue incorrectly considered empty")
					} else if err != nil {
						t.Fatal(err)
					}
					coldata.AssertEquivalentBatches(t, batches[0], b)
					batches = batches[1:]
					i++
				}

				if ok, err := q.Dequeue(b); ok {
					if b.Length() != 0 {
						t.Fatal("queue should be empty")
					}
				} else if err != nil {
					t.Fatal(err)
				}

				// Close queue.
				require.NoError(t, q.Close())

				// Verify no directories are left over.
				files, err := queueCfg.FS.List(queueCfg.Path)
				require.NoError(t, err)
				for _, f := range files {
					if strings.HasPrefix(f, queueCfg.Dir) {
						t.Fatal("files left over after disk queue test")
					}
				}
			})
		}
	}
}

// Flags for BenchmarkQueue.
var (
	bufferSizeBytes = flag.String("bufsize", "128KiB", "number of bytes to buffer in memory before flushing")
	blockSizeBytes  = flag.String("blocksize", "32MiB", "block size for the number of bytes stored ina block. In pebble, this is the value size, with the flat implementation, this is the file size")
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

	queueCfg, cleanup := colcontainer.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()
	queueCfg.BufferSizeBytes = int(bufSize)
	queueCfg.MaxFileSizeBytes = int(blockSize)

	rng, _ := randutil.NewPseudoRand()
	typs := []coltypes.T{coltypes.Int64}
	batch := colexec.RandomBatch(testAllocator, rng, typs, int(coldata.BatchSize()), 0, 0)
	op := colexec.NewRepeatableBatchSource(batch)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		op.ResetBatchesToReturn(numBatches)
		q, err := colcontainer.NewDiskQueue(typs, queueCfg)
		require.NoError(b, err)
		for {
			batchToEnqueue := op.Next(ctx)
			if err := q.Enqueue(batchToEnqueue); err != nil {
				b.Fatal(err)
			}
			if batchToEnqueue.Length() == 0 {
				break
			}
		}
		dequeuedBatch := coldata.NewMemBatch(typs)
		for dequeuedBatch.Length() != 0 {
			if _, err := q.Dequeue(dequeuedBatch); err != nil {
				b.Fatal(err)
			}
		}
		if err := q.Close(); err != nil {
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
