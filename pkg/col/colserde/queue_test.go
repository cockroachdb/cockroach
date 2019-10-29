// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package colserde

import (
	"context"
	"flag"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func testQueue(t *testing.T, q Queue) {
	rng, _ := randutil.NewPseudoRand()
	batches := make([]coldata.Batch, 0, 1024)
	op := colexec.NewRandomDataOp(rng, colexec.RandomDataOpArgs{
		DeterministicTyps: queueTyps,
		NumBatches:        cap(batches),
		BatchAccumulator: func(b coldata.Batch) {
			batches = append(batches, copyBatch(b))
		},
	})
	ctx := context.Background()
	for {
		b := op.Next(ctx)
		require.NoError(t, q.Enqueue(b))
		if b.Length() == 0 {
			break
		}
		if rng.Float64() < 0 {
			b, err := q.Dequeue()
			if err != nil {
				t.Fatal(err)
			}
			assertEqualBatches(t, batches[0], b)
			batches = batches[1:]
		}
	}
	for len(batches) > 0 {
		b, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		assertEqualBatches(t, batches[0], b)
		batches = batches[1:]
	}
}

// TestQueue just ensures that we get the correct behavior from both queue implementations.
func TestQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	path, cleanup := testutils.TempDir(t)
	defer cleanup()

	for _, bufferSizeBytes := range []int{0, 1 << 19 /* 500 KiB */, 1 << 20 /* 1 MiB */, 64 << 20 /* 64 MiB */} {
		t.Run(fmt.Sprintf("bufferSizeBytes=%s", humanizeutil.IBytes(int64(bufferSizeBytes))), func(t *testing.T) {
			t.Run("Flat", func(t *testing.T) {
				for _, maxFileSizeBytes := range []int{10 << 10 /* 10 KiB */, 1 << 20 /* 1 MiB */, 64 << 20 /* 64 MiB */, 128 << 20 /* 128 MiB */} {
					t.Run(fmt.Sprintf("maxFileSizeBytes=%s", humanizeutil.IBytes(int64(maxFileSizeBytes))), func(t *testing.T) {
						q := NewDiskQueue(flatQueueCfg{
							baseQueueCfg: baseQueueCfg{
								path:            path,
								bufferSizeBytes: bufferSizeBytes,
							},
							maxFileSizeBytes: maxFileSizeBytes,
						}, vfs.Default)
						require.NoError(t, q.Init())
						testQueue(t, q)
						require.NoError(t, q.Close())
					})
				}
			})

			t.Run("Pebble", func(t *testing.T) {
				for _, skipRecreateIter := range []bool{false, true} {
					t.Run(fmt.Sprintf("skipRecreateIter=%t", skipRecreateIter), func(t *testing.T) {
						for _, maxValueSizeBytes := range []int{1 << 10 /* 1 KiB */, 32 << 10 /* 32 KiB */, 64 << 10 /* 64 KiB */} {
							t.Run(fmt.Sprintf("maxValueSizeBytes=%s", humanizeutil.IBytes(int64(maxValueSizeBytes))), func(t *testing.T) {
								q, err := NewPebbleQueue(pebbleQueueCfg{
									baseQueueCfg: baseQueueCfg{
										path:            path,
										bufferSizeBytes: bufferSizeBytes,
									},
									skipRecreateIter:  skipRecreateIter,
									maxValueSizeBytes: maxValueSizeBytes,
								})
								require.NoError(t, err)
								testQueue(t, q)
								require.NoError(t, q.Close())
							})
						}
					})
				}
			})
		})
	}
}

// Flags for BenchmarkQueue
var (
	store           = flag.String("store", "", "one of flat or pebble, describes which underlying store to use")
	bufferSizeBytes = flag.String("bufsize", "", "number of bytes to buffer in memory before flushingh")
	blockSizeBytes  = flag.String("blocksize", "", "block size for the number of bytes stored ina block. In pebble, this is the value size, with the flat implementation, this is the file size")
	dataSizeBytes   = flag.String("datasize", "", "size of data in bytes to sort")
)

// BenchmarkQueues benchmarks a queue with parameters provided through flags.
func BenchmarkQueues(b *testing.B) {
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
	numBatches := dataSize / (8 * int64(coldata.BatchSize()))

	path, cleanup := testutils.TempDir(b)
	defer cleanup()

	var qConstructor func() Queue
	switch *store {
	case "flat":
		qConstructor = func() Queue {
			q := NewDiskQueue(flatQueueCfg{
				baseQueueCfg: baseQueueCfg{
					path:            path,
					bufferSizeBytes: int(bufSize),
				},
				maxFileSizeBytes: int(blockSize),
			}, vfs.Default)
			if err := q.Init(); err != nil {
				b.Fatal(err)
			}
			return q
		}
	case "pebble":
		qConstructor = func() Queue {
			q, err := NewPebbleQueue(pebbleQueueCfg{
				baseQueueCfg: baseQueueCfg{
					path:            path,
					bufferSizeBytes: int(bufSize),
				},
				skipRecreateIter:  true,
				maxValueSizeBytes: int(blockSize),
			})
			if err != nil {
				b.Fatal(err)
			}
			return q
		}
	default:
		b.Fatalf("unknown -store argument: %s", *store)
	}

	rng, _ := randutil.NewPseudoRand()
	batch := colexec.RandomBatch(rng, queueTyps, int(coldata.BatchSize()), 0, 0)
	// Uncomment this line and assertEqualBatches to ensure correctness.
	// expectedBatch := copyBatch(batch)
	op := colexec.NewRepeatableBatchSource(batch)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		op.ResetBatchesToReturn(int(numBatches))
		q := qConstructor()
		for {
			batchToEnqueue := op.Next(ctx)
			if err := q.Enqueue(batchToEnqueue); err != nil {
				b.Fatal(err)
			}
			if batchToEnqueue.Length() == 0 {
				break
			}
		}
		for {
			dequeuedBatch, err := q.Dequeue()
			if err != nil {
				b.Fatal(err)
			}
			if dequeuedBatch.Length() == 0 {
				break
			}
			// assertEqualBatches(b, expectedBatch, dequeuedBatch)
		}
		if err := q.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
