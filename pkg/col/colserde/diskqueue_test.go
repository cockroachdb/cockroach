// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package colserde_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fs := vfs.NewMem()
	path := "testing"
	require.NoError(t, fs.MkdirAll("testing", 0755))

	rng, _ := randutil.NewPseudoRand()
	for _, alwaysCompress := range []bool{true, false} {
		t.Run(fmt.Sprintf("AlwaysCompress=%t", alwaysCompress), func(t *testing.T) {
			for _, bufferSizeBytes := range []int{0, 16 << 10 /* 16 KiB */, 1 << 20 /* 1 MiB */} {
				t.Run(fmt.Sprintf("BufferSizeBytes=%s", humanizeutil.IBytes(int64(bufferSizeBytes))), func(t *testing.T) {
					for _, maxFileSizeBytes := range []int{10 << 10 /* 10 KiB */, 1 << 20 /* 1 MiB */, 64 << 20 /* 64 MiB */} {
						t.Run(fmt.Sprintf("MaxFileSizeBytes=%s", humanizeutil.IBytes(int64(maxFileSizeBytes))), func(t *testing.T) {
							// Create random input.
							batches := make([]coldata.Batch, 0, 1+rng.Intn(2048))
							op := colexec.NewRandomDataOp(testAllocator, rng, colexec.RandomDataOpArgs{
								AvailableTyps: coltypes.AllTypes,
								NumBatches:    cap(batches),
								BatchSize:     1 + rng.Intn(int(coldata.BatchSize())),
								Nulls:         true,
								BatchAccumulator: func(b coldata.Batch) {
									batches = append(batches, copyBatch(b))
								},
							})
							typs := op.Typs()

							// Create queue.
							filenamePrefix := uuid.FastMakeV4().String()
							queueCfg := colserde.QueueCfg{
								Typs:             typs,
								Path:             path,
								FilenamePrefix:   filenamePrefix,
								BufferSizeBytes:  bufferSizeBytes,
								MaxFileSizeBytes: maxFileSizeBytes,
							}
							queueCfg.TestingKnobs.AlwaysCompress = alwaysCompress
							q := colserde.NewDiskQueue(queueCfg, fs)
							require.NoError(t, q.Init())

							// Run verification.
							ctx := context.Background()
							for {
								b := op.Next(ctx)
								require.NoError(t, q.Enqueue(b))
								if b.Length() == 0 {
									break
								}
								if rng.Float64() < 0.5 {
									b.ResetInternalBatch()
									if ok, err := q.Dequeue(b); !ok {
										t.Fatal("queue incorrectly considered empty")
									} else if err != nil {
										t.Fatal(err)
									}
									assertEqualBatches(t, batches[0], b)
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
								assertEqualBatches(t, batches[0], b)
								batches = batches[1:]
								i++
							}

							// Close queue.
							require.NoError(t, q.Close())

							// Verify no files with the filename prefix are left since all
							// batches were dequeued.
							files, err := fs.List(path)
							require.NoError(t, err)
							for _, f := range files {
								if strings.HasPrefix(f, filenamePrefix) {
									t.Fatal("files left over after disk queue test")
								}
							}
						})
					}
				})
			}
		})
	}
}

// BenchmarkQueues benchmarks a queue with parameters provided through flags.
func BenchmarkQueues(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}

	var (
		bufSize     = 64 << 10  /* 64 KiB */
		maxFileSize = 32 << 20  /* 32 MiB */
		dataSize    = 512 << 20 /* 512 MiB */
	)
	numBatches := dataSize / int(8*coldata.BatchSize())

	path, cleanup := testutils.TempDir(b)
	defer cleanup()

	rng, _ := randutil.NewPseudoRand()
	typs := []coltypes.T{coltypes.Int64}
	batch := colexec.RandomBatch(testAllocator, rng, typs, int(coldata.BatchSize()), 0, 0)
	op := colexec.NewRepeatableBatchSource(batch)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		op.ResetBatchesToReturn(int(numBatches))
		q := colserde.NewDiskQueue(colserde.QueueCfg{
			Path:             path,
			BufferSizeBytes:  int(bufSize),
			MaxFileSizeBytes: int(maxFileSize),
		}, vfs.Default)
		if err := q.Init(); err != nil {
			b.Fatal(err)
		}
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
}
