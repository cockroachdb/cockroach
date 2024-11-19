// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colserde_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestFileRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	typs, b := randomBatch(testAllocator)
	rng, _ := randutil.NewTestRand()
	ctx := context.Background()

	t.Run(`mem`, func(t *testing.T) {
		// Make copies of the original batch because the converter modifies and
		// casts data without copying for performance reasons.
		original := coldatatestutils.CopyBatch(b, typs, testColumnFactory)
		bCopy := coldatatestutils.CopyBatch(b, typs, testColumnFactory)

		var buf bytes.Buffer
		s, err := colserde.NewFileSerializer(&buf, typs, testMemAcc)
		require.NoError(t, err)
		require.NoError(t, s.AppendBatch(ctx, b))
		// Append the same batch again.
		require.NoError(t, s.AppendBatch(ctx, bCopy))
		require.NoError(t, s.Finish())
		s.Close(ctx)

		// Parts of the deserialization modify things (null bitmaps) in place, so
		// run it twice to make sure those modifications don't leak back to the
		// buffer.
		for i := 0; i < 2; i++ {
			func() {
				roundtrip := testAllocator.NewMemBatchWithFixedCapacity(typs, b.Length())
				d, err := colserde.NewFileDeserializerFromBytes(typs, buf.Bytes())
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close(ctx)) }()
				require.Equal(t, typs, d.Typs())
				require.Equal(t, 2, d.NumBatches())

				// Check the first batch.
				require.NoError(t, d.GetBatch(0, roundtrip))
				coldata.AssertEquivalentBatches(t, original, roundtrip)

				// Modify the returned batch (by appending some other random
				// batch) to make sure that the second serialized batch is
				// unchanged.
				length := rng.Intn(original.Length()) + 1
				args := coldatatestutils.RandomVecArgs{Rand: rng, NullProbability: rng.Float64()}
				r := coldatatestutils.RandomBatch(testAllocator, args, typs, length, length)
				for vecIdx, vec := range roundtrip.ColVecs() {
					vec.Append(coldata.SliceArgs{
						Src:       r.ColVec(vecIdx),
						DestIdx:   original.Length(),
						SrcEndIdx: length,
					})
				}
				roundtrip.SetLength(original.Length() + length)

				// Now check the second batch.
				require.NoError(t, d.GetBatch(1, roundtrip))
				coldata.AssertEquivalentBatches(t, original, roundtrip)
			}()
		}
	})

	t.Run(`disk`, func(t *testing.T) {
		dir, cleanup := testutils.TempDir(t)
		defer cleanup()
		path := filepath.Join(dir, `rng.arrow`)

		// Make a copy of the original batch because the converter modifies and
		// casts data without copying for performance reasons.
		original := coldatatestutils.CopyBatch(b, typs, testColumnFactory)

		f, err := os.Create(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		s, err := colserde.NewFileSerializer(f, typs, testMemAcc)
		require.NoError(t, err)
		require.NoError(t, s.AppendBatch(ctx, b))
		require.NoError(t, s.Finish())
		s.Close(ctx)
		require.NoError(t, f.Sync())

		// Parts of the deserialization modify things (null bitmaps) in place, so
		// run it twice to make sure those modifications don't leak back to the
		// file.
		for i := 0; i < 2; i++ {
			func() {
				roundtrip := testAllocator.NewMemBatchWithFixedCapacity(typs, b.Length())
				d, err := colserde.NewTestFileDeserializerFromPath(typs, path)
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close(ctx)) }()
				require.Equal(t, typs, d.Typs())
				require.Equal(t, 1, d.NumBatches())
				require.NoError(t, d.GetBatch(0, roundtrip))

				coldata.AssertEquivalentBatches(t, original, roundtrip)
			}()
		}
	})
}

func TestFileIndexing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	const numInts = 10
	typs := []*types.T{types.Int}
	batchSize := 1

	var buf bytes.Buffer
	s, err := colserde.NewFileSerializer(&buf, typs, testMemAcc)
	require.NoError(t, err)

	for i := 0; i < numInts; i++ {
		b := testAllocator.NewMemBatchWithFixedCapacity(typs, batchSize)
		b.SetLength(batchSize)
		b.ColVec(0).Int64()[0] = int64(i)
		require.NoError(t, s.AppendBatch(ctx, b))
	}
	require.NoError(t, s.Finish())

	d, err := colserde.NewFileDeserializerFromBytes(typs, buf.Bytes())
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close(ctx)) }()
	require.Equal(t, typs, d.Typs())
	require.Equal(t, numInts, d.NumBatches())
	for batchIdx := numInts - 1; batchIdx >= 0; batchIdx-- {
		b := testAllocator.NewMemBatchWithFixedCapacity(typs, batchSize)
		require.NoError(t, d.GetBatch(batchIdx, b))
		require.Equal(t, batchSize, b.Length())
		require.Equal(t, 1, b.Width())
		require.Equal(t, int64(batchIdx), b.ColVec(0).Int64()[0])
	}
}
