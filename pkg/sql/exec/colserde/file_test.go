// Copyright 2019 The Cockroach Authors.
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

package colserde

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestFileRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	typs, b := randomBatch()

	t.Run(`mem`, func(t *testing.T) {
		// Make a copy of the original batch because the converter modifies and
		// casts data without copying for performance reasons.
		original := copyBatch(b)

		var buf bytes.Buffer
		s, err := NewFileSerializer(&buf, typs)
		require.NoError(t, err)
		require.NoError(t, s.AppendBatch(b))
		require.NoError(t, s.Finish())

		roundtrip := coldata.NewMemBatchWithSize(nil, 0)
		d, err := FileDeserializerFromBytes(buf.Bytes())
		require.NoError(t, err)
		require.Equal(t, typs, d.Typs())
		require.Equal(t, 1, d.NumBatches())
		require.NoError(t, d.GetBatch(0, roundtrip))

		assertEqualBatches(t, original, roundtrip)
	})

	t.Run(`disk`, func(t *testing.T) {
		dir, cleanup := testutils.TempDir(t)
		defer cleanup()
		path := filepath.Join(dir, `rng.arrow`)

		// Make a copy of the original batch because the converter modifies and
		// casts data without copying for performance reasons.
		original := copyBatch(b)

		f, err := os.Create(path)
		require.NoError(t, err)
		defer f.Close()
		s, err := NewFileSerializer(f, typs)
		require.NoError(t, err)
		require.NoError(t, s.AppendBatch(b))
		require.NoError(t, s.Finish())
		require.NoError(t, f.Sync())

		roundtrip := coldata.NewMemBatchWithSize(nil, 0)
		d, err := FileDeserializerFromPath(path)
		require.NoError(t, err)
		require.Equal(t, typs, d.Typs())
		require.Equal(t, 1, d.NumBatches())
		require.NoError(t, d.GetBatch(0, roundtrip))

		assertEqualBatches(t, original, roundtrip)
	})
}

func TestFileIndexing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numInts = 10
	typs := []types.T{types.Int64}

	var buf bytes.Buffer
	s, err := NewFileSerializer(&buf, typs)
	require.NoError(t, err)

	for i := 0; i < numInts; i++ {
		b := coldata.NewMemBatchWithSize(typs, 1)
		b.SetLength(1)
		b.ColVec(0).Int64()[0] = int64(i)
		require.NoError(t, s.AppendBatch(b))
	}
	require.NoError(t, s.Finish())

	d, err := FileDeserializerFromBytes(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, typs, d.Typs())
	require.Equal(t, numInts, d.NumBatches())
	for batchIdx := numInts - 1; batchIdx >= 0; batchIdx-- {
		b := coldata.NewMemBatchWithSize(nil, 0)
		require.NoError(t, d.GetBatch(batchIdx, b))
		require.Equal(t, uint16(1), b.Length())
		require.Equal(t, 1, b.Width())
		require.Equal(t, types.Int64, b.ColVec(0).Type())
		require.Equal(t, int64(batchIdx), b.ColVec(0).Int64()[0])
	}
}
