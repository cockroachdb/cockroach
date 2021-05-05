// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBufferOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	inputTuples := colexectestutils.Tuples{{int64(1)}, {int64(2)}, {int64(3)}}
	input := colexectestutils.NewOpTestInput(testAllocator, coldata.BatchSize(), inputTuples, []*types.T{types.Int})
	buffer := NewBufferOp(input).(*bufferOp)
	buffer.Init(context.Background())

	t.Run("TestBufferReturnsInputCorrectly", func(t *testing.T) {
		buffer.advance()
		b := buffer.Next()
		require.Nil(t, b.Selection())
		require.Equal(t, len(inputTuples), b.Length())
		for i, val := range inputTuples {
			require.Equal(t, val[0], b.ColVec(0).Int64()[i])
		}

		// We've read over the batch, so we now should get a zero-length batch.
		b = buffer.Next()
		require.Nil(t, b.Selection())
		require.Equal(t, 0, b.Length())
	})
}
