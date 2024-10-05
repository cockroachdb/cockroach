// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestCancelChecker verifies that CancelChecker panics with appropriate error
// when the context is canceled.
func TestCancelChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())
	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	op := NewCancelChecker(colexecop.NewRepeatableBatchSource(testAllocator, batch, typs))
	op.Init(ctx)
	cancel()
	err := colexecerror.CatchVectorizedRuntimeError(func() {
		op.Next()
	})
	require.True(t, errors.Is(err, cancelchecker.QueryCanceledError))
}
