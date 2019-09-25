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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestCancelChecker verifies that CancelChecker panics with appropriate error
// when the context is canceled.
func TestCancelChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	batch := coldata.NewMemBatch([]coltypes.T{coltypes.Int64})
	op := NewCancelChecker(NewNoop(NewRepeatableBatchSource(batch)))
	cancel()
	err := execerror.CatchVectorizedRuntimeError(func() {
		op.Next(ctx)
	})
	require.Equal(t, sqlbase.QueryCanceledError, err)
}
