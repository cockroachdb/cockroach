// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/stretchr/testify/require"
)

// TestCancelChecker verifies that CancelChecker panics with appropriate error
// when the context is canceled.
func TestCancelChecker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	batch := coldata.NewMemBatch([]types.T{types.Int64})
	op := NewCancelChecker(NewNoop(NewRepeatableBatchSource(batch)))
	cancel()
	require.PanicsWithValue(t, sqlbase.QueryCanceledError, func() {
		op.Next(ctx)
	})
}
