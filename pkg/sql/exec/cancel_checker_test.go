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
