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

package colrpc

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestOutboxCatchesPanics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		ctx      = context.Background()
		input    = exec.NewBatchBuffer()
		rpcLayer = makeMockFlowStreamRPCLayer()
	)
	outbox, err := NewOutbox(input, []types.T{types.Int64})
	require.NoError(t, err)

	// This test relies on the fact that BatchBuffer panics when there are no
	// batches to return. Verify this assumption.
	require.Panics(t, func() { input.Next(ctx) })

	// Close the client csChan so that the Outbox's Recv goroutine returns.
	close(rpcLayer.client.csChan)

	// The actual test verifies that the Outbox handles input execution tree
	// panics by not panicking and returning.
	// TODO(asubiotto): Extend this test by verifying that the Outbox sends this
	// error as metadata to an Inbox.
	require.NotPanics(t, func() { outbox.runWithStream(ctx, rpcLayer.client, nil /* cancelFn */) })
}
