// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package plan

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestAllocatorToken asserts that the allocator token is acquired by at most
// one processor at a time.
func TestAllocatorToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	token := &AllocatorToken{}

	require.NoError(t, token.TryAcquire(ctx, "p2"))
	require.True(t, token.mu.acquired)
	require.Equal(t, "p2", token.mu.acquiredName)
	// Multiple acquirers should fail.
	require.ErrorIs(t, token.TryAcquire(ctx, "p1"), ErrAllocatorToken{holder: "p2"})
	require.ErrorIs(t, token.TryAcquire(ctx, "p2"), ErrAllocatorToken{holder: "p2"})
	token.Release(ctx)
	require.False(t, token.mu.acquired)
	require.Equal(t, "", token.mu.acquiredName)
	// Double releases are not allowed.
	require.Panics(t, func() { token.Release(ctx) })
}
