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
// one processor at a time, calls onFail processor callbacks on release,
// de-duplicates onFail processor callbacks and does not allow double release.
func TestAllocatorToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	token := &AllocatorToken{}

	processCounter := map[string]int{
		"p1": 0,
		"p2": 0,
		"p3": 0,
	}

	bump := func(p string) {
		processCounter[p]++
	}

	require.NoError(t, token.Acquire(ctx, "p2", nil))
	require.True(t, token.mu.acquired)
	require.Equal(t, "p2", token.mu.acquiredName)
	require.ErrorIs(t, token.Acquire(ctx, "p1", nil), ErrAllocatorToken)
	require.Len(t, token.mu.onReleaseMap, 0)
	require.Len(t, token.mu.onReleaseNamed, 0)

	require.ErrorIs(t, token.Acquire(ctx, "p1", func(ctx context.Context) {
		bump("p1")
		// Expect this token acquisition to de-dupe with above, so the counter will
		// only be bumped once from the below callback.
		require.NoError(t, token.Acquire(ctx, "p1", func(ctx context.Context) {
			bump("p1")
		}))
	}), ErrAllocatorToken)

	require.Equal(t, map[string]int{"p1": 0, "p2": 0, "p3": 0}, processCounter)
	// p2 releases the token and p1 (callback) acquires it and also bumps p1.
	token.Release(ctx)
	require.Equal(t, map[string]int{"p1": 1, "p2": 0, "p3": 0}, processCounter)
	// p1 releases the token, there are no callbacks registered so expect no
	// bumps.
	token.Release(ctx)
	require.Equal(t, map[string]int{"p1": 1, "p2": 0, "p3": 0}, processCounter)
	require.Panics(t, func() { token.Release(ctx) })
}
