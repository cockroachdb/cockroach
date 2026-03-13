// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestRetryResetOnProgress verifies that calling Reset()
// on the changefeed Retry wrapper resets the backoff to
// its initial value.
func TestRetryResetOnProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	r := getRetry(ctx, 10*time.Minute, 10*time.Minute)

	require.True(t, r.Next())
	require.Equal(t, 0, r.CurrentAttempt())
	initialBackoff := r.NextBackoff()

	for i := 0; i < 5; i++ {
		require.True(t, r.Next())
	}
	require.Equal(t, 5, r.CurrentAttempt())
	elevatedBackoff := r.NextBackoff()
	require.Greater(t, elevatedBackoff, initialBackoff,
		"backoff should increase after multiple attempts")

	r.Reset()
	require.Equal(t, 0, r.CurrentAttempt(),
		"attempt counter should be zero after reset")
	require.Equal(t, initialBackoff, r.NextBackoff(),
		"backoff should return to initial value after reset")
}
