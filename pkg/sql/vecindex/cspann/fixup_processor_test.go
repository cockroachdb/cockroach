// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestDelayInsertOrDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Set up the fixup processor with a minimum delay and allowed ops/sec = 1.
	var fp FixupProcessor
	fp.minDelay = time.Hour
	fp.Init(ctx, stopper, nil /* index */, 42 /* seed */)
	fp.mu.pacer.Init(1, 0, fp.mu.pacer.monoNow)

	// Cancel the context and verify that DelayInsertOrDelete immediately
	// returns with an error rather than waiting out the delay.
	cancel()
	require.ErrorIs(t, fp.DelayInsertOrDelete(ctx), context.Canceled)

	// Validate that the pacer token was returned to the bucket, since operation
	// didn't actually happen.
	require.GreaterOrEqual(t, fp.mu.pacer.currentTokens, 0.0)
}
