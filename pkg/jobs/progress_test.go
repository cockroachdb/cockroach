// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestChunkProgressLoggerLimitsFloatingPointError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	defer TestingSetProgressThresholds()()

	rangeCount := 1240725

	var lastReported float32
	l := NewChunkProgressLogger(func(_ context.Context, pct float32) error {
		require.Less(t, pct, float32(1.01))
		lastReported = pct
		return nil
	}, rangeCount, 0)
	for i := 0; i < rangeCount; i++ {
		require.NoError(t, l.chunkFinished(ctx), "failed at update %d", i)
	}
	require.Greater(t, lastReported, float32(0.99))
}
