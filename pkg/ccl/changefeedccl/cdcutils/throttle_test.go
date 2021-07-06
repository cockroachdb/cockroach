// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdcutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNodeLevelThrottler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sv := &cluster.MakeTestingClusterSettings().SV

	throttler := NodeLevelThrottler(sv)

	// Default: no throttling
	require.True(t, throttler.messageLimiter.AdmitN(10000000))
	require.True(t, throttler.byteLimiter.AdmitN(10000000))
	require.True(t, throttler.flushLimiter.AdmitN(10000000))

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		require.NoError(t, throttler.AcquireMessageQuota(ctx, 100000000000))
		require.NoError(t, throttler.AcquireFlushQuota(ctx))
	}

	// Update config and verify throttler been updated.
	changefeedbase.NodeSinkThrottleConfig.Override(
		ctx, sv, `{"MessageRate": 1, "ByteRate": 1, "FlushRate": 1}`,
	)
	require.True(t, throttler.messageLimiter.AdmitN(1))
	require.False(t, throttler.messageLimiter.AdmitN(1))
	require.True(t, throttler.byteLimiter.AdmitN(1))
	require.False(t, throttler.byteLimiter.AdmitN(1))
	require.True(t, throttler.flushLimiter.AdmitN(1))
	require.False(t, throttler.flushLimiter.AdmitN(1))
}
