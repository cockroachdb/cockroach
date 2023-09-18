// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tenantrate"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesauthorizer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReplicaRateLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	tenantrate.KVCURateLimit.Override(ctx, &cfg.Settings.SV, 1)
	cfg.TestingKnobs.DisableMergeWaitForReplicasInit = true
	cfg.TestingKnobs.TenantRateKnobs.TimeSource = tc.manualClock
	cfg.TestingKnobs.TenantRateKnobs.Authorizer = tenantcapabilitiesauthorizer.New(cfg.Settings, nil)
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// A range for tenant 123 appears via a split.
	ten123 := roachpb.MustMakeTenantID(123)
	splitKey := keys.MustAddr(keys.MakeSQLCodec(ten123).TenantPrefix())
	leftRepl := tc.store.LookupReplica(splitKey)
	require.NotNil(t, leftRepl)
	splitTestRange(tc.store, splitKey, t)
	tenRepl := tc.store.LookupReplica(splitKey)
	require.NotNil(t, tenRepl)
	require.NotNil(t, tenRepl.tenantMetricsRef)
	require.NotNil(t, tenRepl.tenantLimiter)

	tenCtx := roachpb.ContextWithClientTenant(ctx, ten123)
	put := func(timeout time.Duration) error {
		ba := &kvpb.BatchRequest{}
		req := putArgs(splitKey.AsRawKey(), []byte{1, 2, 7})
		ba.Add(&req)
		ctx, cancel := context.WithTimeout(tenCtx, timeout)
		defer cancel()
		return tenRepl.maybeRateLimitBatch(ctx, ba)
	}

	// Verify that first few writes succeed fast, but eventually requests start
	// timing out because of the rate limiter.
	const timeout = 10 * time.Millisecond
	require.NoError(t, put(timeout))
	require.NoError(t, put(timeout))
	block := func() bool {
		for i := 0; i < 1000; i++ {
			if err := put(timeout); errors.Is(err, context.DeadlineExceeded) {
				return true
			}
		}
		return false
	}
	require.True(t, block())

	// Verify that the rate limiter eventually starts allowing requests again.
	tc.manualClock.Advance(100 * time.Second)
	require.NoError(t, put(timeout))
	// But will block them again if there are too many.
	require.True(t, block())

	// Now the rate limiter is saturated. If we try to write a request to the
	// replica now, the rate limiter will block it. If this races with a range
	// destruction (for example, due to a merge like below), maybeRateLimitBatch()
	// returns a quota pool closed error.
	g := ctxgroup.WithContext(ctx)
	g.Go(func() error {
		_, pErr := leftRepl.AdminMerge(ctx, kvpb.AdminMergeRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: leftRepl.Desc().StartKey.AsRawKey(),
			},
		}, "testing")
		return pErr.GoError()
	})
	err := put(5 * time.Second)
	require.True(t, testutils.IsError(err, "123 pool closed: released"), err)

	require.NoError(t, g.Wait())
}
