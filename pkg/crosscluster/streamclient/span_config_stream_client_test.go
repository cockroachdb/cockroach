// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestSpanConfigClient ensures the spanConfigClient surfaces errors.
//
// TODO(msbutler): add a few more compontents to this test once the span config
// client api is finalized.
func TestSpanConfigClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	h, cleanup := replicationtestutils.NewReplicationHelper(t,
		base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	)

	defer cleanup()

	testTenantName := roachpb.TenantName("test-tenant")
	h.SysSQL.Exec(t, "CREATE TENANT $1", testTenantName)

	ctx := context.Background()

	maybeInlineURL := h.MaybeGenerateInlineURL(t)
	client, err := streamclient.NewSpanConfigStreamClient(ctx, maybeInlineURL)
	defer func() {
		require.NoError(t, client.Close(ctx))
	}()
	require.NoError(t, err)
	sub, err := client.SetupSpanConfigsStream(testTenantName)
	require.NoError(t, err)

	rf := replicationtestutils.MakeReplicationFeed(t, &subscriptionFeedSource{sub: sub})
	defer rf.Close(ctx)

	ctxWithCancel, cancelFn := context.WithCancel(ctx)
	cg := ctxgroup.WithContext(ctxWithCancel)
	cg.GoCtx(sub.Subscribe)

	// Ensure span config events are replicating.
	rf.ObserveAnySpanConfigRecord(ctx)

	// Test if Subscribe can react to cancellation signal.
	cancelFn()

	// When the context is cancelled, lib/pq sends a query cancellation message to
	// the server. Occasionally, we see the error from this cancellation before
	// the subscribe function sees our local context cancellation.
	err = cg.Wait()
	require.True(t, errors.Is(err, context.Canceled) || isQueryCanceledError(err))

	rf.ObserveError(ctx, func(err error) bool {
		return errors.Is(err, context.Canceled) || isQueryCanceledError(err)
	})
}
