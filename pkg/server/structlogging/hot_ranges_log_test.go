// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package structlogging_test

import (
	"context"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/structlogging"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestHotRangesStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	ctx := context.Background()
	cleanup := logtestutils.InstallLogFileSink(sc, t, logpb.Channel_TELEMETRY)
	defer cleanup()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		StoreSpecs: []base.StoreSpec{
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
		},
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				ReplicaPlannerKnobs: plan.ReplicaPlannerTestingKnobs{
					DisableReplicaRebalancing: true,
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	logcrash.DiagnosticsReportingEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	structlogging.TelemetryHotRangesStatsEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	structlogging.TelemetryHotRangesStatsInterval.Override(ctx, &s.ClusterSettings().SV, 500*time.Millisecond)
	structlogging.TelemetryHotRangesStatsLoggingDelay.Override(ctx, &s.ClusterSettings().SV, 10*time.Millisecond)

	tenantID := roachpb.MustMakeTenantID(2)
	tt, err := s.StartTenant(ctx, base.TestTenantArgs{
		TenantID: tenantID,
		Settings: s.ClusterSettings(),
	})
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		ss := tt.TenantStatusServer().(serverpb.TenantStatusServer)
		resp, err := ss.HotRangesV2(ctx, &serverpb.HotRangesRequest{TenantID: tenantID.String()})
		if err != nil {
			return err
		}
		if len(resp.Ranges) == 0 {
			return errors.New("waiting for hot ranges to be collected")
		}
		return nil
	})

	testutils.SucceedsWithin(t, func() error {
		log.FlushFiles()
		entries, err := log.FetchEntriesFromFiles(
			0,
			math.MaxInt64,
			10000,
			regexp.MustCompile(`"EventType":"hot_ranges_stats"`),
			log.WithMarkedSensitiveData,
		)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return errors.New("waiting for logs")
		}
		return nil
	}, 5*time.Second)
}
