// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestStatusEngineStatsJson ensures that the output response for the engine
// stats contains the required fields.
func TestStatusEngineStatsJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer srv.Stopper().Stop(ctx)

	if srv.DeploymentMode().IsExternal() {
		// Explicitly enabling CanViewNodeInfo capability for the secondary/application tenant
		// when in external process mode, as shared process mode already has all capabilities.
		require.NoError(t, srv.GrantTenantCapabilities(
			ctx, serverutils.TestTenantID(),
			map[tenantcapabilitiespb.ID]string{tenantcapabilitiespb.CanViewNodeInfo: "true"}))
	}

	s := srv.ApplicationLayer()

	t.Logf("using admin URL %s", s.AdminURL())

	var engineStats serverpb.EngineStatsResponse
	// Using SucceedsSoon because we have seen in the wild that
	// occasionally requests don't go through with error "transport:
	// error while dialing: connection interrupted (did the remote node
	// shut down or are there networking issues?)"
	testutils.SucceedsSoon(t, func() error {
		return srvtestutils.GetStatusJSONProto(s, "enginestats/local", &engineStats)
	})

	formattedStats := debug.FormatLSMStats(engineStats.StatsByStoreId)
	re := regexp.MustCompile(`^(Store \d+:(.|\n)+?)+$`)
	if !re.MatchString(formattedStats) {
		t.Fatal(errors.Errorf("expected engine metrics to be correctly formatted, got:\n %s", formattedStats))
	}
}

func TestStatusEngineStatsJsonWithoutTenantCapability(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		// Note: We're only testing external-process mode because shared service
		// mode tenants have all capabilities.
		DefaultTestTenant: base.ExternalTestTenantAlwaysEnabled,
	})
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()

	var engineStats serverpb.EngineStatsResponse
	// Using SucceedsSoon because we have seen in the wild that
	// occasionally requests don't go through with error "transport:
	// error while dialing: connection interrupted (did the remote node
	// shut down or are there networking issues?)"
	testutils.SucceedsSoon(t, func() error {
		actualErr := srvtestutils.GetStatusJSONProto(s, "enginestats/local", &engineStats)
		require.Error(t, actualErr)
		if !strings.Contains(actualErr.Error(), "client tenant does not have capability to query cluster node metadata") {
			return errors.Wrap(actualErr, "unexpected error message")
		}
		return nil
	})

}
