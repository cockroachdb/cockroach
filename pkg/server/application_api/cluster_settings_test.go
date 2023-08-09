// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package application_api

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestClusterSettingsAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	nonAdminUser := apiconstants.TestingUserNameNoAdmin().Normalized()
	consoleSettings := []string{
		"cross_cluster_replication.enabled",
		"keyvisualizer.enabled",
		"keyvisualizer.sample_interval",
		"sql.index_recommendation.drop_unused_duration",
		"sql.insights.anomaly_detection.latency_threshold",
		"sql.insights.high_retry_count.threshold",
		"sql.insights.latency_threshold",
		"sql.stats.automatic_collection.enabled",
		"timeseries.storage.resolution_10s.ttl",
		"timeseries.storage.resolution_30m.ttl",
		"ui.display_timezone",
		"version",
	}

	var resp serverpb.SettingsResponse
	// Admin should return all cluster settings
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings", &resp, true); err != nil {
		t.Fatal(err)
	}
	require.True(t, len(resp.KeyValues) > len(consoleSettings))

	// Admin requesting specific cluster setting should return that cluster setting
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings?keys=sql.stats.persisted_rows.max", &resp, true); err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, resp.KeyValues["sql.stats.persisted_rows.max"])
	require.True(t, len(resp.KeyValues) == 1)

	// Non-admin with no permission should return error message
	err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings", &resp, false)
	require.Error(t, err, "this operation requires the VIEWCLUSTERSETTING or MODIFYCLUSTERSETTING system privileges")

	// Non-admin with VIEWCLUSTERSETTING permission should return all cluster settings
	_, err = db.Exec(fmt.Sprintf("ALTER USER %s VIEWCLUSTERSETTING", nonAdminUser))
	require.NoError(t, err)
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings", &resp, false); err != nil {
		t.Fatal(err)
	}
	require.True(t, len(resp.KeyValues) > len(consoleSettings))

	// Non-admin with VIEWCLUSTERSETTING permission requesting specific cluster setting should return that cluster setting
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings?keys=sql.stats.persisted_rows.max", &resp, false); err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, resp.KeyValues["sql.stats.persisted_rows.max"])
	require.True(t, len(resp.KeyValues) == 1)

	// Non-admin with VIEWCLUSTERSETTING and VIEWACTIVITY permission should return all cluster settings
	_, err = db.Exec(fmt.Sprintf("ALTER USER %s VIEWACTIVITY", nonAdminUser))
	require.NoError(t, err)
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings", &resp, false); err != nil {
		t.Fatal(err)
	}
	require.True(t, len(resp.KeyValues) > len(consoleSettings))

	// Non-admin with VIEWCLUSTERSETTING and VIEWACTIVITY permission requesting specific cluster setting
	// should return that cluster setting
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings?keys=sql.stats.persisted_rows.max", &resp, false); err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, resp.KeyValues["sql.stats.persisted_rows.max"])
	require.True(t, len(resp.KeyValues) == 1)

	// Non-admin with VIEWACTIVITY and not VIEWCLUSTERSETTING should only see console cluster settings
	_, err = db.Exec(fmt.Sprintf("ALTER USER %s NOVIEWCLUSTERSETTING", nonAdminUser))
	require.NoError(t, err)
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings", &resp, false); err != nil {
		t.Fatal(err)
	}
	require.True(t, len(resp.KeyValues) == len(consoleSettings))
	for k := range resp.KeyValues {
		require.True(t, slices.Contains(consoleSettings, k))
	}

	// Non-admin with VIEWACTIVITY and not VIEWCLUSTERSETTING permission requesting specific cluster setting
	// from console should return that cluster setting
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings?keys=ui.display_timezone", &resp, false); err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, resp.KeyValues["ui.display_timezone"])
	require.True(t, len(resp.KeyValues) == 1)

	// Non-admin with VIEWACTIVITY and not VIEWCLUSTERSETTING permission requesting specific cluster setting
	// that is not from console should not return that cluster setting
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings?keys=sql.stats.persisted_rows.max", &resp, false); err != nil {
		t.Fatal(err)
	}
	require.True(t, len(resp.KeyValues) == 0)

}
