// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/outliers"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

func TestOutliersIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	// Enable outlier detection by setting a latencyThreshold > 0.
	latencyThreshold := 250 * time.Millisecond
	outliers.LatencyThreshold.Override(ctx, &settings.SV, latencyThreshold)

	// See no recorded outliers.
	var count int
	row := conn.QueryRowContext(ctx, "SELECT count(*) FROM crdb_internal.node_execution_outliers")
	err := row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	// Execute a "long-running" statement, running longer than our latencyThreshold.
	_, err = conn.ExecContext(ctx, "SELECT pg_sleep($1)", 2*latencyThreshold.Seconds())
	require.NoError(t, err)

	// See one recorded outlier.
	row = conn.QueryRowContext(ctx, "SELECT count(*) FROM crdb_internal.node_execution_outliers")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
