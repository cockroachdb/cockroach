// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestGetStatementActivities(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					SynchronousSQLStats: true,
				},
			},
		},
	})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)
	ts := testCluster.Server(0)
	conn := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))
	conn.Exec(t, "CREATE TABLE test (x int PRIMARY KEY)")
	conn.Exec(t, "INSERT INTO test VALUES (1)")
	ts.SQLServer().(*sql.Server).GetSQLStatsProvider().MaybeFlush(ctx, testCluster.Stopper())

	client, err := ts.GetAdminHTTPClient()
	require.NoError(t, err)
	mdResp := makeApiRequest[GetStatementActivitiesResponse](t, client, ts.AdminURL().WithPath("/api/v2/sql_activity/statements/?sortBy=cnt&topK=100").String(), http.MethodGet)
	jsonBytes, _ := json.MarshalIndent(mdResp, "", "  ")
	fmt.Println(string(jsonBytes))
}
