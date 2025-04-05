// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLStatsTTLChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	defer db.Close()

	ts, err := tc.Server(0).TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID:   serverutils.TestTenantID(),
		TenantName: "tenant",
	})
	require.NoError(t, err)
	tenantDb := ts.SQLConn(t)
	defer tenantDb.Close()

	tables := []string{
		"system.public.statement_statistics",
		"system.public.transaction_statistics",
		"system.public.statement_activity",
		"system.public.transaction_activity",
	}
	testCases := []struct {
		name  string
		dbCon *gosql.DB
	}{{"system", db}, {"tenant", tenantDb}}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db := testCase.dbCon
			var target string
			var rawConfigSql string
			for _, table := range tables {
				row := db.QueryRow(fmt.Sprintf("SHOW ZONE CONFIGURATION FROM TABLE %s", table))
				err := row.Scan(&target, &rawConfigSql)
				require.NoError(t, err)

				assert.Equal(t, fmt.Sprintf("TABLE %s", table), target)
				assert.Contains(t, rawConfigSql, "gc.ttlseconds = 3600")
			}
		})
	}
}
