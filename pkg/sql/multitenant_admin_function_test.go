// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func createSQLRunners(
	t *testing.T,
) (systemTenant *sqlutils.SQLRunner, secondaryTenant *sqlutils.SQLRunner, cleanup func()) {
	createTestClusterAndServer := func() (serverutils.TestClusterInterface, serverutils.TestServerInterface) {
		systemTenantTestCluster := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
		return systemTenantTestCluster, systemTenantTestCluster.Server(0)
	}

	systemTenantTestCluster, systemTenantTestServer := createTestClusterAndServer()
	systemTenantDB := serverutils.OpenDBConn(
		t,
		systemTenantTestServer.ServingSQLAddr(),
		"",    /* useDatabase */
		false, /* insecure */
		systemTenantTestServer.Stopper(),
	)

	secondaryTenantTestCluster, secondaryTenantTestServer := createTestClusterAndServer()
	_, secondaryTenantDB := serverutils.StartTenant(
		t, secondaryTenantTestServer, base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
		},
	)
	return sqlutils.MakeSQLRunner(systemTenantDB), sqlutils.MakeSQLRunner(secondaryTenantDB), func() {
		systemTenantTestCluster.Stopper().Stop(context.Background())
		secondaryTenantTestCluster.Stopper().Stop(context.Background())
	}
}

func TestMultiTenantAdminFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc  string
		query string
	}{
		{
			desc: "ALTER RANGE x RELOCATE LEASE",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE LEASE TO 1;
`,
		},
		{
			desc: "ALTER RANGE RELOCATE LEASE",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER RANGE RELOCATE LEASE TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);
`,
		},
		{
			desc: "ALTER RANGE x RELOCATE VOTERS",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE VOTERS FROM 1 TO 1;
`,
		},
		{
			desc: "ALTER RANGE RELOCATE VOTERS",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER RANGE RELOCATE VOTERS FROM 1 TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);
`,
		},
		{
			desc: "ALTER TABLE x EXPERIMENTAL_RELOCATE LEASE",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER TABLE t EXPERIMENTAL_RELOCATE LEASE SELECT 1, 1;
`,
		},
		{
			desc: "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS SELECT ARRAY[1], 1;
`,
		},
		{
			desc: "ALTER TABLE x SPLIT AT",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER TABLE t SPLIT AT VALUES (1);
`,
		},
		{
			desc: "ALTER INDEX x SPLIT AT",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
CREATE INDEX idx on t(i);
ALTER INDEX t@idx SPLIT AT VALUES (1);
`,
		},
		{
			// todo(ewall): Support 'ALTER TABLE x SPLIT AT' first.
			desc: "ALTER TABLE x UNSPLIT AT",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER TABLE t SPLIT AT VALUES (1);
ALTER TABLE t UNSPLIT AT VALUES (1);
`,
		},
		{
			// todo(ewall): Support 'ALTER INDEX x SPLIT AT' first.
			desc: "ALTER INDEX x UNSPLIT AT",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
CREATE INDEX idx on t(i);
ALTER INDEX t@idx SPLIT AT VALUES (1);
ALTER INDEX t@idx UNSPLIT AT VALUES (1);
`,
		},
		{
			// todo(ewall): Support 'ALTER TABLE x SPLIT AT' first.
			desc: "TRUNCATE TABLE x",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER TABLE t SPLIT AT VALUES (1);
TRUNCATE TABLE t;
`,
		},
		{
			desc: "ALTER TABLE x UNSPLIT ALL",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER TABLE t UNSPLIT ALL;
`,
		},
		{
			desc: "ALTER INDEX x UNSPLIT ALL",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
CREATE INDEX idx on t(i);
ALTER INDEX t@idx UNSPLIT ALL;
`,
		},
		{
			desc: "ALTER TABLE x SCATTER",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER TABLE t SCATTER;
`,
		},
		{
			desc: "ALTER INDEX x SCATTER",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
CREATE INDEX idx on t(i);
ALTER INDEX t@idx SCATTER;
`,
		},
		{
			desc: "CONFIGURE ZONE",
			query: `
CREATE TABLE t(i int PRIMARY KEY);
ALTER TABLE t CONFIGURE ZONE DISCARD;
`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			systemTenant, secondaryTenant, cleanup := createSQLRunners(t)
			defer cleanup()

			ctx := context.Background()
			query := tc.query

			// Test system tenant.
			{
				_, err := systemTenant.DB.ExecContext(ctx, query)
				require.NoError(t, err)
			}

			// Test secondary tenant.
			{
				_, err := secondaryTenant.DB.ExecContext(ctx, query)
				require.Error(t, err)
				require.Contains(t, err.Error(), errorutil.UnsupportedWithMultiTenancyMessage)
			}
		})
	}
}
