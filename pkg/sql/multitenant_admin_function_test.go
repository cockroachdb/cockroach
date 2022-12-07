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
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

const createTable = "CREATE TABLE t(i int PRIMARY KEY);"
const rangeErrorMessage = "RangeIterator failed to seek"

func createTestServer(t *testing.T) (serverutils.TestServerInterface, func()) {
	testCluster := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	return testCluster.Server(0), func() {
		testCluster.Stopper().Stop(context.Background())
	}
}

func createSystemTenantDB(t *testing.T, testServer serverutils.TestServerInterface) *gosql.DB {
	return serverutils.OpenDBConn(
		t,
		testServer.ServingSQLAddr(),
		"",    /* useDatabase */
		false, /* insecure */
		testServer.Stopper(),
	)
}

func createSecondaryTenantDB(
	t *testing.T,
	testServer serverutils.TestServerInterface,
	allowSplitAndScatter bool,
	skipSQLSystemTenantCheck bool,
) *gosql.DB {
	_, db := serverutils.StartTenant(
		t, testServer, base.TestTenantArgs{
			TestingKnobs: base.TestingKnobs{
				TenantTestingKnobs: &TenantTestingKnobs{
					AllowSplitAndScatter:      allowSplitAndScatter,
					SkipSQLSystemTentantCheck: skipSQLSystemTenantCheck,
				},
			},
			TenantID: serverutils.TestTenantID(),
		},
	)
	return db
}

func TestMultiTenantAdminFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(ewall): When moving tenant capability check to KV,
	//  merge test w/ SkipSQLSystemTenantCheck variant into single test case if
	//  1) test case has expectedErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage
	//  and
	//  2) SkipSQLSystemTenantCheck variant has no error message
	testCases := []struct {
		desc                     string
		setup                    string
		setups                   []string
		query                    string
		expectedErrorMessage     string
		allowSplitAndScatter     bool
		skipSQLSystemTenantCheck bool
	}{
		{
			desc:                 "ALTER RANGE x RELOCATE LEASE",
			query:                "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE LEASE TO 1;",
			expectedErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER RANGE x RELOCATE LEASE SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE LEASE TO 1;",
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                 "ALTER RANGE RELOCATE LEASE",
			query:                "ALTER RANGE RELOCATE LEASE TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER RANGE RELOCATE LEASE SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE RELOCATE LEASE TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                 "ALTER RANGE x RELOCATE VOTERS",
			query:                "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE VOTERS FROM 1 TO 1;",
			expectedErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER RANGE x RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE VOTERS FROM 1 TO 1;",
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                 "ALTER RANGE RELOCATE VOTERS",
			query:                "ALTER RANGE RELOCATE VOTERS FROM 1 TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER RANGE RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE RELOCATE VOTERS FROM 1 TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                 "ALTER TABLE x EXPERIMENTAL_RELOCATE LEASE",
			query:                "ALTER TABLE t EXPERIMENTAL_RELOCATE LEASE SELECT 1, 1;",
			expectedErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER TABLE x EXPERIMENTAL_RELOCATE LEASE SkipSQLSystemTenantCheck",
			query:                    "ALTER TABLE t EXPERIMENTAL_RELOCATE LEASE SELECT 1, 1;",
			expectedErrorMessage:     rangeErrorMessage,
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                 "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS",
			query:                "ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS SELECT ARRAY[1], 1;",
			expectedErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS SELECT ARRAY[1], 1;",
			expectedErrorMessage:     rangeErrorMessage,
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                 "ALTER TABLE x SPLIT AT",
			query:                "ALTER TABLE t SPLIT AT VALUES (1);",
			expectedErrorMessage: "request [1 AdmSplit] not permitted",
		},
		{
			desc:                 "ALTER INDEX x SPLIT AT",
			setup:                "CREATE INDEX idx on t(i);",
			query:                "ALTER INDEX t@idx SPLIT AT VALUES (1);",
			expectedErrorMessage: "request [1 AdmSplit] not permitted",
		},
		{
			desc:                 "ALTER TABLE x UNSPLIT AT",
			setup:                "ALTER TABLE t SPLIT AT VALUES (1);",
			query:                "ALTER TABLE t UNSPLIT AT VALUES (1);",
			expectedErrorMessage: "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter: true,
		},
		{
			desc: "ALTER INDEX x UNSPLIT AT",
			// Multiple setup statements required because of https://github.com/cockroachdb/cockroach/issues/90535.
			setups: []string{
				"CREATE INDEX idx on t(i);",
				"ALTER INDEX t@idx SPLIT AT VALUES (1);",
			},
			query:                "ALTER INDEX t@idx UNSPLIT AT VALUES (1);",
			expectedErrorMessage: "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter: true,
		},
		{
			desc:                 "ALTER TABLE x UNSPLIT ALL",
			setup:                "ALTER TABLE t SPLIT AT VALUES (1);",
			query:                "ALTER TABLE t UNSPLIT ALL;",
			expectedErrorMessage: "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter: true,
		},
		{
			desc: "ALTER INDEX x UNSPLIT ALL",
			// Multiple setup statements required because of https://github.com/cockroachdb/cockroach/issues/90535.
			setups: []string{
				"CREATE INDEX idx on t(i);",
				"ALTER INDEX t@idx SPLIT AT VALUES (1);",
			},
			query:                "ALTER INDEX t@idx UNSPLIT ALL;",
			expectedErrorMessage: "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter: true,
		},
		{
			desc:                 "ALTER TABLE x SCATTER",
			query:                "ALTER TABLE t SCATTER;",
			expectedErrorMessage: "request [1 AdmScatter] not permitted",
		},
		{
			desc:                 "ALTER INDEX x SCATTER",
			setup:                "CREATE INDEX idx on t(i);",
			query:                "ALTER INDEX t@idx SCATTER;",
			expectedErrorMessage: "request [1 AdmScatter] not permitted",
		},
		{
			desc:                 "CONFIGURE ZONE",
			query:                "ALTER TABLE t CONFIGURE ZONE DISCARD;",
			expectedErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			execQueries := func(db *gosql.DB) error {
				setups := tc.setups
				setup := tc.setup
				if setup != "" {
					setups = []string{setup}
				}
				setups = append([]string{createTable}, setups...)
				for _, setup := range setups {
					_, err := db.ExecContext(ctx, setup)
					require.NoError(t, err, setup)
				}
				_, err := db.ExecContext(ctx, tc.query)
				return err
			}

			// Test system tenant.
			func() {
				testServer, cleanup := createTestServer(t)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				err := execQueries(db)
				require.NoError(t, err)
			}()

			// Test secondary tenant.
			func() {
				testServer, cleanup := createTestServer(t)
				defer cleanup()
				db := createSecondaryTenantDB(t, testServer, tc.allowSplitAndScatter, tc.skipSQLSystemTenantCheck)
				err := execQueries(db)
				expectedErrorMessage := tc.expectedErrorMessage
				if expectedErrorMessage == "" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), expectedErrorMessage)
				}
			}()
		})
	}
}

func TestTruncateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	execQueries := func(db *gosql.DB, expectedStartKeys []string, expectedEndKeys []string) {
		_, err := db.ExecContext(ctx, createTable)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "ALTER TABLE t SPLIT AT VALUES (1);")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "TRUNCATE TABLE t;")
		require.NoError(t, err)
		rows, err := db.QueryContext(ctx, "SELECT start_key, end_key from [SHOW RANGES FROM TABLE t];")
		require.NoError(t, err)
		i := 0
		for ; rows.Next(); i++ {
			var startKey, endKey gosql.NullString
			err := rows.Scan(&startKey, &endKey)
			require.NoError(t, err, i)
			require.Less(t, i, len(expectedStartKeys), i)
			require.Equal(t, startKey.String, expectedStartKeys[i], i)
			require.Less(t, i, len(expectedEndKeys), i)
			require.Equal(t, endKey.String, expectedEndKeys[i], i)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, len(expectedStartKeys), i)
		require.Equal(t, len(expectedEndKeys), i)
	}

	// Test system tenant.
	func() {
		testServer, cleanup := createTestServer(t)
		defer cleanup()
		db := createSystemTenantDB(t, testServer)
		execQueries(db, []string{"", "/1"}, []string{"/1", ""})
	}()

	// Test secondary tenant.
	func() {
		testServer, cleanup := createTestServer(t)
		defer cleanup()
		db := createSecondaryTenantDB(
			t,
			testServer,
			true,  /* allowSplitAndScatter */
			false, /* skipSQLSystemTenantCheck */
		)
		// TODO(ewall): Retain splits after `TRUNCATE` for secondary tenants.
		execQueries(db, []string{""}, []string{""})
	}()
}
