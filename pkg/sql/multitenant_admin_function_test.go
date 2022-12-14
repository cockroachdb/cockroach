// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

const createTable = "CREATE TABLE t(i int PRIMARY KEY);"
const rangeErrorMessage = "RangeIterator failed to seek"

func createTestServer(t *testing.T) (serverutils.TestServerInterface, func()) {
	testCluster := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					AllowUnsynchronizedReplicationChanges: true,
				},
			},
		},
	})
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
				TenantTestingKnobs: &sql.TenantTestingKnobs{
					AllowSplitAndScatter:      allowSplitAndScatter,
					SkipSQLSystemTentantCheck: skipSQLSystemTenantCheck,
				},
			},
			TenantID: serverutils.TestTenantID(),
		},
	)
	return db
}

func verifyResults(t *testing.T, tenant string, rows *gosql.Rows, expectedResults [][]string) {
	i := 0
	columns, err := rows.Columns()
	require.NoErrorf(t, err, "tenant=%s", tenant)
	actualNumColumns := len(columns)
	for ; rows.Next(); i++ {
		require.Greaterf(t, len(expectedResults), i, "tenant=%s row=%d", tenant, i)
		expectedRowResult := expectedResults[i]
		require.Equal(t, len(expectedRowResult), actualNumColumns, "tenant=%s row=%d", tenant, i)
		actualRowResult := make([]interface{}, actualNumColumns)
		for i := range actualRowResult {
			actualRowResult[i] = new(gosql.NullString)
		}
		err := rows.Scan(actualRowResult...)
		require.NoErrorf(t, err, "tenant=%s row=%d", tenant, i)
		for j := 0; j < actualNumColumns; j++ {
			actualColResult := actualRowResult[j].(*gosql.NullString).String
			expectedColResult := expectedRowResult[j]
			if expectedColResult == "" {
				require.Emptyf(t, actualColResult, "tenant=%s row=%d col=%d", tenant, i, j)
			} else {
				require.Containsf(t, actualColResult, expectedColResult, "tenant=%s row=%d col=%d", tenant, i, j)
			}
		}
	}
	require.NoErrorf(t, rows.Err(), "tenant=%s", tenant)
	require.Equalf(t, len(expectedResults), i, "tenant=%s", tenant)
}

func TestMultiTenantAdminFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(ewall): When moving tenant capability check to KV,
	//  merge test w/ SkipSQLSystemTenantCheck variant into single test case if
	//  1) test case has expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage
	//  and
	//  2) SkipSQLSystemTenantCheck variant has no error result AND no error inside "success" result
	testCases := []struct {
		desc string
		// Prereq setup statement(s) that must succeed.
		setup string
		// Multiple setup statements required because of https://github.com/cockroachdb/cockroach/issues/90535.
		setups []string
		// Query being tested.
		query string
		// If set, the test query must return these results for the system tenant.
		expectedSystemResult [][]string
		// If set, the test query must fail with this error message for the system tenant.
		expectedSystemErrorMessage string
		// If set, the test query must return these results for secondary tenants.
		expectedSecondaryResult [][]string
		// If set, the test query must fail with this error message for secondary tenants.
		expectedSecondaryErrorMessage string
		// Used for tests that have SPLIT AT as a prereq (eg UNSPLIT AT).
		allowSplitAndScatter bool
		// Used as a stop-gap to bypass the system tenant check and test incomplete functionality.
		skipSQLSystemTenantCheck bool
	}{
		{
			desc:                          "ALTER RANGE x RELOCATE LEASE",
			query:                         "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE LEASE TO 1;",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER RANGE x RELOCATE LEASE SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE LEASE TO 1;",
			expectedSystemResult:     [][]string{{"54", "/Table/53", "ok"}},
			expectedSecondaryResult:  [][]string{{"55", "", rangeErrorMessage}},
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                          "ALTER RANGE RELOCATE LEASE",
			query:                         "ALTER RANGE RELOCATE LEASE TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER RANGE RELOCATE LEASE SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE RELOCATE LEASE TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedSystemResult:     [][]string{{"54", "/Table/53", "ok"}},
			expectedSecondaryResult:  [][]string{{"55", "", rangeErrorMessage}},
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                          "ALTER RANGE x RELOCATE VOTERS",
			query:                         "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE VOTERS FROM 1 TO 1;",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		// TODO(ewall): Set up test to avoid "trying to add a voter to a store that already has a VOTER_FULL" error.
		{
			desc:                     "ALTER RANGE x RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE VOTERS FROM 1 TO 1;",
			expectedSystemResult:     [][]string{{"54", "/Table/53", "trying to add a voter to a store that already has a VOTER_FULL"}},
			expectedSecondaryResult:  [][]string{{"55", "", rangeErrorMessage}},
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                          "ALTER RANGE RELOCATE VOTERS",
			query:                         "ALTER RANGE RELOCATE VOTERS FROM 1 TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		// TODO(ewall): Set up test to avoid "trying to add a voter to a store that already has a VOTER_FULL" error.
		{
			desc:                     "ALTER RANGE RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE RELOCATE VOTERS FROM 1 TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedSystemResult:     [][]string{{"54", "/Table/53", "trying to add a voter to a store that already has a VOTER_FULL"}},
			expectedSecondaryResult:  [][]string{{"55", "", rangeErrorMessage}},
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                          "ALTER RANGE x RELOCATE NONVOTERS",
			query:                         "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE NONVOTERS FROM 1 TO 1;",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		// TODO(ewall): Set up test to avoid "type of replica being removed (VOTER_FULL) does not match expectation for change" error.
		{
			desc:                     "ALTER RANGE x RELOCATE NONVOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE NONVOTERS FROM 1 TO 1;",
			expectedSystemResult:     [][]string{{"54", "/Table/53", "type of replica being removed (VOTER_FULL) does not match expectation for change"}},
			expectedSecondaryResult:  [][]string{{"55", "", rangeErrorMessage}},
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                          "ALTER RANGE RELOCATE NONVOTERS",
			query:                         "ALTER RANGE RELOCATE NONVOTERS FROM 1 TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		// TODO(ewall): Set up test to avoid "type of replica being removed (VOTER_FULL) does not match expectation for change" error.
		{
			desc:                     "ALTER RANGE RELOCATE NONVOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE RELOCATE NONVOTERS FROM 1 TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedSystemResult:     [][]string{{"54", "/Table/53", "type of replica being removed (VOTER_FULL) does not match expectation for change"}},
			expectedSecondaryResult:  [][]string{{"55", "", rangeErrorMessage}},
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE LEASE",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE LEASE SELECT 1, 1;",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE LEASE SkipSQLSystemTenantCheck",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE LEASE SELECT 1, 1;",
			expectedSecondaryErrorMessage: rangeErrorMessage,
			skipSQLSystemTenantCheck:      true,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS SELECT ARRAY[1], 1;",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS SELECT ARRAY[1], 1;",
			expectedSecondaryErrorMessage: rangeErrorMessage,
			skipSQLSystemTenantCheck:      true,
		},
		// TODO(ewall): Set up test to avoid "list of voter targets overlaps with the list of non-voter targets" error.
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE NONVOTERS",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE NONVOTERS SELECT ARRAY[1], 1;",
			expectedSystemErrorMessage:    "list of voter targets overlaps with the list of non-voter targets",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		// TODO(ewall): Set up test to avoid "list of voter targets overlaps with the list of non-voter targets" error.
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE NONVOTERS SkipSQLSystemTenantCheck",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE NONVOTERS SELECT ARRAY[1], 1;",
			expectedSystemErrorMessage:    "list of voter targets overlaps with the list of non-voter targets",
			expectedSecondaryErrorMessage: rangeErrorMessage,
			skipSQLSystemTenantCheck:      true,
		},
		{
			desc:                          "ALTER TABLE x SPLIT AT",
			query:                         "ALTER TABLE t SPLIT AT VALUES (1);",
			expectedSecondaryErrorMessage: "request [1 AdmSplit] not permitted",
		},
		{
			desc:                          "ALTER INDEX x SPLIT AT",
			setup:                         "CREATE INDEX idx on t(i);",
			query:                         "ALTER INDEX t@idx SPLIT AT VALUES (1);",
			expectedSecondaryErrorMessage: "request [1 AdmSplit] not permitted",
		},
		{
			desc:                          "ALTER TABLE x UNSPLIT AT",
			setup:                         "ALTER TABLE t SPLIT AT VALUES (1);",
			query:                         "ALTER TABLE t UNSPLIT AT VALUES (1);",
			expectedSecondaryErrorMessage: "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter:          true,
		},
		{
			desc: "ALTER INDEX x UNSPLIT AT",
			setups: []string{
				"CREATE INDEX idx on t(i);",
				"ALTER INDEX t@idx SPLIT AT VALUES (1);",
			},
			query:                         "ALTER INDEX t@idx UNSPLIT AT VALUES (1);",
			expectedSecondaryErrorMessage: "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter:          true,
		},
		{
			desc:                          "ALTER TABLE x UNSPLIT ALL",
			setup:                         "ALTER TABLE t SPLIT AT VALUES (1);",
			query:                         "ALTER TABLE t UNSPLIT ALL;",
			expectedSecondaryErrorMessage: "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter:          true,
		},
		{
			desc: "ALTER INDEX x UNSPLIT ALL",
			setups: []string{
				"CREATE INDEX idx on t(i);",
				"ALTER INDEX t@idx SPLIT AT VALUES (1);",
			},
			query:                         "ALTER INDEX t@idx UNSPLIT ALL;",
			expectedSecondaryErrorMessage: "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter:          true,
		},
		{
			desc:                          "ALTER TABLE x SCATTER",
			query:                         "ALTER TABLE t SCATTER;",
			expectedSecondaryErrorMessage: "request [1 AdmScatter] not permitted",
		},
		{
			desc:                          "ALTER INDEX x SCATTER",
			setup:                         "CREATE INDEX idx on t(i);",
			query:                         "ALTER INDEX t@idx SCATTER;",
			expectedSecondaryErrorMessage: "request [1 AdmScatter] not permitted",
		},
		{
			desc:                          "CONFIGURE ZONE",
			query:                         "ALTER TABLE t CONFIGURE ZONE DISCARD;",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			execQueries := func(db *gosql.DB, tenant string, expectedResults [][]string, expectedErrorMessage string) {
				setups := tc.setups
				setup := tc.setup
				if setup != "" {
					setups = []string{setup}
				}
				setups = append([]string{createTable}, setups...)
				for _, setup := range setups {
					_, err := db.ExecContext(ctx, setup)
					require.NoErrorf(t, err, setup, "tenant=%s setup=%s", tenant, setup)
				}
				rows, err := db.QueryContext(ctx, tc.query)
				if expectedErrorMessage == "" {
					require.NoErrorf(t, err, "tenant=%s", tenant)
					if len(expectedResults) > 0 {
						verifyResults(t, tenant, rows, expectedResults)
					}
				} else {
					require.Errorf(t, err, "tenant=%s", tenant)
					require.Containsf(t, err.Error(), expectedErrorMessage, "tenant=%s", tenant)
				}
			}

			// Test system tenant.
			func() {
				testServer, cleanup := createTestServer(t)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				execQueries(db, "system", tc.expectedSystemResult, tc.expectedSystemErrorMessage)
			}()

			// Test secondary tenant.
			func() {
				testServer, cleanup := createTestServer(t)
				defer cleanup()
				db := createSecondaryTenantDB(t, testServer, tc.allowSplitAndScatter, tc.skipSQLSystemTenantCheck)
				execQueries(db, "secondary", tc.expectedSecondaryResult, tc.expectedSecondaryErrorMessage)
			}()
		})
	}
}

func TestTruncateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	execQueries := func(db *gosql.DB, tenant string, expectedResults [][]string) {
		_, err := db.ExecContext(ctx, createTable)
		require.NoErrorf(t, err, "tenant=%s", tenant)
		_, err = db.ExecContext(ctx, "ALTER TABLE t SPLIT AT VALUES (1);")
		require.NoErrorf(t, err, "tenant=%s", tenant)
		_, err = db.ExecContext(ctx, "TRUNCATE TABLE t;")
		require.NoErrorf(t, err, "tenant=%s", tenant)
		rows, err := db.QueryContext(ctx, "SELECT start_key, end_key from [SHOW RANGES FROM INDEX t@primary];")
		require.NoErrorf(t, err, "tenant=%s", tenant)
		verifyResults(t, tenant, rows, expectedResults)
	}

	// Test system tenant.
	func() {
		testServer, cleanup := createTestServer(t)
		defer cleanup()
		db := createSystemTenantDB(t, testServer)
		execQueries(db, "system", [][]string{{"", "/1"}, {"/1", ""}})
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
		execQueries(db, "secondary", [][]string{{"", "/104/2/1"}, {"/104/2/1", ""}})
	}()
}
