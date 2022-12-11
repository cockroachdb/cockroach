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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

const createTable = "CREATE TABLE t(i int PRIMARY KEY);"
const rangeErrorMessage = "RangeIterator failed to seek"

type testServerCfg struct {
	base.TestClusterArgs
	numNodes int
}

func createTestServer(t *testing.T, cfg testServerCfg) (serverutils.TestServerInterface, func()) {
	testingKnobs := &cfg.ServerArgs.Knobs
	if testingKnobs.Store == nil {
		testingKnobs.Store = &kvserver.StoreTestingKnobs{}
	}
	testingKnobs.Store.(*kvserver.StoreTestingKnobs).AllowUnsynchronizedReplicationChanges = true

	numNodes := cfg.numNodes
	if numNodes == 0 {
		numNodes = 1
	}
	testCluster := serverutils.StartNewTestCluster(t, numNodes, cfg.TestClusterArgs)
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
	actualResults, err := sqlutils.RowsToStrMatrix(rows)
	require.NoErrorf(t, err, "tenant=%s", tenant)
	require.Equalf(t, len(expectedResults), len(actualResults), "tenant=%s", tenant)
	for i, actualRowResult := range actualResults {
		expectedRowResult := expectedResults[i]
		require.Equalf(t, len(expectedRowResult), len(actualRowResult), "tenant=%s row=%d", tenant, i)
		for j, actualColResult := range actualRowResult {
			expectedColResult := expectedRowResult[j]
			if expectedColResult == "" {
				require.Emptyf(t, actualColResult, "tenant=%s row=%d col=%d", tenant, i, j)
			} else {
				require.Containsf(t, actualColResult, expectedColResult, "tenant=%s row=%d col=%d", tenant, i, j)
			}
		}
	}
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
			expectedSystemResult:          [][]string{{"54", "/Table/53", "ok"}},
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
			expectedSystemResult:          [][]string{{"54", "/Table/53", "ok"}},
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
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE LEASE",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE LEASE SELECT 1, 1;",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE LEASE SkipSQLSystemTenantCheck",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE LEASE SELECT 1, 1;",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: rangeErrorMessage,
			skipSQLSystemTenantCheck:      true,
		},
		{
			desc:                          "ALTER TABLE x SPLIT AT",
			query:                         "ALTER TABLE t SPLIT AT VALUES (1);",
			expectedSystemResult:          [][]string{{"\xf0\x89\x89", "/1", "2262-04-11 23:47:16.854776 +0000 +0000"}},
			expectedSecondaryErrorMessage: "request [1 AdmSplit] not permitted",
		},
		{
			desc:                          "ALTER INDEX x SPLIT AT",
			setup:                         "CREATE INDEX idx on t(i);",
			query:                         "ALTER INDEX t@idx SPLIT AT VALUES (1);",
			expectedSystemResult:          [][]string{{"\xf0\x8a\x89", "/1", "2262-04-11 23:47:16.854776 +0000 +0000"}},
			expectedSecondaryErrorMessage: "request [1 AdmSplit] not permitted",
		},
		{
			desc:                          "ALTER TABLE x UNSPLIT AT",
			setup:                         "ALTER TABLE t SPLIT AT VALUES (1);",
			query:                         "ALTER TABLE t UNSPLIT AT VALUES (1);",
			expectedSystemResult:          [][]string{{"\xf0\x89\x89", "/1"}},
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
			expectedSystemResult:          [][]string{{"\xf0\x8a\x89", "/1"}},
			expectedSecondaryErrorMessage: "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter:          true,
		},
		{
			desc:                          "ALTER TABLE x UNSPLIT ALL",
			setup:                         "ALTER TABLE t SPLIT AT VALUES (1);",
			query:                         "ALTER TABLE t UNSPLIT ALL;",
			expectedSystemResult:          [][]string{{"\xf0\x89\x89", "/Table/104/1/1"}},
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
			expectedSystemResult:          [][]string{{"\xf0\x8a", "/Table/104/2"}, {"\xf0\x8a\x89", "/Table/104/2/1"}},
			expectedSecondaryErrorMessage: "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter:          true,
		},
		{
			desc:                          "ALTER TABLE x SCATTER",
			query:                         "ALTER TABLE t SCATTER;",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: "request [1 AdmScatter] not permitted",
		},
		{
			desc:                          "ALTER INDEX x SCATTER",
			setup:                         "CREATE INDEX idx on t(i);",
			query:                         "ALTER INDEX t@idx SCATTER;",
			expectedSystemResult:          [][]string{{"\xf0\x8a", "/1"}},
			expectedSecondaryErrorMessage: "request [1 AdmScatter] not permitted",
		},
		{
			desc:                          "CONFIGURE ZONE",
			query:                         "ALTER TABLE t CONFIGURE ZONE DISCARD;",
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
	}

	cfg := testServerCfg{}
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
					verifyResults(t, tenant, rows, expectedResults)
				} else {
					require.Errorf(t, err, "tenant=%s", tenant)
					require.Containsf(t, err.Error(), expectedErrorMessage, "tenant=%s", tenant)
				}
			}

			// Test system tenant.
			func() {
				testServer, cleanup := createTestServer(t, cfg)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				execQueries(db, "system", tc.expectedSystemResult, tc.expectedSystemErrorMessage)
			}()

			// Test secondary tenant.
			func() {
				testServer, cleanup := createTestServer(t, cfg)
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
	cfg := testServerCfg{}
	execQueries := func(db *gosql.DB, tenant string, expectedResults [][]string) {
		_, err := db.ExecContext(ctx, createTable)
		require.NoErrorf(t, err, "tenant=%s", tenant)
		_, err = db.ExecContext(ctx, "ALTER TABLE t SPLIT AT VALUES (1);")
		require.NoErrorf(t, err, "tenant=%s", tenant)
		_, err = db.ExecContext(ctx, "TRUNCATE TABLE t;")
		require.NoErrorf(t, err, "tenant=%s", tenant)
		rows, err := db.QueryContext(ctx, "SELECT start_key, end_key from [SHOW RANGES FROM TABLE t];")
		require.NoErrorf(t, err, "tenant=%s", tenant)
		verifyResults(t, tenant, rows, expectedResults)
	}

	// Test system tenant.
	func() {
		testServer, cleanup := createTestServer(t, cfg)
		defer cleanup()
		db := createSystemTenantDB(t, testServer)
		execQueries(db, "system", [][]string{{"NULL", "/1"}, {"/1", "NULL"}})
	}()

	// Test secondary tenant.
	func() {
		testServer, cleanup := createTestServer(t, cfg)
		defer cleanup()
		db := createSecondaryTenantDB(
			t,
			testServer,
			true,  /* allowSplitAndScatter */
			false, /* skipSQLSystemTenantCheck */
		)
		// TODO(ewall): Retain splits after `TRUNCATE` for secondary tenants.
		execQueries(db, "secondary", [][]string{{"NULL", "NULL"}})
	}()
}

func parseArray(array string) []string {
	return strings.Split(strings.Trim(array, "{}"), ",")
}

func TestRelocateVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc string
		// Query being tested.
		query string
		// If set, the test query must return these results for the system tenant.
		expectedSystemResult [][]string
		// If set, the test query must fail with this error message for the system tenant.
		expectedSecondaryResult [][]string
		// If set, the test query must fail with this error message for secondary tenants.
		expectedSecondaryErrorMessage string
		// Used as a stop-gap to bypass the system tenant check and test incomplete functionality.
		skipSQLSystemTenantCheck bool
	}{
		{
			desc:                          "ALTER RANGE x RELOCATE VOTERS",
			query:                         "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE VOTERS FROM %[1]s TO %[2]s;",
			expectedSystemResult:          [][]string{{"54", "/Table/53", "ok"}},
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER RANGE x RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE VOTERS FROM %[1]s TO %[2]s;",
			expectedSystemResult:     [][]string{{"54", "/Table/53", "ok"}},
			expectedSecondaryResult:  [][]string{{"55", "", rangeErrorMessage}},
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                          "ALTER RANGE RELOCATE VOTERS",
			query:                         "ALTER RANGE RELOCATE VOTERS FROM %[1]s TO %[2]s FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedSystemResult:          [][]string{{"54", "/Table/53", "ok"}},
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER RANGE RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE RELOCATE VOTERS FROM %[1]s TO %[2]s FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedSystemResult:     [][]string{{"54", "/Table/53", "ok"}},
			expectedSecondaryResult:  [][]string{{"55", "", rangeErrorMessage}},
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS SELECT ARRAY[%[1]s], %[2]s;",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS SELECT ARRAY[%[1]s], %[2]s;",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: rangeErrorMessage,
			skipSQLSystemTenantCheck:      true,
		},
	}

	const numNodes = 4
	cfg := testServerCfg{
		numNodes: numNodes,
	}
	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			execQueries := func(db *gosql.DB, tenant string, expectedResults [][]string, expectedErrorMessage string) {
				_, err := db.ExecContext(ctx, createTable)
				require.NoErrorf(t, err, "tenant=%s", tenant)
				rows, err := db.QueryContext(ctx, "SELECT replicas FROM [SHOW RANGES FROM TABLE t];")
				require.NoErrorf(t, err, "tenant=%s", tenant)
				rowNestedStrings, err := sqlutils.RowsToStrMatrix(rows)
				require.NoErrorf(t, err, "tenant=%s", tenant)
				require.Lenf(t, rowNestedStrings, 1, "tenant=%s", tenant)
				rowStrings := rowNestedStrings[0]
				require.Lenf(t, rowStrings, 1, "tenant=%s", tenant)
				actualReplicas := parseArray(rowStrings[0])
				numReplicas := len(actualReplicas)
				require.Equalf(t, numNodes-1, numReplicas, "tenant=%s", tenant)
				to := ""
				for i := 0; i < numReplicas; i++ {
					actualReplica := actualReplicas[i]
					expectedReplica := strconv.Itoa(i + 1)
					if actualReplica != expectedReplica {
						to = expectedReplica
						break
					}
				}
				if to == "" {
					to = strconv.Itoa(numNodes)
				}
				from := actualReplicas[0]
				query := fmt.Sprintf(tc.query, from, to)
				rows, err = db.QueryContext(ctx, query)
				if expectedErrorMessage == "" {
					require.NoErrorf(t, err, "tenant=%s", tenant)
					verifyResults(t, tenant, rows, expectedResults)
				} else {
					require.Errorf(t, err, "tenant=%s", tenant)
					require.Containsf(t, err.Error(), expectedErrorMessage, "tenant=%s", tenant)
				}
			}

			// Test system tenant.
			func() {
				testServer, cleanup := createTestServer(t, cfg)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				execQueries(db, "system", tc.expectedSystemResult, "")
			}()

			// Test secondary tenant.
			func() {
				testServer, cleanup := createTestServer(t, cfg)
				defer cleanup()
				db := createSecondaryTenantDB(
					t,
					testServer,
					false, /* allowSplitAndScatter */
					tc.skipSQLSystemTenantCheck,
				)
				execQueries(db, "secondary", tc.expectedSecondaryResult, tc.expectedSecondaryErrorMessage)
			}()
		})
	}
}

func TestRelocateNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc string
		// Query being tested.
		query string
		// If set, the test query must return these results for the system tenant.
		expectedSystemResult [][]string
		// If set, the test query must fail with this error message for the system tenant.
		expectedSecondaryResult    [][]string
		expectedSystemErrorMessage string
		// If set, the test query must fail with this error message for secondary tenants.
		expectedSecondaryErrorMessage string
		// Used as a stop-gap to bypass the system tenant check and test incomplete functionality.
		skipSQLSystemTenantCheck bool
	}{
		{
			desc:                          "ALTER RANGE x RELOCATE NONVOTERS",
			query:                         "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE NONVOTERS FROM %[1]s TO %[2]s;",
			expectedSystemResult:          [][]string{{"54", "/Table/53", "ok"}},
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER RANGE x RELOCATE NONVOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE NONVOTERS FROM %[1]s TO %[2]s;",
			expectedSystemResult:     [][]string{{"54", "/Table/53", "ok"}},
			expectedSecondaryResult:  [][]string{{"55", "", rangeErrorMessage}},
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                          "ALTER RANGE RELOCATE NONVOTERS",
			query:                         "ALTER RANGE RELOCATE NONVOTERS FROM %[1]s TO %[2]s FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedSystemResult:          [][]string{{"54", "/Table/53", "ok"}},
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                     "ALTER RANGE RELOCATE NONVOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE RELOCATE NONVOTERS FROM %[1]s TO %[2]s FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			expectedSystemResult:     [][]string{{"54", "/Table/53", "ok"}},
			expectedSecondaryResult:  [][]string{{"55", "", rangeErrorMessage}},
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE NONVOTERS",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE NONVOTERS SELECT ARRAY[%[1]s], %[2]s;",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE NONVOTERS SkipSQLSystemTenantCheck",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE NONVOTERS SELECT ARRAY[%[1]s], %[2]s;",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: rangeErrorMessage,
			skipSQLSystemTenantCheck:      true,
		},
	}

	const numNodes = 5
	numReplicas := 3
	zoneCfg := zonepb.DefaultZoneConfig()
	zoneCfg.NumReplicas = proto.Int32(int32(numReplicas))
	zoneCfg.NumVoters = proto.Int32(1)
	cfg := testServerCfg{
		numNodes: numNodes,
		TestClusterArgs: base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						DefaultZoneConfigOverride:       &zoneCfg,
						DefaultSystemZoneConfigOverride: &zoneCfg,
					},
				},
			},
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			execQueries := func(db *gosql.DB, tenant string, expectedResults [][]string, expectedErrorMessage string) {
				_, err := db.ExecContext(ctx, createTable)
				require.NoErrorf(t, err, "tenant=%s", tenant)
				var actualReplicas, actualNonVotingReplicas []string
				// Wait until numReplicas equals actualNumReplicas.
				testutils.SucceedsSoon(t, func() error {
					rows, err := db.QueryContext(ctx, "SELECT replicas, non_voting_replicas FROM [SHOW RANGES FROM TABLE t]")
					require.NoErrorf(t, err, "tenant=%s", tenant)
					rowNestedStrings, err := sqlutils.RowsToStrMatrix(rows)
					require.NoErrorf(t, err, "tenant=%s", tenant)
					require.Lenf(t, rowNestedStrings, 1, "tenant=%s", tenant)
					rowStrings := rowNestedStrings[0]
					require.Lenf(t, rowStrings, 2, "tenant=%s", tenant)
					actualReplicas = parseArray(rowStrings[0])
					actualNonVotingReplicas = parseArray(rowStrings[1])
					if len(actualNonVotingReplicas) == 1 && actualNonVotingReplicas[0] == "" {
						return errors.Newf("actualNonVotingReplicas=%s is empty tenant=%s", actualNonVotingReplicas, tenant)
					}
					actualNumReplicas := len(actualReplicas)
					if numReplicas != actualNumReplicas {
						return errors.Newf("expectedNumReplicas=%s does not equal actualNumReplicas=%s tenant=%s", numReplicas, actualReplicas, tenant)
					}
					return nil
				})
				toReplica := ""
				for i := 0; i < numReplicas; i++ {
					actualReplica := actualReplicas[i]
					expectedReplica := strconv.Itoa(i + 1)
					if actualReplica != expectedReplica {
						toReplica = expectedReplica
						break
					}
				}
				if toReplica == "" {
					toReplica = strconv.Itoa(numNodes)
				}
				fromReplica := actualNonVotingReplicas[0]
				query := fmt.Sprintf(tc.query, fromReplica, toReplica)
				rows, err := db.QueryContext(ctx, query)
				if expectedErrorMessage == "" {
					require.NoErrorf(t, err, "tenant=%s", tenant)
					verifyResults(t, tenant, rows, expectedResults)
				} else {
					require.Errorf(t, err, "tenant=%s", tenant)
					require.Containsf(t, err.Error(), expectedErrorMessage, "tenant=%s", tenant)
				}
			}

			// Test system tenant.
			func() {
				testServer, cleanup := createTestServer(t, cfg)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				execQueries(db, "system", tc.expectedSystemResult, tc.expectedSystemErrorMessage)
			}()

			// Test secondary tenant.
			func() {
				testServer, cleanup := createTestServer(t, cfg)
				defer cleanup()
				db := createSecondaryTenantDB(
					t,
					testServer,
					false, /* allowSplitAndScatter */
					tc.skipSQLSystemTenantCheck,
				)
				execQueries(db, "secondary", tc.expectedSecondaryResult, tc.expectedSecondaryErrorMessage)
			}()
		})
	}
}
