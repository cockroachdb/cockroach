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
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
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

type testClusterCfg struct {
	base.TestClusterArgs
	numNodes int
}

func createTestClusterArgs(numReplicas, numVoters int32) base.TestClusterArgs {
	zoneCfg := zonepb.DefaultZoneConfig()
	zoneCfg.NumReplicas = proto.Int32(numReplicas)
	zoneCfg.NumVoters = proto.Int32(numVoters)
	return base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DefaultZoneConfigOverride:       &zoneCfg,
					DefaultSystemZoneConfigOverride: &zoneCfg,
				},
			},
		},
	}
}

func createTestCluster(
	t *testing.T, cfg testClusterCfg,
) (serverutils.TestClusterInterface, serverutils.TestServerInterface, func()) {
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
	return testCluster, testCluster.Server(0), func() {
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

func verifyResults(t *testing.T, message string, rows *gosql.Rows, expectedResults [][]string) {
	actualResults, err := sqlutils.RowsToStrMatrix(rows)
	require.NoErrorf(t, err, message)
	require.Equalf(t, len(expectedResults), len(actualResults), message)
	for i, actualRowResult := range actualResults {
		expectedRowResult := expectedResults[i]
		require.Equalf(t, len(expectedRowResult), len(actualRowResult), "%s row=%d", message, i)
		for j, actualColResult := range actualRowResult {
			expectedColResult := expectedRowResult[j]
			if expectedColResult == "" {
				require.Emptyf(t, actualColResult, "%s row=%d col=%d", message, i, j)
			} else {
				require.Containsf(t, actualColResult, expectedColResult, "%s row=%d col=%d", message, i, j)
			}
		}
	}
}

// replicaState stores the parsed output of SHOW RANGES.
type replicaState struct {
	leaseholder       roachpb.NodeID
	replicas          []roachpb.NodeID
	votingReplicas    []roachpb.NodeID
	nonVotingReplicas []roachpb.NodeID
}

// stringToNodeID is used with RowsToStrMatrix to convert a result row column to a roachpb.NodeID.
func stringToNodeID(t *testing.T, nodeIDString string, message string) roachpb.NodeID {
	nodeIDInt, err := strconv.Atoi(nodeIDString)
	require.NoErrorf(t, err, message)
	return roachpb.NodeID(nodeIDInt)
}

// stringToNodeID is used with RowsToStrMatrix to convert a result row array column to a []roachpb.NodeID.
func arrayStringToSortedNodeIDs(t *testing.T, array string, message string) []roachpb.NodeID {
	nodeIDStrings := sqltestutils.ArrayStringToSlice(t, array, message)
	nodeIDs := make([]roachpb.NodeID, len(nodeIDStrings))
	for i, nodeIDString := range nodeIDStrings {
		nodeIDs[i] = stringToNodeID(t, nodeIDString, message)
	}
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})
	return nodeIDs
}

// getReplicaState returns the state of {all,voting,nonvoting} replicas in the cluster.
// The state is initialized async so this function blocks until it matches the expected inputs.
func getReplicaState(
	t *testing.T,
	ctx context.Context,
	db *gosql.DB,
	expectedNumReplicas int,
	expectedNumVotingReplicas int,
	expectedNumNonVotingReplicas int,
	message string,
) replicaState {
	checkCount := func(kind string, numExpected int, actual []roachpb.NodeID) error {
		numActual := len(actual)
		if numExpected != numActual {
			return errors.Newf(
				"%s expected=%d actual=%d %s",
				kind, numExpected, numActual, message,
			)
		}
		return nil
	}

	var (
		leaseholder                                 roachpb.NodeID
		replicas, votingReplicas, nonVotingReplicas []roachpb.NodeID
	)
	// Actual #replicas can be < expected #replicas even after WaitForFullReplication.
	testutils.SucceedsSoon(t, func() error {
		rows, err := db.QueryContext(ctx, `
SELECT lease_holder, replicas, voting_replicas, non_voting_replicas
FROM [SHOW RANGES FROM INDEX t@primary WITH DETAILS]`)
		require.NoErrorf(t, err, message)
		rowNestedStrings, err := sqlutils.RowsToStrMatrix(rows)
		require.NoErrorf(t, err, message)
		require.Lenf(t, rowNestedStrings, 1, message)
		rowStrings := rowNestedStrings[0]
		leaseholder = stringToNodeID(t, rowStrings[0], message)
		replicas = arrayStringToSortedNodeIDs(t, rowStrings[1], message)
		if err := checkCount("numReplicas", expectedNumReplicas, replicas); err != nil {
			return err
		}
		votingReplicas = arrayStringToSortedNodeIDs(t, rowStrings[2], message)
		if err := checkCount("numVotingReplicas", expectedNumVotingReplicas, votingReplicas); err != nil {
			return err
		}
		nonVotingReplicas = arrayStringToSortedNodeIDs(t, rowStrings[3], message)
		if err := checkCount("numNonVotingReplicas", expectedNumNonVotingReplicas, nonVotingReplicas); err != nil {
			return err
		}
		return nil
	})
	return replicaState{
		leaseholder:       leaseholder,
		replicas:          replicas,
		votingReplicas:    votingReplicas,
		nonVotingReplicas: nonVotingReplicas,
	}
}

// getToReplica determines the "to" node for a relocate command by finding the NodeID
// in clusterNodeIDs that is not in replicaNodeIDs.
func getToReplica(clusterNodeIDs []roachpb.NodeID, replicaNodeIDs []roachpb.NodeID) roachpb.NodeID {
	// clusterNodeIDs and replicaNodeIDs must be sorted for this to work.
	for i, clusterNodeID := range clusterNodeIDs {
		if len(replicaNodeIDs) == i || clusterNodeID != replicaNodeIDs[i] {
			return clusterNodeID
		}
	}
	panic(fmt.Sprintf("all replica nodes exist in cluster %s", clusterNodeIDs))
}

// getReplicaStateMessage generates useful debug info containing the state of
// the cluster and the from/to NodeIDs for relocation.
func getReplicaStateMessage(
	tenant string,
	query string,
	replicaState replicaState,
	fromReplica roachpb.NodeID,
	toReplica roachpb.NodeID,
) string {
	return fmt.Sprintf("tenant=%s query=`%s` leaseholder=%s replicas=%s voting_replicas=%s non_voting_replicas=%s fromReplica=%s toReplica=%s",
		tenant, query, replicaState.leaseholder, replicaState.replicas, replicaState.votingReplicas, replicaState.nonVotingReplicas, fromReplica, toReplica)
}

// nodeIDsToArrayString generates a string for input into EXPERIMENTAL_RELOCATE.
func nodeIDsToArrayString(nodeIDs []roachpb.NodeID) string {
	nodeIDStrings := make([]string, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		nodeIDStrings[i] = nodeID.String()
	}
	return strings.Join(nodeIDStrings, ",")
}

// TestMultiTenantAdminFunction tests the "simple" admin functions that do not require complex setup.
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

	cfg := testClusterCfg{}
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
				message := fmt.Sprintf("tenant=%s", tenant)
				for _, setup := range setups {
					_, err := db.ExecContext(ctx, setup)
					require.NoErrorf(t, err, setup, "%s setup=%s", message, setup)
				}
				rows, err := db.QueryContext(ctx, tc.query)
				if expectedErrorMessage == "" {
					require.NoErrorf(t, err, message)
					verifyResults(t, message, rows, expectedResults)
				} else {
					require.Errorf(t, err, message)
					require.Containsf(t, err.Error(), expectedErrorMessage, message)
				}
			}

			// Test system tenant.
			func() {
				_, testServer, cleanup := createTestCluster(t, cfg)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				execQueries(db, "system", tc.expectedSystemResult, tc.expectedSystemErrorMessage)
			}()

			// Test secondary tenant.
			func() {
				_, testServer, cleanup := createTestCluster(t, cfg)
				defer cleanup()
				db := createSecondaryTenantDB(t, testServer, tc.allowSplitAndScatter, tc.skipSQLSystemTenantCheck)
				execQueries(db, "secondary", tc.expectedSecondaryResult, tc.expectedSecondaryErrorMessage)
			}()
		})
	}
}

// TestTruncateTable tests that range splits are retained after a table is truncated.
func TestTruncateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cfg := testClusterCfg{}
	ctx := context.Background()
	execQueries := func(db *gosql.DB, tenant string, expectedResults [][]string) {
		_, err := db.ExecContext(ctx, createTable)
		message := fmt.Sprintf("tenant=%s", tenant)
		require.NoErrorf(t, err, message)
		_, err = db.ExecContext(ctx, "ALTER TABLE t SPLIT AT VALUES (1);")
		require.NoErrorf(t, err, message)
		_, err = db.ExecContext(ctx, "TRUNCATE TABLE t;")
		require.NoErrorf(t, err, message)
		rows, err := db.QueryContext(ctx, "SELECT start_key, end_key from [SHOW RANGES FROM INDEX t@primary];")
		require.NoErrorf(t, err, message)
		verifyResults(t, message, rows, expectedResults)
	}

	// Test system tenant.
	func() {
		_, testServer, cleanup := createTestCluster(t, cfg)
		defer cleanup()
		db := createSystemTenantDB(t, testServer)
		execQueries(db, "system", [][]string{
			{"<before:/Table/104/1/1>", "…/1"},
			{"…/1", "<after:/Max>"}})
	}()

	// Test secondary tenant.
	func() {
		_, testServer, cleanup := createTestCluster(t, cfg)
		defer cleanup()
		db := createSecondaryTenantDB(
			t,
			testServer,
			true,  /* allowSplitAndScatter */
			false, /* skipSQLSystemTenantCheck */
		)
		execQueries(db, "secondary", [][]string{
			{"<before:/Tenant/10/Table/104/1/1>", "…/104/2/1"},
			{"…/104/2/1", "<after:/Max>"},
		})
	}()
}

// TestRelocateVoters tests that a range can be relocated from a
// non-leaseholder VOTER replica node to a non-replica node via RELOCATE.
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
	}

	const (
		numNodes                     = 4
		expectedNumVotingReplicas    = 3
		expectedNumNonVotingReplicas = 0
		expectedNumReplicas          = expectedNumVotingReplicas + expectedNumNonVotingReplicas
	)
	cfg := testClusterCfg{
		numNodes:        numNodes,
		TestClusterArgs: createTestClusterArgs(expectedNumReplicas, expectedNumVotingReplicas),
	}
	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			execQueries := func(testCluster serverutils.TestClusterInterface, db *gosql.DB, tenant string, expectedResults [][]string, expectedErrorMessage string) {
				_, err := db.ExecContext(ctx, createTable)
				message := fmt.Sprintf("tenant=%s", tenant)
				require.NoErrorf(t, err, message)
				err = testCluster.WaitForFullReplication()
				require.NoErrorf(t, err, message)
				replicaState := getReplicaState(
					t,
					ctx,
					db,
					expectedNumReplicas,
					expectedNumVotingReplicas,
					expectedNumNonVotingReplicas,
					message,
				)
				replicas := replicaState.replicas
				// Set toReplica to the node that does not have a voting replica for t.
				toReplica := getToReplica(testCluster.NodeIDs(), replicas)
				// Set fromReplica to the first non-leaseholder voting replica for t.
				fromReplica := replicas[0]
				if fromReplica == replicaState.leaseholder {
					fromReplica = replicas[1]
				}
				query := fmt.Sprintf(tc.query, fromReplica, toReplica)
				rows, err := db.QueryContext(ctx, query)
				message = getReplicaStateMessage(tenant, query, replicaState, fromReplica, toReplica)
				if expectedErrorMessage == "" {
					require.NoErrorf(t, err, message)
					verifyResults(t, message, rows, expectedResults)
				} else {
					require.Errorf(t, err, message)
					require.Containsf(t, err.Error(), expectedErrorMessage, message)
				}
			}

			// Test system tenant.
			func() {
				testCluster, testServer, cleanup := createTestCluster(t, cfg)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				execQueries(testCluster, db, "system", tc.expectedSystemResult, "")
			}()

			// Test secondary tenant.
			func() {
				testCluster, testServer, cleanup := createTestCluster(t, cfg)
				defer cleanup()
				db := createSecondaryTenantDB(
					t,
					testServer,
					false, /* allowSplitAndScatter */
					tc.skipSQLSystemTenantCheck,
				)
				execQueries(testCluster, db, "secondary", tc.expectedSecondaryResult, tc.expectedSecondaryErrorMessage)
			}()
		})
	}
}

// TestExperimentalRelocateVoters tests that a range can be relocated from a
// non-leaseholder VOTER replica node to a non-replica node via
// EXPERIMENTAL_RELOCATE.
func TestExperimentalRelocateVoters(t *testing.T) {
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
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS VALUES (ARRAY[%[1]s], 1);",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS VALUES (ARRAY[%[1]s], 1);",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: rangeErrorMessage,
			skipSQLSystemTenantCheck:      true,
		},
	}

	const (
		numNodes                     = 4
		expectedNumVotingReplicas    = 3
		expectedNumNonVotingReplicas = 0
		expectedNumReplicas          = expectedNumVotingReplicas + expectedNumNonVotingReplicas
	)
	cfg := testClusterCfg{
		numNodes:        numNodes,
		TestClusterArgs: createTestClusterArgs(expectedNumReplicas, expectedNumVotingReplicas),
	}
	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			execQueries := func(testCluster serverutils.TestClusterInterface, db *gosql.DB, tenant string, expectedResults [][]string, expectedErrorMessage string) {
				_, err := db.ExecContext(ctx, createTable)
				message := fmt.Sprintf("tenant=%s", tenant)
				require.NoErrorf(t, err, message)
				err = testCluster.WaitForFullReplication()
				require.NoErrorf(t, err, message)
				replicaState := getReplicaState(
					t,
					ctx,
					db,
					expectedNumReplicas,
					expectedNumVotingReplicas,
					expectedNumNonVotingReplicas,
					message,
				)
				votingReplicas := replicaState.votingReplicas
				newVotingReplicas := make([]roachpb.NodeID, len(votingReplicas))
				newVotingReplicas[0] = votingReplicas[0]
				newVotingReplicas[1] = votingReplicas[1]
				newVotingReplicas[2] = getToReplica(testCluster.NodeIDs(), votingReplicas)
				query := fmt.Sprintf(tc.query, nodeIDsToArrayString(newVotingReplicas))
				rows, err := db.QueryContext(ctx, query)
				message = getReplicaStateMessage(tenant, query, replicaState, votingReplicas[2], newVotingReplicas[2])
				if expectedErrorMessage == "" {
					require.NoErrorf(t, err, message)
					verifyResults(t, message, rows, expectedResults)
				} else {
					require.Errorf(t, err, message)
					require.Containsf(t, err.Error(), expectedErrorMessage, message)
				}
			}

			// Test system tenant.
			func() {
				testCluster, testServer, cleanup := createTestCluster(t, cfg)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				execQueries(testCluster, db, "system", tc.expectedSystemResult, "")
			}()

			// Test secondary tenant.
			func() {
				testCluster, testServer, cleanup := createTestCluster(t, cfg)
				defer cleanup()
				db := createSecondaryTenantDB(
					t,
					testServer,
					false, /* allowSplitAndScatter */
					tc.skipSQLSystemTenantCheck,
				)
				execQueries(testCluster, db, "secondary", tc.expectedSecondaryResult, tc.expectedSecondaryErrorMessage)
			}()
		})
	}
}

// TestRelocateNonVoters tests that a range can be relocated from a
// NONVOTER replica node to a non-replica node via RELOCATE.
func TestRelocateNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc string
		// Query being tested.
		query string
		// If set, the test query must return these results for the system tenant.
		expectedSystemResult [][]string
		// If set, the test query must return these results for the secondary tenant.
		expectedSecondaryResult [][]string
		// If set, the test query must fail with this error message for the system tenant.
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
	}

	const (
		numNodes                     = 5
		expectedNumVotingReplicas    = 3
		expectedNumNonVotingReplicas = 1
		expectedNumReplicas          = expectedNumVotingReplicas + expectedNumNonVotingReplicas
	)
	cfg := testClusterCfg{
		numNodes:        numNodes,
		TestClusterArgs: createTestClusterArgs(expectedNumReplicas, expectedNumVotingReplicas),
	}
	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			execQueries := func(testCluster serverutils.TestClusterInterface, db *gosql.DB, tenant string, expectedResults [][]string, expectedErrorMessage string) {
				_, err := db.ExecContext(ctx, createTable)
				message := fmt.Sprintf("tenant=%s", tenant)
				require.NoErrorf(t, err, message)
				err = testCluster.WaitForFullReplication()
				require.NoErrorf(t, err, message)
				replicaState := getReplicaState(
					t,
					ctx,
					db,
					expectedNumReplicas,
					expectedNumVotingReplicas,
					expectedNumNonVotingReplicas,
					message,
				)
				// Set toReplica to the node that does not have a voting replica for t.
				toReplica := getToReplica(testCluster.NodeIDs(), replicaState.replicas)
				// Set fromReplica to the first non-leaseholder voting replica for t.
				fromReplica := replicaState.nonVotingReplicas[0]
				query := fmt.Sprintf(tc.query, fromReplica, toReplica)
				message = getReplicaStateMessage(tenant, query, replicaState, fromReplica, toReplica)
				rows, err := db.QueryContext(ctx, query)
				if expectedErrorMessage == "" {
					require.NoErrorf(t, err, message)
					verifyResults(t, message, rows, expectedResults)
				} else {
					require.Errorf(t, err, message)
					require.Containsf(t, err.Error(), expectedErrorMessage, message)
				}
			}

			// Test system tenant.
			func() {
				testCluster, testServer, cleanup := createTestCluster(t, cfg)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				execQueries(testCluster, db, "system", tc.expectedSystemResult, tc.expectedSystemErrorMessage)
			}()

			// Test secondary tenant.
			func() {
				testCluster, testServer, cleanup := createTestCluster(t, cfg)
				defer cleanup()
				db := createSecondaryTenantDB(
					t,
					testServer,
					false, /* allowSplitAndScatter */
					tc.skipSQLSystemTenantCheck,
				)
				execQueries(testCluster, db, "secondary", tc.expectedSecondaryResult, tc.expectedSecondaryErrorMessage)
			}()
		})
	}
}

// TestExperimentalRelocateNonVoters tests that a range can be relocated from a
// NONVOTER replica node to a non-replica node via EXPERIMENTAL_RELOCATE.
func TestExperimentalRelocateNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc string
		// Query being tested.
		query string
		// If set, the test query must return these results for the system tenant.
		expectedSystemResult [][]string
		// If set, the test query must return these results for the secondary tenant.
		expectedSecondaryResult [][]string
		// If set, the test query must fail with this error message for the system tenant.
		expectedSystemErrorMessage string
		// If set, the test query must fail with this error message for secondary tenants.
		expectedSecondaryErrorMessage string
		// Used as a stop-gap to bypass the system tenant check and test incomplete functionality.
		skipSQLSystemTenantCheck bool
	}{
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE NONVOTERS",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE NONVOTERS VALUES (ARRAY[%[1]s], 1);",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
		},
		{
			desc:                          "ALTER TABLE x EXPERIMENTAL_RELOCATE NONVOTERS SkipSQLSystemTenantCheck",
			query:                         "ALTER TABLE t EXPERIMENTAL_RELOCATE NONVOTERS VALUES (ARRAY[%[1]s], 1);",
			expectedSystemResult:          [][]string{{"\xbd", "/Table/53"}},
			expectedSecondaryErrorMessage: rangeErrorMessage,
			skipSQLSystemTenantCheck:      true,
		},
	}

	const (
		numNodes                     = 5
		expectedNumVotingReplicas    = 3
		expectedNumNonVotingReplicas = 1
		expectedNumReplicas          = expectedNumVotingReplicas + expectedNumNonVotingReplicas
	)
	cfg := testClusterCfg{
		numNodes:        numNodes,
		TestClusterArgs: createTestClusterArgs(expectedNumReplicas, expectedNumVotingReplicas),
	}
	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			execQueries := func(testCluster serverutils.TestClusterInterface, db *gosql.DB, tenant string, expectedResults [][]string, expectedErrorMessage string) {
				_, err := db.ExecContext(ctx, createTable)
				message := fmt.Sprintf("tenant=%s", tenant)
				require.NoErrorf(t, err, message)
				err = testCluster.WaitForFullReplication()
				require.NoErrorf(t, err, message)
				replicaState := getReplicaState(
					t,
					ctx,
					db,
					expectedNumReplicas,
					expectedNumVotingReplicas,
					expectedNumNonVotingReplicas,
					message,
				)
				newNonVotingReplicas := []roachpb.NodeID{getToReplica(testCluster.NodeIDs(), replicaState.replicas)}
				query := fmt.Sprintf(tc.query, nodeIDsToArrayString(newNonVotingReplicas))
				rows, err := db.QueryContext(ctx, query)
				message = getReplicaStateMessage(tenant, query, replicaState, replicaState.nonVotingReplicas[0], newNonVotingReplicas[0])
				if expectedErrorMessage == "" {
					require.NoErrorf(t, err, message)
					verifyResults(t, message, rows, expectedResults)
				} else {
					require.Errorf(t, err, message)
					require.Containsf(t, err.Error(), expectedErrorMessage, message)
				}
			}

			// Test system tenant.
			func() {
				testCluster, testServer, cleanup := createTestCluster(t, cfg)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				execQueries(testCluster, db, "system", tc.expectedSystemResult, tc.expectedSystemErrorMessage)
			}()

			// Test secondary tenant.
			func() {
				testCluster, testServer, cleanup := createTestCluster(t, cfg)
				defer cleanup()
				db := createSecondaryTenantDB(
					t,
					testServer,
					false, /* allowSplitAndScatter */
					tc.skipSQLSystemTenantCheck,
				)
				execQueries(testCluster, db, "secondary", tc.expectedSecondaryResult, tc.expectedSecondaryErrorMessage)
			}()
		})
	}
}
