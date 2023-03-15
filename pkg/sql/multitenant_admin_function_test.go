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
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

const (
	createTable        = "CREATE TABLE t(i int PRIMARY KEY);"
	systemRangeID      = "58"
	secondaryRangeID   = "59"
	systemKey          = "\xc1"
	systemKeyPretty    = "/Table/57"
	secondaryKeyPretty = "/Tenant/10"
	maxTimestamp       = "2262-04-11 23:47:16.854776 +0000 +0000"
)

var ctx = context.Background()

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

type testCase struct {
	desc string
	// Prereq setup statement(s) that must succeed.
	setup string
	// Multiple setup statements required because of
	// https://github.com/cockroachdb/cockroach/issues/90535.
	setups []string
	// Query being tested.
	query string
	// Expected system tenant results.
	system tenantExpected
	// Expected secondary tenant results.
	secondary tenantExpected
	// Expected secondary tenant without required cluster setting
	// (not all functions are gated by cluster setting).
	secondaryWithoutClusterSetting tenantExpected
	// Expected secondary tenant without required capability.
	secondaryWithoutCapability tenantExpected
	// Used for tests that have a cluster setting prereq
	// (eq SPLIT AT is required for UNSPLIT AT).
	setupClusterSetting *settings.BoolSetting
	// Cluster setting required for secondary tenant query.
	queryClusterSetting *settings.BoolSetting
	// Used for tests that have a capability prereq
	// (eq SPLIT AT is required for UNSPLIT AT).
	setupCapability capValue
	// Capability required for secondary tenant query.
	queryCapability capValue
}

type capValue struct {
	cap   tenantcapabilities.CapabilityID
	value string
}

func (tc testCase) runTest(
	t *testing.T,
	cfg testClusterCfg,
	execQueries func(serverutils.TestClusterInterface, *gosql.DB, string, tenantExpected),
) {

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
	defer testCluster.Stopper().Stop(ctx)

	testServer := testCluster.Server(0)

	systemDB := serverutils.OpenDBConn(
		t,
		testServer.ServingSQLAddr(),
		"",    /* useDatabase */
		false, /* insecure */
		testServer.Stopper(),
	)

	var secondaryTenants []serverutils.TestTenantInterface
	createSecondaryDB := func(
		tenantID roachpb.TenantID,
		skipSQLSystemTentantCheck bool,
		clusterSettings ...*settings.BoolSetting,
	) *gosql.DB {
		testingClusterSettings := cluster.MakeTestingClusterSettings()
		for _, clusterSetting := range clusterSettings {
			// Filter out nil cluster settings.
			if clusterSetting != nil {
				clusterSetting.Override(ctx, &testingClusterSettings.SV, true)
			}
		}
		tenant, db := serverutils.StartTenant(
			t, testServer, base.TestTenantArgs{
				Settings: testingClusterSettings,
				TestingKnobs: base.TestingKnobs{
					TenantTestingKnobs: &sql.TenantTestingKnobs{
						SkipSQLSystemTentantCheck: skipSQLSystemTentantCheck,
					},
				},
				TenantID: tenantID,
			},
		)
		secondaryTenants = append(secondaryTenants, tenant)
		return db
	}

	var waitForTenantCapabilitiesFns []func()
	setCapabilities := func(
		tenantID roachpb.TenantID,
		capVals ...capValue,
	) {
		// Filter out empty capabilities.
		var caps []capValue
		for _, capVal := range capVals {
			if capVal.cap == 0 {
				// This can happen if e.g. setupCapability / queryCapability
				// are unset in a test.
				continue
			}
			caps = append(caps, capVal)
		}
		capVals = caps
		if len(capVals) > 0 {
			expected := map[tenantcapabilities.CapabilityID]string{}
			for _, capVal := range capVals {
				query := fmt.Sprintf("ALTER TENANT [$1] GRANT CAPABILITY %s = %s", capVal.cap.String(), capVal.value)
				_, err := systemDB.ExecContext(ctx, query, tenantID.ToUint64())
				require.NoError(t, err, query)
				expected[capVal.cap] = capVal.value
			}

			waitForTenantCapabilitiesFns = append(waitForTenantCapabilitiesFns, func() {
				testCluster.WaitForTenantCapabilities(t, tenantID, expected)
			})
		}
	}

	tenantID1 := serverutils.TestTenantID()
	secondaryDB := createSecondaryDB(
		tenantID1,
		true, /* skipSQLSystemTentantCheck */
		tc.setupClusterSetting,
		tc.queryClusterSetting,
	)
	setCapabilities(tenantID1, tc.setupCapability, tc.queryCapability)

	tenantID2 := serverutils.TestTenantID2()
	secondaryWithoutClusterSettingDB := createSecondaryDB(
		tenantID2,
		false, /* skipSQLSystemTentantCheck */
		tc.setupClusterSetting,
	)
	setCapabilities(tenantID2, tc.setupCapability)

	tenantID3 := serverutils.TestTenantID3()
	secondaryWithoutCapabilityDB := createSecondaryDB(
		tenantID3,
		false, /* skipSQLSystemTentantCheck */
		tc.setupClusterSetting,
		tc.queryClusterSetting,
	)
	setCapabilities(tenantID3, tc.setupCapability)

	// Wait for cluster settings to propagate async.
	for _, fn := range waitForTenantCapabilitiesFns {
		fn()
	}

	// Wait for splits after starting all tenants to make test start up faster.
	for _, tenant := range secondaryTenants {
		err := tenant.WaitForTenantEndKeySplit(ctx)
		require.NoError(t, err)
	}

	execQueries(testCluster, systemDB, "system", tc.system)
	if tc.secondary.isSet() {
		execQueries(testCluster, secondaryDB, "secondary", tc.secondary)
	}
	if tc.secondaryWithoutClusterSetting.isSet() {
		execQueries(testCluster, secondaryWithoutClusterSettingDB, "secondary_without_cluster_setting", tc.secondaryWithoutClusterSetting)
	}
	if tc.secondaryWithoutCapability.isSet() {
		execQueries(testCluster, secondaryWithoutCapabilityDB, "secondary_without_capability", tc.secondaryWithoutCapability)
	}
}

// tenantExpected is the expected results for one tenant.
type tenantExpected struct {
	// If set, the test query must return these results.
	result [][]string
	// If set, the test query must fail with this error message.
	errorMessage string
}

func (te tenantExpected) isSet() bool {
	return len(te.result) > 0 || te.errorMessage != ""
}

func (te tenantExpected) validate(t *testing.T, rows *gosql.Rows, err error, message string) {
	errorMessage := te.errorMessage
	if errorMessage == "" {
		require.NoErrorf(t, err, message)
		verifyResults(t, message, rows, te.result)
	} else {
		require.Errorf(t, err, message)
		require.Containsf(t, err.Error(), errorMessage, message)
	}
}

// TestRelocateVoters tests that a range can be relocated from a
// non-leaseholder VOTER replica node to a non-replica node via RELOCATE.
func TestRelocateVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []testCase{
		{
			desc:  "ALTER RANGE x RELOCATE VOTERS",
			query: "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE VOTERS FROM %[1]s TO %[2]s;",
			system: tenantExpected{
				result: [][]string{{systemRangeID, systemKeyPretty, "ok"}},
			},
			secondary: tenantExpected{
				result: [][]string{{secondaryRangeID, secondaryKeyPretty, "ok"}},
			},
			secondaryWithoutCapability: tenantExpected{
				errorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
			},
		},
		{
			desc:  "ALTER RANGE RELOCATE VOTERS",
			query: "ALTER RANGE RELOCATE VOTERS FROM %[1]s TO %[2]s FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			system: tenantExpected{
				result: [][]string{{systemRangeID, systemKeyPretty, "ok"}},
			},
			secondary: tenantExpected{
				result: [][]string{{secondaryRangeID, secondaryKeyPretty, "ok"}},
			},
			secondaryWithoutCapability: tenantExpected{
				errorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
			},
		},
	}

	const (
		numNodes                     = 4
		expectedNumVotingReplicas    = 3
		expectedNumNonVotingReplicas = 0
		expectedNumReplicas          = expectedNumVotingReplicas + expectedNumNonVotingReplicas
	)
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tc.runTest(
				t,
				testClusterCfg{
					numNodes:        numNodes,
					TestClusterArgs: createTestClusterArgs(expectedNumReplicas, expectedNumVotingReplicas),
				},
				func(testCluster serverutils.TestClusterInterface, db *gosql.DB, tenant string, tExp tenantExpected) {
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
					tExp.validate(t, rows, err, message)
				},
			)
		})
	}
}

// TestExperimentalRelocateVoters tests that a range can be relocated from a
// non-leaseholder VOTER replica node to a non-replica node via
// EXPERIMENTAL_RELOCATE.
func TestExperimentalRelocateVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []testCase{
		{
			desc:  "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS",
			query: "ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS VALUES (ARRAY[%[1]s], 1);",
			system: tenantExpected{
				result: [][]string{{systemKey, systemKeyPretty}},
			},
			secondary: tenantExpected{
				result: [][]string{{"\xfe\x92", secondaryKeyPretty}},
			},
			secondaryWithoutCapability: tenantExpected{
				errorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
			},
		},
	}

	const (
		numNodes                     = 4
		expectedNumVotingReplicas    = 3
		expectedNumNonVotingReplicas = 0
		expectedNumReplicas          = expectedNumVotingReplicas + expectedNumNonVotingReplicas
	)
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tc.runTest(
				t,
				testClusterCfg{
					numNodes:        numNodes,
					TestClusterArgs: createTestClusterArgs(expectedNumReplicas, expectedNumVotingReplicas),
				},
				func(testCluster serverutils.TestClusterInterface, db *gosql.DB, tenant string, expected tenantExpected) {
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
					expected.validate(t, rows, err, message)
				},
			)
		})
	}
}

// TestRelocateNonVoters tests that a range can be relocated from a
// NONVOTER replica node to a non-replica node via RELOCATE.
func TestRelocateNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 97320, "flaky test")

	testCases := []testCase{
		{
			desc:  "ALTER RANGE x RELOCATE NONVOTERS",
			query: "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE NONVOTERS FROM %[1]s TO %[2]s;",
			system: tenantExpected{
				result: [][]string{{systemRangeID, systemKeyPretty, "ok"}},
			},
			secondary: tenantExpected{
				result: [][]string{{secondaryRangeID, secondaryKeyPretty, "ok"}},
			},
			secondaryWithoutCapability: tenantExpected{
				errorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
			},
		},
		{
			desc:  "ALTER RANGE RELOCATE NONVOTERS",
			query: "ALTER RANGE RELOCATE NONVOTERS FROM %[1]s TO %[2]s FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			system: tenantExpected{
				result: [][]string{{systemRangeID, systemKeyPretty, "ok"}},
			},
			secondary: tenantExpected{
				result: [][]string{{secondaryRangeID, secondaryKeyPretty, "ok"}},
			},
			secondaryWithoutCapability: tenantExpected{
				errorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
			},
		},
	}

	const (
		numNodes                     = 5
		expectedNumVotingReplicas    = 3
		expectedNumNonVotingReplicas = 1
		expectedNumReplicas          = expectedNumVotingReplicas + expectedNumNonVotingReplicas
	)
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tc.runTest(
				t,
				testClusterCfg{
					numNodes:        numNodes,
					TestClusterArgs: createTestClusterArgs(expectedNumReplicas, expectedNumVotingReplicas),
				},
				func(testCluster serverutils.TestClusterInterface, db *gosql.DB, tenant string, expected tenantExpected) {
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
					expected.validate(t, rows, err, message)
				},
			)
		})
	}
}

// TestExperimentalRelocateNonVoters tests that a range can be relocated from a
// NONVOTER replica node to a non-replica node via EXPERIMENTAL_RELOCATE.
func TestExperimentalRelocateNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []testCase{
		{
			desc:  "ALTER TABLE x EXPERIMENTAL_RELOCATE NONVOTERS",
			query: "ALTER TABLE t EXPERIMENTAL_RELOCATE NONVOTERS VALUES (ARRAY[%[1]s], 1);",
			system: tenantExpected{
				result: [][]string{{systemKey, systemKeyPretty}},
			},
			secondary: tenantExpected{
				result: [][]string{{"\xfe\x92", secondaryKeyPretty}},
			},
			secondaryWithoutCapability: tenantExpected{
				errorMessage: errorutil.UnsupportedWithMultiTenancyMessage,
			},
		},
	}

	const (
		numNodes                     = 5
		expectedNumVotingReplicas    = 3
		expectedNumNonVotingReplicas = 1
		expectedNumReplicas          = expectedNumVotingReplicas + expectedNumNonVotingReplicas
	)
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tc.runTest(
				t,
				testClusterCfg{
					numNodes:        numNodes,
					TestClusterArgs: createTestClusterArgs(expectedNumReplicas, expectedNumVotingReplicas),
				},
				func(testCluster serverutils.TestClusterInterface, db *gosql.DB, tenant string, tExp tenantExpected) {
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
					tExp.validate(t, rows, err, message)
				},
			)
		})
	}
}
