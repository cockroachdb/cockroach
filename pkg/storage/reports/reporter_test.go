// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reports

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Test the constraint conformance report in a real cluster.
func TestConstraintConformanceReportIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#40919")

	ctx := context.Background()
	tc := serverutils.StartTestCluster(t, 5, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r1"}}}},
			1: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r1"}}}},
			2: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}}},
			3: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}}},
			4: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}}},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	// Speed up the generation of the
	_, err := db.Exec("set cluster setting kv.replication_reports.interval = '1ms'")
	require.NoError(t, err)

	// Create a table and a zone config for it.
	// The zone will be configured with a constraints that can't be satisfied
	// because there are not enough nodes in the requested region.
	_, err = db.Exec("create table t(x int primary key); " +
		"alter table t configure zone using constraints='[+region=r1]'")
	require.NoError(t, err)

	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from crdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Wait for the violation to be detected.
	testutils.SucceedsSoon(t, func() error {
		r := db.QueryRow(
			"select violating_ranges from system.replication_constraint_stats where zone_id = $1",
			zoneID)
		var numViolations int
		if err := r.Scan(&numViolations); err != nil {
			return err
		}
		if numViolations == 0 {
			return fmt.Errorf("violation not detected yet")
		}
		return nil
	})

	// Now change the constraint asking for t to be placed in r2. This time it can be satisfied.
	_, err = db.Exec("alter table t configure zone using constraints='[+region=r2]'")
	require.NoError(t, err)

	// Wait for the violation to clear.
	testutils.SucceedsSoon(t, func() error {
		r := db.QueryRow(
			"select violating_ranges from system.replication_constraint_stats where zone_id = $1",
			zoneID)
		var numViolations int
		if err := r.Scan(&numViolations); err != nil {
			return err
		}
		if numViolations > 0 {
			return fmt.Errorf("still reporting violations")
		}
		return nil
	})
}

// Test the critical localities report in a real cluster.
func TestCriticalLocalitiesReportIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	// 2 regions, 3 dcs per region.
	tc := serverutils.StartTestCluster(t, 6, base.TestClusterArgs{
		// We're going to do our own replication.
		// All the system ranges will start with a single replica on node 1.
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc1"}},
				},
			},
			1: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc2"}},
				},
			},
			2: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc3"}},
				},
			},
			3: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc4"}},
				},
			},
			4: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc5"}},
				},
			},
			5: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc6"}},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	// Speed up the generation of the reports.
	_, err := db.Exec("set cluster setting kv.replication_reports.interval = '1ms'")
	require.NoError(t, err)

	// Since we're using ReplicationManual, all the ranges will start with a
	// single replica on node 1. So, the node's dc and the node's region are
	// critical. Let's verify that.

	// Collect all the zones that exist at cluster bootstrap.
	systemZoneIDs := make([]int, 0, 10)
	systemZones := make([]config.ZoneConfig, 0, 10)
	{
		rows, err := db.Query("select id, config from system.zones")
		require.NoError(t, err)
		for rows.Next() {
			var zoneID int
			var buf []byte
			cfg := config.ZoneConfig{}
			require.NoError(t, rows.Scan(&zoneID, &buf))
			require.NoError(t, protoutil.Unmarshal(buf, &cfg))
			systemZoneIDs = append(systemZoneIDs, zoneID)
			systemZones = append(systemZones, cfg)
		}
		require.NoError(t, rows.Err())
	}
	require.True(t, len(systemZoneIDs) > 0, "expected some system zones, got none")
	// Remove the entries in systemZoneIDs that don't get critical locality reports.
	i := 0
	for j, zid := range systemZoneIDs {
		if zoneChangesReplication(&systemZones[j]) {
			systemZoneIDs[i] = zid
			i++
		}
	}
	systemZoneIDs = systemZoneIDs[:i]

	expCritLoc := []string{"region=r1", "region=r1,dc=dc1"}

	// Wait for the report to be generated.
	{
		var rowCount int
		testutils.SucceedsSoon(t, func() error {
			r := db.QueryRow("select count(1) from system.replication_critical_localities")
			require.NoError(t, r.Scan(&rowCount))
			if rowCount == 0 {
				return fmt.Errorf("no report yet")
			}
			return nil
		})
		require.Equal(t, 2*len(systemZoneIDs), rowCount)
	}

	// Check that we have all the expected rows.
	for _, zid := range systemZoneIDs {
		for _, s := range expCritLoc {
			r := db.QueryRow(
				"select at_risk_ranges from system.replication_critical_localities "+
					"where zone_id=$1 and locality=$2",
				zid, s)
			var numRanges int
			require.NoError(t, r.Scan(&numRanges))
			require.NotEqual(t, 0, numRanges)
		}
	}

	// Now create a table and a zone for it. At first n1 should be critical for it.
	// Then we'll upreplicate it in different ways.

	// Create a table with a dummy zone config. Configuring the zone is useful
	// only for creating the zone; we don't actually care about the configuration.
	// Also do a split by hand. With manual replication, we're not getting the
	// split for the table automatically.
	_, err = db.Exec("create table t(x int primary key); " +
		"alter table t configure zone using num_replicas=3; " +
		"alter table t split at values (0);")
	require.NoError(t, err)
	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from crdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Check initial conditions.
	require.NoError(t, checkCritical(db, zoneID, "region=r1", "region=r1,dc=dc1"))

	// Upreplicate to 2 dcs. Now they're both critical.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2], 1)")
	require.NoError(t, err)
	require.NoError(t, checkCritical(db, zoneID, "region=r1", "region=r1,dc=dc1", "region=r1,dc=dc2"))

	// Upreplicate to one more dc. Now no dc is critical, only the region.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3], 1)")
	require.NoError(t, err)
	require.NoError(t, checkCritical(db, zoneID, "region=r1"))

	// Move two replicas to the other region. Now that region is critical.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,4,5], 1)")
	require.NoError(t, err)
	require.NoError(t, checkCritical(db, zoneID, "region=r2"))
}

func checkCritical(db *gosql.DB, zoneID int, locs ...string) error {
	return testutils.SucceedsSoonError(func() error {
		rows, err := db.Query(
			"select locality, at_risk_ranges from system.replication_critical_localities "+
				"where zone_id=$1", zoneID)
		if err != nil {
			return err
		}
		critical := make(map[string]struct{})
		for rows.Next() {
			var numRanges int
			var loc string
			err := rows.Scan(&loc, &numRanges)
			if err != nil {
				return err
			}
			if numRanges == 0 {
				return fmt.Errorf("expected ranges_at_risk for %s", loc)
			}
			critical[loc] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(locs) != len(critical) {
			return fmt.Errorf("expected critical: %s, got: %s", locs, critical)
		}
		for _, l := range locs {
			if _, ok := critical[l]; !ok {
				return fmt.Errorf("missing critical locality: %s", l)
			}
		}
		return nil
	})
}

// Test the replication status report in a real cluster.
func TestReplicationStatusReportIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := serverutils.StartTestCluster(t, 4, base.TestClusterArgs{
		// We're going to do our own replication.
		// All the system ranges will start with a single replica on node 1.
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	// Speed up the generation of the
	_, err := db.Exec("set cluster setting kv.replication_reports.interval = '1ms'")
	require.NoError(t, err)

	// Create a table with a dummy zone config. Configuring the zone is useful
	// only for creating the zone; we don't actually care about the configuration.
	// Also do a split by hand. With manual replication, we're not getting the
	// split for the table automatically.
	_, err = db.Exec("create table t(x int primary key); " +
		"alter table t configure zone using num_replicas=3; " +
		"alter table t split at values (0);")
	require.NoError(t, err)
	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from crdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Upreplicate the range.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3], 1)")
	require.NoError(t, err)
	require.NoError(t, checkZoneReplication(db, zoneID, 1, 0, 0, 0))

	// Over-replicate.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3,4], 1)")
	require.NoError(t, err)
	require.NoError(t, checkZoneReplication(db, zoneID, 1, 0, 1, 0))

	// TODO(andrei): I'd like to downreplicate to one replica and then stop that
	// node and check that the range is counter us "unavailable", but stopping a
	// node makes the report generation simply block sometimes trying to scan
	// Meta2. I believe I believe it's due to #40529.
	// Once stopping a node works, next thing is to start it up again.
	// Take inspiration from replica_learner_test.go.

	//// Down-replicate to one node and then kill the node. Check that the range becomes unavailable.
	//_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[4], 1)")
	//require.NoError(t, err)
	//tc.StopServer(3)
	//require.NoError(t, checkZoneReplication(db, zoneID, 1, 1, 0, 1))
}

func checkZoneReplication(db *gosql.DB, zoneID, total, under, over, unavailable int) error {
	return testutils.SucceedsSoonError(func() error {
		r := db.QueryRow(
			"select total_ranges, under_replicated_ranges, over_replicated_ranges, "+
				"unavailable_ranges from system.replication_stats where zone_id=$1",
			zoneID)
		var gotTotal, gotUnder, gotOver, gotUnavailable int
		if err := r.Scan(&gotTotal, &gotUnder, &gotOver, &gotUnavailable); err != nil {
			return err
		}
		if total != gotTotal {
			return fmt.Errorf("expected total: %d, got: %d", total, gotTotal)
		}
		if under != gotUnder {
			return fmt.Errorf("expected under: %d, got: %d", total, gotUnder)
		}
		if over != gotOver {
			return fmt.Errorf("expected over: %d, got: %d", over, gotOver)
		}
		if unavailable != gotUnavailable {
			return fmt.Errorf("expected unavailable: %d, got: %d", unavailable, gotUnavailable)
		}
		return nil
	})
}

func TestMeta2RangeIter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// First make an interator with a large page size and use it to determine the numner of ranges.
	iter := makeMeta2RangeIter(db, 10000 /* batchSize */)
	numRanges := 0
	for {
		rd, err := iter.Next(ctx)
		require.NoError(t, err)
		if rd.RangeID == 0 {
			break
		}
		numRanges++
	}
	require.True(t, numRanges > 20, "expected over 20 ranges, got: %d", numRanges)

	// Now make an interator with a small page size and check that we get just as many ranges.
	iter = makeMeta2RangeIter(db, 2 /* batch size */)
	numRangesPaginated := 0
	for {
		rd, err := iter.Next(ctx)
		require.NoError(t, err)
		if rd.RangeID == 0 {
			break
		}
		numRangesPaginated++
	}
	require.Equal(t, numRanges, numRangesPaginated)
}

// Test that a retriable error returned from the range iterator is properly
// handled by resetting the report.
func TestRetriableErrorWhenGenerationReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	cfg := s.GossipI().(*gossip.Gossip).GetSystemConfig()
	dummyNodeChecker := func(id roachpb.NodeID) bool { return true }

	saver := makeReplicationStatsReportSaver()
	v := makeReplicationStatsVisitor(ctx, cfg, dummyNodeChecker, &saver)
	realIter := makeMeta2RangeIter(db, 10000 /* batchSize */)
	require.NoError(t, visitRanges(ctx, &realIter, cfg, &v))
	expReport := v.report
	require.True(t, len(expReport.stats) > 0, "unexpected empty report")

	realIter = makeMeta2RangeIter(db, 10000 /* batchSize */)
	errorIter := erroryRangeIterator{
		iter:           realIter,
		injectErrAfter: 3,
	}
	v = makeReplicationStatsVisitor(ctx, cfg, func(id roachpb.NodeID) bool { return true }, &saver)
	require.NoError(t, visitRanges(ctx, &errorIter, cfg, &v))
	require.True(t, len(v.report.stats) > 0, "unexpected empty report")
	require.Equal(t, expReport, v.report)
}

type erroryRangeIterator struct {
	iter           meta2RangeIter
	rangesReturned int
	injectErrAfter int
}

var _ RangeIterator = &erroryRangeIterator{}

func (it *erroryRangeIterator) Next(ctx context.Context) (roachpb.RangeDescriptor, error) {
	if it.rangesReturned == it.injectErrAfter {
		// Don't inject any more errors.
		it.injectErrAfter = -1

		var err error
		err = roachpb.NewTransactionRetryWithProtoRefreshError(
			"injected err", uuid.Nil, roachpb.Transaction{})
		// Let's wrap the error to check the unwrapping.
		err = errors.Wrap(err, "dummy wrapper")
		// Feed the error to the underlying iterator to reset it.
		it.iter.handleErr(ctx, err)
		return roachpb.RangeDescriptor{}, err
	}
	it.rangesReturned++
	rd, err := it.iter.Next(ctx)
	return rd, err
}

func (it *erroryRangeIterator) Close(ctx context.Context) {
	it.iter.Close(ctx)
}

// computeConstraintConformanceReport iterates through all the ranges and
// generates the constraint conformance report.
func computeConstraintConformanceReport(
	ctx context.Context,
	rangeStore RangeIterator,
	cfg *config.SystemConfig,
	storeResolver StoreResolver,
) (*replicationConstraintStatsReportSaver, error) {
	saver := makeReplicationConstraintStatusReportSaver()
	v := makeConstraintConformanceVisitor(ctx, cfg, storeResolver, &saver)
	err := visitRanges(ctx, rangeStore, cfg, &v)
	return v.report, err
}

// computeReplicationStatsReport iterates through all the ranges and generates
// the replication stats report.
func computeReplicationStatsReport(
	ctx context.Context, rangeStore RangeIterator, checker nodeChecker, cfg *config.SystemConfig,
) (*replicationStatsReportSaver, error) {
	saver := makeReplicationStatsReportSaver()
	v := makeReplicationStatsVisitor(ctx, cfg, checker, &saver)
	err := visitRanges(ctx, rangeStore, cfg, &v)
	return v.report, err
}
