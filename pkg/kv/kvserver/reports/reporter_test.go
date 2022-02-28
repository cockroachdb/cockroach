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
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/keysutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Test the constraint conformance report in a real cluster.
func TestConstraintConformanceReportIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test takes seconds because of replication vagaries.
	skip.UnderShort(t)
	// Under stressrace, replication changes seem to hit 1m deadline errors and
	// don't make progress.
	skip.UnderStressRace(t)
	skip.UnderRace(t, "takes >1min under race")

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 5, base.TestClusterArgs{
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
	tdb := sqlutils.MakeSQLRunner(db)
	// Speed up the generation of the reports.
	tdb.Exec(t, "SET CLUSTER SETTING kv.replication_reports.interval = '1ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10 ms'")

	// Create a table and a zone config for it.
	// The zone will be configured with a constraints that can't be satisfied
	// because there are not enough nodes in the requested region.
	tdb.Exec(t, "create table t(x int primary key); "+
		"alter table t configure zone using constraints='[+region=r1]'")

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
	tdb.Exec(t, "alter table t configure zone using constraints='[+region=r2]'")

	// Wait for the violation to clear.
	testutils.SucceedsSoon(t, func() error {
		// Kick the replication queues, given that our rebalancing is finicky.
		for i := 0; i < tc.NumServers(); i++ {
			if err := tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				return s.ForceReplicationScanAndProcess()
			}); err != nil {
				t.Fatal(err)
			}
		}
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	// 2 regions, 3 dcs per region.
	tc := serverutils.StartNewTestCluster(t, 6, base.TestClusterArgs{
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
	tdb := sqlutils.MakeSQLRunner(db)
	// Speed up the generation of the reports.
	tdb.Exec(t, "SET CLUSTER SETTING kv.replication_reports.interval = '1ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10 ms'")

	// Since we're using ReplicationManual, all the ranges will start with a
	// single replica on node 1. So, the node's dc and the node's region are
	// critical. Let's verify that.

	// Collect all the zones that exist at cluster bootstrap.
	systemZoneIDs := make([]int, 0, 10)
	systemZones := make([]zonepb.ZoneConfig, 0, 10)
	{
		rows, err := db.Query("select id, config from system.zones")
		require.NoError(t, err)
		for rows.Next() {
			var zoneID int
			var buf []byte
			cfg := zonepb.ZoneConfig{}
			require.NoError(t, rows.Scan(&zoneID, &buf))
			require.NoError(t, protoutil.Unmarshal(buf, &cfg))
			systemZoneIDs = append(systemZoneIDs, zoneID)
			systemZones = append(systemZones, cfg)
		}
		require.NoError(t, rows.Err())
	}
	require.Greater(t, len(systemZoneIDs), 0, "expected some system zones, got none")
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
			msg := fmt.Sprintf("zone_id: %d, locality: %s", zid, s)
			require.NoError(t, r.Scan(&numRanges), msg)
			require.NotEqual(t, 0, numRanges, msg)
		}
	}

	// Now create a table and a zone for it. At first n1 should be critical for it.
	// Then we'll upreplicate it in different ways.

	// Create a table with a dummy zone config. Configuring the zone is useful
	// only for creating the zone; we don't actually care about the configuration.
	// Also do a split by hand. With manual replication, we're not getting the
	// split for the table automatically.
	tdb.Exec(t, "create table t(x int primary key); "+
		"alter table t configure zone using num_replicas=3; "+
		"alter table t split at values (0);")
	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from crdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Check initial conditions.
	require.NoError(t, checkCritical(db, zoneID, "region=r1", "region=r1,dc=dc1"))

	// Upreplicate to 2 dcs. Now they're both critical.
	tdb.Exec(t, "ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2], 1)")

	require.NoError(t, checkCritical(db, zoneID, "region=r1", "region=r1,dc=dc1", "region=r1,dc=dc2"))

	// Upreplicate to one more dc. Now no dc is critical, only the region.
	tdb.Exec(t, "ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3], 1)")
	require.NoError(t, checkCritical(db, zoneID, "region=r1"))

	// Move two replicas to the other region. Now that region is critical.
	tdb.Exec(t, "ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,4,5], 1)")
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 4, base.TestClusterArgs{
		// We're going to do our own replication.
		// All the system ranges will start with a single replica on node 1.
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(db)
	// Speed up the generation of the reports.
	tdb.Exec(t, "SET CLUSTER SETTING kv.replication_reports.interval = '1ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10 ms'")

	// Create a table with a dummy zone config. Configuring the zone is useful
	// only for creating the zone; we don't actually care about the configuration.
	// Also do a split by hand. With manual replication, we're not getting the
	// split for the table automatically.
	tdb.Exec(t, "create table t(x int primary key); "+
		"alter table t configure zone using num_replicas=3; "+
		"alter table t split at values (0);")
	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from crdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Upreplicate the range.
	tdb.Exec(t, "ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3], 1)")
	require.NoError(t, checkZoneReplication(db, zoneID, 1, 0, 0, 0))

	// Over-replicate.
	tdb.Exec(t, "ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3,4], 1)")
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
	defer log.Scope(t).Close(t)
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
	require.Greater(t, numRanges, 20, "expected over 20 ranges, got: %d", numRanges)

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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	cfg := s.ExecutorConfig().(sql.ExecutorConfig).SystemConfig.GetSystemConfig()
	dummyNodeChecker := func(id roachpb.NodeID) bool { return true }

	v := makeReplicationStatsVisitor(ctx, cfg, dummyNodeChecker)
	realIter := makeMeta2RangeIter(db, 10000 /* batchSize */)
	require.NoError(t, visitRanges(ctx, &realIter, cfg, &v))
	expReport := v.Report()
	require.Greater(t, len(expReport), 0, "unexpected empty report")

	realIter = makeMeta2RangeIter(db, 10000 /* batchSize */)
	errorIter := erroryRangeIterator{
		iter:           realIter,
		injectErrAfter: 3,
	}
	v = makeReplicationStatsVisitor(ctx, cfg, func(id roachpb.NodeID) bool { return true })
	require.NoError(t, visitRanges(ctx, &errorIter, cfg, &v))
	require.Greater(t, len(v.report), 0, "unexpected empty report")
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

func TestZoneChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	type tc struct {
		split          string
		newZone        bool
		newRootZoneCfg *zonepb.ZoneConfig
		newZoneKey     ZoneKey
	}
	// NB: IDs need to be beyond MaxSystemConfigDescID, otherwise special logic
	// kicks in for mapping keys to zones. They also need to not overlap with any
	// system table IDs.
	dbID := int(bootstrap.TestingMinUserDescID())
	t1ID := dbID + 1
	t1 := table{name: "t1",
		partitions: []partition{
			{
				name:  "p1",
				start: []int{100},
				end:   []int{200},
				zone:  &zone{constraints: "[+p1]"},
			},
			{
				name:  "p2",
				start: []int{300},
				end:   []int{400},
				zone:  &zone{constraints: "[+p2]"},
			},
		},
	}
	t1.addPKIdx()
	// Create a table descriptor to be used for creating the zone config.
	t1Desc, err := makeTableDesc(t1, t1ID, dbID)
	require.NoError(t, err)
	t1Zone, err := generateTableZone(t1, t1Desc)
	require.NoError(t, err)
	p1SubzoneIndex := 0
	p2SubzoneIndex := 1
	require.Equal(t, "p1", t1Zone.Subzones[p1SubzoneIndex].PartitionName)
	require.Equal(t, "p2", t1Zone.Subzones[p2SubzoneIndex].PartitionName)
	t1ZoneKey := MakeZoneKey(config.SystemTenantObjectID(t1ID), NoSubzone)
	p1ZoneKey := MakeZoneKey(config.SystemTenantObjectID(t1ID), base.SubzoneIDFromIndex(p1SubzoneIndex))
	p2ZoneKey := MakeZoneKey(config.SystemTenantObjectID(t1ID), base.SubzoneIDFromIndex(p2SubzoneIndex))

	ranges := []tc{
		{
			split:          "/Table/t1/pk/1",
			newZone:        true,
			newZoneKey:     t1ZoneKey,
			newRootZoneCfg: t1Zone,
		},
		{
			split:   "/Table/t1/pk/2",
			newZone: false,
		},
		{
			// p1's zone
			split:          "/Table/t1/pk/100",
			newZone:        true,
			newZoneKey:     p1ZoneKey,
			newRootZoneCfg: t1Zone,
		},
		{
			split:   "/Table/t1/pk/101",
			newZone: false,
		},
		{
			// Back to t1's zone
			split:          "/Table/t1/pk/200",
			newZone:        true,
			newZoneKey:     t1ZoneKey,
			newRootZoneCfg: t1Zone,
		},
		{
			// p2's zone
			split:          "/Table/t1/pk/305",
			newZone:        true,
			newZoneKey:     p2ZoneKey,
			newRootZoneCfg: t1Zone,
		},
	}

	splits := make([]split, len(ranges))
	for i := range ranges {
		splits[i].key = ranges[i].split
	}
	keyScanner := keysutils.MakePrettyScannerForNamedTables(
		map[string]int{"t1": t1ID} /* tableNameToID */, nil /* idxNameToID */)
	rngs, err := processSplits(keyScanner, splits, nil /* stores */)
	require.NoError(t, err)

	var zc zoneResolver
	for i, tc := range ranges {
		sameZone := zc.checkSameZone(ctx, &rngs[i])
		newZone := !sameZone
		require.Equal(t, tc.newZone, newZone, "failed at: %d (%s)", i, tc.split)
		if newZone {
			objectID, _ := config.DecodeKeyIntoZoneIDAndSuffix(rngs[i].StartKey)
			zc.setZone(objectID, tc.newZoneKey, tc.newRootZoneCfg)
		}
	}
}

// TestRangeIteration checks that visitRanges() correctly informs range
// visitors whether ranges fall in the same zone vs a new zone.
func TestRangeIteration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	schema := baseReportTestCase{
		schema: []database{{
			name: "db1",
			zone: &zone{
				replicas: 3,
			},
			tables: []table{
				{
					name: "t1",
					partitions: []partition{
						{
							name:  "p1",
							start: []int{100},
							end:   []int{200},
							zone:  &zone{},
						},
						{
							name:  "p2",
							start: []int{200},
							end:   []int{300},
							zone:  &zone{},
						},
					},
				},
				{
					name: "t2",
				},
			},
		},
		},
		splits: []split{
			{key: "/Table/t1/pk/1"},
			{key: "/Table/t1/pk/2"},
			{key: "/Table/t1/pk/100"},
			{key: "/Table/t1/pk/101"},
			{key: "/Table/t1/pk/200"},
			{key: "/Table/t1/pk/305"},
			{key: "/Table/t2/pk/1"},
		},
		defaultZone: zone{},
	}

	compiled, err := compileTestCase(schema)
	require.NoError(t, err)
	v := recordingRangeVisitor{}
	require.NoError(t, visitRanges(ctx, &compiled.iter, compiled.cfg, &v))

	type entry struct {
		newZone bool
		key     string
	}
	exp := []entry{
		{newZone: true, key: "/Table/101/1/1"},
		{newZone: false, key: "/Table/101/1/2"},
		{newZone: true, key: "/Table/101/1/100"},
		{newZone: false, key: "/Table/101/1/101"},
		{newZone: true, key: "/Table/101/1/200"},
		{newZone: true, key: "/Table/101/1/305"},
		{newZone: true, key: "/Table/102/1/1"},
	}
	got := make([]entry, len(v.rngs))
	for i, r := range v.rngs {
		got[i].newZone = r.newZone
		got[i].key = r.rng.StartKey.String()
	}
	require.Equal(t, exp, got)
}

type recordingRangeVisitor struct {
	rngs []visitorEntry
}

var _ rangeVisitor = &recordingRangeVisitor{}

func (r *recordingRangeVisitor) visitNewZone(
	_ context.Context, rng *roachpb.RangeDescriptor,
) error {
	r.rngs = append(r.rngs, visitorEntry{newZone: true, rng: *rng})
	return nil
}

func (r *recordingRangeVisitor) visitSameZone(_ context.Context, rng *roachpb.RangeDescriptor) {
	r.rngs = append(r.rngs, visitorEntry{newZone: false, rng: *rng})
}

func (r *recordingRangeVisitor) failed() bool {
	return false
}

func (r *recordingRangeVisitor) reset(ctx context.Context) {
	r.rngs = nil
}

type visitorEntry struct {
	newZone bool
	rng     roachpb.RangeDescriptor
}
