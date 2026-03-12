// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// registerSuperRegionFailover registers a roachtest that validates failover
// behavior for super regions on a geo-distributed cluster. It verifies that
// when a region within a super region fails, leases transfer to another region
// within the same super region and replicas remain confined.
func registerSuperRegionFailover(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "multi-region/super-region-failover",
		Owner: registry.OwnerSQLFoundations,
		// The test kills nodes and expects them to be dead at the end of
		// scenario 1 (before recovery). We recover them, but we also need
		// SkipPostValidations in case the test fails mid-scenario.
		SkipPostValidations: registry.PostValidationNoDeadNodes,
		Timeout:             45 * time.Minute,
		Cluster: r.MakeClusterSpec(
			12,
			spec.CPU(2),
			spec.Mem(spec.Low),
			spec.Geo(),
			spec.GCEZones(
				"europe-west2-a,europe-west2-b,europe-west2-c,"+
					"us-east1-b,us-east1-c,us-east1-d,"+
					"us-central1-a,us-central1-b,us-central1-c,"+
					"us-west1-a,us-west1-b,us-west1-c",
			),
		),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSuperRegionFailover(ctx, t, c)
		},
	})
}

// superRegionTopology describes the cluster topology.
type superRegionTopology struct {
	// regionName maps a GCE zone to the CockroachDB region name.
	regionName map[string]string
	// nodeRegion maps a 1-indexed node ID to a CockroachDB region name.
	nodeRegion map[int]string
	// regionNodes maps a region name to a list of 1-indexed node IDs.
	regionNodes map[string][]int
	// superRegionMembers are the regions in the "americas" super region.
	superRegionMembers map[string]bool
}

func buildTopology(t test.Test, zones string) superRegionTopology {
	regionName := map[string]string{
		"europe-west2-a": "europe-west2",
		"europe-west2-b": "europe-west2",
		"europe-west2-c": "europe-west2",
		"us-east1-b":     "us-east1",
		"us-east1-c":     "us-east1",
		"us-east1-d":     "us-east1",
		"us-central1-a":  "us-central1",
		"us-central1-b":  "us-central1",
		"us-central1-c":  "us-central1",
		"us-west1-a":     "us-west1",
		"us-west1-b":     "us-west1",
		"us-west1-c":     "us-west1",
	}

	zoneList := strings.Split(zones, ",")
	nodeRegion := make(map[int]string)
	regionNodes := make(map[string][]int)
	for i, z := range zoneList {
		nodeID := i + 1
		r, ok := regionName[z]
		if !ok {
			t.Fatalf("unrecognized zone %q", z)
		}
		nodeRegion[nodeID] = r
		regionNodes[r] = append(regionNodes[r], nodeID)
	}

	superRegionMembers := map[string]bool{
		"us-east1":    true,
		"us-central1": true,
		"us-west1":    true,
	}

	return superRegionTopology{
		regionName:         regionName,
		nodeRegion:         nodeRegion,
		regionNodes:        regionNodes,
		superRegionMembers: superRegionMembers,
	}
}

func runSuperRegionFailover(ctx context.Context, t test.Test, c cluster.Cluster) {
	topo := buildTopology(t, c.Spec().GCE.Zones)

	t.Status("starting cluster")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	// Connect to node 1 (europe-west-2) for initial setup.
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	t.Status("waiting for initial up-replication")
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	t.Status("setting up multi-region database with super region")
	setupDatabase(ctx, t, conn)

	// --- Scenario 1: SURVIVE REGION FAILURE ---
	t.Status("scenario 1: SURVIVE REGION FAILURE")
	runSurviveRegionFailure(ctx, t, c, topo)

	// --- Scenario 2: SURVIVE ZONE FAILURE ---
	t.Status("scenario 2: SURVIVE ZONE FAILURE")
	runSurviveZoneFailure(ctx, t, c, topo)
}

func setupDatabase(ctx context.Context, t test.Test, conn *gosql.DB) {
	stmts := []string{
		`SET CLUSTER SETTING sql.defaults.primary_region = ''`,
		`SET CLUSTER SETTING server.time_until_store_dead = '30s'`,
		`CREATE DATABASE test_sr PRIMARY REGION "europe-west2"
			REGIONS "us-east1", "us-central1", "us-west1"`,
		`ALTER DATABASE test_sr ADD SUPER REGION "americas"
			VALUES "us-east1", "us-central1", "us-west1"`,

		// REGIONAL BY TABLE in a super region member.
		`CREATE TABLE test_sr.rbt_americas (k INT PRIMARY KEY, v INT)
			LOCALITY REGIONAL BY TABLE IN "us-east1"`,
		`INSERT INTO test_sr.rbt_americas VALUES (1, 10), (2, 20), (3, 30)`,

		// REGIONAL BY TABLE outside the super region (in primary region).
		`CREATE TABLE test_sr.rbt_standalone (k INT PRIMARY KEY, v INT)
			LOCALITY REGIONAL BY TABLE IN "europe-west2"`,
		`INSERT INTO test_sr.rbt_standalone VALUES (1, 100), (2, 200)`,

		// REGIONAL BY ROW spanning all regions.
		`CREATE TABLE test_sr.rbr (k INT PRIMARY KEY, v INT)
			LOCALITY REGIONAL BY ROW`,
		`INSERT INTO test_sr.rbr (crdb_region, k, v) VALUES
			('us-east1', 1, 10),
			('us-central1', 2, 20),
			('us-west1', 3, 30),
			('europe-west2', 4, 40)`,
	}
	for _, stmt := range stmts {
		_, err := conn.ExecContext(ctx, stmt)
		require.NoError(t, err, "failed to execute: %s", stmt)
	}
}

func runSurviveRegionFailure(
	ctx context.Context, t test.Test, c cluster.Cluster, topo superRegionTopology,
) {
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	_, err := conn.ExecContext(ctx, `ALTER DATABASE test_sr SURVIVE REGION FAILURE`)
	require.NoError(t, err)

	t.Status("waiting for constraint conformance after SURVIVE REGION FAILURE")
	waitForConstraintConformance(ctx, t, conn)

	t.Status("waiting for initial lease placement")
	waitForLeasesInRegion(ctx, t, conn, topo, "test_sr.rbt_americas", "us-east1")
	waitForLeasesInRegion(ctx, t, conn, topo, "test_sr.rbt_standalone", "europe-west2")

	// Kill us-east-1 nodes.
	usEastNodes := topo.regionNodes["us-east1"]
	t.Status(fmt.Sprintf("killing us-east-1 nodes %v", usEastNodes))
	stopOpts := option.DefaultStopOpts()
	stopOpts.RoachprodOpts.Sig = 9
	c.Stop(ctx, t.L(), stopOpts, c.Nodes(usEastNodes...))

	// Connect through a surviving node in the super region (us-central-1).
	// USE test_sr so that REGIONAL BY ROW writes can resolve crdb_region.
	survivingNode := topo.regionNodes["us-central1"][0]
	sConn := c.Conn(ctx, t.L(), survivingNode)
	defer sConn.Close()
	_, err = sConn.ExecContext(ctx, `USE test_sr`)
	require.NoError(t, err)

	t.Status("waiting for lease transfer after us-east-1 failure")
	waitForLeasesOffNodes(ctx, t, sConn, "test_sr.rbt_americas", usEastNodes)

	t.Status("verifying failover lease placement")
	// rbt_americas leases should be within the super region.
	verifyLeasesInSuperRegion(ctx, t, sConn, topo, "test_sr.rbt_americas")
	// rbt_standalone leases may be transiently displaced during region failure;
	// wait for them to settle back to europe-west2.
	waitForLeasesInRegion(ctx, t, sConn, topo, "test_sr.rbt_standalone", "europe-west2")

	t.Status("verifying data is readable and writable after failover")
	verifyDataAvailable(ctx, t, sConn, "test_sr.rbt_americas")
	verifyDataAvailable(ctx, t, sConn, "test_sr.rbt_standalone")
	verifyDataAvailable(ctx, t, sConn, "test_sr.rbr")

	t.Status("verifying replica confinement for americas tables")
	verifyReplicaConfinement(ctx, t, sConn, topo, "test_sr.rbt_americas")

	// Recover us-east-1.
	t.Status("recovering us-east-1 nodes")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(),
		c.Nodes(usEastNodes...))

	// Reconnect to node 1 for recovery checks.
	rConn := c.Conn(ctx, t.L(), 1)
	defer rConn.Close()

	t.Status("waiting for lease return to us-east-1")
	waitForLeasesInRegion(ctx, t, rConn, topo, "test_sr.rbt_americas", "us-east1")

	t.Status("verifying recovery: constraint conformance")
	waitForConstraintConformance(ctx, t, rConn)
}

func runSurviveZoneFailure(
	ctx context.Context, t test.Test, c cluster.Cluster, topo superRegionTopology,
) {
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	_, err := conn.ExecContext(ctx, `ALTER DATABASE test_sr SURVIVE ZONE FAILURE`)
	require.NoError(t, err)

	t.Status("waiting for constraint conformance after SURVIVE ZONE FAILURE")
	waitForConstraintConformance(ctx, t, conn)

	t.Status("waiting for initial lease placement for zone failure scenario")
	waitForLeasesInRegion(ctx, t, conn, topo, "test_sr.rbt_americas", "us-east1")

	// Kill one node in us-east-1 (simulate zone failure).
	targetNode := topo.regionNodes["us-east1"][0] // n4
	t.Status(fmt.Sprintf("killing node %d in us-east-1", targetNode))
	stopOpts := option.DefaultStopOpts()
	stopOpts.RoachprodOpts.Sig = 9
	c.Stop(ctx, t.L(), stopOpts, c.Node(targetNode))

	// Connect through the surviving node in us-east-1.
	// USE test_sr so that REGIONAL BY ROW writes can resolve crdb_region.
	survivingUsEast := topo.regionNodes["us-east1"][1] // n5
	sConn := c.Conn(ctx, t.L(), survivingUsEast)
	defer sConn.Close()
	_, err = sConn.ExecContext(ctx, `USE test_sr`)
	require.NoError(t, err)

	t.Status("verifying leases remain in super region after zone failure")
	waitForLeasesOffNodes(ctx, t, sConn, "test_sr.rbt_americas", []int{targetNode})
	waitForLeasesInRegion(ctx, t, sConn, topo, "test_sr.rbt_americas", "us-east1")

	t.Status("verifying data availability after zone failure")
	verifyDataAvailable(ctx, t, sConn, "test_sr.rbt_americas")
	verifyDataAvailable(ctx, t, sConn, "test_sr.rbr")

	t.Status("verifying replica confinement after zone failure")
	verifyReplicaConfinement(ctx, t, sConn, topo, "test_sr.rbt_americas")

	// Recover the killed node.
	t.Status(fmt.Sprintf("recovering node %d", targetNode))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(),
		c.Node(targetNode))

	rConn := c.Conn(ctx, t.L(), 1)
	defer rConn.Close()

	t.Status("verifying recovery after zone failure")
	waitForConstraintConformance(ctx, t, rConn)
}

// waitForConstraintConformance polls system.replication_constraint_stats until
// there are no voter_constraint violations.
func waitForConstraintConformance(ctx context.Context, t test.Test, conn *gosql.DB) {
	t.Helper()
	retryOpts := retry.Options{
		InitialBackoff: 15 * time.Second,
		MaxBackoff:     15 * time.Second,
		MaxRetries:     20, // 5 minutes
	}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		var count int
		// TODO(#165087): Remove the voter_constraint filter once the underlying
		// issue is resolved. Under SURVIVE ZONE FAILURE with super regions,
		// non-voting replica constraints (type='constraint') are persistently
		// violated — the zone configs require more non-voting replicas per
		// region than the replication factor allows. All constraint types
		// should converge.
		err := conn.QueryRowContext(ctx,
			`SELECT count(*) FROM system.replication_constraint_stats
			 WHERE violating_ranges > 0
			   AND type = 'voter_constraint'`).Scan(&count)
		if err != nil {
			t.L().Printf("error checking constraint conformance: %v", err)
			continue
		}
		if count == 0 {
			t.L().Printf("voter constraint conformance achieved")
			return
		}
		t.L().Printf("still have %d violating voter constraint entries, retrying...", count)
	}
	// Log the actual violations and replica placements before failing to aid
	// debugging.
	logConstraintViolations(ctx, t, conn)
	logAllTablePlacements(ctx, t, conn)
	t.Fatal("timed out waiting for voter constraint conformance")
}

// logConstraintViolations queries and logs the current constraint violations
// to aid debugging when conformance is not achieved.
func logConstraintViolations(ctx context.Context, t test.Test, conn *gosql.DB) {
	rows, err := conn.QueryContext(ctx,
		`SELECT zone_id, subzone_id, type, config, violating_ranges
		 FROM system.replication_constraint_stats
		 WHERE violating_ranges > 0`)
	if err != nil {
		t.L().Printf("error fetching constraint violation details: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var zoneID, subzoneID, violating int
		var typ, config string
		if err := rows.Scan(&zoneID, &subzoneID, &typ, &config, &violating); err != nil {
			t.L().Printf("error scanning violation row: %v", err)
			continue
		}
		t.L().Printf(
			"constraint violation: zone_id=%d subzone_id=%d type=%s config=%s violating_ranges=%d",
			zoneID, subzoneID, typ, config, violating,
		)
	}
	require.NoError(t, rows.Err())
}

// logTableReplicaPlacement queries SHOW RANGES FROM TABLE ... WITH DETAILS and
// logs per-range placement details (lease holder, lease holder locality,
// voting replicas, replica localities) for the given table.
func logTableReplicaPlacement(ctx context.Context, t test.Test, conn *gosql.DB, table string) {
	rows, err := conn.QueryContext(ctx,
		fmt.Sprintf(
			`SELECT range_id, lease_holder, lease_holder_locality,
			        voting_replicas, replica_localities
			 FROM [SHOW RANGES FROM TABLE %s WITH DETAILS]`,
			table,
		),
	)
	if err != nil {
		t.L().Printf("error fetching replica placement for %s: %v", table, err)
		return
	}
	defer rows.Close()
	t.L().Printf("replica placement for %s:", table)
	for rows.Next() {
		var rangeID, leaseHolder int
		var leaseLocality, votingReplicas, replicaLocalities string
		if err := rows.Scan(
			&rangeID, &leaseHolder, &leaseLocality, &votingReplicas, &replicaLocalities,
		); err != nil {
			t.L().Printf("  error scanning row: %v", err)
			continue
		}
		t.L().Printf(
			"  r%d: lease_holder=n%d (%s) voting_replicas=%s replica_localities=%s",
			rangeID, leaseHolder, leaseLocality, votingReplicas, replicaLocalities,
		)
	}
	require.NoError(t, rows.Err())
}

// logAllTablePlacements logs replica placement details for all test tables.
func logAllTablePlacements(ctx context.Context, t test.Test, conn *gosql.DB) {
	for _, table := range []string{
		"test_sr.rbt_standalone",
		"test_sr.rbt_americas",
		"test_sr.rbr",
	} {
		logTableReplicaPlacement(ctx, t, conn, table)
	}
}

// verifyLeasesInSuperRegion checks that all leases for a table are held by
// nodes within the super region. It retries on transient errors (e.g. ranges
// still recovering after a region failure).
func verifyLeasesInSuperRegion(
	ctx context.Context, t test.Test, conn *gosql.DB, topo superRegionTopology, table string,
) {
	retryOpts := retry.Options{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     15 * time.Second,
		MaxRetries:     30,
	}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		leaseHolders, err := getLeaseHolders(ctx, t, conn, table)
		if err != nil {
			t.L().Printf("error querying lease holders for %s (retrying): %v", table, err)
			continue
		}
		if len(leaseHolders) == 0 {
			t.L().Printf("no ranges found for %s (retrying)", table)
			continue
		}
		allInSuperRegion := true
		for _, lh := range leaseHolders {
			region := topo.nodeRegion[lh]
			if !topo.superRegionMembers[region] {
				allInSuperRegion = false
				t.L().Printf("lease holder n%d is in region %s outside super region (retrying)",
					lh, region)
				break
			}
		}
		if allInSuperRegion {
			t.L().Printf("verified all leases for %s are within super region", table)
			return
		}
	}
	t.Fatal("timed out verifying leases in super region for ", table)
}

// waitForLeasesInRegion waits until all leases for a table move to the
// expected region.
func waitForLeasesInRegion(
	ctx context.Context,
	t test.Test,
	conn *gosql.DB,
	topo superRegionTopology,
	table string,
	expectedRegion string,
) {
	retryOpts := retry.Options{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     15 * time.Second,
		MaxRetries:     60,
	}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		leaseHolders, err := getLeaseHolders(ctx, t, conn, table)
		if err != nil {
			t.L().Printf("error querying lease holders for %s (retrying): %v", table, err)
			continue
		}
		allInRegion := true
		for _, lh := range leaseHolders {
			if topo.nodeRegion[lh] != expectedRegion {
				allInRegion = false
				break
			}
		}
		if len(leaseHolders) > 0 && allInRegion {
			t.L().Printf("all leases for %s are in %s", table, expectedRegion)
			return
		}
		t.L().Printf("waiting for leases of %s to move to %s (current holders: %v)",
			table, expectedRegion, leaseHolders)
	}
	t.Fatal("timed out waiting for leases to return to region ", expectedRegion)
}

// waitForLeasesOffNodes waits until no leases for the given table are held
// by any of the specified nodes.
func waitForLeasesOffNodes(
	ctx context.Context, t test.Test, conn *gosql.DB, table string, nodes []int,
) {
	deadNodes := make(map[int]bool)
	for _, n := range nodes {
		deadNodes[n] = true
	}

	retryOpts := retry.Options{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     15 * time.Second,
		MaxRetries:     60,
	}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		leaseHolders, err := getLeaseHolders(ctx, t, conn, table)
		if err != nil {
			t.L().Printf("error querying lease holders for %s (retrying): %v", table, err)
			continue
		}
		anyOnDead := false
		for _, lh := range leaseHolders {
			if deadNodes[lh] {
				anyOnDead = true
				break
			}
		}
		if len(leaseHolders) > 0 && !anyOnDead {
			t.L().Printf("no leases for %s on dead nodes %v", table, nodes)
			return
		}
		t.L().Printf("still have leases for %s on dead nodes, retrying...", table)
	}
	t.Fatal("timed out waiting for leases to move off dead nodes")
}

// getLeaseHolders returns the lease holder node IDs for all ranges of a table.
func getLeaseHolders(
	ctx context.Context, t test.Test, conn *gosql.DB, table string,
) ([]int, error) {
	rows, err := conn.QueryContext(ctx,
		fmt.Sprintf(
			`SELECT lease_holder FROM [SHOW RANGES FROM TABLE %s WITH DETAILS]`,
			table,
		),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var holders []int
	for rows.Next() {
		var lh int
		if err := rows.Scan(&lh); err != nil {
			return nil, err
		}
		holders = append(holders, lh)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return holders, nil
}

// verifyDataAvailable confirms that data can be read from and written to a
// table through the given connection.
func verifyDataAvailable(ctx context.Context, t test.Test, conn *gosql.DB, table string) {
	// Read.
	var count int
	err := conn.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&count)
	require.NoError(t, err, "failed to read from %s", table)
	require.Greater(t, count, 0, "expected rows in %s", table)

	// Write.
	_, err = conn.ExecContext(ctx,
		fmt.Sprintf(
			`INSERT INTO %s (k, v) VALUES (9000 + (random()*1000)::int, 42)
			 ON CONFLICT (k) DO UPDATE SET v = 42`,
			table,
		),
	)
	require.NoError(t, err, "failed to write to %s", table)

	t.L().Printf("verified data available for %s (read %d rows, write ok)", table, count)
}

// verifyReplicaConfinement checks that all replicas of a table are on nodes
// within the super region. This verifies that no replica has escaped to a
// node outside the super region.
func verifyReplicaConfinement(
	ctx context.Context, t test.Test, conn *gosql.DB, topo superRegionTopology, table string,
) {
	rows, err := conn.QueryContext(ctx,
		fmt.Sprintf(
			`SELECT range_id, replicas FROM [SHOW RANGES FROM TABLE %s WITH DETAILS]`,
			table,
		),
	)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var rangeID int
		var replicas pq.Int64Array
		require.NoError(t, rows.Scan(&rangeID, &replicas))

		for _, nodeID := range replicas {
			region := topo.nodeRegion[int(nodeID)]
			require.True(t, topo.superRegionMembers[region],
				"range r%d has replica on n%d in region %s which is outside super region",
				rangeID, nodeID, region)
		}
	}
	require.NoError(t, rows.Err())
	t.L().Printf("verified all replicas for %s are within super region", table)
}
