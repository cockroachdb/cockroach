// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bufio"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgtype"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func registerFollowerReads(r registry.Registry) {
	register := func(
		survival survivalGoal, locality localitySetting, rc readConsistency, insufficientQuorum bool,
	) {
		name := fmt.Sprintf("follower-reads/survival=%s/locality=%s/reads=%s", survival, locality, rc)
		if insufficientQuorum {
			name = name + "/insufficient-quorum"
		}
		r.Add(registry.TestSpec{
			Name:  name,
			Owner: registry.OwnerKV,
			Cluster: r.MakeClusterSpec(
				6, /* nodeCount */
				spec.CPU(4),
				spec.Geo(),
				spec.GCEZones("us-east1-b,us-east1-b,us-east1-b,us-west1-b,us-west1-b,europe-west2-b"),
			),
			CompatibleClouds: registry.OnlyGCE,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.Cloud() == spec.GCE && c.Spec().Arch == vm.ArchARM64 {
					t.Skip("arm64 in GCE is available only in us-central1")
				}
				c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
				topology := topologySpec{
					multiRegion:       true,
					locality:          locality,
					survival:          survival,
					deadPrimaryRegion: insufficientQuorum,
				}
				conns := struct {
					mu      syncutil.Mutex
					mapping map[int]*gosql.DB
				}{
					mapping: make(map[int]*gosql.DB),
				}
				connFunc := func(node int) *gosql.DB {
					conns.mu.Lock()
					defer conns.mu.Unlock()

					if _, ok := conns.mapping[node]; !ok {
						conn := c.Conn(ctx, t.L(), node)
						conns.mapping[node] = conn
					}

					return conns.mapping[node]
				}

				defer func() {
					for _, c := range conns.mapping {
						c.Close()
					}
				}()

				rng, _ := randutil.NewPseudoRand()
				data := initFollowerReadsDB(ctx, t, t.L(), c, connFunc, connFunc, rng, topology)
				runFollowerReadsTest(ctx, t, t.L(), c, rng, topology, rc, data)
			},
		})
	}
	for _, survival := range []survivalGoal{zone, region} {
		for _, locality := range []localitySetting{regional, global} {
			for _, rc := range []readConsistency{strong, exactStaleness, boundedStaleness} {
				if rc == strong && locality != global {
					// Only GLOBAL tables can perform strongly consistent reads off followers.
					continue
				}
				register(survival, locality, rc, false /* insufficientQuorum */)
			}
		}

		// Register an additional variant that cuts off the primary region and
		// verifies that bounded staleness reads are still available elsewhere.
		register(survival, regional, boundedStaleness, true /* insufficientQuorum */)
	}

	r.Add(registry.TestSpec{
		Name:  "follower-reads/mixed-version/single-region",
		Owner: registry.OwnerKV,
		Cluster: r.MakeClusterSpec(
			4, /* nodeCount */
			spec.CPU(2),
		),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:       true,
		Run:              runFollowerReadsMixedVersionSingleRegionTest,
	})

	r.Add(registry.TestSpec{
		Name:  "follower-reads/mixed-version/survival=region/locality=global/reads=strong",
		Owner: registry.OwnerKV,
		Cluster: r.MakeClusterSpec(
			6, /* nodeCount */
			spec.CPU(4),
			spec.Geo(),
			spec.GCEZones("us-east1-b,us-east1-b,us-east1-b,us-west1-b,us-west1-b,europe-west2-b"),
		),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:       true,
		Run:              runFollowerReadsMixedVersionGlobalTableTest,
	})
}

// The survival goal of a multi-region database: ZONE or REGION.
type survivalGoal string

// The locality setting of a multi-region table: REGIONAL or GLOBAL.
type localitySetting string

// The type of read to perform: strongly consistent, exact staleness, or bounded staleness.
type readConsistency string

const (
	zone   survivalGoal = "zone"
	region survivalGoal = "region"

	regional localitySetting = "regional"
	global   localitySetting = "global"

	strong           readConsistency = "strong"
	exactStaleness   readConsistency = "exact-staleness"
	boundedStaleness readConsistency = "bounded-staleness"
)

// topologySpec defines the settings of a follower-reads test.
type topologySpec struct {
	// multiRegion, if set, makes the cluster and database be multi-region.
	multiRegion bool
	// locality and survival are only relevant when multiRegion is set.
	locality localitySetting
	survival survivalGoal
	// deadPrimaryRegion, if set, indicates that the nodes in the database's
	// primary region should be stopped while running the read load. Only relevant
	// when multiRegion is set.
	deadPrimaryRegion bool
}

// runFollowerReadsTest is a basic litmus test that follower reads work.
// The test does the following:
//
//   - Creates a multi-region database and table.
//   - Configures the database's survival goals.
//   - Configures the table's locality setting.
//   - Installs a number of rows into that table.
//   - Queries the data initially with a recent timestamp and expecting an
//     error because the table does not exist in the past immediately following
//     creation.
//   - If using a REGIONAL table, waits until the required duration has elapsed
//     such that the installed data can be read with a follower read issued using
//     `follower_read_timestamp()`.
//   - Performs a few select query against a single row on all of the nodes and
//     then observes the counter metric for store-level follower reads ensuring
//     that they occurred on at least two of the nodes. If using a REGIONAL table,
//     these reads are stale through the use of `follower_read_timestamp()`.
//   - Performs reads against the written data on all of the nodes at a steady
//     rate for 20 seconds, ensure that the 90-%ile SQL latencies during that
//     time are under 10ms which implies that no WAN RPCs occurred.
func runFollowerReadsTest(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	rng *rand.Rand,
	topology topologySpec,
	rc readConsistency,
	data map[int]int64,
) {
	// Set the default_transaction_isolation variable for each connection to a
	// random isolation level, to ensure that the test exercises all available
	// isolation levels. These isolation levels may be promoted to different
	// levels in the mixed-version variant of this test than they are on master.
	isoLevels := []string{"read committed", "snapshot", "serializable"}
	require.NoError(t, func() error {
		db := c.Conn(ctx, l, 1)
		defer db.Close()
		err := enableIsolationLevels(ctx, t, db)
		if err != nil && strings.Contains(err.Error(), "unknown cluster setting") {
			// v23.1 and below does not have these cluster settings. That's fine, as
			// all isolation levels will be transparently promoted to "serializable".
			err = nil
		}
		return err
	}())

	var conns []*gosql.DB
	for i := 0; i < c.Spec().NodeCount; i++ {
		isoLevel := isoLevels[rng.Intn(len(isoLevels))]
		conn := c.Conn(ctx, l, i+1, option.ConnectionOption("default_transaction_isolation", isoLevel))
		//nolint:deferloop TODO(#137605)
		defer conn.Close()
		conns = append(conns, conn)
	}
	db := conns[0]

	// chooseKV picks a random key-value pair by exploiting the pseudo-random
	// ordering of keys when traversing a map within a range statement.
	chooseKV := func() (k int, v int64) {
		for k, v = range data {
			return k, v
		}
		panic("data is empty")
	}

	var aost string
	switch rc {
	case strong:
		aost = ""
	case exactStaleness:
		aost = "AS OF SYSTEM TIME follower_read_timestamp()"
	case boundedStaleness:
		aost = "AS OF SYSTEM TIME with_max_staleness('10m')"
	default:
		t.Fatalf("unexpected readConsistency %s", rc)
	}

	verifySelect := func(ctx context.Context, node, k int, expectedVal int64) func() error {
		return func() error {
			nodeDB := conns[node-1]
			q := fmt.Sprintf("SELECT v FROM mr_db.test %s WHERE k = $1", aost)
			r := nodeDB.QueryRowContext(ctx, q, k)
			var got int64
			if err := r.Scan(&got); err != nil {
				// Ignore errors due to cancellation.
				if ctx.Err() != nil {
					return nil
				}
				return err
			}

			if got != expectedVal {
				return errors.Errorf("Didn't get expected val on node %d: %v != %v",
					node, got, expectedVal)
			}
			return nil
		}
	}
	doSelects := func(node int) task.Func {
		return func(ctx context.Context, _ *logger.Logger) error {
			for ctx.Err() == nil {
				k, v := chooseKV()
				err := verifySelect(ctx, node, k, v)()
				if err != nil && ctx.Err() == nil {
					return err
				}
			}
			return nil
		}
	}

	if rc != strong {
		// For stale reads, wait for follower_read_timestamp() historical reads to
		// have data. For strongly consistent reads tables, this isn't needed.
		followerReadDuration, err := computeFollowerReadDuration(ctx, db)
		if err != nil {
			t.Fatalf("failed to compute follower read duration: %v", err)
		}
		select {
		case <-time.After(followerReadDuration):
		case <-ctx.Done():
			t.Fatalf("context canceled: %v", ctx.Err())
		}
	}

	// Enable the slow query log so we have a shot at identifying why follower
	// reads are not being served after the fact when this test fails. Use a
	// latency threshold of 25ms, which should be well below the latency of a
	// cross-region hop to read from the leaseholder but well above the latency
	// of a follower read.
	const maxLatencyThreshold = 25 * time.Millisecond
	_, err := db.ExecContext(
		ctx, fmt.Sprintf(
			"SET CLUSTER SETTING sql.trace.stmt.enable_threshold = '%s'",
			maxLatencyThreshold,
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Read the follower read counts before issuing the follower reads to observe
	// the delta and protect from follower reads which might have happened due to
	// system queries.
	time.Sleep(10 * time.Second) // wait a bit, otherwise sometimes I get a 404 below.
	followerReadsBefore, err := getFollowerReadCounts(ctx, t, c)
	if err != nil {
		t.Fatalf("failed to get follower read counts: %v", err)
	}

	// Perform reads on each node and ensure we get the expected value. Do so for
	// 15 seconds to give closed timestamps a chance to propagate and caches time
	// to warm up.
	l.Printf("warming up reads")
	g := t.NewGroup(task.WithContext(ctx))

	k, v := chooseKV()
	until := timeutil.Now().Add(15 * time.Second)
	for i := 1; i <= c.Spec().NodeCount; i++ {
		g.Go(func(gCtx context.Context, l *logger.Logger) error {
			fn := verifySelect(gCtx, i, k, v)
			for {
				if timeutil.Now().After(until) {
					return nil
				}
				if err := fn(); err != nil {
					return errors.Wrap(err, "error verifying node values")
				}
			}
		})
	}
	g.Wait()
	// Verify that the follower read count increments on at least two nodes -
	// which we expect to be in the non-primary regions.
	expNodesToSeeFollowerReads := 2
	followerReadsAfter, err := getFollowerReadCounts(ctx, t, c)
	if err != nil {
		t.Fatalf("failed to get follower read counts: %v", err)
	}
	nodesWhichSawFollowerReads := 0
	for i := 0; i < len(followerReadsAfter); i++ {
		if followerReadsAfter[i] > followerReadsBefore[i] {
			nodesWhichSawFollowerReads++
		}
	}
	if nodesWhichSawFollowerReads < expNodesToSeeFollowerReads {
		t.Fatalf("fewer than %v follower reads occurred: saw %v before and %v after",
			expNodesToSeeFollowerReads, followerReadsBefore, followerReadsAfter)
	}

	// Kill nodes, if necessary.
	liveNodes, deadNodes := make(map[int]struct{}), make(map[int]struct{})
	for i := 1; i <= c.Spec().NodeCount; i++ {
		if topology.deadPrimaryRegion && i <= 3 {
			stopOpts := option.DefaultStopOpts()
			stopOpts.RoachprodOpts.Sig = 9
			c.Stop(ctx, l, stopOpts, c.Node(i))
			deadNodes[i] = struct{}{}
		} else {
			liveNodes[i] = struct{}{}
		}
	}

	l.Printf("starting read load")
	const loadDuration = 4 * time.Minute
	timeoutCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	time.AfterFunc(loadDuration, func() {
		l.Printf("stopping load")
		cancel()
	})
	g = t.NewGroup(task.WithContext(timeoutCtx), task.ErrorHandler(
		func(_ context.Context, name string, l *logger.Logger, err error) error {
			return errors.Wrapf(err, "error reading data")
		},
	))
	const concurrency = 32
	var cur int
	for i := 0; cur < concurrency; i++ {
		node := i%c.Spec().NodeCount + 1
		if _, ok := liveNodes[node]; ok {
			g.Go(doSelects(node))
			cur++
		}
	}
	start := timeutil.Now()

	g.Wait()
	end := timeutil.Now()
	l.Printf("load stopped")

	// Depending on the test's topology, we expect a different set of nodes to
	// perform follower reads.
	var expectedLowRatioNodes int
	if !topology.multiRegion {
		require.Equal(t, 4, c.Spec().NodeCount)
		// We expect single-region tests to have 4 nodes but only 3 replicas. We
		// expect all replicas to serve follower reads except the leaseholder.
		// There's also one node in the cluster that doesn't have a replica - so
		// that node also won't serve follower reads either.
		expectedLowRatioNodes = 2
	} else {
		require.Equal(t, 6, c.Spec().NodeCount)
		// We expect all the replicas to serve follower reads, except the
		// leaseholder. In both the zone and the region-survival cases, there's one
		// node in the cluster that doesn't have a replica - so that node also won't
		// serve follower reads.
		expectedLowRatioNodes = 2
		// However, if the primary region is dead, then we'll be consulting fewer
		// nodes in verifyHighFollowerReadRatios (only liveNodes), so the expected
		// number of nodes with a low follower read ratio drops. With zone survival,
		// 2 of the remaining 3 nodes remaining will hold a non-voting replicas, so
		// we expect only 1 node to not serve follower reads. With region survival,
		// all 3 remaining nodes will hold a voting replica, but one will take over
		// as the leaseholder, so we also expect 1 node to not serve follower reads.
		if topology.deadPrimaryRegion {
			expectedLowRatioNodes = 1
		}
	}
	verifyHighFollowerReadRatios(ctx, t, l, c, liveNodes, start, end, expectedLowRatioNodes)

	if topology.multiRegion {
		// Perform a ts query to verify that the SQL latencies were well below the
		// WAN latencies which should be at least 50ms.
		//
		// We don't do this for singleRegion since, in a single region, there's no
		// low latency and high-latency regimes.
		verifySQLLatency(ctx, t, l, c, liveNodes, start, end, maxLatencyThreshold)
	}

	// Restart dead nodes, if necessary.
	for i := range deadNodes {
		c.Start(ctx, l, option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(i))
	}
}

// initFollowerReadsDB initializes a database for the follower reads test.
// Returns the data inserted into the test table.
//
// The `connFunc` (and `systemConnFunc`) parameters capture how to
// connect to the database in a test run. This allows us to use the
// mixedersion helpers in mixed-version tests. We need to connect to
// the system tenant to change relevant SystemOnly cluster settings,
// and some mixed-version tests run in a multi-tenant deployment.
func initFollowerReadsDB(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	connectFunc, systemConnectFunc func(int) *gosql.DB,
	rng *rand.Rand,
	topology topologySpec,
) (data map[int]int64) {
	systemDB := systemConnectFunc(1)
	db := connectFunc(1)

	// Disable load based splitting and range merging because splits and merges
	// interfere with follower reads. This test's workload regularly triggers load
	// based splitting in the first phase creating small ranges which later
	// in the test are merged. The merging tends to coincide with the final phase
	// of the test which attempts to observe low latency reads leading to
	// flakiness.
	_, err := systemDB.ExecContext(ctx, "SET CLUSTER SETTING kv.range_split.by_load_enabled = 'false'")
	require.NoError(t, err)
	_, err = systemDB.ExecContext(ctx, "SET CLUSTER SETTING kv.range_merge.queue_enabled = 'false'")
	require.NoError(t, err)

	// Check the cluster regions.
	if topology.multiRegion {
		if err := testutils.SucceedsSoonError(func() error {
			rows, err := db.QueryContext(ctx, "SELECT region, zones[1] FROM [SHOW REGIONS FROM CLUSTER] ORDER BY 1")
			require.NoError(t, err)
			defer rows.Close()

			matrix, err := sqlutils.RowsToStrMatrix(rows)
			require.NoError(t, err)

			expMatrix := [][]string{
				{"europe-west2", "europe-west2-b"},
				{"us-east1", "us-east1-b"},
				{"us-west1", "us-west1-b"},
			}
			if !reflect.DeepEqual(matrix, expMatrix) {
				return errors.Errorf("unexpected cluster regions: want %+v, got %+v", expMatrix, matrix)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Create a multi-region database and table.
	_, err = db.ExecContext(ctx, `CREATE DATABASE mr_db`)
	require.NoError(t, err)
	if topology.multiRegion {
		_, err = db.ExecContext(ctx, `ALTER DATABASE mr_db SET PRIMARY REGION "us-east1"`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `ALTER DATABASE mr_db ADD REGION "us-west1"`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `ALTER DATABASE mr_db ADD REGION "europe-west2"`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER DATABASE mr_db SURVIVE %s FAILURE`, topology.survival))
		require.NoError(t, err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE mr_db.test ( k INT8, v INT8, PRIMARY KEY (k) )`)
	require.NoError(t, err)
	if topology.multiRegion {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE mr_db.test SET LOCALITY %s`, topology.locality))
		require.NoError(t, err)
	}

	ensureUpreplicationAndPlacement(ctx, t, l, topology, db)

	const rows = 100
	const concurrency = 32
	sem := make(chan struct{}, concurrency)
	data = make(map[int]int64)
	insert := func(k int) task.Func {
		v := rng.Int63()
		data[k] = v
		return func(ctx context.Context, _ *logger.Logger) error {
			sem <- struct{}{}
			defer func() { <-sem }()
			_, err := db.ExecContext(ctx, "INSERT INTO mr_db.test VALUES ( $1, $2 )", k, v)
			return errors.Wrap(err, "failed to insert data")
		}
	}

	// Insert the data.
	g := t.NewGroup(task.WithContext(ctx))
	for i := 0; i < rows; i++ {
		g.Go(insert(i))
	}
	g.Wait()

	return data
}

func ensureUpreplicationAndPlacement(
	ctx context.Context, t test.Test, l *logger.Logger, topology topologySpec, db *gosql.DB,
) {
	// Wait until the table has completed up-replication.
	l.Printf("waiting for up-replication...")
	retryOpts := retry.Options{MaxBackoff: 15 * time.Second}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		// Check that the table has the expected number and location of voting and
		// non-voting replicas. The valid location sets can be larger than the
		// expected number of replicas, in which case, multiple valid combinations
		// with that replica count are possible.
		var votersCount, nonVotersCount int
		var votersSet, nonVotersSet []int
		if !topology.multiRegion {
			votersCount, votersSet = 3, []int{1, 2, 3, 4}
			nonVotersCount, nonVotersSet = 0, []int{}
		} else if topology.survival == zone {
			// Expect 3 voting replicas in the primary region and 2 non-voting
			// replicas in other regions.
			votersCount, votersSet = 3, []int{1, 2, 3}
			nonVotersCount, nonVotersSet = 2, []int{4, 5, 6}
		} else {
			// Expect 5 voting replicas and 0 non-voting replicas.
			votersCount, votersSet = 5, []int{1, 2, 3, 4, 5, 6}
			nonVotersCount, nonVotersSet = 0, []int{}
		}

		// NOTE: joining crdb_internal.ranges_no_leases and SHOW RANGES is awkward,
		// but we do it because crdb_internal.ranges_no_leases does not contain a
		// table_name column in v23.1 and SHOW RANGES does not contain either a
		// voting_replicas or non_voting_replicas column in v22.2. This query works
		// with either version.
		const q1 = `
			SELECT
			  (    coalesce(array_length(voting_replicas,     1), 0) = $1
			   AND coalesce(array_length(non_voting_replicas, 1), 0) = $2
			   AND voting_replicas     <@ $3
			   AND non_voting_replicas <@ $4
			  ) AS ok,
			  voting_replicas,
			  non_voting_replicas
			FROM
			  crdb_internal.ranges_no_leases
			WHERE
			  range_id = (SELECT range_id FROM [SHOW RANGES FROM TABLE mr_db.test])`

		var ok bool
		var voters, nonVoters pq.Int64Array
		err := db.QueryRowContext(
			ctx, q1, votersCount, nonVotersCount, pq.Array(votersSet), pq.Array(nonVotersSet),
		).Scan(&ok, &voters, &nonVoters)
		if errors.Is(err, gosql.ErrNoRows) {
			l.Printf("up-replication not complete, missing range")
			continue
		}
		require.NoError(t, err)

		if ok {
			break
		}

		l.Printf("up-replication not complete, "+
			"found voters = %v (want %d in set %v) and non_voters = %v (want %d in set %v)",
			voters, votersCount, votersSet, nonVoters, nonVotersCount, nonVotersSet)
	}

	if topology.multiRegion {
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			// Check that one of these replicas exists in each region. Do so by
			// parsing the replica_localities array using the same pattern as the
			// one used by SHOW REGIONS.
			const q2 = `
			SELECT count(DISTINCT substring(unnested, 'region=([^,]*)'))
			FROM (
				SELECT unnest(replica_localities) AS unnested
				FROM [SHOW RANGES FROM TABLE mr_db.test]
			)`

			var distinctRegions int
			require.NoError(t, db.QueryRowContext(ctx, q2).Scan(&distinctRegions))
			if distinctRegions == 3 {
				break
			}

			t.L().Printf("rebalancing not complete, table in %d regions", distinctRegions)
		}

		if topology.deadPrimaryRegion {
			for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
				// If we're going to be killing nodes in a multi-region cluster, make
				// sure system ranges have all upreplicated as expected as well. Do so
				// using replication reports.
				roachtestutil.WaitForUpdatedReplicationReport(ctx, t, db)

				var expAtRisk int
				if topology.survival == zone {
					// Only the 'test' table's range should be at risk of a region
					// failure.
					expAtRisk = 1
				} else {
					// No range should be at risk of a region failure.
					expAtRisk = 0
				}

				const q3 = `
				SELECT
					coalesce(sum(at_risk_ranges), 0)
				FROM
					system.replication_critical_localities
				WHERE
					locality LIKE '%region%'
				AND
					locality NOT LIKE '%zone%'`

				var atRisk int
				require.NoError(t, db.QueryRowContext(ctx, q3).Scan(&atRisk))
				if atRisk == expAtRisk {
					break
				}

				l.Printf("rebalancing not complete, expected %d at risk ranges, "+
					"found %d", expAtRisk, atRisk)
			}
		}
	}
}

func computeFollowerReadDuration(ctx context.Context, db *gosql.DB) (time.Duration, error) {
	var d pgtype.Interval
	err := db.QueryRowContext(ctx, "SELECT now() - follower_read_timestamp()").Scan(&d)
	if err != nil {
		return 0, err
	}
	var lag time.Duration
	err = d.AssignTo(&lag)
	if err != nil {
		return 0, err
	}
	return lag, nil
}

// verifySQLLatency verifies that the client-facing SQL latencies in the 90th
// percentile remain below target latency 80% of the time between start and end
// ignoring the first 20s.
func verifySQLLatency(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	liveNodes map[int]struct{},
	start, end time.Time,
	targetLatency time.Duration,
) {
	// Query needed information over the timespan of the query.
	var adminNode int
	for i := range liveNodes {
		adminNode = i
		break
	}
	adminURLs, err := c.ExternalAdminUIAddr(ctx, l, c.Node(adminNode))
	if err != nil {
		t.Fatal(err)
	}
	url := "https://" + adminURLs[0] + "/ts/query"
	var sources []string
	for i := range liveNodes {
		sources = append(sources, strconv.Itoa(i))
	}
	request := tspb.TimeSeriesQueryRequest{
		StartNanos: start.UnixNano(),
		EndNanos:   end.UnixNano(),
		// Ask for 10s intervals.
		SampleNanos: (10 * time.Second).Nanoseconds(),
		Queries: []tspb.Query{{
			Name:             "cr.node.sql.service.latency-p90",
			Sources:          sources,
			SourceAggregator: tspb.TimeSeriesQueryAggregator_MAX.Enum(),
		}},
	}
	client := roachtestutil.DefaultHTTPClient(c, l)
	var response tspb.TimeSeriesQueryResponse
	if err := client.PostProtobuf(ctx, url, &request, &response); err != nil {
		t.Fatal(err)
	}
	perTenSeconds := response.Results[0].Datapoints
	// Drop the first 20 seconds of datapoints as a "ramp-up" period.
	if len(perTenSeconds) < 3 {
		t.Fatalf("not enough ts data to verify latency")
	}
	perTenSeconds = perTenSeconds[2:]
	var above []time.Duration
	for _, dp := range perTenSeconds {
		if val := time.Duration(dp.Value); val > targetLatency {
			above = append(above, val)
		}
	}
	if permitted := int(.2 * float64(len(perTenSeconds))); len(above) > permitted {
		t.Fatalf("%d latency values (%v) are above target latency %v, %d permitted",
			len(above), above, targetLatency, permitted)
	}
}

// verifyHighFollowerReadRatios analyzes the follower_reads.success_count and
// the sql.select.count timeseries and checks that, in the majority of 10s
// quantas, the ratio between the two is high (i.e. we're serving the load as
// follower reads) for all but a few nodes (toleratedNodes). We tolerate a few
// wacky quantas because, around leaseholder changes, the one node's ratio drops
// to zero (the new leaseholder), and another one's rises (the old leaseholder).
func verifyHighFollowerReadRatios(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	liveNodes map[int]struct{},
	start, end time.Time,
	toleratedNodes int,
) {
	// Start reading timeseries 10s into the test. If the start time is too soon
	// after cluster startup, it's possible that not every node returns the first
	// datapoint (I think because it had not yet produced a sample before the
	// start point?).
	start = start.Add(10 * time.Second)

	// Query needed information over the timespan of the query.
	var adminNode int
	for i := range liveNodes {
		adminNode = i
		break
	}
	adminURLs, err := c.ExternalAdminUIAddr(
		ctx, l, c.Node(adminNode), option.VirtualClusterName(install.SystemInterfaceName),
	)
	require.NoError(t, err)

	url := "https://" + adminURLs[0] + "/ts/query"
	request := tspb.TimeSeriesQueryRequest{
		StartNanos: start.UnixNano(),
		EndNanos:   end.UnixNano(),
		// Ask for 10s intervals.
		SampleNanos: (10 * time.Second).Nanoseconds(),
	}
	for i := range liveNodes {
		nodeID := strconv.Itoa(i)
		request.Queries = append(request.Queries, tspb.Query{
			Name:       "cr.store.follower_reads.success_count",
			Sources:    []string{nodeID},
			Derivative: tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(),
		})
		request.Queries = append(request.Queries, tspb.Query{
			Name:       "cr.node.sql.select.count",
			Sources:    []string{nodeID},
			Derivative: tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(),
		})
	}
	// Make sure to connect to the system tenant in case this test
	// is running on a multitenant deployment.
	client := roachtestutil.DefaultHTTPClient(
		c, l, roachtestutil.VirtualCluster(install.SystemInterfaceName),
	)
	var response tspb.TimeSeriesQueryResponse
	if err := client.PostProtobuf(ctx, url, &request, &response); err != nil {
		t.Fatal(err)
	}

	minDataPoints := len(response.Results[0].Datapoints)
	for _, res := range response.Results[1:] {
		if len(res.Datapoints) < minDataPoints {
			minDataPoints = len(res.Datapoints)
		}
	}
	if minDataPoints < 3 {
		t.Fatalf("not enough ts data to verify follower reads")
	}

	// Go through the timeseries and process them into a better format.
	stats := make([]intervalStats, minDataPoints)
	for i := range stats {
		var ratios []float64
		for n := 0; n < len(response.Results); n += 2 {
			followerReadsPerTenSeconds := response.Results[n].Datapoints[i]
			selectsPerTenSeconds := response.Results[n+1].Datapoints[i]
			ratios = append(ratios, followerReadsPerTenSeconds.Value/selectsPerTenSeconds.Value)
		}
		intervalEnd := timeutil.Unix(0, response.Results[0].Datapoints[i].TimestampNanos)
		stats[i] = intervalStats{
			ratiosPerNode: ratios,
			start:         intervalEnd.Add(-10 * time.Second),
			end:           intervalEnd,
		}
	}

	l.Printf("interval stats: %s", intervalsToString(stats))

	// Now count how many intervals have more than the tolerated number of nodes
	// with low follower read ratios.
	const threshold = 0.9
	var badIntervals []intervalStats
	for _, stat := range stats {
		var nodesWithLowRatios int
		for _, ratio := range stat.ratiosPerNode {
			if ratio < threshold {
				nodesWithLowRatios++
			}
		}
		if nodesWithLowRatios > toleratedNodes {
			badIntervals = append(badIntervals, stat)
		}
	}
	permitted := int(.2 * float64(len(stats)))
	if len(badIntervals) > permitted {
		t.Fatalf("too many intervals with more than %d nodes with low follower read ratios: "+
			"%d intervals > %d threshold. Bad intervals:\n%s",
			toleratedNodes, len(badIntervals), permitted, intervalsToString(badIntervals))
	}
}

type intervalStats struct {
	ratiosPerNode []float64
	start, end    time.Time
}

func intervalsToString(stats []intervalStats) string {
	var s strings.Builder
	for _, interval := range stats {
		s.WriteString(fmt.Sprintf("interval %s-%s: ",
			interval.start.Format("15:04:05"), interval.end.Format("15:04:05")))
		for i, r := range interval.ratiosPerNode {
			s.WriteString(fmt.Sprintf("n%d ratio: %.3f ", i+1, r))
		}
		s.WriteRune('\n')
	}
	return s.String()
}

const followerReadsMetric = "follower_reads_success_count"

// getFollowerReadCounts returns a slice from node to follower read count
// according to the metric.
func getFollowerReadCounts(ctx context.Context, t test.Test, c cluster.Cluster) ([]int, error) {
	followerReadCounts := make([]int, c.Spec().NodeCount)
	getFollowerReadCount := func(node int) task.Func {
		return func(ctx context.Context, l *logger.Logger) error {
			adminUIAddrs, err := c.ExternalAdminUIAddr(
				ctx, l, c.Node(node), option.VirtualClusterName(install.SystemInterfaceName),
			)
			if err != nil {
				return err
			}
			url := "https://" + adminUIAddrs[0] + "/_status/vars"
			// Make sure to connect to the system tenant in case this test
			// is running on a multitenant deployment.
			client := roachtestutil.DefaultHTTPClient(
				c, l, roachtestutil.VirtualCluster(install.SystemInterfaceName),
			)
			resp, err := client.Get(ctx, url)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return errors.Errorf("invalid non-200 status code %v from node %d", resp.StatusCode, node)
			}
			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				m, ok := parsePrometheusMetric(scanner.Text())
				if ok {
					if m.metric == followerReadsMetric {
						v, err := strconv.ParseFloat(m.value, 64)
						if err != nil {
							return err
						}
						followerReadCounts[node-1] = int(v)
					}
				}
			}
			return nil
		}
	}
	g := t.NewErrorGroup(task.WithContext(ctx))
	for i := 1; i <= c.Spec().NodeCount; i++ {
		g.Go(getFollowerReadCount(i), task.Name(fmt.Sprintf("follower-read-count-%d", i)))
	}
	if err := g.WaitE(); err != nil {
		return nil, err
	}
	return followerReadCounts, nil
}

// parse rows like:
// sql_select_count  1.652807e+06
// follower_reads_success_count{store="1"} 27606
var prometheusMetricStringPattern = `^(?P<metric>\w+)(?:\{` +
	`(?P<labelvalues>(\w+=\".*\",)*(\w+=\".*\")?)\})?\s+(?P<value>.*)$`
var promethusMetricStringRE = regexp.MustCompile(prometheusMetricStringPattern)

type prometheusMetric struct {
	metric      string
	labelValues string
	value       string
}

func parsePrometheusMetric(s string) (*prometheusMetric, bool) {
	matches := promethusMetricStringRE.FindStringSubmatch(s)
	if matches == nil {
		return nil, false
	}
	return &prometheusMetric{
		metric:      matches[1],
		labelValues: matches[2],
		value:       matches[5],
	}, true
}

// runFollowerReadsMixedVersionSingleRegionTest runs a follower-reads test in a
// single region while performing a cluster upgrade. The point is to exercise
// the closed-timestamp mechanism in a mixed-version cluster. Running in a
// single region is sufficient for this purpose; we're not testing non-voting
// replicas here (which are used in multi-region tests).
func runFollowerReadsMixedVersionSingleRegionTest(
	ctx context.Context, t test.Test, c cluster.Cluster,
) {
	topology := topologySpec{multiRegion: false}
	runFollowerReadsMixedVersionTest(ctx, t, c, topology, exactStaleness,
		// This test is incompatible with separate process mode as it queries metrics from
		// TSDB. Separate process clusters currently do not write to TSDB as serverless uses
		// third party metrics persistence solutions instead.
		//
		// TODO(darrylwong): Once #137625 is complete, we can switch to querying prometheus using
		// `clusterstats` instead and re-enable separate process.
		mixedversion.EnabledDeploymentModes(
			mixedversion.SystemOnlyDeployment,
			mixedversion.SharedProcessDeployment,
		),
		mixedversion.MinimumSupportedVersion("v23.2.0"),
	)
}

// runFollowerReadsMixedVersionGlobalTableTest runs a multi-region follower-read
// test with a region-survivable global table while performing a cluster upgrade.
// The point is to exercise global tables in a mixed-version cluster.
func runFollowerReadsMixedVersionGlobalTableTest(
	ctx context.Context, t test.Test, c cluster.Cluster,
) {
	topology := topologySpec{
		multiRegion: true,
		locality:    global,
		survival:    region,
	}
	runFollowerReadsMixedVersionTest(ctx, t, c, topology, strong,
		// Disable fixtures because we're using a 6-node, multi-region cluster.
		mixedversion.NeverUseFixtures,
		// Use a longer upgrade timeout to give the migrations enough time to finish
		// considering the cross-region latency.
		mixedversion.UpgradeTimeout(60*time.Minute),

		// This test is flaky when upgrading from v23.1 to v23.2 for follower
		// reads in shared-process deployments. There were a number of changes
		// to tenant health checks since then which appear to have addressed
		// this issue.
		mixedversion.MinimumSupportedVersion("v23.2.0"),

		// This test is incompatible with separate process mode as it queries metrics from
		// TSDB. Separate process clusters currently do not write to TSDB as serverless uses
		// third party metrics persistence solutions instead.
		//
		// TODO(darrylwong): Once #137625 is complete, we can switch to querying prometheus using
		// `clusterstats` instead and re-enable separate process.
		mixedversion.EnabledDeploymentModes(
			mixedversion.SystemOnlyDeployment,
			mixedversion.SharedProcessDeployment,
		),
	)
}

// runFollowerReadsMixedVersionSingleRegionTest runs a follower-reads test while
// performing a cluster upgrade.
func runFollowerReadsMixedVersionTest(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	topology topologySpec,
	rc readConsistency,
	opts ...mixedversion.CustomOption,
) {
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All(), opts...)

	var data map[int]int64
	runInit := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		if topology.multiRegion {
			if err := enableTenantMultiRegion(l, r, h); err != nil {
				return err
			}
		}

		data = initFollowerReadsDB(ctx, t, l, c, h.Connect, h.System.Connect, r, topology)
		return nil
	}

	runFollowerReads := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		ensureUpreplicationAndPlacement(ctx, t, l, topology, h.Connect(1))
		runFollowerReadsTest(ctx, t, l, c, r, topology, rc, data)
		return nil
	}

	mvt.OnStartup("init database", runInit)
	mvt.InMixedVersion("run follower reads", runFollowerReads)
	mvt.AfterUpgradeFinalized("run follower reads", runFollowerReads)
	mvt.Run()
}

// enableTenantMultiRegion enables multi-region features on the
// mixedversion tenant if necessary (no-op otherwise).
func enableTenantMultiRegion(l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
	if !h.IsMultitenant() || h.Context().FromVersion.AtLeast(mixedversion.TenantsAndSystemAlignedSettingsVersion) {
		return nil
	}

	const setting = "sql.multi_region.allow_abstractions_for_secondary_tenants.enabled"
	err := setTenantSetting(l, r, h, setting, true)
	return errors.Wrapf(err, "setting %s", setting)
}
