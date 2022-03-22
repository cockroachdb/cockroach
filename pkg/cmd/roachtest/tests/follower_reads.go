// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"bufio"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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
				spec.Zones("us-east1-b,us-east1-b,us-east1-b,us-west1-b,us-west1-b,europe-west2-b"),
			),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				c.Put(ctx, t.Cockroach(), "./cockroach")
				c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
				topology := topologySpec{
					multiRegion:       true,
					locality:          locality,
					survival:          survival,
					deadPrimaryRegion: insufficientQuorum,
				}
				data := initFollowerReadsDB(ctx, t, c, topology)
				runFollowerReadsTest(ctx, t, c, topology, rc, data)
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
			3, /* nodeCount */
			spec.CPU(2),
		),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runFollowerReadsMixedVersionSingleRegionTest(ctx, t, c, *t.BuildVersion())
		},
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
//  * Creates a multi-region database and table.
//  * Configures the database's survival goals.
//  * Configures the table's locality setting.
//  * Installs a number of rows into that table.
//  * Queries the data initially with a recent timestamp and expecting an
//    error because the table does not exist in the past immediately following
//    creation.
//  * If using a REGIONAL table, waits until the required duration has elapsed
//    such that the installed data can be read with a follower read issued using
//    `follower_read_timestamp()`.
//  * Performs a few select query against a single row on all of the nodes and
//    then observes the counter metric for store-level follower reads ensuring
//    that they occurred on at least two of the nodes. If using a REGIONAL table,
//    these reads are stale through the use of `follower_read_timestamp()`.
//  * Performs reads against the written data on all of the nodes at a steady
//    rate for 20 seconds, ensure that the 90-%ile SQL latencies during that
//    time are under 10ms which implies that no WAN RPCs occurred.
//
func runFollowerReadsTest(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	topology topologySpec,
	rc readConsistency,
	data map[int]int64,
) {
	var conns []*gosql.DB
	for i := 0; i < c.Spec().NodeCount; i++ {
		conns = append(conns, c.Conn(ctx, t.L(), i+1))
		defer conns[i].Close()
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
			q := fmt.Sprintf("SELECT v FROM test.test %s WHERE k = $1", aost)
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
	doSelects := func(ctx context.Context, node int) func() error {
		return func() error {
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
		// 20.2 doesn't have this setting.
		if !strings.Contains(err.Error(), "unknown cluster setting") {
			t.Fatal(err)
		}
	}

	// Read the follower read counts before issuing the follower reads to observe
	// the delta and protect from follower reads which might have happened due to
	// system queries.
	time.Sleep(10 * time.Second) // wait a bit, otherwise sometimes I get a 404 below.
	followerReadsBefore, err := getFollowerReadCounts(ctx, t, c)
	if err != nil {
		t.Fatalf("failed to get follower read counts: %v", err)
	}

	// Perform reads on each node and ensure we get the expected value. Do so a
	// few times on each follower to give caches time to warm up.
	t.L().Printf("warming up reads")
	g, gCtx := errgroup.WithContext(ctx)
	k, v := chooseKV()
	for i := 1; i <= c.Spec().NodeCount; i++ {
		fn := verifySelect(gCtx, i, k, v)
		g.Go(func() error {
			for j := 0; j < 100; j++ {
				if err := fn(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("error verifying node values: %v", err)
	}
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
			c.Stop(ctx, t.L(), stopOpts, c.Node(i))
			deadNodes[i] = struct{}{}
		} else {
			liveNodes[i] = struct{}{}
		}
	}

	t.L().Printf("starting read load")
	const loadDuration = 4 * time.Minute
	timeoutCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	time.AfterFunc(loadDuration, func() {
		t.L().Printf("stopping load")
		cancel()
	})
	g, gCtx = errgroup.WithContext(timeoutCtx)
	const concurrency = 32
	var cur int
	for i := 0; cur < concurrency; i++ {
		node := i%c.Spec().NodeCount + 1
		if _, ok := liveNodes[node]; ok {
			g.Go(doSelects(gCtx, node))
			cur++
		}
	}
	start := timeutil.Now()

	if err := g.Wait(); err != nil && timeoutCtx.Err() == nil {
		t.Fatalf("error reading data: %v", err)
	}
	end := timeutil.Now()
	t.L().Printf("load stopped")

	// Depending on the test's topology, we expect a different set of nodes to
	// perform follower reads.
	var expectedLowRatioNodes int
	if !topology.multiRegion {
		// We expect one node - the leaseholder - to have a low ratio because it
		// doesn't perform follower reads; the other 2 replicas should serve
		// follower reads. We assume single-region tests to have 3 nodes.
		expectedLowRatioNodes = 1
	} else {
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
	verifyHighFollowerReadRatios(ctx, t, c, liveNodes, start, end, expectedLowRatioNodes)

	if topology.multiRegion {
		// Perform a ts query to verify that the SQL latencies were well below the
		// WAN latencies which should be at least 50ms.
		//
		// We don't do this for singleRegion since, in a single region, there's no
		// low latency and high-latency regimes.
		verifySQLLatency(ctx, t, c, liveNodes, start, end, maxLatencyThreshold)
	}

	// Restart dead nodes, if necessary.
	for i := range deadNodes {
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(i))
	}
}

// initFollowerReadsDB initializes a database for the follower reads test.
// Returns the data inserted into the test table.
func initFollowerReadsDB(
	ctx context.Context, t test.Test, c cluster.Cluster, topology topologySpec,
) (data map[int]int64) {
	db := c.Conn(ctx, t.L(), 1)
	// Disable load based splitting and range merging because splits and merges
	// interfere with follower reads. This test's workload regularly triggers load
	// based splitting in the first phase creating small ranges which later
	// in the test are merged. The merging tends to coincide with the final phase
	// of the test which attempts to observe low latency reads leading to
	// flakiness.
	_, err := db.ExecContext(ctx, "SET CLUSTER SETTING kv.range_split.by_load_enabled = 'false'")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "SET CLUSTER SETTING kv.range_merge.queue_enabled = 'false'")
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
	_, err = db.ExecContext(ctx, `CREATE DATABASE test`)
	require.NoError(t, err)
	if topology.multiRegion {
		_, err = db.ExecContext(ctx, `ALTER DATABASE test SET PRIMARY REGION "us-east1"`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `ALTER DATABASE test ADD REGION "us-west1"`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `ALTER DATABASE test ADD REGION "europe-west2"`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER DATABASE test SURVIVE %s FAILURE`, topology.survival))
		require.NoError(t, err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE test.test ( k INT8, v INT8, PRIMARY KEY (k) )`)
	require.NoError(t, err)
	if topology.multiRegion {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE test.test SET LOCALITY %s`, topology.locality))
		require.NoError(t, err)
	}

	// Wait until the table has completed up-replication.
	t.L().Printf("waiting for up-replication...")
	retryOpts := retry.Options{MaxBackoff: 15 * time.Second}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		// Check that the table has the expected number of voting and non-voting
		// replicas.
		var votersCol, nonVotersCol string
		if topology.multiRegion {
			votersCol = "coalesce(array_length(voting_replicas, 1), 0)"
			nonVotersCol = "coalesce(array_length(non_voting_replicas, 1), 0)"
		} else {
			// Hack to support v20.2 which doesn't have the non_voting_replicas
			// column.
			votersCol = "coalesce(array_length(replicas, 1), 0)"
			nonVotersCol = "0"
		}

		q1 := fmt.Sprintf(`
			SELECT
				%s, %s
			FROM
				crdb_internal.ranges_no_leases
			WHERE
				table_name = 'test'`, votersCol, nonVotersCol)

		var voters, nonVoters int
		err := db.QueryRowContext(ctx, q1).Scan(&voters, &nonVoters)
		if errors.Is(err, gosql.ErrNoRows) {
			t.L().Printf("up-replication not complete, missing range")
			continue
		}
		require.NoError(t, err)

		var ok bool
		if !topology.multiRegion {
			ok = voters == 3
		} else if topology.survival == zone {
			// Expect 3 voting replicas and 2 non-voting replicas.
			ok = voters == 3 && nonVoters == 2
		} else {
			// Expect 5 voting replicas and 0 non-voting replicas.
			ok = voters == 5 && nonVoters == 0
		}
		if ok {
			break
		}

		t.L().Printf("up-replication not complete, found %d voters and %d non_voters", voters, nonVoters)
	}

	if topology.multiRegion {
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			// Check that one of these replicas exists in each region. Do so by
			// parsing the replica_localities array using the same pattern as the
			// one used by SHOW REGIONS.
			const q2 = `
			SELECT
				count(distinct substring(unnest(replica_localities), 'region=([^,]*)'))
			FROM
				crdb_internal.ranges_no_leases
			WHERE
				table_name = 'test'`

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
				WaitForUpdatedReplicationReport(ctx, t, db)

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

				t.L().Printf("rebalancing not complete, expected %d at risk ranges, "+
					"found %d", expAtRisk, atRisk)
			}
		}
	}

	const rows = 100
	const concurrency = 32
	sem := make(chan struct{}, concurrency)
	data = make(map[int]int64)
	insert := func(ctx context.Context, k int) func() error {
		v := rand.Int63()
		data[k] = v
		return func() error {
			sem <- struct{}{}
			defer func() { <-sem }()
			_, err := db.ExecContext(ctx, "INSERT INTO test.test VALUES ( $1, $2 )", k, v)
			return err
		}
	}

	// Insert the data.
	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < rows; i++ {
		g.Go(insert(gCtx, i))
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	return data
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
	adminURLs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(adminNode))
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + adminURLs[0] + "/ts/query"
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
			Name:    "cr.node.sql.service.latency-p90",
			Sources: sources,
		}},
	}
	var response tspb.TimeSeriesQueryResponse
	if err := httputil.PostJSON(http.Client{}, url, &request, &response); err != nil {
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
	adminURLs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(adminNode))
	require.NoError(t, err)
	url := "http://" + adminURLs[0] + "/ts/query"
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

	var response tspb.TimeSeriesQueryResponse
	if err := httputil.PostJSON(http.Client{}, url, &request, &response); err != nil {
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

	t.L().Printf("interval stats: %s", intervalsToString(stats))

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
	getFollowerReadCount := func(ctx context.Context, node int) func() error {
		return func() error {
			adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(node))
			if err != nil {
				return err
			}
			url := "http://" + adminUIAddrs[0] + "/_status/vars"
			resp, err := httputil.Get(ctx, url)
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
	g, gCtx := errgroup.WithContext(ctx)
	for i := 1; i <= c.Spec().NodeCount; i++ {
		g.Go(getFollowerReadCount(gCtx, i))
	}
	if err := g.Wait(); err != nil {
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

// runFollowerReadsMixedVersionSingleRegionTest runs a follower-reads test while
// performing a cluster upgrade. The point is to exercise the closed-timestamp
// mechanism in a mixed-version cluster. Running in a single region is
// sufficient for this purpose; we're not testing non-voting replicas here
// (which are used in multi-region tests).
func runFollowerReadsMixedVersionSingleRegionTest(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	require.NoError(t, err)
	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const curVersion = ""

	// Start the cluster at the old version.
	settings := install.MakeClusterSettings()
	settings.Binary = uploadVersion(ctx, t, c, c.All(), predecessorVersion)
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.All())
	topology := topologySpec{multiRegion: false}
	data := initFollowerReadsDB(ctx, t, c, topology)

	// Upgrade one node to the new version and run the test.
	randNode := 1 + rand.Intn(c.Spec().NodeCount)
	t.L().Printf("upgrading n%d to current version", randNode)
	nodeToUpgrade := c.Node(randNode)
	upgradeNodes(ctx, nodeToUpgrade, curVersion, t, c)
	runFollowerReadsTest(ctx, t, c, topologySpec{multiRegion: false}, exactStaleness, data)

	// Upgrade the remaining nodes to the new version and run the test.
	var remainingNodes option.NodeListOption
	for i := 0; i < c.Spec().NodeCount; i++ {
		if i+1 == randNode {
			continue
		}
		remainingNodes = remainingNodes.Merge(c.Node(i + 1))
	}
	t.L().Printf("upgrading nodes %s to current version", remainingNodes)
	upgradeNodes(ctx, remainingNodes, curVersion, t, c)
	runFollowerReadsTest(ctx, t, c, topologySpec{multiRegion: false}, exactStaleness, data)
}
