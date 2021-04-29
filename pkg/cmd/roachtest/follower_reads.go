// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

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
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func registerFollowerReads(r *testRegistry) {
	register := func(survival survivalGoal, locality localitySetting) {
		r.Add(testSpec{
			Name:  fmt.Sprintf("follower-reads/survival=%s/locality=%s", survival, locality),
			Owner: OwnerKV,
			Cluster: makeClusterSpec(
				6, /* nodeCount */
				cpu(2),
				geo(),
				// This zone option looks strange, but it makes more sense once you
				// understand what the test is doing. The test creates a multi-region
				// database with either ZONE or REGION survivability and with a PRIMARY
				// REGION of us-east1. This means that for ZONE survivability, the test
				// wants 3 nodes in us-east1, 1 in us-west1, and 1 in europe-west2. For
				// REGION surviability, the test wants 2 nodes in us-east1, 2 (or 1) in
				// us-west1, and 1 (or 2) in europe-west2.
				zones("us-east1-b,us-east1-b,us-east1-b,us-west1-b,us-west1-b,europe-west2-b"),
			),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runFollowerReadsTest(ctx, t, c, survival, locality)
			},
		})
	}
	for _, survival := range []survivalGoal{zone, region} {
		for _, locality := range []localitySetting{regional, global} {
			register(survival, locality)
		}
	}
}

// The survival goal of a multi-region database: ZONE or REGION.
type survivalGoal string

// The locality setting of a multi-region table: REGIONAL or GLOBAL.
type localitySetting string

const (
	zone   survivalGoal = "zone"
	region survivalGoal = "region"

	regional localitySetting = "regional"
	global   localitySetting = "global"
)

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
	ctx context.Context, t *test, c *cluster, survival survivalGoal, locality localitySetting,
) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Wipe(ctx)
	c.Start(ctx, t)

	var conns []*gosql.DB
	for i := 0; i < c.spec.NodeCount; i++ {
		conns = append(conns, c.Conn(ctx, i+1))
		defer conns[i].Close()
	}
	db := conns[0]

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

	// Create a multi-region database and table.
	_, err = db.ExecContext(ctx, `CREATE DATABASE test`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `ALTER DATABASE test SET PRIMARY REGION "us-east1"`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `ALTER DATABASE test ADD REGION "us-west1"`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `ALTER DATABASE test ADD REGION "europe-west2"`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER DATABASE test SURVIVE %s FAILURE`, survival))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE test.test ( k INT8, v INT8, PRIMARY KEY (k) )`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE test.test SET LOCALITY %s`, locality))
	require.NoError(t, err)

	// Wait until the table has completed up-replication.
	t.l.Printf("waiting for up-replication...")
	require.NoError(t, retry.ForDuration(5*time.Minute, func() error {
		// Check that the table has the expected number of voting and non-voting
		// replicas.
		const q1 = `
			SELECT
				coalesce(array_length(voting_replicas, 1), 0),
				coalesce(array_length(non_voting_replicas, 1), 0)
			FROM
				crdb_internal.ranges_no_leases
			WHERE
				table_name = 'test'`
		var voters, nonVoters int
		if err := db.QueryRowContext(ctx, q1).Scan(&voters, &nonVoters); err != nil {
			t.l.Printf("retrying: %v\n", err)
			return err
		}

		var ok bool
		if survival == zone {
			// Expect 3 voting replicas and 2 non-voting replicas.
			ok = voters == 3 && nonVoters == 2
		} else {
			// Expect 5 voting replicas and 0 non-voting replicas.
			ok = voters == 5 && nonVoters == 0
		}
		if !ok {
			return errors.Newf("up-replication not complete, found %d voters and %d non_voters", voters, nonVoters)
		}

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
		if err := db.QueryRowContext(ctx, q2).Scan(&distinctRegions); err != nil {
			t.l.Printf("retrying: %v\n", err)
			return err
		}
		if distinctRegions != 3 {
			return errors.Newf("rebalancing not complete, table in %d regions", distinctRegions)
		}
		return nil
	}))

	const rows = 100
	const concurrency = 32
	sem := make(chan struct{}, concurrency)
	data := make(map[int]int64)
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
	// chooseKV picks a random key-value pair by exploiting the pseudo-random
	// ordering of keys when traversing a map within a range statement.
	chooseKV := func() (k int, v int64) {
		for k, v = range data {
			return k, v
		}
		panic("data is empty")
	}
	verifySelect := func(ctx context.Context, node, k int, expectedVal int64) func() error {
		return func() error {
			nodeDB := conns[node-1]
			var aost string
			if locality == regional {
				// For REGIONAL tables, only stale reads can be served off
				// followers.
				aost = "AS OF SYSTEM TIME follower_read_timestamp()"
			} else {
				// For GLOBAL tables, we can perfrom consistent reads and expect
				// them to be served off followers.
				aost = ""
			}
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
	// Insert the data.
	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < rows; i++ {
		g.Go(insert(gCtx, i))
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	if locality == regional {
		// For REGIONAL tables, wait for follower_read_timestamp() historical
		// reads to have data. For GLOBAL tables, this isn't needed, because
		// we will be reading consistently (non-stale).
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
	// latency threshold of 50ms, which should be well below the latency of a
	// cross-region hop to read from the leaseholder but well above the latency
	// of a follower read.
	_, err = db.ExecContext(ctx, "SET CLUSTER SETTING sql.trace.stmt.enable_threshold = '50ms'")
	require.NoError(t, err)

	// Read the follower read counts before issuing the follower reads to observe
	// the delta and protect from follower reads which might have happened due to
	// system queries.
	followerReadsBefore, err := getFollowerReadCounts(ctx, c)
	if err != nil {
		t.Fatalf("failed to get follower read counts: %v", err)
	}
	// Perform reads on each node and ensure we get the expected value. Do so a
	// few times on each follower to give caches time to warm up.
	g, gCtx = errgroup.WithContext(ctx)
	k, v := chooseKV()
	for i := 1; i <= c.spec.NodeCount; i++ {
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
	followerReadsAfter, err := getFollowerReadCounts(ctx, c)
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
	// Run reads for 3m which given the metrics window of 10s should guarantee
	// that the most recent SQL latency time series data should relate to at least
	// some of these reads.
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	g, gCtx = errgroup.WithContext(timeoutCtx)
	for i := 0; i < concurrency; i++ {
		g.Go(doSelects(gCtx, rand.Intn(c.spec.NodeCount)+1))
	}
	start := timeutil.Now()
	if err := g.Wait(); err != nil && timeoutCtx.Err() == nil {
		t.Fatalf("error reading data: %v", err)
	}
	// Perform a ts query to verify that the SQL latencies were well below the
	// WAN latencies which should be at least 50ms.
	verifySQLLatency(ctx, c, t, c.Node(1), start, timeutil.Now(), 20*time.Millisecond)
}

func computeFollowerReadDuration(ctx context.Context, db *gosql.DB) (time.Duration, error) {
	var targetDurationStr string
	err := db.QueryRowContext(ctx, "SELECT value FROM crdb_internal.cluster_settings WHERE variable = 'kv.closed_timestamp.target_duration'").Scan(&targetDurationStr)
	if err != nil {
		return 0, err
	}
	targetDuration, err := time.ParseDuration(targetDurationStr)
	if err != nil {
		return 0, err
	}
	var closeFraction float64
	err = db.QueryRowContext(ctx, "SELECT value FROM crdb_internal.cluster_settings WHERE variable = 'kv.closed_timestamp.close_fraction'").Scan(&closeFraction)
	if err != nil {
		return 0, err
	}
	// target_multiple is a hidden setting which cannot be read from crdb_internal
	// so for now hard code to the default value.
	const targetMultiple = 3
	return time.Duration(float64(targetDuration) * (1 + targetMultiple*closeFraction)), nil
}

// verifySQLLatency verifies that the client-facing SQL latencies in the 90th
// percentile remain below target latency 80% of the time between start and end
// ignoring the first 20s.
func verifySQLLatency(
	ctx context.Context,
	c *cluster,
	t *test,
	adminNode nodeListOption,
	start, end time.Time,
	targetLatency time.Duration,
) {
	// Query needed information over the timespan of the query.
	adminURLs := c.ExternalAdminUIAddr(ctx, adminNode)
	url := "http://" + adminURLs[0] + "/ts/query"
	request := tspb.TimeSeriesQueryRequest{
		StartNanos: start.UnixNano(),
		EndNanos:   end.UnixNano(),
		// Ask for 10s intervals.
		SampleNanos: (10 * time.Second).Nanoseconds(),
		Queries: []tspb.Query{
			{
				Name: "cr.node.sql.service.latency-p90",
			},
		},
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

const followerReadsMetric = "follower_reads_success_count"

// getFollowerReadCounts returns a slice from node to follower read count
// according to the metric.
func getFollowerReadCounts(ctx context.Context, c *cluster) ([]int, error) {
	followerReadCounts := make([]int, c.spec.NodeCount)
	getFollowerReadCount := func(ctx context.Context, node int) func() error {
		return func() error {
			url := "http://" + c.ExternalAdminUIAddr(ctx, c.Node(node))[0] + "/_status/vars"
			resp, err := httputil.Get(ctx, url)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return errors.Errorf("invalid non-200 status code %v", resp.StatusCode)
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
	for i := 1; i <= c.spec.NodeCount; i++ {
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
