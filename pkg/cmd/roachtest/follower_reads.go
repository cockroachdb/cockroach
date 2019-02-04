// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"bufio"
	"context"
	gosql "database/sql"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func registerFollowerReads(r *registry) {
	r.Add(testSpec{
		Name: "follower-reads/nodes=3",
		Cluster: clusterSpec{
			NodeCount: 3,
			CPUs:      2,
			Geo:       true,
		},
		Run: runFollowerReadsTest,
	})
}

// runFollowerReadsTest is a basic litmus test that follower reads work.
// The test does the following:
//
//  * Creates a database and table.
//  * Installs a number of rows into that table.
//  * Queries the data initially with a recent timestamp and expecting an
//    error because the table does not exist in the past immediately following
//    creation.
//  * Waits until the required duration has elapsed such that the installed
//    data can be read with a follower read issued using `follower_timestamp()`.
//  * Performs a single `follower_timestamp()` select query against a single
//    row on all of the nodes and then observes the counter metric for
//    store-level follower reads ensuring that they occurred on at least
//    two of the nodes.
//  * Performs reads against the written data on all of the nodes at a steady
//    rate for 20 seconds, ensure that the 90-%ile SQL latencies during that
//    time are under 10ms which implies that no WAN RPCs occurred.
//
func runFollowerReadsTest(ctx context.Context, t *test, c *cluster) {
	crdbNodes := c.Range(1, c.nodes)
	c.Put(ctx, cockroach, "./cockroach", crdbNodes)
	c.Wipe(ctx, crdbNodes)
	c.Start(ctx, t, crdbNodes)

	var conns []*gosql.DB
	for i := 0; i < c.nodes; i++ {
		conns = append(conns, c.Conn(ctx, i+1))
		defer conns[i].Close()
	}
	db := conns[0]

	if _, err := db.ExecContext(ctx, "SET CLUSTER SETTING kv.closed_timestamp.follower_reads_enabled = 'true'"); err != nil {
		t.Fatalf("failed to enable follower reads: %v", err)
	}
	if r, err := db.ExecContext(ctx, "CREATE DATABASE test;"); err != nil {
		t.Fatalf("failed to create database: %v %v", err, r)
	}
	if r, err := db.ExecContext(ctx, "CREATE TABLE test.test ( k INT8, v INT8, PRIMARY KEY (k) )"); err != nil {
		t.Fatalf("failed to create table: %v %v", err, r)
	}

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
	chooseKV := func() (k int, v int64) {
		for k, v = range data {
			return
		}
		return
	}
	verifySelect := func(ctx context.Context, node, k int, expectError bool, expectedVal int64) func() error {
		return func() error {
			db := conns[node-1]
			r := db.QueryRowContext(ctx, "SELECT v FROM test.test AS OF SYSTEM TIME follower_read_timestamp() WHERE k = $1", k)
			var got int64
			if err := r.Scan(&got); err != nil {
				// Ignore errors due to cancellation.
				if ctx.Err() != nil {
					return nil
				}
				if expectError {
					return nil
				}
				return err
			}

			if expectError {
				return errors.Errorf("failed to get expected error on node %d", node)
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
				err := verifySelect(ctx, node, k, false, v)()
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
	// Verify error on immediate read.
	g, gCtx = errgroup.WithContext(ctx)
	for i := 1; i <= c.nodes; i++ {
		// Expect an error performing a historical read at first because the table
		// won't have been created yet.
		g.Go(verifySelect(gCtx, i, 0, true, 0))
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("unexpected error performing historical reads: %v", err)
	}
	// Wait for follower_timestamp() historical reads to have data.
	// TODO(ajwerner): introspect the database to determine how long to wait.
	const followerReadsDur = 48 * time.Second
	select {
	case <-time.After(followerReadsDur):
	case <-ctx.Done():
		t.Fatalf("context canceled: %v", ctx.Err())
	}
	// Read the follower read counts before issuing the follower reads to observe
	// the delta and protect from follower reads which might have happened due to
	// system queries.
	followerReadsBefore, err := getFollowerReadCounts(ctx, c)
	if err != nil {
		t.Fatalf("failed to get follower read counts: %v", err)
	}
	// Perform reads at follower_timestamp() and ensure we get the expected value.
	g, gCtx = errgroup.WithContext(ctx)
	k, v := chooseKV()
	for i := 1; i <= c.nodes; i++ {
		g.Go(verifySelect(gCtx, i, k, false, v))
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("error verifying node values: %v", err)
	}
	// Verify that the follower read count increments on at least two nodes.
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
	if nodesWhichSawFollowerReads < 2 {
		t.Fatalf("fewer than 2 follower reads occurred: saw %v before and %v after",
			followerReadsBefore, followerReadsAfter)
	}
	// Run reads for 60s which given the metrics window of 10s should guarantee
	// that the most recent SQL latency time series data should relate to at least
	// some of these reads.
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	g, gCtx = errgroup.WithContext(timeoutCtx)
	for i := 0; i < concurrency; i++ {
		g.Go(doSelects(gCtx, rand.Intn(c.nodes)+1))
	}
	start := timeutil.Now()
	if err := g.Wait(); err != nil && timeoutCtx.Err() == nil {
		t.Fatalf("error reading data: %v", err)
	}
	// Perform a ts query to verify that the SQL latencies were well below the
	// WAN latencies which should be at least 50ms.
	verifySQLLatency(ctx, c, t, c.Node(1), start, timeutil.Now(), 20*time.Millisecond)
}

// verifySQLLatency verifies that the client-facing SQL latencies in the 99th
// percentile remain below target latency between start and end ignoring the
// first 20s.
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
				Name: "cr.node.sql.service.latency-p99",
			},
		},
	}
	var response tspb.TimeSeriesQueryResponse
	if err := httputil.PostJSON(http.Client{}, url, &request, &response); err != nil {
		t.Fatal(err)
	}
	perTenSeconds := response.Results[0].Datapoints[2:]
	// Drop the first 20 seconds of datapoints as a "ramp-up" period.
	if len(perTenSeconds) < 2 {
		t.Fatalf("not enough ts data to verify latency")
	}
	for _, dp := range perTenSeconds {
		if time.Duration(dp.Value) > targetLatency {
			t.Fatalf("latency value %v for ts data point %v is above target latency %v",
				time.Duration(dp.Value), dp, targetLatency)
		}
	}
}

const followerReadsMetric = "follower_reads_success_count"

// getFollowerReadCounts returns a slice from node to
func getFollowerReadCounts(ctx context.Context, c *cluster) ([]int, error) {
	followerReadCounts := make([]int, c.nodes)
	getFollowerReadCount := func(ctx context.Context, node int) func() error {
		return func() error {
			url := "http://" + c.ExternalAdminUIAddr(ctx, c.Node(node))[0] + "/_status/vars"
			resp, err := http.Get(url)
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
	for i := 1; i <= c.nodes; i++ {
		g.Go(getFollowerReadCount(gCtx, i))
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return followerReadCounts, nil
}

var prometheusMetricStringPattern = "(?P<metric>\\w+)\\{" +
	"(?P<labelvalues>(\\w+=\".*\",)*(\\w+=\".*\")?)\\}\\s+(?P<value>.*)"
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
