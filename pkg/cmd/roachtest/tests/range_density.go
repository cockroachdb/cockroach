// Copyright 2022 The Cockroach Authors.
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
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/search"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/ttycolor"
	"github.com/pkg/errors"
)

func verifyWriteProberLatency(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	start, end time.Time,
	targetLatency time.Duration,
) error {
	adminURLs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(1))
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + adminURLs[0] + "/ts/query"
	request := tspb.TimeSeriesQueryRequest{
		StartNanos: start.UnixNano(),
		EndNanos:   end.UnixNano(),
		// Ask for 10s intervals.
		SampleNanos: (10 * time.Second).Nanoseconds(),
		Queries: []tspb.Query{{
			Name:             "cr.node.kv.prober.write.latency-p90",
			SourceAggregator: tspb.TimeSeriesQueryAggregator_AVG.Enum(),
		}},
	}
	var response tspb.TimeSeriesQueryResponse
	if err := httputil.PostJSON(http.Client{}, url, &request, &response); err != nil {
		t.Fatal(err)
	}
	perTenSeconds := response.Results[0].Datapoints
	if len(perTenSeconds) < 3 {
		t.Fatalf("not enough ts data to verify latency; got %v", perTenSeconds)
	}
	maxLatency := time.Duration(0)
	for _, dp := range perTenSeconds {
		val := time.Duration(dp.Value)
		if val > targetLatency {
			return errors.Errorf("write latency exceeded target: %s > %s", val, targetLatency)
		}
		if val > maxLatency {
			maxLatency = val
		}
	}

	t.L().Printf("verified write prober latency; max reading: %s", humanizeutil.Duration(maxLatency))
	return nil
}

func verifyLivenessFailures(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	start, end time.Time,
	maxAllowedLivenessFailures int,
) error {
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(1))
	if err != nil {
		t.Fatal(err)
	}
	adminURL := adminUIAddrs[0]
	response := mustGetMetrics(
		t, adminURL, start, end, []tsQuery{{"cr.node.liveness.heartbeatfailures", total}},
	)

	// Look at the latest datapoint and verify that there were less than
	// `maxAllowedLivenessFailures` at that point.
	latestDatapoint := response.Results[0].Datapoints[len(response.Results[0].Datapoints)-1]
	if int(latestDatapoint.Value) > maxAllowedLivenessFailures {
		return errors.Errorf(
			"liveness failures exceeded max allowed: %f > %f",
			latestDatapoint.Value,
			maxAllowedLivenessFailures,
		)
	}

	t.L().Printf("verified liveness failures do not exceed %d (observed %d)", maxAllowedLivenessFailures, int(latestDatapoint.Value))
	return nil
}

func toggleWriteProber(db *gosql.DB, enable bool) {
	if _, err := db.Exec(fmt.Sprintf(`SET CLUSTER SETTING kv.prober.write.enabled = %t`, enable)); err != nil {
		panic(err)
	}
}

func verifyClusterHealth(ctx context.Context, c cluster.Cluster, db *gosql.DB, t test.Test) error {
	toggleWriteProber(db, true)
	// Only keep the prober active when we're not adding splits, to avoid muddling
	// the prober metrics.
	defer toggleWriteProber(db, false)

	start := timeutil.Now()
	time.Sleep(1 * time.Minute)
	end := timeutil.Now()

	if err := verifyWriteProberLatency(
		ctx, c, t, start, end, 100*time.Millisecond, /* targetLatency */
	); err != nil {
		return err
	}
	if err := verifyLivenessFailures(
		ctx, c, t, start, end, 0, /* maxAllowedLivenessFailures */
	); err != nil {
		return err
	}
	return nil
}

func addSplits(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	start, end, splitConcurrency int,
	scatter bool,
) error {
	g := ctxgroup.WithContext(ctx)
	splits := make(chan int, end-start)
	splitPoints := make([]int, 0, end-start)
	for i := start; i < end; i++ {
		splitPoints = append(splitPoints, i)
	}
	// Split in random order, not serially in order to avoid a single node doing
	// all the work.
	rand.Shuffle(
		len(splitPoints),
		func(i, j int) { splitPoints[i], splitPoints[j] = splitPoints[j], splitPoints[i] },
	)
	for _, split := range splitPoints {
		splits <- split
	}
	close(splits)

	db := c.Conn(ctx, t.L(), 1)
	// Create the first 100 split points beforehand and scatter them out to all
	// the nodes. We do this a bunch of times to avoid dealing with
	// https://github.com/cockroachdb/cockroach/issues/35907.
	for i := 0; i < int(math.Min(100, float64(end-start))); i++ {
		split := <-splits
		if _, err := db.Exec(
			fmt.Sprintf(`ALTER TABLE test SPLIT AT VALUES (%d)`, split),
		); err != nil {
			return err
		}
		if scatter && i%10 == 0 {
			if _, err := db.Exec(`ALTER TABLE test SCATTER`); err != nil {
				return err
			}
		}
	}

	for w := 0; w < splitConcurrency; w++ {
		// Give each worker its own connection.
		conn := c.Conn(ctx, t.L(), w%3+1 /* node */)
		g.GoCtx(func(ctx context.Context) error {
			for split := range splits {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if _, err := conn.Exec(fmt.Sprintf(`ALTER TABLE test SPLIT AT VALUES (%d)`, split)); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}

	return g.Wait()
}

// runRangeDensity runs a `LineSearcher` to find the max number of inactive
// replicas a node can support. On each iteration of the search, the test
// classifies the iteration as "passing" if, for the 1 minute directly after the
// replicas were created, the KV write prober latencies (at the tail) haven't
// exceeded a pre-defined threshold (set to a generous 50ms) and there are no
// liveness failures. This passing criteria has been picked with the rationale
// that if these checks were failing, the cluster would essentially be unusable.
func runRangeDensity(ctx context.Context, t test.Test, c cluster.Cluster, disableQuiescence bool) {
	roachNodes := c.Spec().NodeCount - 1

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Search between 20K and 400K ranges per vCPU. Start at 80K.
	minNumSplits := 2e4 * c.Spec().CPUs
	maxNumSplits := 4e5 * c.Spec().CPUs
	startNumSplits := 80000 * c.Spec().CPUs
	// NB: We keep the stepsize small since we're picking `startNumSplits` to be
	// fairly close to the final result.
	stepSize := 1250 * c.Spec().CPUs
	precision := 5000 * c.Spec().CPUs
	if c.IsLocal() {
		minNumSplits = 1e2
		maxNumSplits = 4e3
		startNumSplits = 1.5e2
		stepSize = int(1e2)
		precision = int(5e2)
	}
	searcher := search.NewLineSearcher(minNumSplits, maxNumSplits, startNumSplits, stepSize, precision)

	lastIterSucceeded := false
	lastIterNumSplits := 0
	// If the last iteration was a failure, start with a fresh cluster.
	setup := func() {
		if lastIterSucceeded {
			return
		}
		c.Wipe(ctx, c.Range(1, roachNodes))
		c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, roachNodes))
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(roachNodes+1))
		settings := install.MakeClusterSettings()
		settings.Env = append(
			settings.Env, "COCKROACH_MEMPROF_INTERVAL=1m",
			"COCKROACH_DISABLE_QUIESCENCE="+strconv.FormatBool(disableQuiescence),
		)
		c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Range(1, roachNodes))
		_, err := db.Exec(
			fmt.Sprintf(
				`CREATE TABLE test (k INT PRIMARY KEY); INSERT INTO test SELECT generate_series(1, %d)`,
				maxNumSplits,
			),
		)
		if err != nil {
			t.Fatal(err)
		}
		// Set up kv prober to probe more frequently on each node.
		if _, err := db.ExecContext(
			ctx, `SET CLUSTER SETTING kv.prober.write.interval = '500ms'`,
		); err != nil {
			t.Fatal(err)
		}
		// Try our best to avoid leases being concentrated on a single node.
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING kv.allocator.min_lease_transfer_interval='0s'",
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false",
		); err != nil {
			t.Fatal(err)
		}
	}

	searchPredicate := func(numSplits int) (ok bool, err error) {
		defer func() {
			// Record the result of the last iteration to avoid re-doing work in the
			// next one.
			lastIterSucceeded = ok
			lastIterNumSplits = 0
			if ok {
				lastIterNumSplits = numSplits
			}
		}()

		t.Status(fmt.Sprintf("testing %d splits", numSplits))
		setup()
		m := c.NewMonitor(ctx, c.Range(1, roachNodes))

		m.Go(func(ctx context.Context) error {
			splitsToAdd := numSplits
			if lastIterSucceeded {
				// If the last iteration was a success, only add the extra splits.
				splitsToAdd -= lastIterNumSplits
			}
			t.L().Printf("adding %d splits", splitsToAdd)
			splitConcurrency := 6 * c.Spec().CPUs
			err := addSplits(
				ctx, t, c, lastIterNumSplits, numSplits,
				splitConcurrency,
				// Only scatter at the beginning, not if we're reusing the same cluster.
				splitsToAdd == numSplits, /* scatter */
			)
			if err != nil {
				return errors.Wrapf(err, "failed to add %d splits", numSplits)
			}

			// After we're done creating the required number of splits, check for
			// any liveness failures, and ensure that the kv write prober latency
			// did not cross our threshold.
			t.L().Printf("verifying cluster health")
			if err := verifyClusterHealth(ctx, c, db, t); err != nil {
				return err
			}
			return nil
		})
		if err := m.WaitE(); err != nil {
			t.L().Printf("iteration with %d splits failed with: %v", numSplits, err)
			return false, nil
		}
		return true, nil
	}
	res, err := searcher.Search(searchPredicate)
	if err != nil {
		t.Fatal(errors.Wrapf(err, "line search failed with error: %v", err))
	}

	// TODO: Emit to roachperf
	ttycolor.Stdout(ttycolor.Green)
	t.L().Printf("------\nMAX RANGES = %d\n------\n\n", res)
	ttycolor.Stdout(ttycolor.Reset)
}

type rangeDensitySpec struct {
	CPUs              int
	HighMem           bool
	DisableQuiescence bool
	Skip              bool
}

// registerRangeDensity registers the range density roachtest. This test is
// supposed to search for the maximum number of empty ranges a cluster can
// support without either crashing, its nodes failing liveness heartbeats or
// without the kv prober latencies going out of control.
func registerRangeDensity(r registry.Registry) {
	specs := []rangeDensitySpec{
		// Standard machines
		{CPUs: 4},
		{CPUs: 8},
		{CPUs: 16},
		// TODO(aayush): This ooms because n1-standard-32 machines have 32GB of RAM
		// instead of 120GB on roachtests for some reason. Figure out why that's
		// happening
		{CPUs: 32, Skip: true},

		// High memory machines.
		{CPUs: 4, HighMem: true},
		{CPUs: 8, HighMem: true},
		{CPUs: 16, HighMem: true},
		{CPUs: 32, HighMem: true},

		// Disable quiescence.
		{CPUs: 4, DisableQuiescence: true},
		{CPUs: 8, DisableQuiescence: true},
		{CPUs: 16, DisableQuiescence: true},
	}
	for _, densitySpec := range specs {
		r.Add(registry.TestSpec{
			Skip: func() string {
				if densitySpec.Skip {
					return "skipped"
				}
				return ""
			}(),
			Name: fmt.Sprintf(
				"range-density/cpus=%d/highmem=%t/quiescence=%t",
				densitySpec.CPUs, densitySpec.HighMem, !densitySpec.DisableQuiescence,
			),
			Owner: registry.OwnerKV,
			// TODO(aayush): Consider making this a weekly test.
			Cluster: r.MakeClusterSpec(
				4 /* nodeCount */, spec.CPU(densitySpec.CPUs), spec.HighMem(densitySpec.HighMem),
			),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runRangeDensity(ctx, t, c, densitySpec.DisableQuiescence)
			},
			Timeout: 18 * time.Hour,
		})
	}
}
