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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func registerReplicaCircuitBreaker(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "replica-circuit-breaker",
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(6),
		Tags:    []string{"weekly"},
		Run:     runReplicaCircuitBreaker,
	})
}

func runReplicaCircuitBreaker(ctx context.Context, t test.Test, c cluster.Cluster) {
	const (
		dur             = 10 * time.Minute
		loseQuorumAfter = dur / 2
		warehouses      = 50  // NB: if this ever changes, qps counts might need adjustment
		minHealthyQPS   = 5.0 // min successes per second before quorum loss
		minUnhealthyQPS = 5.0 // min (request+errors) per second after quorum lost for a while
	)

	// Run TPCC and half-way through lose two nodes, which is likely to render
	// some of the TPCC ranges (but not system ranges, which are 5x replicated)
	// unavailable. We let TPCC tolerate errors. When it is done, we look at the
	// time series and verify
	//
	// a) workload was healthy before outage, as measured by min qps.
	// b) workload made "progress" during the outage (after a transition period), as
	//    measured by min qps.
	//
	// At the time of writing, we see ~11 qps during a) and ~15-20 qps during b).
	//
	// At select points in the test we also verify bounded staleness and vanilla follower reads.
	chaosEventCh := make(chan ChaosEvent)
	opts := tpccOptions{
		Warehouses:     warehouses,
		ExtraRunArgs:   "--tolerate-errors",
		ExtraSetupArgs: "",
		Chaos: func() Chaos {
			return Chaos{
				Timer: Periodic{
					Period:   loseQuorumAfter,
					DownTime: 99999 * time.Hour, // stay down forever; node restarts when chaos winds down
				},
				Target: func() option.NodeListOption {
					// Stopping two nodes is very likely to lose quorum on at least one TPCC
					// range, while keeping the system ranges (which are 5x replicated)
					// healthy.
					//
					// NB: we spare n1 since it's special cased by the tpcc checks that run
					// after completion of the workload.
					return c.Nodes(2, 3)
				},
				Stopper:      time.After(dur),
				ChaosEventCh: chaosEventCh,
			}
		},
		Duration:              dur,
		EnableCircuitBreakers: true,
	}
	firstGoodCh := make(chan time.Time, 1)
	lastGoodCh := make(chan time.Time, 1)
	downCh := make(chan time.Time, 1)
	upCh := make(chan time.Time, 1)

	checkFollowerReadsCh := make(chan time.Time, 100)

	// We make a monitor for the test goroutine that consumes chaos events and runs one-off checks.
	// Note that `runTPCC` uses its own monitor internally.
	m := c.NewMonitor(ctx, option.NoNodes{})
	m.Go(func(ctx context.Context) error {
		for ev := range chaosEventCh {
			t.L().Printf("%+v", ev)
			switch ev.Type {
			case ChaosEventTypeStart:
				maybeSendTime(checkFollowerReadsCh, ev.Time)
				maybeSendTime(firstGoodCh, ev.Time)
			case ChaosEventTypePreShutdown:
				maybeSendTime(checkFollowerReadsCh, ev.Time)
				maybeSendTime(lastGoodCh, ev.Time)
			case ChaosEventTypeShutdownComplete:
				maybeSendTime(checkFollowerReadsCh, time.Time{})
				maybeSendTime(downCh, ev.Time)
			case ChaosEventTypePreStartup:
				maybeSendTime(checkFollowerReadsCh, time.Time{})
			case ChaosEventTypeStartupComplete:
				maybeSendTime(upCh, ev.Time)
			case ChaosEventTypeEnd:
				maybeSendTime(checkFollowerReadsCh, time.Time{})
				close(checkFollowerReadsCh)
				return nil
			}
		}
		return errors.New("chaos stopping") // should not be hit
	})
	m.Go(func(ctx context.Context) error {
		var tss []time.Time
		// Run an expanding set of checks. The channel getting a zero time is the signal
		// to only run checks on all timestamps seen  so far.
		for ts := range checkFollowerReadsCh {
			if !ts.IsZero() {
				tss = append(tss, ts)
			}
			for _, ts := range tss {
				r := sqlutils.MakeSQLRunner(c.Conn(ctx, t.L(), 1))
				queryTS := ts.Add(-10 * time.Second).Format("2006-01-02 15:04:05")
				q1 := `SELECT count(*) FROM tpcc.order_line AS OF SYSTEM TIME '` + queryTS + `'`
				q2 := `SELECT * FROM tpcc.order_line AS OF SYSTEM TIME with_min_timestamp('` + queryTS + `', true)
	WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 1 AND ol_number = 1`
				r.QueryStr(t, q1)
				t.L().Printf("ok: %s", q1)
				r.QueryStr(t, q2)
				t.L().Printf("ok: %s", q2)
			}
		}
		return nil
	})
	m.Go(func(ctx context.Context) error {
		runTPCC(ctx, t, c, opts)
		return nil
	})
	m.Wait()

	// The test has finished, so let's examine the metrics.
	firstGood, lastGood, down, up :=
		maybeRecvTime(ctx, firstGoodCh), maybeRecvTime(ctx, lastGoodCh), maybeRecvTime(ctx, downCh), maybeRecvTime(ctx, upCh)
	require.NotZero(t, firstGood)
	require.NotZero(t, lastGood)
	require.NotZero(t, down)
	require.NotZero(t, up)

	// Quorum loss just occurred. Depending on luck reads may succeed for a few more seconds
	// until the lease times out, but either way now's a good time to start some additional checks.
	// Concretely, follower reads should work, and so should bounded staleness reads.
	t.L().Printf("cluster healthy from %s to %s, quorum loss at %s, restored at %s", firstGood, lastGood, down, up)

	promAddr := func() string {
		sl, err := c.ExternalIP(ctx, t.L(), c.Node(c.Spec().NodeCount))
		require.NoError(t, err)
		require.Len(t, sl, 1)
		return fmt.Sprintf("http://%s:9090", sl[0])
	}()

	{
		// No errors before chaos kicked in.
		const q = `rate(workload_tpcc_newOrder_error_total[1m])`
		visitPromRangeQuery(
			ctx, t, promAddr, q, firstGood, lastGood,
			func(ts time.Time, val model.SamplePair) {
				if val.Value > 0 {
					t.Errorf("at %s: nonzero TPCC error rate while cluster should have been healthy: %.2f", ts, val.Value)
				}
			},
		)
	}

	{
		// At least some throughput before chaos kicked in.
		const q = `rate(workload_tpcc_newOrder_success_total[1m])`
		min, _ := visitPromRangeQuery(
			// NB: we intentionally check only at the very end, right before chaos
			// strikes. The workload tends to ramp up (even with ramp disabled),
			// possibly due to lease movement, etc.
			ctx, t, promAddr, q, lastGood, lastGood,
			func(ts time.Time, val model.SamplePair) {
				if val.Value < minHealthyQPS {
					t.Errorf("at %s: very low qps before chaos kicked in: %.2f", ts, val.Value)
				}
			},
		)
		t.L().Printf("under healthy cluster: ~%.2f qps", min)
	}

	{
		// Starting at one minute after quorum was lost, we should see at least a
		// steady trickle of errors, or a steady trickle of successes. (If
		// miraculously no range lost quorum it would be all successes, if they all
		// lost quorum it would be just errors).
		const q = `rate(workload_tpcc_newOrder_error_total[1m])+rate(workload_tpcc_newOrder_success_total[1m])`
		min, max := visitPromRangeQuery(
			ctx, t, promAddr, q, down.Add(time.Minute), up,
			func(ts time.Time, val model.SamplePair) {
				if val.Value < minUnhealthyQPS {
					t.Errorf("at %s: unexpectedly low combined qps rate: %.2f", ts, val.Value)
				}
			},
		)
		t.L().Printf("under unhealthy cluster: [%.2f, %.2f] (combined error+success) qps", min, max)
	}
}

func visitPromRangeQuery(
	ctx context.Context,
	t require.TestingT,
	promAddr string,
	query string,
	from, to time.Time,
	visit func(time.Time, model.SamplePair),
) (_min, _max float64) {
	// Stay away from the edges of the interval, to avoid unexpected outcomes
	// due to prometheus interpolation, etc.
	from = from.Add(prometheus.DefaultScrapeInterval)
	to = to.Add(-prometheus.DefaultScrapeInterval)
	if from.After(to) {
		from = to
	}

	client, err := promapi.NewClient(promapi.Config{
		Address: promAddr,
	})
	require.NoError(t, err)
	promClient := promv1.NewAPI(client)
	mtr, ws, err := promClient.QueryRange(ctx, query, promv1.Range{Start: from, End: to, Step: 15 * time.Second})
	if err != nil {
		t.Errorf("%s", err)
		t.FailNow()
	}
	if len(ws) != 0 {
		t.Errorf("warnings: %v", ws)
	}
	require.Len(t, mtr.(model.Matrix), 1, "multiple time series found: %v", mtr)

	var min, max float64
	var n int
	for _, val := range mtr.(model.Matrix)[0].Values {
		n++
		visit(timeutil.Unix(int64(val.Timestamp/1000), 0), val)
		v := float64(val.Value)
		if n == 1 || min > v {
			min = v
		}
		if n == 1 || max < v {
			max = v
		}
	}
	require.NotZero(t, n, "no data returned for %s on %s-%s", query, from, to)
	return min, max
}

func maybeSendTime(c chan time.Time, t time.Time) {
	select {
	case c <- t:
	default:
	}
}

func maybeRecvTime(ctx context.Context, c chan time.Time) time.Time {
	select {
	case t := <-c:
		return t
	case <-ctx.Done():
		return time.Time{}
	}
}
