// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package tests

import (
	"context"
	"fmt"
	"math"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
)

// kvRangefeedTest is a performance test for rangefeeds. They run a workload
// with a changefeed and ensure that (1) the changefeed enters a steady state
// and (2) the foreground SQL workload isn't impacted.
type kvRangefeedTest struct {
	// writeMaxRate is the req/s of the write workload.
	//
	// TODO(ssd): An issue to keep in mind is that we want read load as well so
	// that we can push CPU on the
	writeMaxRate int64

	// duration is how long the test will run. The duration must be long enough
	// for the catch-up scan to complete if expectChangefeedCatchesUp is true.
	duration time.Duration

	// How long of a catch-up scan should this test simulate. The test will use
	// this to determine how many rows to insert before the workload is started.
	//
	// NB: This is NOT the expected length of the catch-up scan. Rather this is
	// how long the changefeed is "offline" for.
	catchUpInterval time.Duration
	// changefeedDelay is how long the workload will run before the changefeed is
	// started. This period is used to calculate the pre-changefeed baseline for
	// the foreground workload latency.
	changefeedDelay time.Duration

	// sinkProvisioning is the throughput of the sink expressed in a percentage of
	// the writeMaxRate.
	sinkProvisioning float64
	// splits is the number of splits to initialize the KV table with.
	splits int

	// expectChangefeedCatchesUp is whether or not we expect the changefeed to
	// catch up in the given configuration. We don't expect under-provisioned
	// changefeeds to catch up.
	expectChangefeedCatchesUp bool
}

func (t kvRangefeedTest) changefeedMaxRate() int64 {
	return int64(float64(t.writeMaxRate) * t.sinkProvisioning)
}

func (t kvRangefeedTest) insertCount() (int64, error) {
	intervalForInsert := t.catchUpInterval - t.changefeedDelay
	if intervalForInsert < 0 {
		return 0, errors.AssertionFailedf("invalid test configuration: catchup interval (%s) smaller than changefeed delay (%s)",
			t.catchUpInterval,
			t.changefeedDelay,
		)
	}
	return int64(intervalForInsert.Seconds() * float64(t.writeMaxRate)), nil

}

// expectedCatchUpDuration returns how long it should take for the resolved timestamp to be "caught up."
func (t kvRangefeedTest) expectedCatchupDuration() (time.Duration, error) {
	catchUpRows := int64(t.catchUpInterval.Seconds() * float64(t.writeMaxRate))
	catchUpRate := t.changefeedMaxRate() - t.writeMaxRate
	if catchUpRate <= 0 {
		return 0, errors.AssertionFailedf("invalid test configuration: catch-up rate (%d) is not positive, catch up will not complete", catchUpRate)
	}
	catchUpDuration := time.Duration((catchUpRows / catchUpRate)) * time.Second
	return catchUpDuration, nil
}

func makeKVRangefeedOptions(c cluster.Cluster) (option.StartOpts, install.ClusterSettings) {
	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	settings := install.MakeClusterSettings()
	settings.ClusterSettings["kv.rangefeed.enabled"] = "true"
	// TODO(ssd): We might need something like this for overload cases. But for
	// now let's have the default cases test the default configuration.
	//
	// settings.ClusterSettings["kv.rangefeed.concurrent_catchup_iterators"] = "64"
	//
	// We set the per-changefeed memory limit to a low value because the goal of
	// this test is see the effects of a slow sink on the server-side catch-up
	// scan machinery. Setting it to a low value makes it easy to understand what
	// is happening server-side since we'll have less work queued client-side.
	settings.ClusterSettings["changefeed.memory.per_changefeed_limit"] = "512KiB"
	return startOpts, settings
}

func runKVRangefeed(ctx context.Context, t test.Test, c cluster.Cluster, opts kvRangefeedTest) {
	// Check this early to avoid test misconfigurations.
	var catchUpDur time.Duration
	if opts.expectChangefeedCatchesUp {

		var err error
		catchUpDur, err = opts.expectedCatchupDuration()
		if err != nil {
			t.Fatal(err)
		}

		if opts.duration > 0 && opts.duration < catchUpDur+opts.changefeedDelay {
			t.Fatalf("duration (%s) is insufficient for catch up to complete (%s = %s (delay) + %s (catchup))",
				opts.duration,
				catchUpDur+opts.changefeedDelay,
				opts.changefeedDelay,
				catchUpDur)
		}
	}
	insertCount, err := opts.insertCount()
	if err != nil {
		t.Fatal(err)
	}

	nodes := c.Spec().NodeCount - 1
	startOpts, settings := makeKVRangefeedOptions(c)
	c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	t.Status("initializing workload")
	// We initialize the workload in two steps so that we can grab a logical
	// timestamp that will definitely be from after the table creation but before
	// the rows are inserted.
	initCmd := fmt.Sprintf("./cockroach workload init kv --splits=%d --read-percent 0 --min-block-bytes=1024 --max-block-bytes=1024 {pgurl:1-%d}",
		opts.splits, nodes)
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), initCmd)

	if err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db); err != nil {
		t.Fatal(err)
	}
	// TODO(wenyi): We should document why this is necessary.
	time.Sleep(1 * time.Second)

	var cursorStr string
	if err := db.QueryRow("SELECT cluster_logical_timestamp()").Scan(&cursorStr); err != nil {
		t.Fatal(err)
	}

	t.Status("inserting rows")
	initCmd = fmt.Sprintf("./cockroach workload init kv --insert-count %d --read-percent 0 --data-loader=insert --min-block-bytes=1024 --max-block-bytes=1024 {pgurl:1-%d}",
		insertCount, nodes)
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), initCmd)

	t.L().Printf("initializing workload: %s", initCmd)
	if opts.expectChangefeedCatchesUp {
		t.L().Printf("catch-up expected to take %s", catchUpDur)
	}

	const resolvedTarget = 5 * time.Second

	t.Status("running workload with changefeed")
	m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		opts := []string{
			"--tolerate-errors",
			fmt.Sprintf(" --histograms=%s/%s", t.PerfArtifactsDir(), "stats.json"),

			" --changefeed",
			fmt.Sprintf("--changefeed-resolved-target=%s", resolvedTarget),
			fmt.Sprintf("--changefeed-cursor=%s", cursorStr),
			fmt.Sprintf("--changefeed-start-delay=%s", opts.changefeedDelay),
			fmt.Sprintf("--changefeed-max-rate=%d", opts.changefeedMaxRate()),

			fmt.Sprintf("--duration=%s", opts.duration),
			fmt.Sprintf("--max-rate=%d", opts.writeMaxRate),
		}

		cmd := fmt.Sprintf("./cockroach workload run kv --read-percent 0 %s {pgurl:1-%d}",
			strings.Join(opts, " "),
			nodes,
		)
		t.L().Printf("running workload: %s", cmd)
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
		return nil
	})
	m.Wait()

	metrics, err := fetchAndParseMetrics(ctx, t, c)
	if err != nil {
		t.Fatal(err)
	}

	// Assert that the changefeed caught up in a reasonable time.
	if opts.expectChangefeedCatchesUp {
		allowedCatchUpDuration := time.Duration(int64(float64(catchUpDur) * float64(1.1)))
		actualCatchUpDuration := findP99Below(metrics["changefeed-resolved"], resolvedTarget*2) - opts.changefeedDelay
		if actualCatchUpDuration <= 0 {
			t.Fatal("changefeed never caught up")
		} else if actualCatchUpDuration > allowedCatchUpDuration {
			t.Fatalf("changefeed caught up too slowly: %s > %s (%s+10%%)", actualCatchUpDuration, allowedCatchUpDuration, catchUpDur)
		} else {
			t.L().Printf("changefeed caught up quickly enough %s < %s", actualCatchUpDuration, allowedCatchUpDuration)
		}
	}

	// Assert that we didn't impact the worst case latencies too badly. Note that
	// we want to avoid this being too sensitive or we'll end up debugging every
	// latency jump in the underlying workload as part of this test.
	//
	// We look at the p75 of the p99s to see if we've moved the tail latencies
	// non-trivially.
	//
	// NB: If this proves flakey, feel free to change it. We haven't yet put
	// much thought into what the right statistic here would be.
	if opts.changefeedDelay > 0 {
		preChangefeedp99ticks := p99sBetween(metrics["write"], 0, opts.changefeedDelay)
		postChangefeedp99ticks := p99sBetween(metrics["write"], opts.changefeedDelay, time.Duration(math.MaxInt64))

		slices.Sort(preChangefeedp99ticks)
		slices.Sort(postChangefeedp99ticks)

		baseline, err := percentileSortedInput(preChangefeedp99ticks, 0.75)
		if err != nil {
			t.Fatal(err)
		}
		allowed := time.Duration(float64(baseline) * 1.05)
		actual, err := percentileSortedInput(postChangefeedp99ticks, 0.75)
		if err != nil {
			t.Fatal(err)
		}

		if actual > allowed {
			t.Fatalf("changefeed impacted worst case latency too much: %s > %s (%s+5%%)", actual, allowed, baseline)
		} else {
			t.L().Printf("foreground SQL latency within tolerance %s < %s", actual, allowed)
		}

	}

}

func percentileSortedInput(sortedInput []time.Duration, percentile float64) (time.Duration, error) {
	l := len(sortedInput)
	if l == 0 {
		return 0, errors.AssertionFailedf("expected non-empty observations")
	}
	idx := int(float64(l) * percentile)
	idx = min(idx, l-1)
	return sortedInput[idx], nil
}

func p99sBetween(
	ticks []exporter.SnapshotTick, startOffset time.Duration, endOffset time.Duration,
) []time.Duration {
	if len(ticks) == 0 {
		return nil
	}

	ret := make([]time.Duration, 0, len(ticks))
	startTime := ticks[0].Now
	for _, tick := range ticks {
		if tick.Hist == nil {
			continue
		}
		if offset := tick.Now.Sub(startTime); offset >= startOffset && offset < endOffset {
			h := hdrhistogram.Import(tick.Hist)
			if h == nil {
				continue
			}
			ret = append(ret, time.Duration(h.ValueAtQuantile(99)))
		}
	}
	return ret
}

func findP99Below(ticks []exporter.SnapshotTick, target time.Duration) time.Duration {
	if len(ticks) == 0 {
		return 0
	}

	startTime := ticks[0].Now
	for _, tick := range ticks {
		if tick.Hist == nil {
			continue
		}

		h := hdrhistogram.Import(tick.Hist)
		if h == nil {
			continue
		}

		p99 := time.Duration(h.ValueAtQuantile(99))
		if p99 > 0 && p99 < target {
			return tick.Now.Sub(startTime)
		}
	}
	return 0
}

func metricsFileName(t test.Test) string {
	return path.Join(t.PerfArtifactsDir(), "stats.json")
}

func fetchAndParseMetrics(
	ctx context.Context, t test.Test, c cluster.Cluster,
) (map[string][]exporter.SnapshotTick, error) {
	localMetricsFile := path.Join(t.ArtifactsDir(), "stats.json")

	if err := c.Get(ctx, t.L(), metricsFileName(t), localMetricsFile, c.WorkloadNode()); err != nil {
		return nil, err
	}
	return parseMetrics(localMetricsFile)
}

func parseMetrics(metricsFile string) (map[string][]exporter.SnapshotTick, error) {
	byType, err := histogram.DecodeSnapshots(metricsFile)
	if err != nil {
		return nil, err
	}
	if _, ok := byType["changefeed-resolved"]; !ok {
		return nil, errors.AssertionFailedf("expected changefeed-resolved series: %v", byType)
	}
	if _, ok := byType["write"]; !ok {
		return nil, errors.AssertionFailedf("expected write series")
	}
	return byType, nil
}

func registerKVRangefeed(r registry.Registry) {
	testConfigs := []kvRangefeedTest{
		{
			writeMaxRate:              500,
			duration:                  30 * time.Minute,
			sinkProvisioning:          1.1,
			splits:                    100,
			expectChangefeedCatchesUp: false,
			changefeedDelay:           1 * time.Minute,
			catchUpInterval:           5 * time.Minute,
		},
	}

	for _, opts := range testConfigs {
		testName := fmt.Sprintf("kv-rangefeed/write-rate=%d/sink-rate=%d/catchup=%s/splits=%d",
			opts.writeMaxRate,
			int64(float64(opts.writeMaxRate)*opts.sinkProvisioning),
			opts.catchUpInterval,
			opts.splits,
		)
		r.Add(registry.TestSpec{
			Name:      testName,
			Owner:     registry.OwnerKV,
			Benchmark: true,
			Cluster:   r.MakeClusterSpec(4, spec.CPU(8), spec.WorkloadNode(), spec.WorkloadNodeCPU(4)),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runKVRangefeed(ctx, t, c, opts)
			},
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
		})
	}
}
