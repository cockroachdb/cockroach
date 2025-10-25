// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package tests

import (
	"context"
	"fmt"
	"path"
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

	// duration is how long the test will run.
	duration time.Duration

	// catchUpInternal is how much we will delay the changefeed start by while the
	// workload is running.
	catchUpInterval time.Duration
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

func (t kvRangefeedTest) expectedCatchupDuration() (time.Duration, error) {
	writesToCatchUp := t.writeMaxRate * int64(t.catchUpInterval.Seconds())
	catchUpRate := t.changefeedMaxRate() - t.writeMaxRate
	if catchUpRate < 0 {
		return 0, errors.AssertionFailedf("catch-up rate (%d) is negative, catch up will not complete", catchUpRate)
	}
	catchUpTime := time.Duration((writesToCatchUp / catchUpRate)) * time.Second
	return catchUpTime, nil
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
		if opts.duration > 0 && opts.duration < catchUpDur+opts.catchUpInterval {
			t.Fatalf("duration (%s) is insufficient for catch up to complete (%s)", opts.duration, catchUpDur+opts.catchUpInterval)
		}
	}

	nodes := c.Spec().NodeCount - 1
	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	var rfEnabled bool
	if err := db.QueryRow("SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&rfEnabled); err != nil {
		t.Fatal(err)
	}
	if !rfEnabled {
		if _, err := db.Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
			t.Fatal(err)
		}
	}

	// Set per-changefeed memory to a low value so that we don't queue in the
	// changefeed machinery and instead force the buffered sender to queue.
	if _, err := db.Exec("SET CLUSTER SETTING changefeed.memory.per_changefeed_limit='1MiB'"); err != nil {
		t.Fatal(err)
	}

	t.Status("initializing workload")
	initCmd := fmt.Sprintf("./cockroach workload init kv --splits=%d {pgurl:1-%d}",
		opts.splits, nodes)
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), initCmd)

	t.Status("running workload with changefeed")
	t.L().Printf("catch-up starting in %s", opts.catchUpInterval)
	if opts.expectChangefeedCatchesUp {
		t.L().Printf("catch-up expected to take %s (%s after start)", catchUpDur, catchUpDur+opts.catchUpInterval)
	}

	const resolvedTarget = 5 * time.Second

	m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		opts := []string{
			"--tolerate-errors",
			roachtestutil.GetWorkloadHistogramArgs(t, c, nil),
			" --changefeed",
			fmt.Sprintf("--changefeed-resolved-target=%s", resolvedTarget),
			fmt.Sprintf("--changefeed-start-delay=%s", opts.catchUpInterval),
			fmt.Sprintf("--duration=%s", opts.duration),

			fmt.Sprintf("--changefeed-max-rate=%d", opts.changefeedMaxRate()),
			fmt.Sprintf("--max-rate=%d", opts.writeMaxRate),
		}

		cmd := fmt.Sprintf("./cockroach workload run kv %s {pgurl:1-%d}",
			strings.Join(opts, " "),
			nodes,
		)

		t.L().Printf("Running workload: %s", cmd)
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
		return nil
	})
	m.Wait()

	metrics, err := fetchAndParseMetrics(ctx, t, c)
	if err != nil {
		t.Fatal(err)
	}
	if opts.expectChangefeedCatchesUp {
		catchUpDur, err := opts.expectedCatchupDuration()
		if err != nil {
			t.Fatal(err)
		}

		allowedCatchUpDuration := time.Duration(int64(float64(catchUpDur) * float64(1.1)))
		actualCatchUpDuration := findP99Below(metrics["changefeed-resolved"], resolvedTarget*2) - opts.catchUpInterval
		if actualCatchUpDuration == 0 {
			t.Fatal("changefeed never caught up")
		} else if actualCatchUpDuration > allowedCatchUpDuration {
			t.Fatalf("changefeed caught up too slowly: %s > %s (%s+10%%)", actualCatchUpDuration, allowedCatchUpDuration, catchUpDur)
		} else {
			t.L().Printf("changefeed caught up quickly enough %s < %s", actualCatchUpDuration, allowedCatchUpDuration)
		}
	}

}

func findP99Below(ticks []exporter.SnapshotTick, target time.Duration) time.Duration {
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
			duration:                  10 * time.Minute,
			catchUpInterval:           1 * time.Minute,
			sinkProvisioning:          1.2, // Correctly provisioned.
			splits:                    1000,
			expectChangefeedCatchesUp: true,
		},
		// {
		// 	writeMaxRate:              1000,
		// 	catchUpInterval:           5 * time.Minute,
		// 	sinkProvisioning:          0.9, // Under-provisioned.
		// 	splits:                    1000,
		// 	expectChangefeedCatchesUp: false,
		// },
	}

	for _, opts := range testConfigs {
		testName := fmt.Sprintf("kv-rangefeed/write-rate=%d/sink-rate=%d/catchup=%s/splits=%d",
			opts.writeMaxRate,
			opts.changefeedMaxRate(),
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
