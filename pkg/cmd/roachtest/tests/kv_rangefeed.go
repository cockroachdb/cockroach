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
	writeMaxRate int64

	// duration is how long the test will run. The duration must be long enough
	// for the catch-up scan to complete if expectChangefeedCatchesUp is true.
	duration time.Duration

	// zipfian instructs the workload to use a zipfian distribution rather than a
	// uniform distribution.
	zipfian bool

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

	// latencyAssertions controls whether we should assert that the foreground
	// latency was not impacted during the test.
	latencyAssertions bool

	// cpus is the number of vCPUs per CRDB node. Defaults to 8 if not set.
	cpus int
}

const (
	// resolvedTarget is the frequency of the resolved messages we'll request from
	// the changefeed and the checkpoint interval we'll set. In practice, we can't
	// expect to observe resolved timestamps much lower than 2x the resolvedTarget.
	resolvedTarget = 5 * time.Second

	// catchUpToleranceMultiplier controls how much we can miss the best-case
	// catch-up duration by. We set a 10% tolerance to allow for both momentary
	// cluster hiccups that slow us down and re-transmissions that occur because
	// of range events.
	catchUpToleranceMultiplier = 1.1

	// latencyImpactToleranceMultiplier controls how much an impact we allow the
	// changefeed to have on the p75 of the p99 of sql latencies measured over
	// every second. We should do better here. See the code in runKVRangefeed for
	// better ideas.
	latencyImpactToleranceMultiplier = 1.05
)

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
	catchUpRate := t.catchUpRate()
	if catchUpRate <= 0 {
		return 0, errors.AssertionFailedf("invalid test configuration: catch-up rate (%d) is not positive, catch up will not complete", catchUpRate)
	}
	catchUpDuration := time.Duration((t.catchUpRows() / catchUpRate)) * time.Second
	return catchUpDuration, nil
}

// catchUpRate returns the effective catch-up rate assuming the workload is
// running at its steady state rate.
func (t kvRangefeedTest) catchUpRate() int64 {
	return t.changefeedMaxRate() - t.writeMaxRate
}

// catchUpRows returns the number of rows we expect to be written during the
// given "catch up interval." Note some of these rows will be added via an
// insert before the test starts while others will be written during the
// configured delay.
func (t kvRangefeedTest) catchUpRows() int64 {
	return int64(t.catchUpInterval.Seconds() * float64(t.writeMaxRate))
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
	settings.ClusterSettings["kv.rangefeed.buffered_sender.enabled"] = "true"
	// TODO(#161392): Disable per-event elastic CPU control for changefeeds as
	// an experiment to see if the test is stable in the absence of changefeed
	// throttling. The AC pacer's runtime.Yield adds enough delay on the node
	// running the changefeed to reduce throughput below the sink rate assumed
	// by this test.
	settings.ClusterSettings["changefeed.cpu.per_event_elastic_control.enabled"] = "false"
	return startOpts, settings
}

func runKVRangefeed(ctx context.Context, t test.Test, c cluster.Cluster, opts kvRangefeedTest) {
	insertCount, err := opts.insertCount()
	if err != nil {
		t.Fatal(err)
	}

	// Check this early to avoid test misconfigurations.
	var catchUpDur time.Duration
	if opts.expectChangefeedCatchesUp {

		var err error
		catchUpDur, err = opts.expectedCatchupDuration()
		if err != nil {
			t.Fatal(err)
		}

		catchUpTolerance := time.Duration(float64(catchUpDur) * (catchUpToleranceMultiplier - 1))
		catchUpTotalDuration := catchUpDur + opts.changefeedDelay + catchUpTolerance
		if opts.duration > 0 && opts.duration < catchUpTotalDuration {
			t.Fatalf(
				"duration (%s) is insufficient for catch up to complete (%s = %s (catchup) + %s (delay) + %s (tolerance)) at effective catchup rate of %d rows/s for %d rows",
				opts.duration, catchUpTotalDuration, catchUpDur, opts.changefeedDelay, catchUpTolerance,
				opts.catchUpRate(), opts.catchUpRows())
		}
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
	init1Opts := []string{
		fmt.Sprintf("--splits=%d", opts.splits),
	}
	init1Cmd := fmt.Sprintf("./cockroach workload init kv %s {pgurl:1-%d}",
		strings.Join(init1Opts, " "), nodes)
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), init1Cmd)

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
	init2Opts := []string{
		fmt.Sprintf("--insert-count=%d", insertCount),
		"--data-loader=insert",
		"--min-block-bytes=1024",
		"--max-block-bytes=1024",
	}
	if opts.zipfian {
		init2Opts = append(init2Opts, "--zipfian")
	}

	init2Cmd := fmt.Sprintf("./cockroach workload init kv %s {pgurl:1-%d}",
		strings.Join(init2Opts, " "), nodes)
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), init2Cmd)

	t.L().Printf("initializing workload: %s", init2Cmd)
	if opts.expectChangefeedCatchesUp {
		t.L().Printf("catch-up expected to take %s", catchUpDur)
	}

	t.Status("running workload with changefeed")
	m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		runOpts := []string{
			"--tolerate-errors",
			roachtestutil.IfLocal(c, "", " --concurrency="+fmt.Sprint(len(c.CRDBNodes())*64)),
			fmt.Sprintf(" --histograms=%s/%s", t.PerfArtifactsDir(), "stats.json"),
			" --changefeed",
			fmt.Sprintf("--changefeed-resolved-target=%s", resolvedTarget),
			fmt.Sprintf("--changefeed-cursor=%s", cursorStr),
			fmt.Sprintf("--changefeed-start-delay=%s", opts.changefeedDelay),
			fmt.Sprintf("--changefeed-max-rate=%d", opts.changefeedMaxRate()),

			fmt.Sprintf("--duration=%s", opts.duration),
			fmt.Sprintf("--max-rate=%d", opts.writeMaxRate),
		}
		if opts.zipfian {
			runOpts = append(runOpts, "--zipfian")
		}

		// TODO(ssd): Add read load to increase CPU utilization on the cluster.
		cmd := fmt.Sprintf("./cockroach workload run kv --read-percent 0 %s {pgurl:1-%d}",
			strings.Join(runOpts, " "),
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

	// Assert that the changefeed caught up in a reasonable time. We look for the
	// point where we fall below 2*resolvedTarget rather than just resolvedTarget
	// because we will never actually observe the resolved timestamp below
	// resolvedTarget since it also depends on the checkpoint interval.
	caughtUpAt, caughtUp := findP99Below(metrics["changefeed-resolved"], resolvedTarget*2)
	if opts.expectChangefeedCatchesUp {
		if !caughtUp {
			t.Fatalf("changefeed never caught up")
		}
		actualCatchUpDuration := caughtUpAt - opts.changefeedDelay
		// We allow the changefeed to miss the expected runtime by 10% to hopefully
		// cut down on infra-flakes.
		allowedCatchUpDuration := time.Duration(int64(float64(catchUpDur) * catchUpToleranceMultiplier))
		if actualCatchUpDuration > allowedCatchUpDuration {
			t.Fatalf("changefeed caught up too slowly: %s > %s (%s+10%%)", actualCatchUpDuration, allowedCatchUpDuration, catchUpDur)
		}
		t.L().Printf("changefeed caught up quickly enough %s < %s", actualCatchUpDuration, allowedCatchUpDuration)
	} else {
		if caughtUp {
			t.Fatal("changefeed unexpectedly caught up in under-provisioned state, are we testing what we think we are?")
		}
	}

	// Assert that we didn't impact the worst case latencies too badly. Note that
	// we want to avoid this being too sensitive or we'll end up debugging every
	// latency jump in the underlying workload as part of this test.
	//
	// TODO(ssd): Right now we just look at the p75 of the p99s to see if we've
	// moved the tail latencies non-trivially. This isn't very principled. We
	// could do some sort of hypothesis test here on the data from before and
	// after the changefeed starts to see if they came from two different
	// distributions. I've skipped this for now since we are still making sure
	// this test is exercising the parts of the code we want to test.
	//
	// NB: If this proves flakey, feel free to change it. We haven't yet put much
	// thought into what the right statistic here would be.
	if opts.changefeedDelay > 0 && opts.latencyAssertions {
		preChangefeedp99ticks := p99sBetween(metrics["write"], 0, opts.changefeedDelay)
		postChangefeedp99ticks := p99sBetween(metrics["write"], opts.changefeedDelay, time.Duration(math.MaxInt64))

		slices.Sort(preChangefeedp99ticks)
		slices.Sort(postChangefeedp99ticks)

		baseline, err := percentileSortedInput(preChangefeedp99ticks, 0.75)
		if err != nil {
			t.Fatal(err)
		}
		allowed := time.Duration(float64(baseline) * latencyImpactToleranceMultiplier)
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

func findP99Below(ticks []exporter.SnapshotTick, target time.Duration) (time.Duration, bool) {
	if len(ticks) == 0 {
		return 0, false
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

		// The histogram has a minimum latency for recorded values that is > 0. 0s
		// imply no data was recorded in that tick. We use this in the case of
		// changefeed-resolved to indicate no resolved timestamp has been received.
		p99 := time.Duration(h.ValueAtQuantile(99))
		if p99 > 0 && p99 < target {
			return tick.Now.Sub(startTime), true
		}
	}
	return 0, false
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
	splits := 1000
	// NOTE(ssd): writeMaxRate > 35000 may require test setup changes for the
	// single-threaded core changefeed to be able to keep up with.
	testConfigs := []kvRangefeedTest{
		// Adequately provisioned sink
		{
			writeMaxRate:              10000,
			duration:                  30 * time.Minute,
			sinkProvisioning:          1.2,
			splits:                    splits,
			expectChangefeedCatchesUp: true,
			changefeedDelay:           1 * time.Minute,
			catchUpInterval:           5 * time.Minute,
			// TODO(ssd): Re-enable once we can make this more stable.
			latencyAssertions: false,
			// Use more CPUs to provide headroom for rangefeed event processing.
			// See #157216.
			cpus: 16,
		},
		// Underprovisioned sink
		{
			writeMaxRate:              10000,
			duration:                  30 * time.Minute,
			sinkProvisioning:          0.8,
			splits:                    splits,
			expectChangefeedCatchesUp: false,
			changefeedDelay:           1 * time.Minute,
			catchUpInterval:           5 * time.Minute,
			// TODO(ssd): Re-enable once we can make this more stable.
			latencyAssertions: false,
		},
		// Zipfian tests.
		//
		// These tests are currently disabled because neither buffered or unbuffered
		// sender is able to keep up at the given sink provisioning. Maybe this
		// level of sink provisioning is unreasonable for such a workload. But, this
		// could be a useful setup to expose various extreme behaviour.
		// {
		// 	writeMaxRate:              10000,
		// 	zipfian:                   true,
		// 	duration:                  30 * time.Minute,
		// 	sinkProvisioning:          1.2,
		// 	splits:                    splits,
		// 	expectChangefeedCatchesUp: true,
		// 	changefeedDelay:           1 * time.Minute,
		// 	catchUpInterval:           5 * time.Minute,
		// },
		// {
		// 	writeMaxRate:              10000,
		// 	zipfian:                   true,
		// 	duration:                  30 * time.Minute,
		// 	sinkProvisioning:          0.8,
		// 	splits:                    splits,
		// 	expectChangefeedCatchesUp: false,
		// 	changefeedDelay:           1 * time.Minute,
		// 	catchUpInterval:           5 * time.Minute,
		// },
	}

	for _, opts := range testConfigs {
		dist := ""
		if opts.zipfian {
			dist = "/dist=zipfian"
		}
		testName := fmt.Sprintf("kv-rangefeed/write-rate=%d/sink-rate=%d/catchup=%s/splits=%d%s",
			opts.writeMaxRate,
			int64(float64(opts.writeMaxRate)*opts.sinkProvisioning),
			opts.catchUpInterval,
			opts.splits,
			dist,
		)
		cpus := opts.cpus
		if cpus == 0 {
			cpus = 8 // default
		}
		r.Add(registry.TestSpec{
			Name:      testName,
			Owner:     registry.OwnerKV,
			Benchmark: true,
			Cluster: r.MakeClusterSpec(
				4,
				spec.CPU(cpus),
				spec.WorkloadNode(),
				spec.WorkloadNodeCPU(4),
				spec.RandomizeVolumeType(),
				spec.RandomlyUseXfs(),
			),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runKVRangefeed(ctx, t, c, opts)
			},
			// This test is compatible with all clouds, but suspected disk
			// throughput issues (e.g. gp3 on AWS) and lower observability
			// (no Grafana on AWS) prompted us to restrict to GCE for the
			// time being. See #163197.
			CompatibleClouds: registry.OnlyGCE,
			Suites:           registry.Suites(registry.Nightly),
		})
	}
}
