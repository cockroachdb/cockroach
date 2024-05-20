// Copyright 2024 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
)

// outageVariations specifies the various outage variations to test.
// The durations all need to be coordinate for it to work well.
// TODO: Add a diagram here.
//
// T0: Test starts and is initialized.
// T1(fill duration): Test runs at 100% until filled
// T2(stable run): Workload runs at 30% usage until the end - 2x validation duration + outage duration.
// T3(T2 + validation duration): Start the outage.
// T4(T3 + outage duration): End the outage.
// T5(T3 + validation duration): Wait this long and then stop the test.
type outageVariations struct {
	seed               int64
	cluster            cluster.Cluster
	fillDuration       time.Duration
	maxBlockBytes      int
	outageDuration     time.Duration
	validationDuration time.Duration
	splits             int
	stableNodes        int
	targetNode         int
	workloadNode       int
	crdbNodes          int
	udpPort            int
	roachtestAddr      string
	leaseType          registry.LeaseType
}

// failureMode specifies a failure mode.
type outageMode string

const (
	outageModeRestart      outageMode = "restart"
	outageModePartition    outageMode = "partition"
	outageModeDecommission outageMode = "decommission"
	outageModeDisk         outageMode = "disk"
	outageModeIndex        outageMode = "index"
)

var allOutages = []outageMode{
	outageModeRestart,
}

var durationOptions = []time.Duration{
	10 * time.Second,
	1 * time.Minute,
}

var splitOptions = []int{
	1,
	10,
	100,
}

var maxBlockBytes = []int{
	1,
	1000,
	10000,
}

var leases = []registry.LeaseType{
	registry.EpochLeases,
	registry.ExpirationLeases,
}

func (ov outageVariations) String() string {
	return fmt.Sprintf("seed: %d, fill: %s, validation: %s, outage: %s, splits: %d, stable: %d, target: %d, workload: %d, crdb: %d,  lease: %s, udp: %s:%d",
		ov.seed,
		ov.fillDuration,
		ov.validationDuration,
		ov.outageDuration,
		ov.splits,
		ov.stableNodes,
		ov.targetNode,
		ov.workloadNode,
		ov.crdbNodes,
		ov.leaseType,
		ov.roachtestAddr,
		ov.udpPort,
	)
}

func setup() outageVariations {
	prng, seed := randutil.NewPseudoRand()

	ov := outageVariations{}
	ov.seed = seed
	ov.splits = splitOptions[prng.Intn(len(splitOptions))]
	ov.maxBlockBytes = maxBlockBytes[prng.Intn(len(maxBlockBytes))]

	// TODO: Make this longer.
	ov.fillDuration = 2 * time.Minute
	ov.validationDuration = 5 * time.Minute
	ov.outageDuration = durationOptions[prng.Intn(len(durationOptions))]
	listnerAddr, err := getListenAddr(context.Background())
	if err != nil {
		panic(err)
	}
	ov.roachtestAddr = listnerAddr
	ov.leaseType = leases[prng.Intn(len(leases))]
	return ov
}
func (ov *outageVariations) finishSetup(c cluster.Cluster) {
	ov.cluster = c
	ov.crdbNodes = c.Spec().NodeCount - 1
	ov.stableNodes = ov.crdbNodes - 1
	ov.targetNode = ov.crdbNodes
	ov.workloadNode = c.Spec().NodeCount
	if c.IsLocal() {
		ov.fillDuration = 20 * time.Second
		ov.validationDuration = 10 * time.Second
		ov.outageDuration = 10 * time.Second
		ov.roachtestAddr = "localhost"
	}
}

func registerImpact(r registry.Registry) {
	ov := setup()
	r.Add(registry.TestSpec{
		Name:             "admission-control/restart-impact",
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Owner:            registry.OwnerAdmissionControl,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(8)),
		Leases:           ov.leaseType,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			r := histogram.CreateUdpReceiver()
			ov.finishSetup(c)
			ov.udpPort = r.Port()
			t.Status("T0: starting test with setup: ", ov)
			m := c.NewMonitor(ctx, c.Range(1, ov.crdbNodes))

			// Track the three operations that we are sending in this test.
			lm := NewLatencyMonitor([]string{`read`, `write`, `follower-read`})

			// Start all the nodes and initialize the KV workload.
			ov.startNoBackup(ctx, t.L(), c.Range(1, ov.crdbNodes))
			ov.initKV(ctx)

			// Start collecting latency measurements after the workload has been initialized.
			m.Go(r.Listen)
			cancelReporter := m.GoWithCancel(func(ctx context.Context) error {
				return lm.start(ctx, t.L(), r)
			})

			// Start filling the system without a rate.
			t.L().Printf("T1: filling for %s", ov.fillDuration)
			if err := ov.runWorkload(ctx, fmt.Sprintf("--duration=%s", ov.fillDuration)); err != nil {
				t.Fatal(err)
			}
			// Capture the 30% of max rate near the last 1/4 of the fill process.
			sampleWindow := ov.fillDuration / 4
			ratioOfWrites := 0.5
			fillStats := lm.mergeStats(sampleWindow)
			stableRate := int(float64(fillStats[`write`].TotalCount()) / sampleWindow.Seconds() * 0.3 / ratioOfWrites)

			// Start the consistent workload and begin collecting profiles.
			t.L().Printf("T2: running workload with stable rate %d", stableRate)
			cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
				if err := ov.runWorkload(ctx, fmt.Sprintf("--max-rate=%d", stableRate)); !errors.Is(err, context.Canceled) {
					return err
				}
				return nil
			})

			go func() {
				waitDuration(ctx, ov.validationDuration/2)
				initialStats := lm.mergeStats(ov.validationDuration / 2)
				// Wait for the workload to stabilize.
				// Note that this profile threshold may be considerably higher than during a lower workload rate.
				profileThreshold := time.Duration(initialStats[`write`].ValueAtQuantile(99.9))
				t.L().Printf("T2.5: profiling statements that take longer than %s", profileThreshold)
				if err := profileTopStatements(ctx, c, t.L(), profileThreshold); err != nil {
					t.Fatal(err)
				}
			}()

			// Let enough data be written to all nodes in the cluster, then induce the failures.
			waitDuration(ctx, ov.validationDuration)
			baselineStats := lm.worstStats(ov.validationDuration)

			t.L().Printf("T3: inducing outage for %s", ov.outageDuration)
			m.ExpectDeath()
			ov.stopTarget(ctx, t.L(), c.Node(ov.targetNode))
			t.L().Printf("T3: stopped")
			waitDuration(ctx, ov.outageDuration)
			outageStats := lm.worstStats(ov.outageDuration)
			ov.startNoBackup(ctx, t.L(), c.Node(ov.targetNode))
			m.ResetDeaths()

			t.L().Printf("T4: ending the outage")
			waitDuration(ctx, ov.validationDuration)
			afterOutageStats := lm.worstStats(ov.validationDuration)

			t.L().Printf("Fill stats %s", shortString(fillStats))
			t.L().Printf("Baseline stats %s", shortString(baselineStats))
			t.L().Printf("Stats during outage: %s", shortString(outageStats))
			t.L().Printf("Stats after outage: %s", shortString(afterOutageStats))

			t.L().Printf("T5: test complete, shutting down.")
			// Stop all the running threads and wait for them.
			r.Close()
			cancelReporter()
			cancelWorkload()
			m.Wait()

			if err := downloadProfiles(ctx, c, t.L(), t.ArtifactsDir()); err != nil {
				t.Fatal(err)
			}
		}})
}

// Track one hour in the past.
const numOpsToTrack = 3600

// latencyMonitor tracks the latency of operations over a period of time. It has
// a fixed number of operations to track and rolls the previous operations into
// the buffer.
type latencyMonitor struct {
	statNames []string
	// track the relevant stats for the last N operations in a circular buffer.
	mu struct {
		syncutil.Mutex
		index   int
		tracker map[string]*[numOpsToTrack]*hdrhistogram.Histogram
	}
}

func NewLatencyMonitor(statNames []string) *latencyMonitor {
	lm := latencyMonitor{}
	lm.statNames = statNames
	lm.mu.tracker = make(map[string]*[numOpsToTrack]*hdrhistogram.Histogram)
	for _, name := range lm.statNames {
		lm.mu.tracker[name] = &[numOpsToTrack]*hdrhistogram.Histogram{}
	}
	return &lm
}

// mergeStats captures the average over the relevant stats for the last N
// operations.
func (lm *latencyMonitor) mergeStats(window time.Duration) map[string]*hdrhistogram.Histogram {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// startIndex is always between 0 and numOpsToTrack - 1.
	startIndex := (lm.mu.index + numOpsToTrack - int(window.Seconds())) % numOpsToTrack
	// endIndex is always greater than startIndex.
	endIndex := (lm.mu.index + numOpsToTrack) % numOpsToTrack
	if endIndex < startIndex {
		endIndex += numOpsToTrack
	}

	stats := make(map[string]*hdrhistogram.Histogram)
	for name, stat := range lm.mu.tracker {
		s := stat[startIndex]
		for i := startIndex + 1; i < endIndex; i++ {
			s.Merge(stat[i%numOpsToTrack])
		}
		stats[name] = s
	}
	return stats
}

// worstStats captures the worst case over the relevant stats for the last N
func (lm *latencyMonitor) worstStats(window time.Duration) map[string]*hdrhistogram.Histogram {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// startIndex is always between 0 and numOpsToTrack - 1.
	startIndex := (lm.mu.index + numOpsToTrack - int(window.Seconds())) % numOpsToTrack
	// endIndex is always greater than startIndex.
	endIndex := (lm.mu.index + numOpsToTrack) % numOpsToTrack
	if endIndex < startIndex {
		endIndex += numOpsToTrack
	}

	stats := make(map[string]*hdrhistogram.Histogram)
	for name, stat := range lm.mu.tracker {
		s := stat[startIndex]
		for i := startIndex + 1; i < endIndex; i++ {
			if isWorse(stat[i%numOpsToTrack], s) {
				s = stat[i%numOpsToTrack]
			}
			s.Merge(stat[i])
		}
		stats[name] = s
	}
	return stats
}

// returns true if a is worse than b. Note that this is not symmetric, and will
// only return true if a is enough worse.
func isWorse(a, b *hdrhistogram.Histogram) bool {
	// Allow some delta on counts to handle variations on the total and the mean latency.
	delta := 0.95
	if float64(a.TotalCount()) < float64(b.TotalCount())*delta {
		return true
	}
	if float64(a.ValueAtQuantile(50)) < float64(b.ValueAtQuantile(50))*delta {
		return true
	}
	// If the total and the value are close, then the 99.9th percentile is the
	// deciding factor. Don't multiply by delta.
	if float64(a.ValueAtQuantile(99.9)) < float64(b.ValueAtQuantile(99.9)) {
		return true
	}
	return false
}

// start the latency monitor which will run until the context is cancelled.
func (lm *latencyMonitor) start(
	ctx context.Context, l *logger.Logger, r *histogram.UdpReceiver,
) error {
outer:
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
			histograms := r.Tick()
			sMap := make(map[string]*hdrhistogram.Histogram)

			// Pull out the relevant data from the stats.
			for _, name := range lm.statNames {
				if histograms[name] == nil {
					l.Printf("no histogram for %s", name)
					continue outer
				}
				sMap[name] = histograms[name]
			}

			// Roll the latest stats into the tracker under lock.
			func() {
				lm.mu.Lock()
				defer lm.mu.Unlock()
				for _, name := range lm.statNames {
					lm.mu.tracker[name][lm.mu.index] = sMap[name]
				}
				lm.mu.index = (lm.mu.index + 1) % numOpsToTrack
			}()

			l.Printf("%s", shortString(sMap))
		}
	}
}

func shortString(sMap map[string]*hdrhistogram.Histogram) string {
	var outputStr strings.Builder
	var count int64
	for name, hist := range sMap {
		outputStr.WriteString(fmt.Sprintf("%s: %s, ", name, time.Duration(hist.ValueAtQuantile(99))))
		count += hist.TotalCount()
	}
	outputStr.WriteString(fmt.Sprintf("op/s: %d", count))
	return outputStr.String()
}

func longString(sMap map[string]*hdrhistogram.Histogram) string {
	var outputStr strings.Builder
	var count int64
	for name, hist := range sMap {
		outputStr.WriteString(fmt.Sprintf("%s: %s, ", name, time.Duration(hist.ValueAtQuantile(99))))
		count += hist.TotalCount()
	}
	outputStr.WriteString(fmt.Sprintf("op/s: %d", count))
	return outputStr.String()
}

// startNoBackup starts the nodes without enabling backup.
func (ov *outageVariations) startNoBackup(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) {
	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	settings := install.MakeClusterSettings()
	ov.cluster.Start(ctx, l, startOpts, settings, nodes)
}

// stopTarget stops the target node with a graceful shutdown.
func (ov *outageVariations) stopTarget(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) {
	gracefulOpts := option.DefaultStopOpts()
	// SIGTERM for clean shutdown
	gracefulOpts.RoachprodOpts.Sig = 15
	gracefulOpts.RoachprodOpts.Wait = true
	ov.cluster.Stop(ctx, l, gracefulOpts, nodes)
}

func (ov *outageVariations) initKV(ctx context.Context) {
	initCmd := fmt.Sprintf("./cockroach workload init kv --splits %d {pgurl:1}", ov.splits)
	ov.cluster.Run(ctx, option.WithNodes(ov.cluster.Node(ov.workloadNode)), initCmd)
}

// Don't run a workload against the node we're going to shut down.
func (ov *outageVariations) runWorkload(ctx context.Context, customOptions string) error {
	runCmd := fmt.Sprintf(
		"./cockroach workload run kv %s --max-block-bytes=%d --read-percent=50 --follower-read-percent=50 --concurrency=200 --operation-receiver=%s:%d {pgurl:1-%d}",
		customOptions, ov.maxBlockBytes, ov.roachtestAddr, ov.udpPort, ov.stableNodes)
	return ov.cluster.RunE(ctx, option.WithNodes(ov.cluster.Node(ov.workloadNode)), runCmd)
}

// TODO(baptist): Is there a better place for this utility method?
func waitDuration(ctx context.Context, duration time.Duration) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(duration):
	}
}
