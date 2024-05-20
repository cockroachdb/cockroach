package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	wl "github.com/cockroachdb/cockroach/pkg/workload"
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
	logger             *logger.Logger
	receiver           *histogram.UdpReceiver
	lm                 *latencyMonitor
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
	// Choose a random non-root port.
	ov.udpPort = prng.Intn(10000) + 10000
	ov.roachtestAddr = getListenAddr()
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
			ov.finishSetup(c)
			t.Status("T0: starting test with setup: ", ov)
			m := c.NewMonitor(ctx, c.Range(1, ov.crdbNodes))

			// Get the list of KV operations and start monitoring them directly
			// from the roachtest node.
			kv, _ := wl.Get("kv")
			r := histogram.CreateUdpReceiver(fmt.Sprintf(":%d", ov.udpPort), kv.Operations)
			lm := NewLatencyMonitor()

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
			// Capture the average rate near the end of the fill process.
			// TODO: Consider doing this a few seconds before stopping the fill.
			stableRate := lm.avgOps() * 3 / 10

			// Start the consistent workload and begin collecting profiles.
			t.L().Printf("T2: running workload with rate %d", stableRate)
			cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
				if err := ov.runWorkload(ctx, fmt.Sprintf("--max-rate=%d", stableRate)); !errors.Is(err, context.Canceled) {
					return err
				}
				return nil
			})

			if err := profileTopStatements(ctx, c, t.L()); err != nil {
				t.Fatal(err)
			}

			// Let enough data be written to all nodes in the cluster, then induce the failures.
			waitDuration(ctx, ov.validationDuration)
			lm.captureStats(lm.baselineStats)

			t.L().Printf("T3: inducing outage for %s", ov.outageDuration)

			m.ExpectDeath()
			ov.stopTarget(ctx, t.L(), c.Node(ov.targetNode))
			waitDuration(ctx, ov.outageDuration)
			lm.captureStats(lm.outageStats)
			ov.startNoBackup(ctx, t.L(), c.Node(ov.targetNode))
			m.ResetDeaths()

			t.L().Printf("T4: ending the outage")
			waitDuration(ctx, ov.validationDuration)
			lm.captureStats(lm.afterOutageStats)

			t.L().Printf("Baseline stats ", lm.baselineStats)
			t.L().Printf("Stats during outage: %s", lm.outageStats)
			t.L().Printf("Stats after outage: %s", lm.afterOutageStats)

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

// TODO: make this longer for non-local
const numOpsToTrack = 10

// relevantStats tracks the relevant stats for the last operation.
type relevantStats struct {
	operationsPerSecond int
	meanLatency         time.Duration
	maxLatency          time.Duration
	P99Latency          time.Duration
}

func (relevantStats relevantStats) String() string {
	return fmt.Sprintf("%d ops/s, mean: %s, max: %s, p99: %s",
		relevantStats.operationsPerSecond,
		relevantStats.meanLatency,
		relevantStats.maxLatency,
		relevantStats.P99Latency)
}

// latencyMonitor tracks the latency of operations over a period of time. It has
// a fixed number of operations to track and rolls the previous operations into
// the buffer.
type latencyMonitor struct {
	statNames        []string
	baselineStats    map[string]relevantStats
	outageStats      map[string]relevantStats
	afterOutageStats map[string]relevantStats
	// track the relevant stats for the last N operations in a circular buffer.
	mu struct {
		syncutil.Mutex
		index   int
		tracker map[string]*[numOpsToTrack]relevantStats
	}
}

func NewLatencyMonitor() *latencyMonitor {
	lm := latencyMonitor{}
	// TODO(baptist): If we want this for other workloads, pass in the stats here.
	lm.statNames = []string{`read`, `write`, `follower-read`}
	lm.baselineStats = make(map[string]relevantStats)
	lm.outageStats = make(map[string]relevantStats)
	lm.afterOutageStats = make(map[string]relevantStats)
	lm.mu.tracker = make(map[string]*[numOpsToTrack]relevantStats)
	for _, name := range lm.statNames {
		lm.mu.tracker[name] = &[numOpsToTrack]relevantStats{}
	}
	return &lm
}

// captureStats captures the average over the relevant stats for the last N
// operations.
func (lm *latencyMonitor) captureStats(stats map[string]relevantStats) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for _, name := range lm.statNames {
		s := relevantStats{}
		for _, stat := range lm.mu.tracker[name] {
			s.operationsPerSecond += stat.operationsPerSecond
			s.meanLatency += stat.meanLatency
			s.maxLatency += stat.maxLatency
			s.P99Latency += stat.P99Latency
		}
		s.operationsPerSecond /= numOpsToTrack
		s.meanLatency /= numOpsToTrack
		s.maxLatency /= numOpsToTrack
		s.P99Latency /= numOpsToTrack
		stats[name] = s
	}
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
			sMap := make(map[string]relevantStats)

			// Pull out the relevant data from the stats.
			for _, name := range lm.statNames {
				if histograms[name] == nil {
					l.Printf("no histogram for %s", name)
					continue outer
				}
				sMap[name] = extractStats(histograms[name])
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

			var outputStr strings.Builder
			for _, name := range lm.statNames {
				outputStr.WriteString(fmt.Sprintf("%s: %s, ", name, sMap[name].P99Latency))
			}
			outputStr.WriteString(fmt.Sprintf("op/s: %d", lm.avgOps()))
			l.Printf(outputStr.String())
		}
	}
}

// avgOps averages the number of operations per second over the last N
// operations across all the operations we are tracking.
func (lm *latencyMonitor) avgOps() int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	var total int
	for _, stat := range lm.mu.tracker {
		for _, ops := range stat {
			total += ops.operationsPerSecond
		}
	}
	return total / numOpsToTrack
}

// extractStats extracts the relevant stats from a histogram.
func extractStats(h *hdrhistogram.Histogram) relevantStats {
	return relevantStats{
		operationsPerSecond: int(h.TotalCount()),
		meanLatency:         time.Duration(h.Mean()),
		maxLatency:          time.Duration(h.Max()),
		P99Latency:          time.Duration(h.ValueAtQuantile(99)),
	}
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

// TODO(baptist): Move this lookup to a common package.
type IP struct {
	Query string
}

// getListenAddr returns the public IP address of the machine running the test.
func getListenAddr() string {
	req, err := http.Get("http://ip-api.com/json/")
	if err != nil {
		panic(err.Error())
	}
	defer req.Body.Close()

	var ip IP
	if err := json.NewDecoder(req.Body).Decode(&ip); err != nil {
		panic(err.Error())
	}

	return ip.Query
}

// profileTopStatements enables profile collection on the top statements from
// the cluster that exceed 10ms latency.
// TODO(baptist): Move these 2 functions to Cluster.
func profileTopStatements(
	ctx context.Context, cluster cluster.Cluster, logger *logger.Logger,
) error {
	db := cluster.Conn(ctx, logger, 1)
	defer db.Close()

	// Enable continuous statement diagnostics rather than just the first one.
	sql := "SET CLUSTER SETTING sql.stmt_diagnostics.collect_continuously.enabled=true"
	if _, err := db.Exec(sql); err != nil {
		return err
	}

	sql = `
SELECT
    crdb_internal.request_statement_bundle(statement, .001, '10ms'::INTERVAL, '12h'::INTERVAL )
FROM (
	SELECT DISTINCT statement FROM (
		SELECT metadata->>'query' AS statement, 
			CAST(statistics->'execution_statistics'->>'cnt' AS int) AS cnt 
			FROM crdb_internal.statement_statistics
		) 
	WHERE cnt > 1000
)`
	if _, err := db.Exec(sql); err != nil {
		return err
	}
	return nil
}

// downloadProfiles downloads all profiles from the cluster and saves them to the given output directory.
func downloadProfiles(
	ctx context.Context, cluster cluster.Cluster, logger *logger.Logger, outputDir string,
) error {
	stmtDir := filepath.Join(outputDir, "stmtbundle")
	if err := os.MkdirAll(stmtDir, os.ModePerm); err != nil {
		return err
	}
	query := "SELECT id, collected_at FROM system.statement_diagnostics"
	db := cluster.Conn(ctx, logger, 1)
	defer db.Close()
	idRow, err := db.Query(query)
	if err != nil {
		return err
	}
	adminUIAddrs, err := cluster.ExternalAdminUIAddr(ctx, logger, cluster.Node(1))
	if err != nil {
		return err
	}

	client := roachtestutil.DefaultHTTPClient(cluster, logger)
	urlPrefix := `https://` + adminUIAddrs[0] + `/_admin/v1/stmtbundle/`

	var diagID string
	var collectedAt time.Time
	for idRow.Next() {
		if err := idRow.Scan(&diagID, &collectedAt); err != nil {
			return err
		}
		url := urlPrefix + diagID
		filename := fmt.Sprintf("%s-%s.zip", collectedAt.Format("2006-01-02T15_04_05Z07:00"), diagID)
		logger.Printf("Downloading profile %s", filename)
		if err := client.Download(ctx, url, filepath.Join(outputDir, filename)); err != nil {
			return err
		}
	}
	return nil
}
