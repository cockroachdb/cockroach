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
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type clusterInfo struct {
	// name is the name of the tenant
	name string

	// ID is the id of the tenant
	ID int

	// pgurl is a connection string to the system tenant
	pgURL string

	// db provides a connection to the system tenant
	db *gosql.DB

	// sql provides a sql connection to the system tenant
	sysSQL *sqlutils.SQLRunner

	// nodes indicates the roachprod nodes running the cluster's nodes
	nodes option.NodeListOption
}

type c2cSetup struct {
	src *clusterInfo
	dst *clusterInfo

	// workloadNode identifies the node in the roachprod cluster that runs the workload.
	workloadNode option.NodeListOption

	// gatewayNodes  identify the nodes in the source cluster to connect the main workload to.
	gatewayNodes option.NodeListOption
	promCfg      *prometheus.Config
}

var c2cPromMetrics = map[string]clusterstats.ClusterStat{
	"LogicalMegabytes": {
		LabelName: "node",
		Query:     "replication_logical_bytes / 1e6"},
	"PhysicalMegabytes": {
		LabelName: "node",
		Query:     "replication_sst_bytes / 1e6"},
	"PhysicalReplicatedMegabytes": {
		LabelName: "node",
		Query:     "capacity_used / 1e6"},
}

func sumOverLabel(stats map[string]map[string]clusterstats.StatPoint, label string) float64 {
	var mean float64
	for _, stat := range stats[label] {
		mean += stat.Value
	}
	return mean
}

type metricSnapshot struct {
	metrics map[string]float64
	time    time.Time
}

func newMetricSnapshot(
	metricSnapper func(time.Time) map[string]float64, ts time.Time,
) metricSnapshot {
	return metricSnapshot{
		time:    ts,
		metrics: metricSnapper(ts),
	}
}

// calcThroughput estimates throughput between two time intervals as metric_unit/s/node
// for the provided metric, assuming the cluster had the same number of nodes
// over the interval.
func calcThroughput(
	startMetric float64, endMetric float64, interval time.Duration, nodeCount int,
) float64 {
	return (endMetric - startMetric) / (interval.Seconds() * float64(nodeCount))
}

type c2cMetrics struct {
	initalScanStart metricSnapshot

	initialScanEnd metricSnapshot

	// cutoverTo records stats at the system time to which the dst cluster cuts over to.
	cutoverTo metricSnapshot

	cutoverStart metricSnapshot

	cutoverEnd metricSnapshot

	fingerprintingStart time.Time

	fingerprintingEnd time.Time
}

// export summarizes all metrics gathered throughout the test.
func (m c2cMetrics) export(t test.Test, nodeCount int) {

	// aggregate aggregates metric snapshots across two time periods. A non-zero
	// durationOverride will be used instead of the duration between the two
	// passed in snapshots.
	aggregate := func(
		start metricSnapshot,
		end metricSnapshot,
		label string,
		durationOverride time.Duration) {
		if start.metrics == nil || end.metrics == nil {
			return
		}

		metrics := map[string]float64{}
		duration := durationOverride
		if duration == 0 {
			duration = end.time.Sub(start.time)
		}
		metrics["Duration Minutes"] = duration.Minutes()

		for metricName := range start.metrics {
			metrics["Size_"+metricName] = end.metrics[metricName] - start.metrics[metricName]
			metrics["Throughput_"+metricName+"_MB/S/Node"] = calcThroughput(
				start.metrics[metricName], end.metrics[metricName], duration, nodeCount)
		}

		// Print all the metrics for now while we wait for prom/grafana to visualize perf over time.
		// Sort the metrics for pretty printing.
		metricNames := make([]string, 0, len(metrics))
		for name := range metrics {
			metricNames = append(metricNames, name)
		}
		sort.Strings(metricNames)
		t.L().Printf("%s Perf:", label)
		for _, name := range metricNames {
			t.L().Printf("\t%s : %.2f", name, metrics[name])
		}
	}
	aggregate(m.initalScanStart, m.initialScanEnd, "InitialScan", 0)

	aggregate(m.initialScanEnd, m.cutoverStart, "Workload", 0)

	// The _amount_ of data processed during cutover should be the data ingested between the
	// timestamp we cut over to and the start of the cutover process.
	aggregate(m.cutoverTo, m.cutoverStart, "Cutover", m.cutoverEnd.time.Sub(m.cutoverStart.time))
}

type streamingWorkload interface {
	// sourceInitCmd returns a command that will populate the src cluster with data before the
	// replication stream begins
	sourceInitCmd(tenantName string, nodes option.NodeListOption) string

	// sourceRunCmd returns a command that will run a workload
	sourceRunCmd(tenantName string, nodes option.NodeListOption) string

	runDriver(workloadCtx context.Context, c cluster.Cluster, t test.Test, setup *c2cSetup) error
}

func defaultWorkloadDriver(
	workloadCtx context.Context, setup *c2cSetup, c cluster.Cluster, workload streamingWorkload,
) error {
	return c.RunE(workloadCtx, setup.workloadNode, workload.sourceRunCmd(setup.src.name, setup.gatewayNodes))
}

type replicateTPCC struct {
	warehouses int
}

func (tpcc replicateTPCC) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	return fmt.Sprintf(`./workload init tpcc --data-loader import --warehouses %d {pgurl%s:%s}`,
		tpcc.warehouses, nodes, tenantName)
}

func (tpcc replicateTPCC) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	// added --tolerate-errors flags to prevent test from flaking due to a transaction retry error
	return fmt.Sprintf(`./workload run tpcc --warehouses %d --tolerate-errors {pgurl%s:%s}`,
		tpcc.warehouses, nodes, tenantName)
}

func (tpcc replicateTPCC) runDriver(
	workloadCtx context.Context, c cluster.Cluster, t test.Test, setup *c2cSetup,
) error {
	return defaultWorkloadDriver(workloadCtx, setup, c, tpcc)
}

type replicateKV struct {
	readPercent int

	// This field is merely used to debug the c2c framework for finite workloads.
	debugRunDuration time.Duration

	// the number of rows inserted into the cluster before c2c begins
	initRows int

	// max size of raw data written during each insertion
	maxBlockBytes int
}

func (kv replicateKV) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	cmd := roachtestutil.NewCommand(`./workload init kv`).
		MaybeFlag(kv.initRows > 0, "insert-count", kv.initRows).
		MaybeFlag(kv.initRows > 0, "max-block-bytes", kv.maxBlockBytes).
		Flag("splits", 100).
		Option("scatter").
		Arg("{pgurl%s:%s}", nodes, tenantName)
	return cmd.String()
}

func (kv replicateKV) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	cmd := roachtestutil.NewCommand(`./workload run kv`).
		Option("tolerate-errors").
		Flag("max-block-bytes", kv.maxBlockBytes).
		Flag("read-percent", kv.readPercent).
		MaybeFlag(kv.debugRunDuration > 0, "duration", kv.debugRunDuration).
		Arg("{pgurl%s:%s}", nodes, tenantName)
	return cmd.String()
}

func (kv replicateKV) runDriver(
	workloadCtx context.Context, c cluster.Cluster, t test.Test, setup *c2cSetup,
) error {
	return defaultWorkloadDriver(workloadCtx, setup, c, kv)
}

type replicateBulkOps struct {
}

func (bo replicateBulkOps) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	return ""
}

func (bo replicateBulkOps) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	return ""
}

func (bo replicateBulkOps) runDriver(
	workloadCtx context.Context, c cluster.Cluster, t test.Test, setup *c2cSetup,
) error {
	runBackupMVCCRangeTombstones(workloadCtx, t, c, mvccRangeTombstoneConfig{
		skipBackupRestore: true,
		skipClusterSetup:  true,
		tenantName:        setup.src.name})
	return nil
}

// replicationSpec are inputs to a c2c roachtest set during roachtest
// registration, and can not be modified during roachtest execution.
type replicationSpec struct {
	// name specifies the name of the roachtest
	name string

	// srcodes is the number of nodes on the source cluster.
	srcNodes int

	// dstNodes is the number of nodes on the destination cluster.
	dstNodes int

	// cpus is the per node cpu count.
	cpus int

	// pdSize specifies the pd-ssd volume (in GB). If set to 0, local ssds are used.
	pdSize int

	// workload specifies the streaming workload.
	workload streamingWorkload

	// additionalDuration specifies how long the workload will run after the initial scan
	//completes. If the time out is set to 0, it will run until completion.
	additionalDuration time.Duration

	// cutover specifies how soon before the workload ends to choose a cutover timestamp.
	cutover time.Duration

	// timeout specifies when the roachtest should fail due to timeout.
	timeout time.Duration

	expectedNodeDeaths int32

	// If non-empty, the test will be skipped with the supplied reason.
	skip string
}

// replicationDriver manages c2c roachtest execution.
type replicationDriver struct {
	rs replicationSpec

	// beforeWorkloadHook is called before the main workload begins.
	beforeWorkloadHook func()

	// replicationStartHook is called as soon as the replication job begins.
	replicationStartHook func(ctx context.Context, sp *replicationDriver)

	setup   *c2cSetup
	t       test.Test
	c       cluster.Cluster
	metrics *c2cMetrics
	rng     *rand.Rand
}

func makeReplicationDriver(t test.Test, c cluster.Cluster, rs replicationSpec) replicationDriver {
	rng, seed := randutil.NewTestRand()
	t.L().Printf(`Random Seed is %d`, seed)
	return replicationDriver{
		t:   t,
		c:   c,
		rs:  rs,
		rng: rng,
	}
}

func (rd *replicationDriver) setupC2C(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	srcCluster := c.Range(1, rd.rs.srcNodes)
	dstCluster := c.Range(rd.rs.srcNodes+1, rd.rs.srcNodes+rd.rs.dstNodes)
	workloadNode := c.Node(rd.rs.srcNodes + rd.rs.dstNodes + 1)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)

	// TODO(msbutler): allow for backups once this test stabilizes a bit more.
	srcStartOps := option.DefaultStartOptsNoBackups()
	srcStartOps.RoachprodOpts.InitTarget = 1
	srcClusterSetting := install.MakeClusterSettings(install.SecureOption(true))
	c.Start(ctx, t.L(), srcStartOps, srcClusterSetting, srcCluster)

	// TODO(msbutler): allow for backups once this test stabilizes a bit more.
	dstStartOps := option.DefaultStartOptsNoBackups()
	dstStartOps.RoachprodOpts.InitTarget = rd.rs.srcNodes + 1
	dstClusterSetting := install.MakeClusterSettings(install.SecureOption(true))
	c.Start(ctx, t.L(), dstStartOps, dstClusterSetting, dstCluster)

	srcNode := srcCluster.SeededRandNode(rd.rng)
	destNode := dstCluster.SeededRandNode(rd.rng)

	addr, err := c.ExternalPGUrl(ctx, t.L(), srcNode, "")
	require.NoError(t, err)

	srcDB := c.Conn(ctx, t.L(), srcNode[0])
	srcSQL := sqlutils.MakeSQLRunner(srcDB)
	destDB := c.Conn(ctx, t.L(), destNode[0])
	destSQL := sqlutils.MakeSQLRunner(destDB)

	srcClusterSettings(t, srcSQL)
	destClusterSettings(t, destSQL)

	createTenantAdminRole(t, "src-system", srcSQL)
	createTenantAdminRole(t, "dst-system", destSQL)

	srcTenantID, destTenantID := 2, 2
	srcTenantName := "src-tenant"
	destTenantName := "destination-tenant"

	createInMemoryTenant(ctx, t, c, srcTenantName, srcCluster, true)

	pgURL, err := copyPGCertsAndMakeURL(ctx, t, c, srcNode, srcClusterSetting.PGUrlCertsDir, addr[0])
	require.NoError(t, err)

	srcTenantInfo := clusterInfo{
		name:   srcTenantName,
		ID:     srcTenantID,
		pgURL:  pgURL,
		sysSQL: srcSQL,
		db:     srcDB,
		nodes:  srcCluster}
	destTenantInfo := clusterInfo{
		name:   destTenantName,
		ID:     destTenantID,
		sysSQL: destSQL,
		db:     destDB,
		nodes:  dstCluster}

	rd.setup = &c2cSetup{
		src:          &srcTenantInfo,
		dst:          &destTenantInfo,
		workloadNode: workloadNode,
		gatewayNodes: srcTenantInfo.nodes}

	rd.t = t
	rd.c = c
	rd.metrics = &c2cMetrics{}
	rd.replicationStartHook = func(ctx context.Context, sp *replicationDriver) {}
	rd.beforeWorkloadHook = func() {}

	if !c.IsLocal() {
		// TODO(msbutler): pass a proper cluster replication dashboard and figure out why we need to
		// pass a grafana dashboard for this to work
		rd.setup.promCfg = (&prometheus.Config{}).
			WithPrometheusNode(rd.setup.workloadNode.InstallNodes()[0]).
			WithCluster(rd.setup.dst.nodes.InstallNodes()).
			WithNodeExporter(rd.setup.dst.nodes.InstallNodes()).
			WithGrafanaDashboard("https://go.crdb.dev/p/changefeed-roachtest-grafana-dashboard")

		// StartGrafana clutters the test.log. Try logging setup to a separate file.
		promLog, err := rd.t.L().ChildLogger("prom_setup", logger.QuietStderr, logger.QuietStdout)
		if err != nil {
			promLog = rd.t.L()
		}
		require.NoError(rd.t, rd.c.StartGrafana(ctx, promLog, rd.setup.promCfg))
		rd.t.L().Printf("Prom has started")
	}
}

func (rd *replicationDriver) crdbNodes() option.NodeListOption {
	return rd.setup.src.nodes.Merge(rd.setup.dst.nodes)
}

func (rd *replicationDriver) newMonitor(ctx context.Context) cluster.Monitor {
	m := rd.c.NewMonitor(ctx, rd.crdbNodes())
	m.ExpectDeaths(rd.rs.expectedNodeDeaths)
	return m
}

func (rd *replicationDriver) startStatsCollection(
	ctx context.Context,
) func(time.Time) map[string]float64 {

	if rd.c.IsLocal() {
		rd.t.L().Printf("Local test. Don't setup grafana")
		// Grafana does not run locally.
		return func(snapTime time.Time) map[string]float64 {
			return map[string]float64{}
		}
	}

	client, err := clusterstats.SetupCollectorPromClient(ctx, rd.c, rd.t.L(), rd.setup.promCfg)
	require.NoError(rd.t, err, "error creating prometheus client for stats collector")
	collector := clusterstats.NewStatsCollector(ctx, client)

	return func(snapTime time.Time) map[string]float64 {
		metricSnap := make(map[string]float64)
		for name, stat := range c2cPromMetrics {
			point, err := collector.CollectPoint(ctx, rd.t.L(), snapTime, stat.Query)
			if err != nil {
				rd.t.L().Errorf("Could not query prom %s", err.Error())
			}
			// TODO(msbutler): update the CollectPoint api to conduct the sum in Prom instead.
			metricSnap[name] = sumOverLabel(point, stat.LabelName)
			rd.t.L().Printf("%s: %.2f", name, metricSnap[name])
		}
		return metricSnap
	}
}

func (rd *replicationDriver) preStreamingWorkload(ctx context.Context) {
	if initCmd := rd.rs.workload.sourceInitCmd(rd.setup.src.name, rd.setup.src.nodes); initCmd != "" {
		rd.t.Status("populating source cluster before replication")
		initStart := timeutil.Now()
		rd.c.Run(ctx, rd.setup.workloadNode, initCmd)
		rd.t.L().Printf("src cluster workload initialization took %s minutes",
			timeutil.Since(initStart).Minutes())
	}
}

func (rd *replicationDriver) startReplicationStream(ctx context.Context) int {
	streamReplStmt := fmt.Sprintf("CREATE TENANT %q FROM REPLICATION OF %q ON '%s'",
		rd.setup.dst.name, rd.setup.src.name, rd.setup.src.pgURL)
	rd.setup.dst.sysSQL.Exec(rd.t, streamReplStmt)
	rd.replicationStartHook(ctx, rd)
	return getIngestionJobID(rd.t, rd.setup.dst.sysSQL, rd.setup.dst.name)
}

func (rd *replicationDriver) runWorkload(ctx context.Context) error {
	rd.beforeWorkloadHook()
	return rd.rs.workload.runDriver(ctx, rd.c, rd.t, rd.setup)
}

func (rd *replicationDriver) waitForHighWatermark(ingestionJobID int, wait time.Duration) {
	testutils.SucceedsWithin(rd.t, func() error {
		info, err := getStreamIngestionJobInfo(rd.setup.dst.db, ingestionJobID)
		if err != nil {
			return err
		}
		if info.GetHighWater().IsZero() {
			return errors.New("no high watermark")
		}
		return nil
	}, wait)
}

func (rd *replicationDriver) getWorkloadTimeout() time.Duration {
	if rd.rs.additionalDuration != 0 {
		return rd.rs.additionalDuration
	}
	return rd.rs.timeout
}

// getReplicationRetainedTime returns the `retained_time` of the replication
// job.
func (rd *replicationDriver) getReplicationRetainedTime() time.Time {
	var retainedTime time.Time
	rd.setup.dst.sysSQL.QueryRow(rd.t,
		`SELECT retained_time FROM [SHOW TENANT $1 WITH REPLICATION STATUS]`,
		roachpb.TenantName(rd.setup.dst.name)).Scan(&retainedTime)
	return retainedTime
}

func (rd *replicationDriver) stopReplicationStream(
	ctx context.Context, ingestionJob int, cutoverTime time.Time,
) {
	rd.setup.dst.sysSQL.Exec(rd.t, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`, rd.setup.dst.name, cutoverTime)
	err := retry.ForDuration(time.Minute*5, func() error {
		var status string
		var payloadBytes []byte
		res := rd.setup.dst.sysSQL.DB.QueryRowContext(ctx, `SELECT status, payload FROM crdb_internal.system_jobs WHERE id = $1`, ingestionJob)
		if res.Err() != nil {
			// This query can fail if a node shuts down during the query execution;
			// therefore, tolerate errors.
			return res.Err()
		}
		require.NoError(rd.t, res.Scan(&status, &payloadBytes))
		if jobs.Status(status) == jobs.StatusFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				rd.t.Fatalf("job failed: %s", payload.Error)
			}
			rd.t.Fatalf("job failed")
		}
		if e, a := jobs.StatusSucceeded, jobs.Status(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	})
	require.NoError(rd.t, err)
}

func (rd *replicationDriver) compareTenantFingerprintsAtTimestamp(
	ctx context.Context, startTime, endTime time.Time,
) {
	rd.t.Status(fmt.Sprintf("comparing tenant fingerprints between start time %s and end time %s",
		startTime.UTC(), endTime.UTC()))

	// TODO(adityamaru,lidorcarmel): Once we agree on the format and precision we
	// display all user facing timestamps with, we should revisit how we format
	// the start time to ensure we are fingerprinting from the most accurate lower
	// bound.
	microSecondRFC3339Format := "2006-01-02 15:04:05.999999"
	startTimeStr := startTime.Format(microSecondRFC3339Format)
	aost := hlc.Timestamp{WallTime: endTime.UnixNano()}.AsOfSystemTime()
	fingerprintQuery := fmt.Sprintf(`
SELECT *
FROM crdb_internal.fingerprint(crdb_internal.tenant_span($1::INT), '%s'::TIMESTAMPTZ, true)
AS OF SYSTEM TIME '%s'`, startTimeStr, aost)

	var srcFingerprint int64
	fingerPrintMonitor := rd.newMonitor(ctx)
	fingerPrintMonitor.Go(func(ctx context.Context) error {
		rd.setup.src.sysSQL.QueryRow(rd.t, fingerprintQuery, rd.setup.src.ID).Scan(&srcFingerprint)
		return nil
	})
	var destFingerprint int64
	fingerPrintMonitor.Go(func(ctx context.Context) error {
		// TODO(adityamaru): Measure and record fingerprinting throughput.
		rd.metrics.fingerprintingStart = timeutil.Now()
		rd.setup.dst.sysSQL.QueryRow(rd.t, fingerprintQuery, rd.setup.dst.ID).Scan(&destFingerprint)
		rd.metrics.fingerprintingEnd = timeutil.Now()
		fingerprintingDuration := rd.metrics.fingerprintingEnd.Sub(rd.metrics.fingerprintingStart).String()
		rd.t.L().Printf("fingerprinting the destination tenant took %s", fingerprintingDuration)
		return nil
	})

	// If the goroutine gets cancelled or fataled, return before comparing fingerprints.
	require.NoError(rd.t, fingerPrintMonitor.WaitE())
	require.Equal(rd.t, srcFingerprint, destFingerprint)
}

func (rd *replicationDriver) main(ctx context.Context) {
	metricSnapper := rd.startStatsCollection(ctx)
	rd.preStreamingWorkload(ctx)

	// Wait for initial workload to be properly replicated across the source cluster to increase
	// the probability that the producer returns a topology with more than one node in it,
	// else the node shutdown tests can flake.
	if rd.rs.srcNodes >= 3 {
		require.NoError(rd.t, WaitFor3XReplication(ctx, rd.t, rd.setup.src.db))
	}

	rd.t.L().Printf("begin workload on src cluster")
	m := rd.newMonitor(ctx)
	// The roachtest driver can use the workloadCtx to cancel the workload.
	workloadCtx, workloadCancel := context.WithCancel(ctx)
	defer workloadCancel()

	workloadDoneCh := make(chan struct{})
	m.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		err := rd.runWorkload(workloadCtx)
		// The workload should only return an error if the roachtest driver cancels the
		// workloadCtx after rd.additionalDuration has elapsed after the initial scan completes.
		if err != nil && workloadCtx.Err() == nil {
			// Implies the workload context was not cancelled and the workload cmd returned a
			// different error.
			return errors.Wrapf(err, `Workload context was not cancelled. Error returned by workload cmd`)
		}
		rd.t.L().Printf("workload successfully finished")
		return nil
	})

	rd.t.Status("starting replication stream")
	rd.metrics.initalScanStart = newMetricSnapshot(metricSnapper, timeutil.Now())
	ingestionJobID := rd.startReplicationStream(ctx)
	removeTenantRateLimiters(rd.t, rd.setup.dst.sysSQL, rd.setup.dst.name)

	// latency verifier queries may error during a node shutdown event; therefore
	// tolerate errors if we anticipate node deaths.
	lv := makeLatencyVerifier("stream-ingestion", 0, 2*time.Minute, rd.t.L(),
		getStreamIngestionJobInfo, rd.t.Status, rd.rs.expectedNodeDeaths > 0)
	defer lv.maybeLogLatencyHist()

	m.Go(func(ctx context.Context) error {
		return lv.pollLatency(ctx, rd.setup.dst.db, ingestionJobID, time.Second, workloadDoneCh)
	})

	rd.t.L().Printf("waiting for replication stream to finish ingesting initial scan")
	rd.waitForHighWatermark(ingestionJobID, rd.rs.timeout/2)
	rd.metrics.initialScanEnd = newMetricSnapshot(metricSnapper, timeutil.Now())
	rd.t.Status(fmt.Sprintf(`initial scan complete. run workload and repl. stream for another %s minutes`,
		rd.rs.additionalDuration))

	select {
	case <-workloadDoneCh:
		rd.t.L().Printf("workload finished on its own")
	case <-time.After(rd.getWorkloadTimeout()):
		workloadCancel()
		rd.t.L().Printf("workload has cancelled after %s", rd.rs.additionalDuration)
	case <-ctx.Done():
		rd.t.L().Printf(`roachtest context cancelled while waiting for workload duration to complete`)
		return
	}
	var currentTime time.Time
	rd.setup.dst.sysSQL.QueryRow(rd.t, "SELECT clock_timestamp()").Scan(&currentTime)
	cutoverTime := currentTime.Add(-rd.rs.cutover)
	rd.t.Status("cutover time chosen: ", cutoverTime.String())

	retainedTime := rd.getReplicationRetainedTime()

	rd.metrics.cutoverTo = newMetricSnapshot(metricSnapper, cutoverTime)
	rd.metrics.cutoverStart = newMetricSnapshot(metricSnapper, timeutil.Now())

	rd.t.Status(fmt.Sprintf("waiting for replication stream to cutover to %s",
		cutoverTime.String()))
	rd.stopReplicationStream(ctx, ingestionJobID, cutoverTime)
	rd.metrics.cutoverEnd = newMetricSnapshot(metricSnapper, timeutil.Now())

	rd.metrics.export(rd.t, len(rd.setup.src.nodes))

	rd.t.Status("comparing fingerprints")
	rd.compareTenantFingerprintsAtTimestamp(
		ctx,
		retainedTime,
		cutoverTime,
	)
	lv.assertValid(rd.t)
}

func c2cRegisterWrapper(
	r registry.Registry,
	sp replicationSpec,
	run func(ctx context.Context, t test.Test, c cluster.Cluster),
) {

	clusterOps := make([]spec.Option, 0)
	if sp.cpus != 0 {
		clusterOps = append(clusterOps, spec.CPU(sp.cpus))
	}
	if sp.pdSize != 0 {
		clusterOps = append(clusterOps, spec.VolumeSize(sp.pdSize))
	}

	r.Add(registry.TestSpec{
		Name:            sp.name,
		Owner:           registry.OwnerDisasterRecovery,
		Cluster:         r.MakeClusterSpec(sp.dstNodes+sp.srcNodes+1, clusterOps...),
		Timeout:         sp.timeout,
		Skip:            sp.skip,
		RequiresLicense: true,
		Run:             run,
	})
}

func runAcceptanceClusterReplication(ctx context.Context, t test.Test, c cluster.Cluster) {
	if !c.IsLocal() {
		t.Skip("c2c/acceptance is only meant to run on a local cluster")
	}
	sp := replicationSpec{
		srcNodes: 1,
		dstNodes: 1,
		// The timeout field ensures the c2c roachtest driver behaves properly.
		timeout:            10 * time.Minute,
		workload:           replicateKV{readPercent: 0, debugRunDuration: 1 * time.Minute, maxBlockBytes: 1},
		additionalDuration: 0 * time.Minute,
		cutover:            30 * time.Second,
	}
	rd := makeReplicationDriver(t, c, sp)
	rd.setupC2C(ctx, t, c)
	rd.main(ctx)
}

func registerClusterToCluster(r registry.Registry) {
	for _, sp := range []replicationSpec{
		{
			name:     "c2c/tpcc/warehouses=500/duration=10/cutover=5",
			srcNodes: 4,
			dstNodes: 4,
			cpus:     8,
			pdSize:   1000,
			// 500 warehouses adds 30 GB to source
			//
			// TODO(msbutler): increase default test to 1000 warehouses once fingerprinting
			// job speeds up.
			workload:           replicateTPCC{warehouses: 500},
			timeout:            1 * time.Hour,
			additionalDuration: 10 * time.Minute,
			cutover:            5 * time.Minute,
		},
		{
			name:     "c2c/tpcc/warehouses=1000/duration=60/cutover=30",
			srcNodes: 4,
			dstNodes: 4,
			cpus:     8,
			pdSize:   1000,
			// 500 warehouses adds 30 GB to source
			//
			// TODO(msbutler): increase default test to 1000 warehouses once fingerprinting
			// job speeds up.
			workload:           replicateTPCC{warehouses: 1000},
			timeout:            3 * time.Hour,
			additionalDuration: 60 * time.Minute,
			cutover:            30 * time.Minute,
		},
		{
			name:               "c2c/kv0",
			srcNodes:           3,
			dstNodes:           3,
			cpus:               8,
			pdSize:             100,
			workload:           replicateKV{readPercent: 0, maxBlockBytes: 1024},
			timeout:            1 * time.Hour,
			additionalDuration: 10 * time.Minute,
			cutover:            5 * time.Minute,
		},
		{
			name:     "c2c/UnitTest",
			srcNodes: 1,
			dstNodes: 1,
			cpus:     4,
			pdSize:   10,
			workload: replicateKV{readPercent: 0, debugRunDuration: 1 * time.Minute,
				maxBlockBytes: 1024},
			timeout:            5 * time.Minute,
			additionalDuration: 0 * time.Minute,
			cutover:            30 * time.Second,
			skip:               "for local ad hoc testing",
		},
		{
			name:               "c2c/BulkOps",
			srcNodes:           4,
			dstNodes:           4,
			cpus:               8,
			pdSize:             500,
			workload:           replicateBulkOps{},
			timeout:            4 * time.Hour,
			additionalDuration: 0,
			cutover:            5 * time.Minute,
			skip:               "flaky",
		},
	} {
		sp := sp
		c2cRegisterWrapper(r, sp,
			func(ctx context.Context, t test.Test, c cluster.Cluster) {
				rd := makeReplicationDriver(t, c, sp)
				rd.setupC2C(ctx, t, c)

				m := c.NewMonitor(ctx)
				hc := roachtestutil.NewHealthChecker(t, c, rd.crdbNodes())
				m.Go(func(ctx context.Context) error {
					require.NoError(t, hc.Runner(ctx))
					return nil
				})
				defer hc.Done()

				rd.main(ctx)
			})
	}
}

type c2cPhase int

const (
	phaseInitialScan c2cPhase = iota
	phaseSteadyState
	phaseCutover
)

func (c c2cPhase) String() string {
	switch c {
	case phaseInitialScan:
		return "Initial Scan"
	case phaseSteadyState:
		return "Steady State"
	case phaseCutover:
		return "Cutover"
	default:
		return "unknown"
	}
}

// replResilienceSpec defines inputs to the replication resilience tests, set
// during roachtest registration. This can not be modified during roachtest
// execution.
type replResilienceSpec struct {
	replicationSpec

	onSrc         bool
	onCoordinator bool
}

func (rsp *replResilienceSpec) name() string {
	var builder strings.Builder
	builder.WriteString("c2c/shutdown")
	if rsp.onSrc {
		builder.WriteString("/src")
	} else {
		builder.WriteString("/dest")
	}
	if rsp.onCoordinator {
		builder.WriteString("/coordinator")
	} else {
		builder.WriteString("/worker")
	}
	return builder.String()
}

// replResilienceDriver manages the execution of the replication resilience tests.
type replResilienceDriver struct {
	replicationDriver
	rsp replResilienceSpec

	// phase indicates the c2c phase a node shutdown will occur.
	phase c2cPhase

	// the fields below are gathered after the replication stream has started
	srcJobID     jobspb.JobID
	dstJobID     jobspb.JobID
	shutdownNode int
	watcherNode  int
}

func makeReplResilienceDriver(
	t test.Test, c cluster.Cluster, rsp replResilienceSpec,
) replResilienceDriver {
	rd := makeReplicationDriver(t, c, rsp.replicationSpec)
	return replResilienceDriver{
		replicationDriver: rd,
		phase:             c2cPhase(rd.rng.Intn(int(phaseCutover) + 1)),
		rsp:               rsp,
	}
}

func (rrd *replResilienceDriver) getJobIDs(ctx context.Context) {
	jobIDQuery := `SELECT job_id FROM [SHOW JOBS] WHERE job_type = '%s'`
	testutils.SucceedsWithin(rrd.t, func() error {
		if err := rrd.setup.dst.db.QueryRowContext(ctx, fmt.Sprintf(jobIDQuery,
			`STREAM INGESTION`)).Scan(&rrd.dstJobID); err != nil {
			return err
		}
		if err := rrd.setup.src.db.QueryRowContext(ctx, fmt.Sprintf(jobIDQuery,
			`STREAM REPLICATION`)).Scan(&rrd.srcJobID); err != nil {
			return err
		}
		return nil
	}, time.Minute)
}

func (rrd *replResilienceDriver) getTargetInfo() (
	*clusterInfo,
	jobspb.JobID,
	option.NodeListOption,
) {
	if rrd.rsp.onSrc {
		return rrd.setup.src, rrd.srcJobID, rrd.setup.src.nodes
	}
	return rrd.setup.dst, rrd.dstJobID, rrd.setup.dst.nodes
}

func (rrd *replResilienceDriver) getTargetAndWatcherNodes(ctx context.Context) {
	var coordinatorNode int
	info, jobID, nodes := rrd.getTargetInfo()

	// To populate the coordinator_id field, a node needs to claim the job.
	// Give the job system a minute.
	testutils.SucceedsWithin(rrd.t, func() error {
		return info.db.QueryRowContext(ctx,
			`SELECT coordinator_id FROM crdb_internal.jobs WHERE job_id = $1`, jobID).Scan(&coordinatorNode)
	}, time.Minute)
	if !rrd.rsp.onSrc {
		// From the destination cluster's perspective, node ids range from 1 to
		// num_dest_nodes, but from roachprod's perspective they range from
		// num_source_nodes+1 to num_crdb_roachprod nodes. We need to adjust for
		// this to shut down the right node. Example: if the coordinator node on the
		// dest cluster is 1, and there are 4 src cluster nodes, then
		// shut down roachprod node 5.
		coordinatorNode += rrd.rsp.srcNodes
	}

	var targetNode int

	findAnotherNode := func(notThisNode int) int {
		for {
			anotherNode := nodes.SeededRandNode(rrd.rng)[0]
			if notThisNode != anotherNode {
				return anotherNode
			}
		}
	}
	if rrd.rsp.onCoordinator {
		targetNode = coordinatorNode
	} else {
		targetNode = findAnotherNode(coordinatorNode)
	}
	rrd.shutdownNode = targetNode
	rrd.watcherNode = findAnotherNode(targetNode)
}

func (rrd *replResilienceDriver) getPhase() c2cPhase {
	var jobStatus string
	rrd.setup.dst.sysSQL.QueryRow(rrd.t, `SELECT status FROM [SHOW JOBS] WHERE job_id=$1`,
		rrd.dstJobID).Scan(&jobStatus)
	require.Equal(rrd.t, jobs.StatusRunning, jobs.Status(jobStatus))

	progress := getJobProgress(rrd.t, rrd.setup.dst.sysSQL, rrd.dstJobID)
	streamIngestProgress := progress.GetStreamIngest()
	highWater := progress.GetHighWater()

	if highWater == nil || highWater.IsEmpty() {
		return phaseInitialScan
	}
	if streamIngestProgress.CutoverTime.IsEmpty() {
		return phaseSteadyState
	}
	return phaseCutover
}

func (rrd *replResilienceDriver) waitForTargetPhase() error {
	for {
		currentPhase := rrd.getPhase()
		rrd.t.L().Printf("Current Phase %s", currentPhase.String())
		switch {
		case currentPhase < rrd.phase:
			time.Sleep(5 * time.Second)
		case currentPhase == rrd.phase:
			rrd.t.L().Printf("In target phase %s", currentPhase.String())
			return nil
		default:
			return errors.New("c2c job past target phase")
		}
	}
}

func (rrd *replResilienceDriver) sleepBeforeResiliencyEvent() {
	// Assuming every C2C phase lasts at least 5 seconds, introduce some waiting
	// before a resiliency event (e.g. a node shutdown) to ensure the event occurs
	// once we're fully settled into the target phase (e.g. the stream ingestion
	// processors have observed the cutover signal).
	randomSleep := time.Duration(1+rrd.rng.Intn(2)) * time.Second
	rrd.t.L().Printf("Take a %s power nap", randomSleep)
	time.Sleep(randomSleep)
}

func registerClusterReplicationResilience(r registry.Registry) {
	for _, rsp := range []replResilienceSpec{
		{
			onSrc:         true,
			onCoordinator: true,
		},
		{
			onSrc:         true,
			onCoordinator: false,
		},
		{
			onSrc:         false,
			onCoordinator: true,
		},
		{
			onSrc:         false,
			onCoordinator: false,
		},
	} {
		rsp := rsp

		rsp.replicationSpec = replicationSpec{
			name:               rsp.name(),
			srcNodes:           4,
			dstNodes:           4,
			cpus:               8,
			workload:           replicateKV{readPercent: 0, initRows: 5000000, maxBlockBytes: 1024},
			timeout:            20 * time.Minute,
			additionalDuration: 6 * time.Minute,
			cutover:            3 * time.Minute,
			expectedNodeDeaths: 1,
		}

		c2cRegisterWrapper(r, rsp.replicationSpec,
			func(ctx context.Context, t test.Test, c cluster.Cluster) {

				rrd := makeReplResilienceDriver(t, c, rsp)
				rrd.t.L().Printf("Planning to shut down node during %s phase", rrd.phase)
				rrd.setupC2C(ctx, t, c)

				shutdownSetupDone := make(chan struct{})

				rrd.beforeWorkloadHook = func() {
					// Ensure the workload begins after c2c jobs have been set up.
					<-shutdownSetupDone
				}

				rrd.replicationStartHook = func(ctx context.Context, rd *replicationDriver) {
					// Once the C2C job is set up, we need to modify some configs to
					// ensure the shutdown doesn't bother the underlying c2c job and the
					// foreground workload. The shutdownSetupDone channel prevents other
					// goroutines from reading the configs concurrently.

					defer close(shutdownSetupDone)
					rrd.getJobIDs(ctx)
					rrd.getTargetAndWatcherNodes(ctx)

					// To prevent sql connections from connecting to the shutdown node,
					// ensure roachtest process connections to cluster use watcher node
					// from now on.
					watcherDB := c.Conn(ctx, rd.t.L(), rrd.watcherNode)
					watcherSQL := sqlutils.MakeSQLRunner(watcherDB)
					if rsp.onSrc {
						rd.setup.src.db = watcherDB
						rd.setup.src.sysSQL = watcherSQL
						rd.setup.gatewayNodes = c.Node(rrd.watcherNode)
					} else {
						rd.setup.dst.db = watcherDB
						rd.setup.dst.sysSQL = watcherSQL
					}
					t.L().Printf(`%s configured: Shutdown Node %d; Watcher node %d; Gateway nodes %s`,
						rrd.rsp.name(), rrd.shutdownNode, rrd.watcherNode, rrd.setup.gatewayNodes)
				}
				m := rrd.newMonitor(ctx)
				m.Go(func(ctx context.Context) error {
					rrd.main(ctx)
					return nil
				})

				// Don't begin shutdown process until c2c job is set up.
				<-shutdownSetupDone

				// Eagerly listen to cutover signal to exercise node shutdown during actual cutover.
				rrd.setup.dst.sysSQL.Exec(t, `SET CLUSTER SETTING bulkio.stream_ingestion.cutover_signal_poll_interval='5s'`)

				// While executing a node shutdown on either the src or destination
				// cluster, ensure the destination cluster's stream ingestion job
				// completes. If the stream producer job fails, no big deal-- in a real
				// DR scenario the src cluster may have gone belly up during a
				// successful c2c replication execution.
				shutdownStarter := func() jobStarter {
					return func(c cluster.Cluster, t test.Test) (string, error) {
						require.NoError(t, rrd.waitForTargetPhase())
						rrd.sleepBeforeResiliencyEvent()
						return fmt.Sprintf("%d", rrd.dstJobID), nil
					}
				}
				destinationWatcherNode := rrd.watcherNode
				if rsp.onSrc {
					destinationWatcherNode = rrd.setup.dst.nodes[0]
				}
				shutdownCfg := nodeShutdownConfig{
					shutdownNode:    rrd.shutdownNode,
					watcherNode:     destinationWatcherNode,
					crdbNodes:       rrd.crdbNodes(),
					restartSettings: []install.ClusterSettingOption{install.SecureOption(true)},
				}
				executeNodeShutdown(ctx, t, c, shutdownCfg, shutdownStarter())
			},
		)
	}
}

func getIngestionJobID(t test.Test, dstSQL *sqlutils.SQLRunner, dstTenantName string) int {
	var tenantInfoBytes []byte
	var tenantInfo mtinfopb.ProtoInfo
	dstSQL.QueryRow(t, "SELECT info FROM system.tenants WHERE name=$1",
		dstTenantName).Scan(&tenantInfoBytes)
	require.NoError(t, protoutil.Unmarshal(tenantInfoBytes, &tenantInfo))
	return int(tenantInfo.TenantReplicationJobID)
}

type streamIngesitonJobInfo struct {
	status        string
	errMsg        string
	highwaterTime time.Time
	finishedTime  time.Time
}

func (c *streamIngesitonJobInfo) GetHighWater() time.Time    { return c.highwaterTime }
func (c *streamIngesitonJobInfo) GetFinishedTime() time.Time { return c.finishedTime }
func (c *streamIngesitonJobInfo) GetStatus() string          { return c.status }
func (c *streamIngesitonJobInfo) GetError() string           { return c.status }

var _ jobInfo = (*streamIngesitonJobInfo)(nil)

func getStreamIngestionJobInfo(db *gosql.DB, jobID int) (jobInfo, error) {
	var status string
	var payloadBytes []byte
	var progressBytes []byte
	if err := db.QueryRow(
		`SELECT status, payload, progress FROM crdb_internal.system_jobs WHERE id = $1`, jobID,
	).Scan(&status, &payloadBytes, &progressBytes); err != nil {
		return nil, err
	}
	var payload jobspb.Payload
	if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, err
	}
	var progress jobspb.Progress
	if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
		return nil, err
	}
	var highwaterTime time.Time
	highwater := progress.GetHighWater()
	if highwater != nil {
		highwaterTime = highwater.GoTime()
	}
	return &streamIngesitonJobInfo{
		status:        status,
		errMsg:        payload.Error,
		highwaterTime: highwaterTime,
		finishedTime:  time.UnixMicro(payload.FinishedMicros),
	}, nil
}

func srcClusterSettings(t test.Test, db *sqlutils.SQLRunner) {
	db.ExecMultiple(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
}

func destClusterSettings(t test.Test, db *sqlutils.SQLRunner) {
	db.ExecMultiple(t, `SET CLUSTER SETTING cross_cluster_replication.enabled = true;`,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
}

func copyPGCertsAndMakeURL(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	srcNode option.NodeListOption,
	pgURLDir string,
	urlString string,
) (string, error) {
	pgURL, err := url.Parse(urlString)
	if err != nil {
		return "", err
	}

	tmpDir, err := os.MkdirTemp("", "certs")
	if err != nil {
		return "", err
	}
	func() { _ = os.RemoveAll(tmpDir) }()

	if err := c.Get(ctx, t.L(), pgURLDir, tmpDir, srcNode); err != nil {
		return "", err
	}

	sslRootCert, err := os.ReadFile(filepath.Join(tmpDir, "ca.crt"))
	if err != nil {
		return "", err
	}
	sslClientCert, err := os.ReadFile(filepath.Join(tmpDir, "client.root.crt"))
	if err != nil {
		return "", err
	}
	sslClientKey, err := os.ReadFile(filepath.Join(tmpDir, "client.root.key"))
	if err != nil {
		return "", err
	}

	options := pgURL.Query()
	options.Set("sslmode", "verify-full")
	options.Set("sslinline", "true")
	options.Set("sslrootcert", string(sslRootCert))
	options.Set("sslcert", string(sslClientCert))
	options.Set("sslkey", string(sslClientKey))
	pgURL.RawQuery = options.Encode()
	return pgURL.String(), nil
}
