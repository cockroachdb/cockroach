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
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
	src          *clusterInfo
	dst          *clusterInfo
	workloadNode option.NodeListOption
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
	return c.RunE(workloadCtx, setup.workloadNode, workload.sourceRunCmd(setup.src.name, setup.src.nodes))
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
	debugRunDurationMinutes int
}

func (kv replicateKV) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	return ""
}

func (kv replicateKV) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	debugDuration := ""
	if kv.debugRunDurationMinutes != 0 {
		debugDuration = fmt.Sprintf("--duration %dm", kv.debugRunDurationMinutes)
	}
	// added --tolerate-errors flags to prevent test from flaking due to a transaction retry error
	return fmt.Sprintf(`./workload run kv --tolerate-errors --init %s --read-percent %d {pgurl%s:%s}`,
		debugDuration,
		kv.readPercent,
		nodes,
		tenantName)
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

type replicationTestSpec struct {
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

	// replicationStartHook is called as soon as the replication job begins,
	// when there are no other roachtest connections to the database.
	replicationStartHook func(ctx context.Context, sp *replicationTestSpec)

	// fields below are instantiated at runtime
	setup   *c2cSetup
	t       test.Test
	c       cluster.Cluster
	metrics *c2cMetrics
}

func (sp *replicationTestSpec) setupC2C(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	srcCluster := c.Range(1, sp.srcNodes)
	dstCluster := c.Range(sp.srcNodes+1, sp.srcNodes+sp.dstNodes)
	workloadNode := c.Node(sp.srcNodes + sp.dstNodes + 1)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)

	// TODO(msbutler): allow for backups once this test stabilizes a bit more.
	srcStartOps := option.DefaultStartOptsNoBackups()
	srcStartOps.RoachprodOpts.InitTarget = 1
	srcClusterSetting := install.MakeClusterSettings(install.SecureOption(true))
	c.Start(ctx, t.L(), srcStartOps, srcClusterSetting, srcCluster)

	// TODO(msbutler): allow for backups once this test stabilizes a bit more.
	dstStartOps := option.DefaultStartOptsNoBackups()
	dstStartOps.RoachprodOpts.InitTarget = sp.srcNodes + 1
	dstClusterSetting := install.MakeClusterSettings(install.SecureOption(true))
	c.Start(ctx, t.L(), dstStartOps, dstClusterSetting, dstCluster)

	srcNode := srcCluster.RandNode()
	destNode := dstCluster.RandNode()

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

	sp.setup = &c2cSetup{
		src:          &srcTenantInfo,
		dst:          &destTenantInfo,
		workloadNode: workloadNode}
	sp.t = t
	sp.c = c
	sp.metrics = &c2cMetrics{}
	sp.replicationStartHook = func(ctx context.Context, sp *replicationTestSpec) {}

	if !c.IsLocal() {
		// TODO(msbutler): pass a proper cluster replication dashboard and figure out why we need to
		// pass a grafana dashboard for this to work
		sp.setup.promCfg = (&prometheus.Config{}).
			WithPrometheusNode(sp.setup.workloadNode.InstallNodes()[0]).
			WithCluster(sp.setup.dst.nodes.InstallNodes()).
			WithNodeExporter(sp.setup.dst.nodes.InstallNodes()).
			WithGrafanaDashboard("https://go.crdb.dev/p/changefeed-roachtest-grafana-dashboard")

		require.NoError(sp.t, sp.c.StartGrafana(ctx, sp.t.L(), sp.setup.promCfg))
		sp.t.L().Printf("Prom has started")
	}
}

func (sp *replicationTestSpec) crdbNodes() option.NodeListOption {
	return sp.setup.src.nodes.Merge(sp.setup.dst.nodes)
}

func (sp *replicationTestSpec) newMonitor(ctx context.Context) cluster.Monitor {
	m := sp.c.NewMonitor(ctx, sp.crdbNodes())
	m.ExpectDeaths(sp.expectedNodeDeaths)
	return m
}

func (sp *replicationTestSpec) startStatsCollection(
	ctx context.Context,
) func(time.Time) map[string]float64 {

	if sp.c.IsLocal() {
		sp.t.L().Printf("Local test. Don't setup grafana")
		// Grafana does not run locally.
		return func(snapTime time.Time) map[string]float64 {
			return map[string]float64{}
		}
	}

	client, err := clusterstats.SetupCollectorPromClient(ctx, sp.c, sp.t.L(), sp.setup.promCfg)
	require.NoError(sp.t, err, "error creating prometheus client for stats collector")
	collector := clusterstats.NewStatsCollector(ctx, client)

	return func(snapTime time.Time) map[string]float64 {
		metricSnap := make(map[string]float64)
		for name, stat := range c2cPromMetrics {
			point, err := collector.CollectPoint(ctx, sp.t.L(), snapTime, stat.Query)
			if err != nil {
				sp.t.L().Errorf("Could not query prom %s", err.Error())
			}
			// TODO(msbutler): update the CollectPoint api to conduct the sum in Prom instead.
			metricSnap[name] = sumOverLabel(point, stat.LabelName)
			sp.t.L().Printf("%s: %.2f", name, metricSnap[name])
		}
		return metricSnap
	}
}

func (sp *replicationTestSpec) preStreamingWorkload(ctx context.Context) {
	if initCmd := sp.workload.sourceInitCmd(sp.setup.src.name, sp.setup.src.nodes); initCmd != "" {
		sp.t.Status("populating source cluster before replication")
		initStart := timeutil.Now()
		sp.c.Run(ctx, sp.setup.workloadNode, initCmd)
		sp.t.L().Printf("src cluster workload initialization took %s minutes",
			timeutil.Since(initStart).Minutes())
	}
}

func (sp *replicationTestSpec) startReplicationStream(ctx context.Context) int {
	streamReplStmt := fmt.Sprintf("CREATE TENANT %q FROM REPLICATION OF %q ON '%s'",
		sp.setup.dst.name, sp.setup.src.name, sp.setup.src.pgURL)
	sp.setup.dst.sysSQL.Exec(sp.t, streamReplStmt)
	sp.replicationStartHook(ctx, sp)
	return getIngestionJobID(sp.t, sp.setup.dst.sysSQL, sp.setup.dst.name)
}

func (sp *replicationTestSpec) runWorkload(ctx context.Context) error {
	return sp.workload.runDriver(ctx, sp.c, sp.t, sp.setup)
}

func (sp *replicationTestSpec) waitForHighWatermark(ingestionJobID int, wait time.Duration) {
	testutils.SucceedsWithin(sp.t, func() error {
		info, err := getStreamIngestionJobInfo(sp.setup.dst.db, ingestionJobID)
		if err != nil {
			return err
		}
		if info.GetHighWater().IsZero() {
			return errors.New("no high watermark")
		}
		return nil
	}, wait)
}

func (sp *replicationTestSpec) getWorkloadTimeout() time.Duration {
	if sp.additionalDuration != 0 {
		return sp.additionalDuration
	}
	return sp.timeout
}

// getReplicationRetainedTime returns the `retained_time` of the replication
// job.
func (sp *replicationTestSpec) getReplicationRetainedTime() time.Time {
	var retainedTime time.Time
	sp.setup.dst.sysSQL.QueryRow(sp.t,
		`SELECT retained_time FROM [SHOW TENANT $1 WITH REPLICATION STATUS]`,
		roachpb.TenantName(sp.setup.dst.name)).Scan(&retainedTime)
	return retainedTime
}

func (sp *replicationTestSpec) stopReplicationStream(ingestionJob int, cutoverTime time.Time) {
	sp.setup.dst.sysSQL.Exec(sp.t, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`, sp.setup.dst.name, cutoverTime)
	err := retry.ForDuration(time.Minute*5, func() error {
		var status string
		var payloadBytes []byte
		sp.setup.dst.sysSQL.QueryRow(sp.t, `SELECT status, payload FROM crdb_internal.system_jobs WHERE id = $1`,
			ingestionJob).Scan(&status, &payloadBytes)
		if jobs.Status(status) == jobs.StatusFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				sp.t.Fatalf("job failed: %s", payload.Error)
			}
			sp.t.Fatalf("job failed")
		}
		if e, a := jobs.StatusSucceeded, jobs.Status(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	})
	require.NoError(sp.t, err)
}

func (sp *replicationTestSpec) compareTenantFingerprintsAtTimestamp(
	ctx context.Context, startTime, endTime time.Time,
) {
	sp.t.Status(fmt.Sprintf("comparing tenant fingerprints between start time %s and end time %s",
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
	fingerPrintMonitor := sp.newMonitor(ctx)
	fingerPrintMonitor.Go(func(ctx context.Context) error {
		sp.setup.src.sysSQL.QueryRow(sp.t, fingerprintQuery, sp.setup.src.ID).Scan(&srcFingerprint)
		return nil
	})
	var destFingerprint int64
	fingerPrintMonitor.Go(func(ctx context.Context) error {
		// TODO(adityamaru): Measure and record fingerprinting throughput.
		sp.metrics.fingerprintingStart = timeutil.Now()
		sp.setup.dst.sysSQL.QueryRow(sp.t, fingerprintQuery, sp.setup.dst.ID).Scan(&destFingerprint)
		sp.metrics.fingerprintingEnd = timeutil.Now()
		fingerprintingDuration := sp.metrics.fingerprintingEnd.Sub(sp.metrics.fingerprintingStart).String()
		sp.t.L().Printf("fingerprinting the destination tenant took %s", fingerprintingDuration)
		return nil
	})

	// If the goroutine gets cancelled or fataled, return before comparing fingerprints.
	require.NoError(sp.t, fingerPrintMonitor.WaitE())
	require.Equal(sp.t, srcFingerprint, destFingerprint)
}

func (sp *replicationTestSpec) main(ctx context.Context, t test.Test, c cluster.Cluster) {
	metricSnapper := sp.startStatsCollection(ctx)
	sp.preStreamingWorkload(ctx)

	t.L().Printf("begin workload on src cluster")
	m := sp.newMonitor(ctx)
	// The roachtest driver can use the workloadCtx to cancel the workload.
	workloadCtx, workloadCancel := context.WithCancel(ctx)
	defer workloadCancel()

	workloadDoneCh := make(chan struct{})
	m.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		err := sp.runWorkload(workloadCtx)
		// The workload should only return an error if the roachtest driver cancels the
		// workloadCtx after sp.additionalDuration has elapsed after the initial scan completes.
		if err != nil && workloadCtx.Err() == nil {
			// Implies the workload context was not cancelled and the workload cmd returned a
			// different error.
			return errors.Wrapf(err, `Workload context was not cancelled. Error returned by workload cmd`)
		}
		t.L().Printf("workload successfully finished")
		return nil
	})

	t.Status("starting replication stream")
	sp.metrics.initalScanStart = newMetricSnapshot(metricSnapper, timeutil.Now())
	ingestionJobID := sp.startReplicationStream(ctx)

	removeTenantRateLimiters(t, sp.setup.dst.sysSQL, sp.setup.dst.name)

	lv := makeLatencyVerifier("stream-ingestion", 0, 2*time.Minute, t.L(), getStreamIngestionJobInfo, t.Status, false)
	defer lv.maybeLogLatencyHist()

	m.Go(func(ctx context.Context) error {
		return lv.pollLatency(ctx, sp.setup.dst.db, ingestionJobID, time.Second, workloadDoneCh)
	})

	t.L().Printf("waiting for replication stream to finish ingesting initial scan")
	sp.waitForHighWatermark(ingestionJobID, sp.timeout/2)
	sp.metrics.initialScanEnd = newMetricSnapshot(metricSnapper, timeutil.Now())
	t.Status(fmt.Sprintf(`initial scan complete. run workload and repl. stream for another %s minutes`,
		sp.additionalDuration))

	select {
	case <-workloadDoneCh:
		t.L().Printf("workload finished on its own")
	case <-time.After(sp.getWorkloadTimeout()):
		workloadCancel()
		t.L().Printf("workload has cancelled after %s", sp.additionalDuration)
	case <-ctx.Done():
		t.L().Printf(`roachtest context cancelled while waiting for workload duration to complete`)
		return
	}
	var currentTime time.Time
	sp.setup.dst.sysSQL.QueryRow(sp.t, "SELECT clock_timestamp()").Scan(&currentTime)
	cutoverTime := currentTime.Add(-sp.cutover)
	sp.t.Status("cutover time chosen: ", cutoverTime.String())

	retainedTime := sp.getReplicationRetainedTime()

	sp.metrics.cutoverTo = newMetricSnapshot(metricSnapper, cutoverTime)
	sp.metrics.cutoverStart = newMetricSnapshot(metricSnapper, timeutil.Now())

	sp.t.Status(fmt.Sprintf("waiting for replication stream to cutover to %s",
		cutoverTime.String()))
	sp.stopReplicationStream(ingestionJobID, cutoverTime)
	sp.metrics.cutoverEnd = newMetricSnapshot(metricSnapper, timeutil.Now())

	sp.metrics.export(sp.t, len(sp.setup.src.nodes))

	t.Status("comparing fingerprints")
	sp.compareTenantFingerprintsAtTimestamp(
		ctx,
		retainedTime,
		cutoverTime,
	)
	lv.assertValid(t)
}

func c2cRegisterWrapper(
	r registry.Registry,
	sp replicationTestSpec,
	run func(ctx context.Context, t test.Test, c cluster.Cluster),
) {

	clusterOps := make([]spec.Option, 0)
	clusterOps = append(clusterOps, spec.CPU(sp.cpus))
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
		t.Fatal("acceptance tests should only run on a local cluster")
	}
	sp := replicationTestSpec{
		srcNodes: 1,
		dstNodes: 1,
		// The timeout field ensures the c2c roachtest driver behaves properly.
		timeout:            10 * time.Minute,
		workload:           replicateKV{readPercent: 0, debugRunDurationMinutes: 1},
		additionalDuration: 0 * time.Minute,
		cutover:            30 * time.Second,
	}
	sp.setupC2C(ctx, t, c)
	sp.main(ctx, t, c)
}

func registerClusterToCluster(r registry.Registry) {
	for _, sp := range []replicationTestSpec{
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
			workload:           replicateKV{readPercent: 0},
			timeout:            1 * time.Hour,
			additionalDuration: 10 * time.Minute,
			cutover:            5 * time.Minute,
		},
		{
			name:               "c2c/UnitTest",
			srcNodes:           1,
			dstNodes:           1,
			cpus:               4,
			pdSize:             10,
			workload:           replicateKV{readPercent: 0, debugRunDurationMinutes: 1},
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
		},
	} {
		sp := sp
		c2cRegisterWrapper(r, sp,
			func(ctx context.Context, t test.Test, c cluster.Cluster) {
				sp.setupC2C(ctx, t, c)

				m := c.NewMonitor(ctx)
				hc := roachtestutil.NewHealthChecker(t, c, sp.crdbNodes())
				m.Go(func(ctx context.Context) error {
					require.NoError(t, hc.Runner(ctx))
					return nil
				})
				defer hc.Done()

				sp.main(ctx, t, c)
			})
	}
}

type c2cPhase int

const (
	phaseInitialScan c2cPhase = iota
	steadyState
	phaseCutover
)

func (c c2cPhase) String() string {
	switch c {
	case phaseInitialScan:
		return "Initial Scan"
	case steadyState:
		return "Steady State"
	case phaseCutover:
		return "Cutover"
	default:
		return "unknown"
	}
}

type replResilienceSpec struct {
	onSrc         bool
	onCoordinator bool
	// phase indicates the c2c phase a node shutdown will occur.
	phase c2cPhase
	spec  *replicationTestSpec

	// the fields below are gathered after the replication stream has started
	srcJobID     jobspb.JobID
	dstJobID     jobspb.JobID
	shutdownNode int
	watcherNode  int
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
func (rsp *replResilienceSpec) getJobIDs(ctx context.Context) {
	setup := rsp.spec.setup
	jobIDQuery := `SELECT job_id FROM [SHOW JOBS] WHERE job_type = '%s'`
	testutils.SucceedsWithin(rsp.spec.t, func() error {
		if err := setup.dst.db.QueryRowContext(ctx, fmt.Sprintf(jobIDQuery,
			`STREAM INGESTION`)).Scan(&rsp.dstJobID); err != nil {
			return err
		}
		if err := setup.src.db.QueryRowContext(ctx, fmt.Sprintf(jobIDQuery,
			`STREAM REPLICATION`)).Scan(&rsp.srcJobID); err != nil {
			return err
		}
		return nil
	}, time.Minute)
}

func (rsp *replResilienceSpec) getTargetInfo() (*clusterInfo, jobspb.JobID, option.NodeListOption) {
	if rsp.onSrc {
		return rsp.spec.setup.src, rsp.srcJobID, rsp.spec.setup.src.nodes
	}
	return rsp.spec.setup.dst, rsp.dstJobID, rsp.spec.setup.dst.nodes
}

func (rsp *replResilienceSpec) getTargetAndWatcherNodes(ctx context.Context) {
	var coordinatorNode int
	info, jobID, nodes := rsp.getTargetInfo()

	// To populate the coordinator_id field, a node needs to claim the job.
	// Give the job system a minute.
	testutils.SucceedsWithin(rsp.spec.t, func() error {
		return info.db.QueryRowContext(ctx,
			`SELECT coordinator_id FROM crdb_internal.jobs WHERE job_id = $1`, jobID).Scan(&coordinatorNode)
	}, time.Minute)

	var targetNode int

	findAnotherNode := func(notThisNode int) int {
		for {
			anotherNode := nodes.RandNode()[0]
			if notThisNode != anotherNode {
				return anotherNode
			}
		}
	}
	if rsp.onCoordinator {
		targetNode = coordinatorNode
	} else {
		targetNode = findAnotherNode(coordinatorNode)
	}
	rsp.shutdownNode = targetNode
	rsp.watcherNode = findAnotherNode(targetNode)
}

func (rsp *replResilienceSpec) getPhase() c2cPhase {
	progress := getJobProgress(rsp.spec.t, rsp.spec.setup.dst.sysSQL, rsp.dstJobID)
	streamIngestProgress := progress.GetStreamIngest()
	highWater := progress.GetHighWater()

	if highWater == nil || highWater.IsEmpty() {
		return phaseInitialScan
	}
	if streamIngestProgress.CutoverTime.IsEmpty() {
		return steadyState
	}
	// TODO check that job has not complete
	return phaseCutover
}

func (rsp *replResilienceSpec) waitForTargetPhase() error {
	for {
		currentPhase := rsp.getPhase()
		rsp.spec.t.L().Printf("Current Phase %s", currentPhase.String())
		switch {
		case currentPhase < rsp.phase:
			time.Sleep(5 * time.Second)
		case currentPhase == rsp.phase:
			return nil
		default:
			return errors.New("c2c job past target phase")
		}
	}
}

type c2cResilienceKV struct {
	// gatewayNodeCh will contain the crdb node that should act as a gateway for the workload.
	// If 0 is sent, then all src cluster nodes can be gateway nodes.
	gatewayNodeCh chan int
}

func (rkv c2cResilienceKV) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	// TODO(msbutler): add an initial workload to test initial scan shutdown.
	return ""
}

func (rkv c2cResilienceKV) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	// added --tolerate-errors flags to prevent test from flaking due to a transaction retry error
	return fmt.Sprintf(`./workload run kv --tolerate-errors --init --read-percent 0 {pgurl%s:%s}`,
		nodes,
		tenantName)
}

func (rkv c2cResilienceKV) runDriver(
	workloadCtx context.Context, c cluster.Cluster, t test.Test, setup *c2cSetup,
) error {
	// The workload waits to begin until a watcher node is found after the c2c job
	// is set up. If a non-zero workload is received, only connect the workload to
	// that node, else connect workload to all nodes in the src cluster.
	gatewayNodes := setup.src.nodes
	if gatewayNodeOverride := <-rkv.gatewayNodeCh; gatewayNodeOverride != 0 {
		gatewayNodes = c.Node(gatewayNodeOverride)
	}
	t.L().Printf("Resilience Gateway Nodes Chosen: %s", gatewayNodes.String())
	return c.RunE(workloadCtx, setup.workloadNode, rkv.sourceRunCmd(setup.src.name, gatewayNodes))
}

func registerClusterReplicationResilience(r registry.Registry) {
	for _, rsp := range []replResilienceSpec{
		{
			onSrc:         true,
			onCoordinator: true,
			// TODO(msbutler): instead of hardcoding shutdowns to occcur during the main c2c phase,
			// randomly select a phase.
			phase: steadyState,
		},
		{
			onSrc:         true,
			onCoordinator: false,
			phase:         steadyState,
		},
		{
			onSrc:         false,
			onCoordinator: true,
			phase:         steadyState,
		},
		{
			onSrc:         false,
			onCoordinator: false,
			phase:         steadyState,
		},
	} {
		gatewayNodeCh := make(chan int)
		rsp := rsp
		rsp.spec = &replicationTestSpec{
			name:               rsp.name(),
			srcNodes:           4,
			dstNodes:           4,
			cpus:               8,
			workload:           c2cResilienceKV{gatewayNodeCh: gatewayNodeCh},
			timeout:            20 * time.Minute,
			additionalDuration: 5 * time.Minute,
			cutover:            4 * time.Minute,
			expectedNodeDeaths: 1,
		}

		c2cRegisterWrapper(r, *rsp.spec,
			func(ctx context.Context, t test.Test, c cluster.Cluster) {

				sp := rsp.spec
				sp.setupC2C(ctx, t, c)

				shutdownSetupDone := make(chan struct{})
				rsp.spec.replicationStartHook = func(ctx context.Context, sp *replicationTestSpec) {
					defer close(shutdownSetupDone)
					rsp.getJobIDs(ctx)
					rsp.getTargetAndWatcherNodes(ctx)

					// To prevent sql connections from connecting to the shutdown node,
					// ensure roachtest process connections to cluster use watcher node
					// from now on.
					watcherDB := c.Conn(ctx, sp.t.L(), rsp.watcherNode)
					watcherSQL := sqlutils.MakeSQLRunner(watcherDB)
					if rsp.onSrc {
						sp.setup.src.db = watcherDB
						sp.setup.src.sysSQL = watcherSQL

						// Only connect the foreground workload to the watcher node.
						gatewayNodeCh <- rsp.watcherNode

					} else {
						sp.setup.dst.db = watcherDB
						sp.setup.dst.sysSQL = watcherSQL

						// Indicates all src cluster nodes can connect to the workload.
						gatewayNodeCh <- 0
					}
				}
				m := sp.newMonitor(ctx)
				m.Go(func(ctx context.Context) error {
					// start the replication job
					sp.main(ctx, t, c)
					return nil
				})

				// Don't begin shutdown process until c2c job is set up.
				<-shutdownSetupDone

				shutdownStarter := func() jobStarter {
					return func(c cluster.Cluster, t test.Test) (string, error) {
						_, jobID, _ := rsp.getTargetInfo()
						return fmt.Sprintf("%d", jobID), rsp.waitForTargetPhase()
					}
				}
				shutdownCfg := nodeShutdownConfig{
					shutdownNode:    rsp.shutdownNode,
					watcherNode:     rsp.watcherNode,
					crdbNodes:       rsp.spec.crdbNodes(),
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
