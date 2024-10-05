// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"strconv"
	"strings"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
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

	// gatewayNodes identify the nodes that should remain available during the whole roachtest.
	gatewayNodes option.NodeListOption

	// nodes indicates the roachprod nodes running the cluster's nodes
	nodes option.NodeListOption
}

type c2cSetup struct {
	src *clusterInfo
	dst *clusterInfo

	// workloadNode identifies the node in the roachprod cluster that runs the workload.
	workloadNode option.NodeListOption

	promCfg *prometheus.Config
}

const maxExpectedLatencyDefault = 2 * time.Minute

const maxCutoverTimeoutDefault = 5 * time.Minute

var c2cPromMetrics = map[string]clusterstats.ClusterStat{
	"LogicalMegabytes": {
		LabelName: "node",
		Query:     "physical_replication_logical_bytes / 1e6"},
	"PhysicalMegabytes": {
		LabelName: "node",
		Query:     "physical_replication_sst_bytes / 1e6"},
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
	return c.RunE(workloadCtx, setup.workloadNode, workload.sourceRunCmd(setup.src.name, setup.src.gatewayNodes))
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

	// initWithSplitAndScatter splits and scatters the kv table after
	// initialization. This should be set for tests that init little or no data.
	//
	// The kv workload can run split and scatter after the insertion step. After
	// inserting a lot of kv table data (think multiple gb), however, kv has had
	// enough time to split and scatter that table across the cluster, so there
	// isn't a need to run an expensive scatter query.
	//
	// Initing a couple mb of kv table as other c2c tests do, however, does not
	// lead kv to naturally split and scatter the table by the time the c2c job
	// begins, which will lead the job to create a non-distributed initial dist
	// sql plan because the source side dsp.PartitionSpans may only assign one
	// node pair to stream all of the data.
	initWithSplitAndScatter bool

	// maxBlockBytes indicates the maximum size of the kv payload value written
	// during each insertion. The kv workload will randomly choose a value in the
	// interval [1,maxBlockSize] with equal probability, i.e. via
	// x~Uniform[1,maxBlockSize].
	maxBlockBytes int

	// maxQPS caps the queries per second sent to the source cluster after initialization.
	maxQPS int

	// partitionKVDatabaseInRegion constrains the kv database in the specified
	// region and asserts, before cutover, that the replicated span configuration
	// correctly enforces the regional constraint in the destination tenant.
	partitionKVDatabaseInRegion string

	// antiRegion is the region we do not expect any kv data to reside in if
	// partitionKVDatabaseInRegion is set.
	antiRegion string
}

func (kv replicateKV) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	cmd := roachtestutil.NewCommand(`./workload init kv`).
		MaybeFlag(kv.initRows > 0, "insert-count", kv.initRows).
		// Only set the max block byte values for the init command if we
		// actually need to insert rows.
		MaybeFlag(kv.initRows > 0, "max-block-bytes", kv.maxBlockBytes).
		MaybeFlag(kv.initWithSplitAndScatter, "splits", 100).
		MaybeOption(kv.initWithSplitAndScatter, "scatter").
		Arg("{pgurl%s:%s}", nodes, tenantName)
	return cmd.String()
}

func (kv replicateKV) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	cmd := roachtestutil.NewCommand(`./workload run kv`).
		Option("tolerate-errors").
		Flag("max-block-bytes", kv.maxBlockBytes).
		Flag("read-percent", kv.readPercent).
		MaybeFlag(kv.debugRunDuration > 0, "duration", kv.debugRunDuration).
		MaybeFlag(kv.maxQPS > 0, "max-rate", kv.maxQPS).
		Arg("{pgurl%s:%s}", nodes, tenantName)
	return cmd.String()
}

func (kv replicateKV) runDriver(
	workloadCtx context.Context, c cluster.Cluster, t test.Test, setup *c2cSetup,
) error {
	if kv.partitionKVDatabaseInRegion != "" {
		require.NotEqual(t, "", kv.antiRegion, "if partitionKVDatabaseInRegion is set, then antiRegion must be set")
		t.L().Printf("constrain the kv database to region %s", kv.partitionKVDatabaseInRegion)
		alterStmt := fmt.Sprintf("ALTER DATABASE kv CONFIGURE ZONE USING constraints = '[+region=%s]'", kv.partitionKVDatabaseInRegion)
		srcTenantConn := c.Conn(workloadCtx, t.L(), setup.src.nodes.RandNode()[0], option.VirtualClusterName(setup.src.name))
		srcTenantSQL := sqlutils.MakeSQLRunner(srcTenantConn)
		srcTenantSQL.Exec(t, alterStmt)
		defer kv.checkRegionalConstraints(t, setup, srcTenantSQL)
	}
	return defaultWorkloadDriver(workloadCtx, setup, c, kv)
}

// checkRegionalConstraints checks that the kv table is constrained to the
// expected locality.
func (kv replicateKV) checkRegionalConstraints(
	t test.Test, setup *c2cSetup, srcTenantSQL *sqlutils.SQLRunner,
) {

	var kvTableID uint32
	srcTenantSQL.QueryRow(t, `SELECT id FROM system.namespace WHERE name ='kv' AND "parentID" != 0`).Scan(&kvTableID)

	dstTenantCodec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(uint64(setup.dst.ID)))
	tablePrefix := dstTenantCodec.TablePrefix(kvTableID)
	t.L().Printf("Checking replica localities in destination side kv table, id %d and table prefix %s", kvTableID, tablePrefix)

	distinctQuery := fmt.Sprintf(`
SELECT
  DISTINCT replica_localities
FROM
  [SHOW CLUSTER RANGES]
WHERE
  start_key ~ '%s'
`, tablePrefix)

	res := setup.dst.sysSQL.QueryStr(t, distinctQuery)
	require.Equal(t, 1, len(res), "expected only one distinct locality")
	locality := res[0][0]
	require.Contains(t, locality, kv.partitionKVDatabaseInRegion)
	require.False(t, strings.Contains(locality, kv.antiRegion), "region %s is in locality %s", kv.antiRegion, locality)
}

type replicateBulkOps struct {
	// short uses less data during the import and rollback steps. Also only runs one rollback.
	short bool

	// debugSkipRollback skips all rollback steps during the test.
	debugSkipRollback bool
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
		short:             bo.short,
		debugSkipRollback: bo.debugSkipRollback,
		tenantName:        setup.src.name})
	return nil
}

// replicationSpec are inputs to a c2c roachtest set during roachtest
// registration, and can not be modified during roachtest execution.
type replicationSpec struct {
	// name specifies the name of the roachtest
	name string

	// whether this is a performance test
	benchmark bool

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

	// multiregion specifies multiregion cluster specs
	multiregion multiRegionSpecs

	// additionalDuration specifies how long the workload will run after the initial scan
	//completes. If the time out is set to 0, it will run until completion.
	additionalDuration time.Duration

	// cutover specifies how soon before the workload ends to choose a cutover timestamp.
	cutover time.Duration

	// timeout specifies when the roachtest should fail due to timeout.
	timeout time.Duration

	// cutoverTimeout specifies how long we expect cutover to take.
	cutoverTimeout time.Duration

	expectedNodeDeaths int32

	// maxLatency override the maxAcceptedLatencyDefault.
	maxAcceptedLatency time.Duration

	// TODO(msbutler): this knob only exists because the revision history
	// fingerprint can encounter a gc ttl error for large fingerprints. Delete
	// this knob once we lay a pts during fingerprinting.
	nonRevisionHistoryFingerprint bool

	// skipNodeDistributionCheck removes the requirement that multiple source and
	// destination nodes must participate in the replication stream. This should
	// be set if the roachtest runs on single node clusters or if the
	// roachtest completes before the auto replanner distributes the workload.
	skipNodeDistributionCheck bool

	// sometimesTestFingerprintMismatchCode will occasionally test the codepath
	// the roachtest takes if it detects a fingerprint mismatch.
	sometimesTestFingerprintMismatchCode bool

	// If non-empty, the test will be skipped with the supplied reason.
	skip string

	clouds registry.CloudSet
	suites registry.SuiteSet
}

type multiRegionSpecs struct {
	// srcLocalities specifies the zones each src node should live. The length of this array must match the number of src nodes.
	srcLocalities []string

	// destLocalities specifies the zones each src node should live. The length of this array must match the number of dest nodes.
	destLocalities []string

	// workloadNodeZone specifies the zone that the workload node should live
	workloadNodeZone string
}

// replicationDriver manages c2c roachtest execution.
type replicationDriver struct {
	rs replicationSpec

	// beforeWorkloadHook is called before the main workload begins.
	beforeWorkloadHook func()

	// cutoverStarted closes once the driver issues a cutover commmand.
	cutoverStarted chan struct{}

	// replicationStartHook is called as soon as the replication job begins.
	replicationStartHook func(ctx context.Context, sp *replicationDriver)

	setup   *c2cSetup
	t       test.Test
	c       cluster.Cluster
	metrics *c2cMetrics
	rng     *rand.Rand
}

func makeReplicationDriver(t test.Test, c cluster.Cluster, rs replicationSpec) *replicationDriver {
	rng, seed := randutil.NewPseudoRand()
	t.L().Printf(`Random Seed is %d`, seed)
	return &replicationDriver{
		t:   t,
		c:   c,
		rs:  rs,
		rng: rng,
	}
}

func (rd *replicationDriver) setupC2C(
	ctx context.Context, t test.Test, c cluster.Cluster,
) (cleanup func()) {
	if len(rd.rs.multiregion.srcLocalities) != 0 {
		nodeCount := rd.rs.srcNodes + rd.rs.dstNodes
		localityCount := len(rd.rs.multiregion.srcLocalities) + len(rd.rs.multiregion.destLocalities)
		require.Equal(t, nodeCount, localityCount)
		require.NotEqual(t, "", rd.rs.multiregion.workloadNodeZone)
	}

	srcCluster := c.Range(1, rd.rs.srcNodes)
	dstCluster := c.Range(rd.rs.srcNodes+1, rd.rs.srcNodes+rd.rs.dstNodes)
	workloadNode := c.Node(rd.rs.srcNodes + rd.rs.dstNodes + 1)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)

	// TODO(msbutler): allow for backups once this test stabilizes a bit more.
	srcStartOps := option.NewStartOpts(option.NoBackupSchedule)
	srcStartOps.RoachprodOpts.InitTarget = 1

	roachtestutil.SetDefaultAdminUIPort(c, &srcStartOps.RoachprodOpts)
	srcClusterSetting := install.MakeClusterSettings()
	c.Start(ctx, t.L(), srcStartOps, srcClusterSetting, srcCluster)

	// TODO(msbutler): allow for backups once this test stabilizes a bit more.
	dstStartOps := option.NewStartOpts(option.NoBackupSchedule)
	dstStartOps.RoachprodOpts.InitTarget = rd.rs.srcNodes + 1
	roachtestutil.SetDefaultAdminUIPort(c, &dstStartOps.RoachprodOpts)
	dstClusterSetting := install.MakeClusterSettings()
	c.Start(ctx, t.L(), dstStartOps, dstClusterSetting, dstCluster)

	srcNode := srcCluster.SeededRandNode(rd.rng)
	destNode := dstCluster.SeededRandNode(rd.rng)

	addr, err := c.ExternalPGUrl(ctx, t.L(), srcNode, roachprod.PGURLOptions{})
	t.L().Printf("Randomly chosen src node %d for gateway with address %s", srcNode, addr)
	t.L().Printf("Randomly chosen dst node %d for gateway", destNode)

	require.NoError(t, err)

	srcDB := c.Conn(ctx, t.L(), srcNode[0])
	srcSQL := sqlutils.MakeSQLRunner(srcDB)
	destDB := c.Conn(ctx, t.L(), destNode[0])
	destSQL := sqlutils.MakeSQLRunner(destDB)

	srcClusterSettings(t, srcSQL)
	destClusterSettings(t, destSQL, rd.rs.additionalDuration)

	createTenantAdminRole(t, "src-system", srcSQL)
	createTenantAdminRole(t, "dst-system", destSQL)

	srcTenantID, destTenantID := 2, 2
	srcTenantName := "src-tenant"
	destTenantName := "destination-tenant"

	createInMemoryTenant(ctx, t, c, srcTenantName, srcCluster, true)

	pgURL, err := copyPGCertsAndMakeURL(ctx, t, c, srcNode, srcClusterSetting.PGUrlCertsDir, addr[0])
	require.NoError(t, err)

	srcTenantInfo := clusterInfo{
		name:         srcTenantName,
		ID:           srcTenantID,
		pgURL:        pgURL,
		sysSQL:       srcSQL,
		db:           srcDB,
		gatewayNodes: srcCluster,
		nodes:        srcCluster}
	destTenantInfo := clusterInfo{
		name:         destTenantName,
		ID:           destTenantID,
		sysSQL:       destSQL,
		db:           destDB,
		gatewayNodes: dstCluster,
		nodes:        dstCluster}

	rd.setup = &c2cSetup{
		src:          &srcTenantInfo,
		dst:          &destTenantInfo,
		workloadNode: workloadNode,
	}

	rd.t = t
	rd.c = c
	rd.metrics = &c2cMetrics{}
	rd.replicationStartHook = func(ctx context.Context, sp *replicationDriver) {}
	rd.beforeWorkloadHook = func() {}
	rd.cutoverStarted = make(chan struct{})

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
	return func() {
		backgroundCtx := context.Background()
		if t.Failed() {
			// Use a new context to grab the debug zips, as the parent context may
			// have already been cancelled by the roachtest test infra.
			debugCtx, cancel := context.WithTimeout(backgroundCtx, time.Minute*5)
			defer cancel()
			rd.fetchDebugZip(debugCtx, rd.setup.src.nodes, "source_debug.zip")
			rd.fetchDebugZip(debugCtx, rd.setup.dst.nodes, "dest_debug.zip")
		}
		srcDB.Close()
		destDB.Close()
	}
}

func (rd *replicationDriver) fetchDebugZip(
	ctx context.Context, nodes option.NodeListOption, filename string,
) {
	if err := rd.c.FetchDebugZip(ctx, rd.t.L(), filename, nodes); err != nil {
		rd.t.L().Printf("Failed to download debug zip to %s from node %s", filename, nodes)
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
		rd.t.L().Printf("src cluster workload initialization took %s",
			timeutil.Since(initStart))
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

func (rd *replicationDriver) waitForReplicatedTime(ingestionJobID int, wait time.Duration) {
	testutils.SucceedsWithin(rd.t, func() error {
		info, err := getStreamIngestionJobInfo(rd.setup.dst.db, ingestionJobID)
		if err != nil {
			return err
		}
		if info.GetHighWater().IsZero() {
			return errors.New("no replicated time")
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
func (rd *replicationDriver) getReplicationRetainedTime() hlc.Timestamp {
	var retainedTime time.Time
	rd.setup.dst.sysSQL.QueryRow(rd.t,
		`SELECT retained_time FROM [SHOW TENANT $1 WITH REPLICATION STATUS]`,
		roachpb.TenantName(rd.setup.dst.name)).Scan(&retainedTime)
	return hlc.Timestamp{WallTime: retainedTime.UnixNano()}
}

func DecimalTimeToHLC(t test.Test, s string) hlc.Timestamp {
	d, _, err := apd.NewFromString(s)
	require.NoError(t, err)
	ts, err := hlc.DecimalToHLC(d)
	require.NoError(t, err)
	return ts
}

func (rd *replicationDriver) stopReplicationStream(
	ctx context.Context, ingestionJob int, cutoverTime time.Time,
) (actualCutoverTime hlc.Timestamp) {
	var cutoverStr string
	if cutoverTime.IsZero() {
		rd.setup.dst.sysSQL.QueryRow(rd.t, `ALTER TENANT $1 COMPLETE REPLICATION TO LATEST`,
			rd.setup.dst.name).Scan(&cutoverStr)
	} else {
		rd.setup.dst.sysSQL.QueryRow(rd.t, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
			rd.setup.dst.name, cutoverTime).Scan(&cutoverStr)
	}
	actualCutoverTime = DecimalTimeToHLC(rd.t, cutoverStr)
	close(rd.cutoverStarted)
	err := retry.ForDuration(rd.rs.cutoverTimeout, func() error {
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
	return actualCutoverTime
}

func (rd *replicationDriver) compareTenantFingerprintsAtTimestamp(
	ctx context.Context, startTime, endTime hlc.Timestamp,
) {
	rd.t.Status(fmt.Sprintf("comparing tenant fingerprints between start time %s and end time %s",
		startTime, endTime))
	fingerprintQuery := fmt.Sprintf(`
SELECT *
FROM crdb_internal.fingerprint(crdb_internal.tenant_span($1::INT), '%s'::DECIMAL, true)
AS OF SYSTEM TIME '%s'`, startTime.AsOfSystemTime(), endTime.AsOfSystemTime())

	if rd.rs.nonRevisionHistoryFingerprint {
		fingerprintQuery = fmt.Sprintf(`
SELECT *
FROM crdb_internal.fingerprint(crdb_internal.tenant_span($1::INT), 0::DECIMAL, false)
AS OF SYSTEM TIME '%s'`, endTime.AsOfSystemTime())
	}

	var srcFingerprint int64
	fingerPrintMonitor := rd.newMonitor(ctx)
	fingerPrintMonitor.Go(func(ctx context.Context) error {
		rd.setup.src.sysSQL.QueryRow(rd.t, fingerprintQuery, rd.setup.src.ID).Scan(&srcFingerprint)
		rd.t.L().Printf("finished fingerprinting source tenant")
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
	if srcFingerprint != destFingerprint {
		rd.onFingerprintMismatch(ctx, startTime, endTime)
	}
	require.Equal(rd.t, srcFingerprint, destFingerprint)
}

// onFingerprintMismatch runs table level fingerprints and backs up both tenants.
func (rd *replicationDriver) onFingerprintMismatch(
	ctx context.Context, startTime, endTime hlc.Timestamp,
) {
	rd.t.L().Printf("conducting table level fingerprints")
	srcTenantConn := rd.c.Conn(ctx, rd.t.L(), 1, option.VirtualClusterName(rd.setup.src.name))
	defer srcTenantConn.Close()
	dstTenantConn := rd.c.Conn(ctx, rd.t.L(), rd.rs.srcNodes+1, option.VirtualClusterName(rd.setup.dst.name))
	defer dstTenantConn.Close()
	fingerprintBisectErr := replicationutils.InvestigateFingerprints(ctx, srcTenantConn, dstTenantConn,
		startTime,
		endTime)
	// Before failing on this error, back up the source and destination tenants.
	if fingerprintBisectErr != nil {
		rd.t.L().Printf("fingerprint bisect error", fingerprintBisectErr)
	} else {
		rd.t.L().Printf("table level fingerprints seem to match")
	}
	rd.t.L().Printf("backing up src and destination tenants")
	fingerPrintMonitor := rd.newMonitor(ctx)

	fingerPrintMonitor.Go(func(ctx context.Context) error {
		return rd.backupAfterFingerprintMismatch(ctx, srcTenantConn, rd.setup.src.name, startTime, endTime)
	})
	fingerPrintMonitor.Go(func(ctx context.Context) error {
		return rd.backupAfterFingerprintMismatch(ctx, dstTenantConn, rd.setup.dst.name, startTime, endTime)
	})
	fingerprintMonitorError := fingerPrintMonitor.WaitE()
	require.NoError(rd.t, errors.CombineErrors(fingerprintBisectErr, fingerprintMonitorError))
}

// backupAfterFingerprintMismatch runs two backups on the provided tenant: a
// full backup AOST the provided startTime, and an incremental backup AOST the
// provided endTime. Both backups run with revision history.
func (rd *replicationDriver) backupAfterFingerprintMismatch(
	ctx context.Context, conn *gosql.DB, tenantName string, startTime, endTime hlc.Timestamp,
) error {
	if rd.c.IsLocal() {
		rd.t.L().Printf("skip taking backups of tenants on local roachtest run")
		return nil
	}
	prefix := "gs"
	if rd.c.Cloud() == spec.AWS {
		prefix = "s3"
	}
	collection := fmt.Sprintf("%s://%s/c2c-fingerprint-mismatch/%s/%s/%s?AUTH=implicit", prefix, testutils.BackupTestingBucketLongTTL(), rd.rs.name, rd.c.Name(), tenantName)
	fullBackupQuery := fmt.Sprintf("BACKUP INTO '%s' AS OF SYSTEM TIME '%s' with revision_history", collection, startTime.AsOfSystemTime())
	_, err := conn.ExecContext(ctx, fullBackupQuery)
	if err != nil {
		return errors.Wrapf(err, "full backup for %s failed", tenantName)
	}
	// The incremental backup for each tenant must match.
	incBackupQuery := fmt.Sprintf("BACKUP INTO LATEST IN '%s' AS OF SYSTEM TIME '%s' with revision_history", collection, endTime.AsOfSystemTime())
	_, err = conn.ExecContext(ctx, incBackupQuery)
	if err != nil {
		return errors.Wrapf(err, "inc backup for %s failed", tenantName)
	}
	return nil
}

// checkParticipatingNodes asserts that multiple nodes in the source and dest cluster are
// participating in the replication stream.
//
// Note: this isn't a strict requirement of all physical replication streams,
// rather we check this here because we expect a distributed physical
// replication stream in a healthy pair of multinode clusters.
func (rd *replicationDriver) checkParticipatingNodes(ingestionJobId int) {
	if rd.rs.skipNodeDistributionCheck {
		return
	}
	progress := getJobProgress(rd.t, rd.setup.dst.sysSQL, jobspb.JobID(ingestionJobId)).GetStreamIngest()
	require.Greater(rd.t, len(progress.StreamAddresses), 1, "only 1 src node participating")

	var destNodeCount int
	destNodes := make(map[int]struct{})
	for _, dstNode := range progress.PartitionProgress {
		dstNodeID := int(dstNode.DestSQLInstanceID)
		if _, ok := destNodes[dstNodeID]; !ok {
			destNodes[dstNodeID] = struct{}{}
			destNodeCount++
		}
	}
	require.Greater(rd.t, destNodeCount, 1, "only 1 dst node participating")
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

	// Pass a cancellable context to the workload monitor so the driver can cleanly cancel the
	// workload goroutine.
	workloadCtx, workloadCancel := context.WithCancel(ctx)
	workloadMonitor := rd.newMonitor(workloadCtx)
	defer func() {
		workloadCancel()
		workloadMonitor.Wait()
	}()

	workloadDoneCh := make(chan struct{})
	workloadMonitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		err := rd.runWorkload(ctx)
		// The workload should only return an error if the roachtest driver cancels the
		// ctx after the rd.additionalDuration has elapsed after the initial scan completes.
		if err != nil && ctx.Err() == nil {
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
	maxExpectedLatency := maxExpectedLatencyDefault
	if rd.rs.maxAcceptedLatency != 0 {
		maxExpectedLatency = rd.rs.maxAcceptedLatency
	}

	if rd.rs.cutoverTimeout == 0 {
		rd.rs.cutoverTimeout = maxCutoverTimeoutDefault
	}

	lv := makeLatencyVerifier("stream-ingestion", 0, maxExpectedLatency, rd.t.L(),
		getStreamIngestionJobInfo, rd.t.Status, rd.rs.expectedNodeDeaths > 0)
	defer lv.maybeLogLatencyHist()

	latencyMonitor := rd.newMonitor(ctx)
	latencyMonitor.Go(func(ctx context.Context) error {
		return lv.pollLatencyUntilJobSucceeds(ctx, rd.setup.dst.db, ingestionJobID, time.Second, workloadDoneCh)
	})
	defer latencyMonitor.Wait()

	rd.t.L().Printf("waiting for replication stream to finish ingesting initial scan")
	rd.waitForReplicatedTime(ingestionJobID, rd.rs.timeout/2)
	rd.metrics.initialScanEnd = newMetricSnapshot(metricSnapper, timeutil.Now())
	rd.t.Status(fmt.Sprintf(`initial scan complete. run workload and repl. stream for another %s minutes`,
		rd.rs.additionalDuration))

	select {
	case <-workloadDoneCh:
		rd.t.L().Printf("workload finished on its own")
	case <-time.After(rd.getWorkloadTimeout()):
		workloadCancel()
		rd.t.L().Printf("workload was cancelled after %s", rd.rs.additionalDuration)
	case <-ctx.Done():
		rd.t.L().Printf(`roachtest context cancelled while waiting for workload duration to complete`)
		return
	}

	rd.checkParticipatingNodes(ingestionJobID)

	retainedTime := rd.getReplicationRetainedTime()
	var cutoverTime time.Time
	cutoverTo := "LATEST"
	if rd.rs.cutover.Nanoseconds() != 0 {
		var currentTime time.Time
		rd.setup.dst.sysSQL.QueryRow(rd.t, "SELECT clock_timestamp()").Scan(&currentTime)
		cutoverTime = currentTime.Add(-rd.rs.cutover)
		rd.t.Status("cutover time chosen: ", cutoverTime.String())
		cutoverTo = cutoverTime.String()
		require.GreaterOrEqual(rd.t, cutoverTime, retainedTime.GoTime(),
			"cannot cutover to a time below the retained time (did the test already fail?)")
	}

	rd.metrics.cutoverStart = newMetricSnapshot(metricSnapper, timeutil.Now())

	rd.t.Status(fmt.Sprintf("waiting for replication stream to cutover to %s", cutoverTo))
	actualCutoverTime := rd.stopReplicationStream(ctx, ingestionJobID, cutoverTime)

	rd.metrics.cutoverTo = newMetricSnapshot(metricSnapper, actualCutoverTime.GoTime())
	rd.metrics.cutoverEnd = newMetricSnapshot(metricSnapper, timeutil.Now())

	rd.t.L().Printf("starting the destination tenant")
	conn := startInMemoryTenant(ctx, rd.t, rd.c, rd.setup.dst.name, rd.setup.dst.gatewayNodes)
	conn.Close()

	rd.metrics.export(rd.t, len(rd.setup.src.nodes))

	rd.t.Status("comparing fingerprints")
	rd.compareTenantFingerprintsAtTimestamp(
		ctx,
		retainedTime,
		actualCutoverTime,
	)
	if rd.rs.sometimesTestFingerprintMismatchCode && rd.rng.Intn(5) == 0 {
		rd.t.L().Printf("testing fingerprint mismatch path")
		rd.onFingerprintMismatch(ctx, retainedTime, actualCutoverTime)
	}
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

	if len(sp.multiregion.srcLocalities) > 0 {
		allZones := make([]string, 0, sp.srcNodes+sp.dstNodes+1)
		allZones = append(allZones, sp.multiregion.srcLocalities...)
		allZones = append(allZones, sp.multiregion.destLocalities...)
		allZones = append(allZones, sp.multiregion.workloadNodeZone)
		clusterOps = append(clusterOps, spec.GCEZones(strings.Join(allZones, ",")))
		clusterOps = append(clusterOps, spec.Geo())
	}

	r.Add(registry.TestSpec{
		Name:                      sp.name,
		Owner:                     registry.OwnerDisasterRecovery,
		Benchmark:                 sp.benchmark,
		Cluster:                   r.MakeClusterSpec(sp.dstNodes+sp.srcNodes+1, clusterOps...),
		Leases:                    registry.MetamorphicLeases,
		Timeout:                   sp.timeout,
		Skip:                      sp.skip,
		CompatibleClouds:          sp.clouds,
		Suites:                    sp.suites,
		TestSelectionOptOutSuites: sp.suites,
		RequiresLicense:           true,
		Run:                       run,
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
		timeout:                   10 * time.Minute,
		workload:                  replicateKV{readPercent: 0, debugRunDuration: 1 * time.Minute, maxBlockBytes: 1, initWithSplitAndScatter: true},
		additionalDuration:        0 * time.Minute,
		cutover:                   30 * time.Second,
		skipNodeDistributionCheck: true,
		clouds:                    registry.AllExceptAWS,
		suites:                    registry.Suites(registry.Nightly),
	}
	rd := makeReplicationDriver(t, c, sp)
	cleanup := rd.setupC2C(ctx, t, c)
	defer cleanup()

	// Spin up a monitor to capture any node deaths.
	m := rd.newMonitor(ctx)
	m.Go(func(ctx context.Context) error {
		rd.main(ctx)
		return nil
	})
	m.Wait()
}

func registerClusterToCluster(r registry.Registry) {
	for _, sp := range []replicationSpec{
		{
			// Cutover TO LATEST:
			name:      "c2c/tpcc/warehouses=500/duration=10/cutover=0",
			benchmark: true,
			srcNodes:  4,
			dstNodes:  4,
			cpus:      8,
			pdSize:    1000,
			// 500 warehouses adds 30 GB to source
			//
			// TODO(msbutler): increase default test to 1000 warehouses once fingerprinting
			// job speeds up.
			workload:           replicateTPCC{warehouses: 500},
			timeout:            1 * time.Hour,
			additionalDuration: 10 * time.Minute,
			cutover:            0,
			clouds:             registry.AllExceptAWS,
			suites:             registry.Suites(registry.Nightly),
		},
		{
			name:      "c2c/tpcc/warehouses=1000/duration=60/cutover=30",
			benchmark: true,
			srcNodes:  4,
			dstNodes:  4,
			cpus:      8,
			pdSize:    1000,
			// 500 warehouses adds 30 GB to source
			//
			// TODO(msbutler): increase default test to 1000 warehouses once fingerprinting
			// job speeds up.
			workload:           replicateTPCC{warehouses: 1000},
			timeout:            3 * time.Hour,
			additionalDuration: 60 * time.Minute,
			cutover:            30 * time.Minute,
			clouds:             registry.AllExceptAWS,
			suites:             registry.Suites(registry.Nightly),
		},
		{
			name:      "c2c/kv0",
			benchmark: true,
			srcNodes:  3,
			dstNodes:  3,
			cpus:      8,
			pdSize:    100,
			workload: replicateKV{
				readPercent:             0,
				maxBlockBytes:           1024,
				initWithSplitAndScatter: true,
			},
			timeout:                              1 * time.Hour,
			additionalDuration:                   10 * time.Minute,
			cutover:                              5 * time.Minute,
			sometimesTestFingerprintMismatchCode: true,
			clouds:                               registry.AllClouds,
			suites:                               registry.Suites(registry.Nightly),
		},
		{
			// Initial scan perf test.
			name:      "c2c/initialscan/kv0",
			benchmark: true,
			srcNodes:  4,
			dstNodes:  4,
			cpus:      8,
			// With the machine type and size we use, this is the smallest disk that
			// gives us max write BW of 800MB/s.
			pdSize: 1667,
			// Write ~50GB total (~12.5GB per node).
			workload:           replicateKV{readPercent: 0, initRows: 50000000, maxBlockBytes: 2048},
			timeout:            1 * time.Hour,
			additionalDuration: 5 * time.Minute,
			cutover:            0,
			clouds:             registry.AllExceptAWS,
			suites:             registry.Suites(registry.Nightly),
		},
		{
			// Large workload to test our 23.2 perf goals.
			name:                          "c2c/weekly/kv50",
			benchmark:                     true,
			srcNodes:                      8,
			dstNodes:                      8,
			cpus:                          8,
			pdSize:                        1000,
			nonRevisionHistoryFingerprint: true,

			workload: replicateKV{
				// Write a ~2TB initial scan.
				initRows:      350000000,
				readPercent:   50,
				maxBlockBytes: 4096,
				maxQPS:        2000,
			},
			timeout:            12 * time.Hour,
			additionalDuration: 2 * time.Hour,
			cutover:            0,
			clouds:             registry.AllClouds,
			suites:             registry.Suites(registry.Weekly),
		},
		{
			name:      "c2c/MultiRegion/SameRegions/kv0",
			benchmark: true,
			srcNodes:  4,
			dstNodes:  4,
			cpus:      8,
			pdSize:    100,
			workload: replicateKV{
				readPercent:                 0,
				maxBlockBytes:               1024,
				initWithSplitAndScatter:     true,
				partitionKVDatabaseInRegion: "us-west1",
				antiRegion:                  "us-central1",
			},
			timeout:            1 * time.Hour,
			additionalDuration: 10 * time.Minute,
			cutover:            1 * time.Minute,
			multiregion: multiRegionSpecs{
				// gcp specific
				srcLocalities:    []string{"us-west1-b", "us-west1-b", "us-west1-b", "us-central1-b"},
				destLocalities:   []string{"us-central1-b", "us-west1-b", "us-west1-b", "us-west1-b"},
				workloadNodeZone: "us-west1-b",
			},
			clouds: registry.OnlyGCE,
			suites: registry.Suites(registry.Nightly),
		},
		{
			name:     "c2c/UnitTest",
			srcNodes: 1,
			dstNodes: 1,
			cpus:     4,
			pdSize:   10,
			workload: replicateKV{
				readPercent:             0,
				debugRunDuration:        1 * time.Minute,
				initWithSplitAndScatter: true,
				maxBlockBytes:           1024},
			timeout:                   5 * time.Minute,
			additionalDuration:        0 * time.Minute,
			cutover:                   30 * time.Second,
			skipNodeDistributionCheck: true,
			skip:                      "for local ad hoc testing",
			clouds:                    registry.AllExceptAWS,
			suites:                    registry.Suites(registry.Nightly),
		},
		{
			name:               "c2c/BulkOps/full",
			srcNodes:           4,
			dstNodes:           4,
			cpus:               8,
			pdSize:             100,
			workload:           replicateBulkOps{},
			timeout:            2 * time.Hour,
			additionalDuration: 0,
			// Cutover currently takes around 4 minutes, perhaps because we need to
			// revert 10 GB of replicated data.
			//
			// TODO(msbutler): investigate further if cutover can be sped up.
			cutoverTimeout: 10 * time.Minute,
			cutover:        5 * time.Minute,
			// In a few ad hoc runs, the max latency hikes up to 27 minutes before lag
			// replanning and distributed catch up scans fix the poor initial plan. If
			// max accepted latency doubles, then there's likely a regression.
			maxAcceptedLatency: 1 * time.Hour,
			clouds:             registry.AllExceptAWS,
			suites:             registry.Suites(registry.Nightly),
			skip:               "flakes on 23.2. known limitation",
		},
		{
			name:     "c2c/BulkOps/short",
			srcNodes: 4,
			dstNodes: 4,
			cpus:     8,
			pdSize:   100,
			workload: replicateBulkOps{short: true},
			timeout:  2 * time.Hour,
			// Give the cluster plenty of time to catch up after the cutover command is issued.
			cutoverTimeout:     30 * time.Minute,
			additionalDuration: 0,
			cutover:            5 * time.Minute,
			maxAcceptedLatency: 1 * time.Hour,

			// skipNodeDistributionCheck is set to true because the roachtest
			// completes before the automatic replanner can run.
			skipNodeDistributionCheck: true,
			clouds:                    registry.AllExceptAWS,
			suites:                    registry.Suites(registry.Nightly),
			skip:                      "used for debugging when the full test fails",
		},
	} {
		sp := sp
		c2cRegisterWrapper(r, sp,
			func(ctx context.Context, t test.Test, c cluster.Cluster) {
				rd := makeReplicationDriver(t, c, sp)
				cleanup := rd.setupC2C(ctx, t, c)
				defer cleanup()
				// Spin up a monitor to capture any node deaths.
				m := rd.newMonitor(ctx)
				m.Go(func(ctx context.Context) error {
					rd.main(ctx)
					return nil
				})
				m.Wait()
			})
	}
}

type c2cPhase int

const (
	phaseNotReady c2cPhase = iota
	phaseInitialScan
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

// replShutdownSpec defines inputs to the replication node shutdown tests, set
// during roachtest registration. This can not be modified during roachtest
// execution.
type replShutdownSpec struct {
	replicationSpec

	onSrc         bool
	onCoordinator bool
}

func (rsp *replShutdownSpec) name() string {
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

// replShutdownDriver manages the execution of the replication node shutdown tests.
type replShutdownDriver struct {
	*replicationDriver
	rsp replShutdownSpec

	// phase indicates the c2c phase a node shutdown will occur.
	phase c2cPhase

	// the fields below are gathered after the replication stream has started
	srcJobID     jobspb.JobID
	dstJobID     jobspb.JobID
	shutdownNode int
	watcherNode  int
}

func makeReplShutdownDriver(
	t test.Test, c cluster.Cluster, rsp replShutdownSpec,
) replShutdownDriver {
	rd := makeReplicationDriver(t, c, rsp.replicationSpec)
	return replShutdownDriver{
		replicationDriver: rd,
		// choose either initialScan (1), SteadyState (2), or cutover (3)
		phase: c2cPhase(rd.rng.Intn(int(phaseCutover)) + 1),
		rsp:   rsp,
	}
}

func (rrd *replShutdownDriver) getJobIDs(ctx context.Context) {
	jobIDQuery := `SELECT job_id FROM [SHOW JOBS] WHERE job_type = '%s'`
	testutils.SucceedsWithin(rrd.t, func() error {
		if err := rrd.setup.dst.db.QueryRowContext(ctx, fmt.Sprintf(jobIDQuery,
			`REPLICATION STREAM INGESTION`)).Scan(&rrd.dstJobID); err != nil {
			return err
		}
		if err := rrd.setup.src.db.QueryRowContext(ctx, fmt.Sprintf(jobIDQuery,
			`REPLICATION STREAM PRODUCER`)).Scan(&rrd.srcJobID); err != nil {
			return err
		}
		return nil
	}, time.Minute)
}

func (rrd *replShutdownDriver) getTargetInfo() (*clusterInfo, jobspb.JobID, option.NodeListOption) {
	if rrd.rsp.onSrc {
		return rrd.setup.src, rrd.srcJobID, rrd.setup.src.nodes
	}
	return rrd.setup.dst, rrd.dstJobID, rrd.setup.dst.nodes
}

func (rrd *replShutdownDriver) getTargetAndWatcherNodes(ctx context.Context) {
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
	rrd.t.L().Printf("node %d is coordinator for target job %d", coordinatorNode, int(jobID))

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

func getPhase(rd *replicationDriver, dstJobID jobspb.JobID) c2cPhase {
	var jobStatus string
	rd.setup.dst.sysSQL.QueryRow(rd.t, `SELECT status FROM [SHOW JOBS] WHERE job_id=$1`,
		dstJobID).Scan(&jobStatus)
	require.Equal(rd.t, jobs.StatusRunning, jobs.Status(jobStatus))

	streamIngestProgress := getJobProgress(rd.t, rd.setup.dst.sysSQL, dstJobID).GetStreamIngest()

	if streamIngestProgress.ReplicatedTime.IsEmpty() {
		if len(streamIngestProgress.StreamAddresses) == 0 {
			return phaseNotReady
		}
		// Only return phaseInitialScan once all available stream addresses from the
		// source cluster have been persisted.
		return phaseInitialScan
	}
	if streamIngestProgress.CutoverTime.IsEmpty() {
		return phaseSteadyState
	}
	return phaseCutover
}

func waitForTargetPhase(
	ctx context.Context, rd *replicationDriver, dstJobID jobspb.JobID, targetPhase c2cPhase,
) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		currentPhase := getPhase(rd, dstJobID)
		rd.t.L().Printf("current Phase %s", currentPhase.String())
		select {
		case <-rd.cutoverStarted:
			require.Equal(rd.t, phaseCutover, getPhase(rd, dstJobID), "the replication job is not yet in the cutover phase")
			rd.t.L().Printf("cutover phase discovered via channel")
			return nil
		case <-ticker.C:
			switch {
			case currentPhase < targetPhase:
			case currentPhase == targetPhase:
				rd.t.L().Printf("In target phase %s", currentPhase.String())
				return nil
			default:
				return errors.New("c2c job past target phase")
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func sleepBeforeResiliencyEvent(rd *replicationDriver, phase c2cPhase) {
	// Assuming every C2C phase lasts at least 10 seconds, introduce some waiting
	// before a resiliency event (e.g. a node shutdown) to ensure the event occurs
	// once we're fully settled into the target phase (e.g. the stream ingestion
	// processors have observed the cutover signal).
	baseSleep := 1
	if phase == phaseCutover {
		// Cutover can sometimes be fast, so sleep for less time.
		baseSleep = 0
	}
	randomSleep := time.Duration(baseSleep+rd.rng.Intn(2)) * time.Second
	rd.t.L().Printf("Take a %s power nap", randomSleep)
	time.Sleep(randomSleep)
}

// getSrcDestNodePairs return list of src-dest node pairs that are directly connected to each
// other for the replication stream.
func getSrcDestNodePairs(rd *replicationDriver, progress *jobspb.StreamIngestionProgress) [][]int {
	nodePairs := make([][]int, 0)
	for srcID, progress := range progress.PartitionProgress {
		srcNode, err := strconv.Atoi(srcID)
		require.NoError(rd.t, err)

		// The destination cluster indexes nodes starting at 1,
		// but we need to record the roachprod node.
		dstNode := int(progress.DestSQLInstanceID) + rd.rs.srcNodes
		rd.t.L().Printf("Node Pair: Src %d; Dst %d ", srcNode, dstNode)
		nodePairs = append(nodePairs, []int{srcNode, dstNode})
	}
	return nodePairs
}

func registerClusterReplicationResilience(r registry.Registry) {
	for _, rsp := range []replShutdownSpec{
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
			name:                                 rsp.name(),
			srcNodes:                             4,
			dstNodes:                             4,
			cpus:                                 8,
			workload:                             replicateKV{readPercent: 0, initRows: 5000000, maxBlockBytes: 1024, initWithSplitAndScatter: true},
			timeout:                              20 * time.Minute,
			additionalDuration:                   6 * time.Minute,
			cutover:                              3 * time.Minute,
			expectedNodeDeaths:                   1,
			sometimesTestFingerprintMismatchCode: true,
			clouds:                               registry.AllExceptAWS,
			suites:                               registry.Suites(registry.Nightly),
		}

		c2cRegisterWrapper(r, rsp.replicationSpec,
			func(ctx context.Context, t test.Test, c cluster.Cluster) {

				rrd := makeReplShutdownDriver(t, c, rsp)
				rrd.t.L().Printf("Planning to shut down node during %s phase", rrd.phase)
				cleanup := rrd.setupC2C(ctx, t, c)
				defer cleanup()

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
						rd.setup.src.gatewayNodes = c.Node(rrd.watcherNode)
					} else {
						rd.setup.dst.db = watcherDB
						rd.setup.dst.sysSQL = watcherSQL
						rd.setup.dst.gatewayNodes = c.Node(rrd.watcherNode)
					}
					t.L().Printf(`%s configured: Shutdown Node %d; Watcher node %d; Gateway nodes %s`,
						rrd.rsp.name(), rrd.shutdownNode, rrd.watcherNode, rrd.setup.dst.gatewayNodes)
				}
				mainDriverCtx, cancelMain := context.WithCancel(ctx)
				mainMonitor := rrd.newMonitor(mainDriverCtx)
				mainMonitor.Go(func(ctx context.Context) error {
					rrd.main(ctx)
					return nil
				})
				defer cancelMain()
				defer mainMonitor.Wait()

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
					return func(c cluster.Cluster, l *logger.Logger) (jobspb.JobID, error) {
						if err := waitForTargetPhase(ctx, rrd.replicationDriver, rrd.dstJobID, rrd.phase); err != nil {
							return jobspb.JobID(0), err
						}
						sleepBeforeResiliencyEvent(rrd.replicationDriver, rrd.phase)
						return rrd.dstJobID, nil
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
					restartSettings: []install.ClusterSettingOption{},
					rng:             rrd.rng,
				}
				if err := executeNodeShutdown(ctx, t, c, shutdownCfg, shutdownStarter()); err != nil {
					cancelMain()
					t.Fatalf("shutdown execution failed: %s", err)
				}
			},
		)
	}
}

// registerClusterReplicationDisconnect tests that a physical replication stream
// succeeds if a source and destination node pair disconnects. When the pair
// disconnects, we expect its pg connection to time out, causing the job to
// retry. The job will recreate a topology, potentially with different src-node
// pairings. If the disconnected nodes no longer match in the new topology, the
// physical replication stream will make progress in the face of network
// partition. In the unlikely event that the two disconnected nodes continue to
// get paired together, the stream should catch up once the test driver
// reconnects the nodes.
func registerClusterReplicationDisconnect(r registry.Registry) {
	sp := replicationSpec{
		name:               "c2c/disconnect",
		srcNodes:           3,
		dstNodes:           3,
		cpus:               4,
		workload:           replicateKV{readPercent: 0, initRows: 1000000, maxBlockBytes: 1024, initWithSplitAndScatter: true},
		timeout:            20 * time.Minute,
		additionalDuration: 10 * time.Minute,
		cutover:            2 * time.Minute,
		maxAcceptedLatency: 12 * time.Minute,
		clouds:             registry.AllExceptAWS,
		suites:             registry.Suites(registry.Nightly),
	}
	c2cRegisterWrapper(r, sp, func(ctx context.Context, t test.Test, c cluster.Cluster) {
		rd := makeReplicationDriver(t, c, sp)
		cleanup := rd.setupC2C(ctx, t, c)
		defer cleanup()

		shutdownSetupDone := make(chan struct{})

		rd.replicationStartHook = func(ctx context.Context, rd *replicationDriver) {
			defer close(shutdownSetupDone)
		}
		m := rd.newMonitor(ctx)
		m.Go(func(ctx context.Context) error {
			rd.main(ctx)
			return nil
		})
		defer m.Wait()

		// Dont begin node disconnecion until c2c job is setup.
		<-shutdownSetupDone
		dstJobID := jobspb.JobID(getIngestionJobID(t, rd.setup.dst.sysSQL, rd.setup.dst.name))

		// TODO(msbutler): disconnect nodes during a random phase
		require.NoError(t, waitForTargetPhase(ctx, rd, dstJobID, phaseSteadyState))
		sleepBeforeResiliencyEvent(rd, phaseSteadyState)
		ingestionProgress := getJobProgress(t, rd.setup.dst.sysSQL, dstJobID).GetStreamIngest()

		srcDestConnections := getSrcDestNodePairs(rd, ingestionProgress)
		randomNodePair := srcDestConnections[rd.rng.Intn(len(srcDestConnections))]
		disconnectDuration := sp.additionalDuration
		rd.t.L().Printf("Disconnecting Src %d, Dest %d for %.2f minutes", randomNodePair[0],
			randomNodePair[1], disconnectDuration.Minutes())

		// Normally, the blackholeFailer is accessed through the failer interface,
		// at least in the failover tests. Because this test shouldn't use all the
		// failer interface calls (e.g. Setup(), and Ready()), we use the
		// blakholeFailer struct directly. In other words, in this test, we
		// shouldn't treat the blackholeFailer as an abstracted api.
		blackholeFailer := &blackholeFailer{t: rd.t, c: rd.c, input: true, output: true}
		blackholeFailer.FailPartial(ctx, randomNodePair[0], []int{randomNodePair[1]})

		time.Sleep(disconnectDuration)
		ingestionProgressUpdate := getJobProgress(t, rd.setup.dst.sysSQL, dstJobID).GetStreamIngest()

		// Calling this will log the latest topology.
		getSrcDestNodePairs(rd, ingestionProgressUpdate)
		blackholeFailer.Cleanup(ctx)
		rd.t.L().Printf("Nodes reconnected. C2C Job should eventually complete")
	})
}

func getIngestionJobID(t test.Test, dstSQL *sqlutils.SQLRunner, dstTenantName string) int {
	var tenantInfoBytes []byte
	var tenantInfo mtinfopb.ProtoInfo
	dstSQL.QueryRow(t, "SELECT info FROM system.tenants WHERE name=$1",
		dstTenantName).Scan(&tenantInfoBytes)
	require.NoError(t, protoutil.Unmarshal(tenantInfoBytes, &tenantInfo))
	return int(tenantInfo.PhysicalReplicationConsumerJobID)
}

type streamIngesitonJobInfo struct {
	status         string
	errMsg         string
	replicatedTime hlc.Timestamp
	finishedTime   time.Time
}

// GetHighWater returns the replicated time. The GetHighWater name is
// retained here as this is implementing the jobInfo interface used by
// the latency verifier.
func (c *streamIngesitonJobInfo) GetHighWater() time.Time {
	if c.replicatedTime.IsEmpty() {
		return time.Time{}
	}
	return c.replicatedTime.GoTime()
}
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
	return &streamIngesitonJobInfo{
		status:         status,
		errMsg:         payload.Error,
		replicatedTime: replicationutils.ReplicatedTimeFromProgress(&progress),
		finishedTime:   time.UnixMicro(payload.FinishedMicros),
	}, nil
}

func srcClusterSettings(t test.Test, db *sqlutils.SQLRunner) {
	db.ExecMultiple(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
}

func destClusterSettings(t test.Test, db *sqlutils.SQLRunner, additionalDuration time.Duration) {
	db.ExecMultiple(t, `SET CLUSTER SETTING cross_cluster_replication.enabled = true;`,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true;`,
		`SET CLUSTER SETTING stream_replication.replan_flow_threshold = 0.1;`,
		`SET CLUSTER SETTING physical_replication.consumer.node_lag_replanning_threshold = '5m';`)

	if additionalDuration != 0 {
		replanFrequency := additionalDuration / 2
		db.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING stream_replication.replan_flow_frequency = '%s'`,
			replanFrequency))
	}
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

	tmpDir, err := os.MkdirTemp("", install.CockroachNodeCertsDir)
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
	sslClientCert, err := os.ReadFile(filepath.Join(tmpDir, fmt.Sprintf("client.%s.crt", install.DefaultUser)))
	if err != nil {
		return "", err
	}
	sslClientKey, err := os.ReadFile(filepath.Join(tmpDir, fmt.Sprintf("client.%s.key", install.DefaultUser)))
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
