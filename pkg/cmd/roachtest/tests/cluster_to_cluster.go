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
	"strings"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
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
	pgURL *url.URL

	// db provides a connection to the system tenant
	db *gosql.DB

	// sql provides a sql connection to the system tenant
	sysSQL *sqlutils.SQLRunner

	// gatewayNodes identify the nodes that should remain available during the whole roachtest.
	gatewayNodes option.NodeListOption

	// nodes indicates the roachprod nodes running the cluster's nodes
	nodes option.NodeListOption
}

func (i *clusterInfo) PgURLForDatabase(database string) string {
	uri := *i.pgURL
	uri.Path = database
	return uri.String()
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
	return c.RunE(workloadCtx, option.WithNodes(setup.workloadNode), workload.sourceRunCmd(setup.src.name, setup.src.gatewayNodes))
}

type replicateTPCC struct {
	warehouses     int
	duration       time.Duration
	repairOrderIDs bool
	tolerateErrors bool
	readOnly       bool
}

func (tpcc replicateTPCC) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	cmd := roachtestutil.NewCommand(`./cockroach workload init tpcc`).
		Flag("data-loader", "import").
		Flag("warehouses", tpcc.warehouses).
		Arg("{pgurl%s:%s}", nodes, tenantName)
	return cmd.String()
}

func (tpcc replicateTPCC) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	cmd := roachtestutil.NewCommand(`./cockroach workload run tpcc`).
		Flag("warehouses", tpcc.warehouses).
		MaybeFlag(tpcc.duration > 0, "duration", tpcc.duration).
		MaybeOption(tpcc.tolerateErrors, "tolerate-errors").
		MaybeOption(tpcc.repairOrderIDs, "repair-order-ids").
		MaybeFlag(tpcc.readOnly, "mix", "newOrder=0,payment=0,orderStatus=1,delivery=0,stockLevel=1").
		Arg("{pgurl%s:%s}", nodes, tenantName).
		WithEqualsSyntax()
	return cmd.String()
}

func (tpcc replicateTPCC) runDriver(
	workloadCtx context.Context, c cluster.Cluster, t test.Test, setup *c2cSetup,
) error {
	return defaultWorkloadDriver(workloadCtx, setup, c, tpcc)
}

// replicateImportKV is a kv workload that runs the kv init step after the
// replication stream has started, inducing a bulk import catchup scan workload.
type replicateImportKV struct {
	replicateKV
	replicateSplits bool
}

func (ikv replicateImportKV) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	return ""
}

func (ikv replicateImportKV) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	return ikv.replicateKV.sourceInitCmd(tenantName, nodes)
}

func (ikv replicateImportKV) runDriver(
	workloadCtx context.Context, c cluster.Cluster, t test.Test, setup *c2cSetup,
) error {
	if ikv.replicateSplits {
		setup.dst.sysSQL.Exec(t, "SET CLUSTER SETTING physical_replication.consumer.ingest_split_event.enabled = true")
	}
	return defaultWorkloadDriver(workloadCtx, setup, c, ikv)
}

type replicateKV struct {
	readPercent int

	tolerateErrors bool

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

	// readOnly sets the prepare-read-only flag in the kv workload, which elides
	// preparing writing statements. This is necessary to get the workload running
	// properly on a read only standby tenant.
	readOnly bool
}

func (kv replicateKV) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	cmd := roachtestutil.NewCommand(`./cockroach workload init kv`).
		MaybeFlag(kv.initRows > 0, "insert-count", kv.initRows).
		// Only set the max block byte values for the init command if we
		// actually need to insert rows.
		MaybeFlag(kv.initRows > 0, "max-block-bytes", kv.maxBlockBytes).
		MaybeFlag(kv.initWithSplitAndScatter, "splits", 100).
		MaybeOption(kv.initWithSplitAndScatter, "scatter").
		Arg("{pgurl%s:%s}", nodes, tenantName).
		WithEqualsSyntax()
	return cmd.String()
}

func (kv replicateKV) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	cmd := roachtestutil.NewCommand(`./cockroach workload run kv`).
		MaybeOption(kv.tolerateErrors, "tolerate-errors").
		MaybeFlag(kv.maxBlockBytes > 0, "max-block-bytes", kv.maxBlockBytes).
		Flag("read-percent", kv.readPercent).
		MaybeFlag(kv.debugRunDuration > 0, "duration", kv.debugRunDuration).
		MaybeFlag(kv.maxQPS > 0, "max-rate", kv.maxQPS).
		MaybeFlag(kv.readOnly, "prepare-read-only", true).
		Arg("{pgurl%s:%s}", nodes, tenantName).
		WithEqualsSyntax()
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

	// withReaderOnlyWorkload creates a reader tenant that runs the given workload.
	withReaderWorkload streamingWorkload

	// overrideTenantTTL specifies the TTL that will be applied by the system tenant on
	// both the source and destination tenant range.
	overrideTenantTTL time.Duration

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
	beforeWorkloadHook func(ctx context.Context) error

	// cutoverStarted closes once the driver issues a cutover commmand.
	cutoverStarted chan struct{}

	// replicationStartHook is called as soon as the replication job begins.
	replicationStartHook func(ctx context.Context, sp *replicationDriver)

	setup   *c2cSetup
	t       test.Test
	c       cluster.Cluster
	metrics *c2cMetrics
	rng     *rand.Rand

	shutdownNode int
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
	workloadNode := c.WorkloadNode()

	// TODO(msbutler): allow for backups once this test stabilizes a bit more.
	srcStartOps := option.NewStartOpts(option.NoBackupSchedule, option.WithInitTarget(1))

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

	overrideSrcAndDestTenantTTL(t, srcSQL, destSQL, rd.rs.overrideTenantTTL)

	srcTenantID, destTenantID := 3, 3
	srcTenantName := "src-tenant"
	destTenantName := "destination-tenant"

	startOpts := option.StartSharedVirtualClusterOpts(srcTenantName, option.StorageCluster(srcCluster), option.NoBackupSchedule)
	c.StartServiceForVirtualCluster(ctx, t.L(), startOpts, srcClusterSetting)

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
	rd.beforeWorkloadHook = func(_ context.Context) error { return nil }
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
		rd.c.Run(ctx, option.WithNodes(rd.setup.workloadNode), initCmd)
		rd.t.L().Printf("src cluster workload initialization took %s",
			timeutil.Since(initStart))
	}
}

func (rd *replicationDriver) startReplicationStream(ctx context.Context) int {
	streamReplStmt := fmt.Sprintf("CREATE TENANT %q FROM REPLICATION OF %q ON '%s'",
		rd.setup.dst.name, rd.setup.src.name, rd.setup.src.pgURL.String())
	if rd.rs.withReaderWorkload != nil {
		streamReplStmt += " WITH READ VIRTUAL CLUSTER"
	}
	rd.setup.dst.sysSQL.Exec(rd.t, streamReplStmt)
	rd.replicationStartHook(ctx, rd)
	return getIngestionJobID(rd.t, rd.setup.dst.sysSQL, rd.setup.dst.name)
}

func (rd *replicationDriver) runWorkload(ctx context.Context) error {
	if err := rd.beforeWorkloadHook(ctx); err != nil {
		return err
	}
	return rd.rs.workload.runDriver(ctx, rd.c, rd.t, rd.setup)
}

func (rd *replicationDriver) waitForReplicatedTime(ingestionJobID int, wait time.Duration) {
	waitForReplicatedTime(rd.t, ingestionJobID, rd.setup.dst.db, getStreamIngestionJobInfo, wait)
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
		if jobs.State(status) == jobs.StateFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				rd.t.Fatalf("job failed: %s", payload.Error)
			}
			rd.t.Fatalf("job failed")
		}
		if e, a := jobs.StateSucceeded, jobs.State(status); e != a {
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
SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM VIRTUAL CLUSTER $1 WITH START TIMESTAMP = '%s'] AS OF SYSTEM TIME '%s'`,
		startTime.AsOfSystemTime(), endTime.AsOfSystemTime())

	var srcFingerprint int64
	fingerPrintMonitor := rd.newMonitor(ctx)
	fingerPrintMonitor.Go(func(ctx context.Context) error {
		rd.setup.src.sysSQL.QueryRow(rd.t, fingerprintQuery, rd.setup.src.name).Scan(&srcFingerprint)
		rd.t.L().Printf("finished fingerprinting source tenant")
		return nil
	})
	var destFingerprint int64
	fingerPrintMonitor.Go(func(ctx context.Context) error {
		// TODO(adityamaru): Measure and record fingerprinting throughput.
		rd.metrics.fingerprintingStart = timeutil.Now()
		rd.setup.dst.sysSQL.QueryRow(rd.t, fingerprintQuery, rd.setup.dst.name).Scan(&destFingerprint)
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
	if rd.c.Cloud() == spec.Azure {
		rd.t.L().Printf("skip taking backups of tenants on azure, bucket not configured yet")
		return nil
	}
	cloudPrefixes := map[spec.Cloud]string{
		spec.GCE:   "gs",
		spec.AWS:   "s3",
		spec.Azure: "azure",
	}
	if _, ok := cloudPrefixes[rd.c.Cloud()]; !ok {
		return errors.Errorf("backupAfterFingerprintMismatch: unsupported cloud")
	}
	prefix := cloudPrefixes[rd.c.Cloud()]

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

func (rd *replicationDriver) maybeRunReaderTenantWorkload(
	ctx context.Context, workloadMonitor cluster.Monitor,
) {
	if rd.rs.withReaderWorkload != nil {
		rd.t.Status("running reader tenant workload")
		readerTenantName := fmt.Sprintf("%s-readonly", rd.setup.dst.name)
		workloadMonitor.Go(func(ctx context.Context) error {
			err := rd.c.RunE(ctx, option.WithNodes(rd.setup.workloadNode), rd.rs.withReaderWorkload.sourceRunCmd(readerTenantName, rd.setup.dst.gatewayNodes))
			// The workload should only return an error if the roachtest driver cancels the
			// ctx after the rd.additionalDuration has elapsed after the initial scan completes.
			if err != nil && ctx.Err() == nil {
				// Implies the workload context was not cancelled and the workload cmd returned a
				// different error.
				return errors.Wrapf(err, `Workload context was not cancelled. Error returned by workload cmd`)
			}
			return nil
		})
	}
}

// checkParticipatingNodes asserts that multiple nodes in the source and dest cluster are
// participating in the replication stream.
//
// Note: this isn't a strict requirement of all physical replication streams,
// rather we check this here because we expect a distributed physical
// replication stream in a healthy pair of multinode clusters.
func (rd *replicationDriver) checkParticipatingNodes(ctx context.Context, ingestionJobId int) {
	if rd.rs.skipNodeDistributionCheck {
		return
	}

	destNodes := make(map[int]struct{})
	for _, src := range rd.setup.src.nodes {
		if rd.shutdownNode == src {
			continue
		}
		srcTenantSQL := sqlutils.MakeSQLRunner(rd.c.Conn(ctx, rd.t.L(), src))
		var dstNode int
		rows := srcTenantSQL.Query(rd.t, `select distinct split_part(consumer, '[', 1) from crdb_internal.cluster_replication_node_streams`)
		var streams int
		for rows.Next() {
			require.NoError(rd.t, rows.Scan(&dstNode))
			rd.t.L().Printf("stream on %d to %d", src, dstNode)
			streams++
			destNodes[dstNode] = struct{}{}
		}
		rd.t.L().Printf("%d streams on %d", streams, src)
		require.NoError(rd.t, rows.Err())
	}

	require.Greater(rd.t, len(destNodes), 1, "only 1 dst node participating")
}

func (rd *replicationDriver) main(ctx context.Context) {
	metricSnapper := rd.startStatsCollection(ctx)
	rd.preStreamingWorkload(ctx)

	// Wait for initial workload to be properly replicated across the source cluster to increase
	// the probability that the producer returns a topology with more than one node in it,
	// else the node shutdown tests can flake.
	if rd.rs.srcNodes >= 3 {
		require.NoError(rd.t, roachtestutil.WaitFor3XReplication(ctx, rd.t.L(), rd.setup.src.db))
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
	workloadErrCh := make(chan error, 1)
	workloadMonitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		err := rd.runWorkload(ctx)
		// The workload should only return an error if the roachtest driver cancels the
		// ctx after the rd.additionalDuration has elapsed after the initial scan completes.
		if err != nil && ctx.Err() == nil {
			// Implies the workload context was not cancelled and the workload cmd returned a
			// different error.
			rd.t.L().Printf("Workload context was not cancelled. Error returned by workload cmd: %s", err)
			workloadErrCh <- err
			return errors.Wrapf(err, `Workload context was not cancelled. Error returned by workload cmd`)
		}
		workloadErrCh <- nil
		rd.t.L().Printf("workload successfully finished")
		return nil
	})

	rd.t.Status("starting replication stream")
	rd.metrics.initalScanStart = newMetricSnapshot(metricSnapper, timeutil.Now())
	ingestionJobID := rd.startReplicationStream(ctx)
	rd.setup.dst.sysSQL.Exec(
		rd.t,
		`ALTER TENANT $1 GRANT CAPABILITY exempt_from_rate_limiting=true`,
		rd.setup.dst.name,
	)

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
		if err := lv.pollLatencyUntilJobSucceeds(ctx, rd.setup.dst.db, ingestionJobID, time.Second, workloadDoneCh); err != nil {
			// The latency poller may have failed because latency got too high. Grab a
			// debug zip before the replication jobs spin down.
			rd.fetchDebugZip(ctx, rd.setup.src.nodes, "latency_source_debug.zip")
			rd.fetchDebugZip(ctx, rd.setup.dst.nodes, "latency_dest_debug.zip")
			return err
		}
		return nil
	})
	defer latencyMonitor.Wait()

	rd.t.L().Printf("waiting for replication stream to finish ingesting initial scan")
	rd.waitForReplicatedTime(ingestionJobID, rd.rs.timeout/2)
	rd.metrics.initialScanEnd = newMetricSnapshot(metricSnapper, timeutil.Now())
	rd.t.Status(fmt.Sprintf(`initial scan complete. run workload and repl. stream for another %s minutes`,
		rd.rs.additionalDuration))

	rd.maybeRunReaderTenantWorkload(ctx, workloadMonitor)

	select {
	case <-workloadDoneCh:
		rd.t.L().Printf("workload finished on its own")
		if err := <-workloadErrCh; err != nil {
			rd.t.Fatal(err)
		}
	case <-time.After(rd.getWorkloadTimeout()):
		workloadCancel()
		rd.t.L().Printf("workload was cancelled after %s", rd.rs.additionalDuration)
	case <-ctx.Done():
		rd.t.L().Printf(`roachtest context cancelled while waiting for workload duration to complete`)
		return
	}

	rd.checkParticipatingNodes(ctx, ingestionJobID)

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
	startOpts := option.StartSharedVirtualClusterOpts(
		rd.setup.dst.name,
		option.StorageCluster(rd.setup.dst.gatewayNodes),
		option.WithInitTarget(rd.setup.dst.gatewayNodes[0]),
	)
	rd.c.StartServiceForVirtualCluster(ctx, rd.t.L(), startOpts, install.MakeClusterSettings())

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
	clusterOps = append(clusterOps, spec.WorkloadNode(), spec.WorkloadNodeCPU(sp.cpus))

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
		Run:                       run,
	})
}

func runAcceptanceClusterReplication(ctx context.Context, t test.Test, c cluster.Cluster) {
	sp := replicationSpec{
		srcNodes: 1,
		dstNodes: 1,
		// The timeout field ensures the c2c roachtest driver behaves properly.
		timeout:                   10 * time.Minute,
		workload:                  replicateKV{readPercent: 0, debugRunDuration: 1 * time.Minute, maxBlockBytes: 1, initWithSplitAndScatter: true, tolerateErrors: true},
		additionalDuration:        0 * time.Minute,
		cutover:                   30 * time.Second,
		skipNodeDistributionCheck: true,
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
			name:      "c2c/tpcc/warehouses=1000/duration=60/cutover=30",
			benchmark: true,
			srcNodes:  4,
			dstNodes:  4,
			cpus:      8,
			pdSize:    1000,

			workload:           replicateTPCC{warehouses: 1000, tolerateErrors: true},
			withReaderWorkload: replicateTPCC{warehouses: 500, readOnly: true, tolerateErrors: true},
			timeout:            3 * time.Hour,
			additionalDuration: 60 * time.Minute,
			cutover:            30 * time.Minute,
			clouds:             registry.OnlyGCE,
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
			withReaderWorkload:                   replicateKV{readPercent: 100, readOnly: true, tolerateErrors: true},
			sometimesTestFingerprintMismatchCode: true,
			clouds:                               registry.OnlyGCE,
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
			workload:           replicateKV{readPercent: 0, initRows: 50000000, maxBlockBytes: 2048, tolerateErrors: true},
			timeout:            1 * time.Hour,
			additionalDuration: 5 * time.Minute,
			cutover:            0,
			clouds:             registry.OnlyGCE,
			suites:             registry.Suites(registry.Nightly),
		},
		{
			// Catchup scan perf test on 7tb bulk import.
			name:      "c2c/import/7tb/kv0",
			benchmark: true,
			srcNodes:  10,
			dstNodes:  10,
			cpus:      8,
			pdSize:    2000,
			// Write ~7TB data to disk via Import -- takes a little over 1 hour.
			workload: replicateImportKV{
				replicateSplits: true,
				replicateKV:     replicateKV{readPercent: 0, initRows: 5000000000, maxBlockBytes: 1024, tolerateErrors: true}},
			timeout: 3 * time.Hour,
			// While replicating a bulk op, expect the max latency to be the runtime
			// of the bulk op.
			maxAcceptedLatency: 2 * time.Hour,
			// Cutover to one second after the import completes.
			cutover: -1 * time.Second,
			// After the cutover command begins, the destination cluster still needs
			// to catch up. Since we allow a max lag of 2 hours, it may take some time
			// to actually catch up after the bulk op succeeds. That being said, once
			// the import completes, we expect the replication stream to catch up
			// fairly quickly.
			cutoverTimeout: 30 * time.Minute,
			// Because PCR begins on a nearly empty cluster, skip the node distribution check.
			skipNodeDistributionCheck: true,
			clouds:                    registry.OnlyGCE,
			suites:                    registry.Suites(registry.Weekly),
		},
		{
			// Catchup scan perf test on bulk import.
			name:      "c2c/import/kv0",
			benchmark: true,
			srcNodes:  5,
			dstNodes:  5,
			cpus:      8,
			pdSize:    500,
			// Write ~1.2TB data to disk,takes a about 40 minutes.
			workload: replicateImportKV{
				replicateSplits: true,
				replicateKV:     replicateKV{readPercent: 0, initRows: 1000000000, maxBlockBytes: 1024}},
			timeout: 90 * time.Minute,
			// While replicating a bulk op, expect the max latency to be the runtime
			// of the bulk op.
			maxAcceptedLatency: 1 * time.Hour,
			// Cutover to one second after the import completes.
			cutover: -1 * time.Second,
			// After the cutover command begins, the destination cluster still needs
			// to catch up. Since we allow a max lag of 1 hour, it may take some time
			// to actually catch up after the bulk op succeeds. That being said, once
			// the import completes, we expect the replication stream to catch up
			// fairly quickly.
			cutoverTimeout: 20 * time.Minute,
			// Because PCR begins on a nearly empty cluster, skip the node
			// distribution check.
			skipNodeDistributionCheck: true,
			clouds:                    registry.OnlyGCE,
			suites:                    registry.Suites(registry.Weekly),
		},
		{
			// Large workload to test our 23.2 perf goals.
			name:      "c2c/weekly/kv50",
			benchmark: true,
			srcNodes:  8,
			dstNodes:  8,
			cpus:      8,
			pdSize:    1000,

			workload: replicateKV{
				// Write a ~2TB initial scan.
				initRows:       350000000,
				readPercent:    50,
				maxBlockBytes:  4096,
				maxQPS:         2000,
				tolerateErrors: true,
			},
			maxAcceptedLatency: time.Minute * 5,
			timeout:            12 * time.Hour,
			// We bump the TTL on the source and destination tenants to 12h to give
			// the fingerprinting post cutover adequate time to complete before GC
			// kicks in.
			overrideTenantTTL:  12 * time.Hour,
			additionalDuration: 2 * time.Hour,
			cutover:            0,
			clouds:             registry.OnlyGCE,
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
				tolerateErrors:              true,
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
				readPercent:             50,
				debugRunDuration:        10 * time.Minute,
				initWithSplitAndScatter: true,
				maxBlockBytes:           1024},
			timeout:                   30 * time.Minute,
			additionalDuration:        0 * time.Minute,
			cutover:                   30 * time.Second,
			skipNodeDistributionCheck: true,
			skip:                      "for local ad hoc testing",
			clouds:                    registry.OnlyGCE,
			suites:                    registry.Suites(registry.Nightly),
		},
		{
			name:               "c2c/BulkOps",
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
			cutoverTimeout: 20 * time.Minute,
			cutover:        5 * time.Minute,
			// In a few ad hoc runs, the max latency hikes up to 27 minutes before lag
			// replanning and distributed catch up scans fix the poor initial plan. If
			// max accepted latency doubles, then there's likely a regression.
			maxAcceptedLatency: 1 * time.Hour,
			// Skipping node distribution check because there is little data on the
			// source when the replication stream begins.
			skipNodeDistributionCheck: true,
			clouds:                    registry.OnlyGCE,
			suites:                    registry.Suites(registry.Nightly),
		},
		{
			name:               "c2c/BulkOps/singleImport",
			srcNodes:           4,
			dstNodes:           4,
			cpus:               8,
			pdSize:             100,
			workload:           replicateBulkOps{short: true, debugSkipRollback: true},
			timeout:            2 * time.Hour,
			cutoverTimeout:     1 * time.Hour,
			additionalDuration: 0,
			cutover:            1 * time.Minute,
			maxAcceptedLatency: 1 * time.Hour,

			// skipNodeDistributionCheck is set to true because the roachtest
			// completes before the automatic replanner can run.
			skipNodeDistributionCheck: true,
			clouds:                    registry.OnlyGCE,
			suites:                    registry.Suites(registry.Nightly),
			skip:                      "used for debugging when the full test fails",
		},
	} {
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
	srcJobID    jobspb.JobID
	dstJobID    jobspb.JobID
	watcherNode int
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
	rd.setup.dst.sysSQL.QueryRow(rd.t, `SELECT status FROM [SHOW JOB $1]`,
		dstJobID).Scan(&jobStatus)
	require.Equal(rd.t, jobs.StateRunning, jobs.State(jobStatus))

	streamIngestProgress := getJobProgress(rd.t, rd.setup.dst.sysSQL, dstJobID).GetStreamIngest()

	if streamIngestProgress.ReplicatedTime.IsEmpty() {
		if len(streamIngestProgress.PartitionConnUris) == 0 {
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
			workload:                             replicateKV{readPercent: 0, initRows: 5000000, maxBlockBytes: 1024, initWithSplitAndScatter: true, tolerateErrors: true},
			timeout:                              20 * time.Minute,
			additionalDuration:                   6 * time.Minute,
			cutover:                              3 * time.Minute,
			expectedNodeDeaths:                   1,
			sometimesTestFingerprintMismatchCode: true,
			// The job system can take up to 2 minutes to reclaim a job if the
			// coordinator dies, so increase the max expected latency to account for
			// our lovely job system.
			maxAcceptedLatency: 4 * time.Minute,
			clouds:             registry.OnlyGCE,
			suites:             registry.Suites(registry.Nightly),
		}

		c2cRegisterWrapper(r, rsp.replicationSpec,
			func(ctx context.Context, t test.Test, c cluster.Cluster) {

				rrd := makeReplShutdownDriver(t, c, rsp)
				rrd.t.L().Printf("Planning to shut down node during %s phase", rrd.phase)
				cleanup := rrd.setupC2C(ctx, t, c)
				defer cleanup()

				shutdownSetupDone := make(chan struct{})

				rrd.beforeWorkloadHook = func(ctx context.Context) error {
					// Ensure the workload begins after c2c jobs have been set up, or
					// return early if context was cancelled.
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-shutdownSetupDone:
						return nil
					}
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
				select {
				case <-shutdownSetupDone:
				case <-ctx.Done():
					return
				}

				// Eagerly listen to cutover signal to exercise node shutdown during actual cutover.
				rrd.setup.dst.sysSQL.Exec(t, `SET CLUSTER SETTING bulkio.stream_ingestion.failover_signal_poll_interval='5s'`)

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
		workload:           replicateKV{readPercent: 0, initRows: 1000000, maxBlockBytes: 1024, initWithSplitAndScatter: true, tolerateErrors: true},
		timeout:            20 * time.Minute,
		additionalDuration: 10 * time.Minute,
		cutover:            2 * time.Minute,
		maxAcceptedLatency: 12 * time.Minute,
		clouds:             registry.OnlyGCE,
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

		srcNode := rd.setup.src.nodes.RandNode()[0]
		srcTenantSQL := sqlutils.MakeSQLRunner(c.Conn(ctx, t.L(), srcNode))

		var dstNode int
		srcTenantSQL.QueryRow(t, `select split_part(consumer, '[', 1) from crdb_internal.cluster_replication_node_streams order by random() limit 1`).Scan(&dstNode)

		disconnectDuration := sp.additionalDuration
		rd.t.L().Printf("Disconnecting Src %d, Dest %d for %.2f minutes", srcNode,
			dstNode, disconnectDuration.Minutes())

		// Normally, the blackholeFailer is accessed through the failer interface,
		// at least in the failover tests. Because this test shouldn't use all the
		// failer interface calls (e.g. Setup(), and Ready()), we use the
		// blakholeFailer struct directly. In other words, in this test, we
		// shouldn't treat the blackholeFailer as an abstracted api.
		blackholeFailer := &blackholeFailer{t: rd.t, c: rd.c, input: true, output: true}
		blackholeFailer.FailPartial(ctx, srcNode, []int{dstNode})

		time.Sleep(disconnectDuration)
		// Calling this will log the latest topology.
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

type streamIngestionJobInfo struct {
	*jobRecord
}

// GetHighWater returns the replicated time. The GetHighWater name is
// retained here as this is implementing the jobInfo interface used by
// the latency verifier.
func (c *streamIngestionJobInfo) GetHighWater() time.Time {
	replicatedTime := replicationutils.ReplicatedTimeFromProgress(&c.progress)
	if replicatedTime.IsEmpty() {
		return time.Time{}
	}
	return replicatedTime.GoTime()
}

var _ jobInfo = (*streamIngestionJobInfo)(nil)

func getStreamIngestionJobInfo(db *gosql.DB, jobID int) (jobInfo, error) {
	jr, err := getJobRecord(db, jobID)
	if err != nil {
		return nil, err
	}
	return &streamIngestionJobInfo{jr}, nil
}

func srcClusterSettings(t test.Test, db *sqlutils.SQLRunner) {
	db.ExecMultiple(t,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true;`,
		`SET CLUSTER SETTING kv.lease.reject_on_leader_unknown.enabled = true;`,
	)
}

func destClusterSettings(t test.Test, db *sqlutils.SQLRunner, additionalDuration time.Duration) {
	db.ExecMultiple(t,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true;`,
		`SET CLUSTER SETTING kv.lease.reject_on_leader_unknown.enabled = true;`,
		`SET CLUSTER SETTING stream_replication.replan_flow_threshold = 0.1;`,
	)

	if additionalDuration != 0 {
		replanFrequency := additionalDuration / 2
		db.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING stream_replication.replan_flow_frequency = '%s'`,
			replanFrequency))
	}
}

func overrideSrcAndDestTenantTTL(
	t test.Test, srcSQL *sqlutils.SQLRunner, destSQL *sqlutils.SQLRunner, overrideTTL time.Duration,
) {
	if overrideTTL == 0 {
		return
	}
	t.L().Printf("overriding dest and src tenant TTL to %s", overrideTTL)
	srcSQL.Exec(t, `ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = $1`, overrideTTL.Seconds())
	destSQL.Exec(t, `ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = $1`, overrideTTL.Seconds())
}

func waitForReplicatedTimeToReachTimestamp(
	t testutils.TestFataler,
	jobID int,
	db *gosql.DB,
	jf jobFetcher,
	wait time.Duration,
	target time.Time,
) {
	testutils.SucceedsWithin(t, func() error {
		info, err := jf(db, jobID)
		if err != nil {
			return err
		}
		if info.GetHighWater().Compare(target) < 0 {
			return errors.Newf("replicated time %s not yet at %s", info.GetHighWater(), target)
		}
		return nil
	}, wait)
}

func waitForReplicatedTime(
	t testutils.TestFataler, jobID int, db *gosql.DB, jf jobFetcher, wait time.Duration,
) {
	testutils.SucceedsWithin(t, func() error {
		info, err := jf(db, jobID)
		if err != nil {
			return err
		}
		if info.GetHighWater().IsZero() {
			return errors.New("no replicated time")
		}
		return nil
	}, wait)
}

func copyPGCertsAndMakeURL(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	srcNode option.NodeListOption,
	pgURLDir string,
	urlString string,
) (*url.URL, error) {
	pgURL, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}

	tmpDir, err := os.MkdirTemp("", install.CockroachNodeCertsDir)
	if err != nil {
		return nil, err
	}
	func() { _ = os.RemoveAll(tmpDir) }()

	if err := c.Get(ctx, t.L(), pgURLDir, tmpDir, srcNode); err != nil {
		return nil, err
	}

	sslRootCert, err := os.ReadFile(filepath.Join(tmpDir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	sslClientCert, err := os.ReadFile(filepath.Join(tmpDir, fmt.Sprintf("client.%s.crt", install.DefaultUser)))
	if err != nil {
		return nil, err
	}
	sslClientKey, err := os.ReadFile(filepath.Join(tmpDir, fmt.Sprintf("client.%s.key", install.DefaultUser)))
	if err != nil {
		return nil, err
	}

	options := pgURL.Query()
	options.Set("sslmode", "verify-full")
	options.Set("sslinline", "true")
	options.Set("sslrootcert", string(sslRootCert))
	options.Set("sslcert", string(sslClientCert))
	options.Set("sslkey", string(sslClientKey))
	pgURL.RawQuery = options.Encode()
	return pgURL, nil
}
