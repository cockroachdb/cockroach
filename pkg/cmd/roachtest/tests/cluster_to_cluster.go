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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
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

	// pgurl is a connection string to the host cluster
	pgURL string

	// tenant provides a handler to the tenant
	tenant *tenantNode

	// db provides a connection to the host cluster
	db *gosql.DB

	// sql provides a sql connection to the host cluster
	sql *sqlutils.SQLRunner

	// sqlNode indicates the roachprod node running a single sql server
	sqlNode int

	// kvNodes indicates the roachprod nodes running the cluster's kv nodes
	kvNodes option.NodeListOption
}

type c2cSetup struct {
	src     clusterInfo
	dst     clusterInfo
	metrics c2cMetrics
}

// DiskUsageTracker can grab the disk usage of the provided cluster.
//
// TODO(msbutler): move DiskUsageTracker, exportedMetric,
// SizeTime and helper methods to an external package that all
// roachtests can use.
type DiskUsageTracker struct {
	c cluster.Cluster
	l *logger.Logger
}

// GetDiskUsage sums the disk usage for the given nodes in megabytes.
func (du *DiskUsageTracker) GetDiskUsage(ctx context.Context, nodes option.NodeListOption) int {
	var usage int
	for _, n := range nodes {
		cur, err := getDiskUsageInBytes(ctx, du.c, du.l, n)
		if err != nil {
			du.l.Printf("Unable to get disk usage for node %d", n)
			return 0
		}
		usage += cur
	}
	return usage / 1e6
}

func NewDiskUsageTracker(
	c cluster.Cluster, parentLogger *logger.Logger,
) (*DiskUsageTracker, error) {
	diskLogger, err := parentLogger.ChildLogger("disk-usage", logger.QuietStdout)
	if err != nil {
		return nil, err
	}
	return &DiskUsageTracker{c: c, l: diskLogger}, nil
}

// exportedMetric describes a measurement created in the roachtest process that will export to
// roachperf or a prom/grafana instance.
//
// TODO(msbutler): currently, the exported metrics are merely printed at end of
// the roachtest. Refactor these methods to play nice with a roachtest prom endpoint,
// once it exists.
type exportedMetric struct {
	metric float64
	unit   string
}

// newMetric creates a new exportedMetric
func newMetric(metric float64, unit string) exportedMetric {
	return exportedMetric{metric, unit}
}

func (em exportedMetric) String() string {
	return fmt.Sprintf("%.2f %s", em.metric, em.unit)
}

// sizeTime captures the disk size of the nodes at some moment in time
type sizeTime struct {
	// size is the megabytes of the objects
	size      int
	time      time.Time
	nodeCount int
}

func newSizeTime(ctx context.Context, du *DiskUsageTracker, nodes option.NodeListOption) sizeTime {
	return sizeTime{
		size:      du.GetDiskUsage(ctx, nodes),
		time:      timeutil.Now(),
		nodeCount: len(nodes),
	}
}

// diskDiffThroughput estimates throughput between two time intervals as mb/s/node by assuming
// that the total bytes written between the time intervals is diskUsage_End - diskUsage_Start.
func diskDiffThroughput(start sizeTime, end sizeTime) float64 {
	if start.nodeCount != end.nodeCount {
		panic("node count cannot change while measuring throughput")
	}
	return (float64(end.size-start.size) / end.time.Sub(start.time).Seconds()) / float64(start.nodeCount)
}

type c2cMetrics struct {
	start sizeTime

	initialScanEnd sizeTime

	cutoverStart sizeTime

	cutoverEnd sizeTime
}

func (m c2cMetrics) export() map[string]exportedMetric {
	metrics := map[string]exportedMetric{}

	populate := func(start sizeTime, end sizeTime, label string) {
		metrics[label+"Duration"] = newMetric(end.time.Sub(start.time).Minutes(), "Minutes")

		// Describes the cluster size difference between two timestamps.
		metrics[label+"Size"] = newMetric(float64(end.size-start.size), "MB")
		metrics[label+"Throughput"] = newMetric(diskDiffThroughput(start, end), "MB/S/Node")

	}
	if m.initialScanEnd.nodeCount != 0 {
		populate(m.start, m.initialScanEnd, "InitialScan")
	}

	if m.cutoverEnd.nodeCount != 0 {
		populate(m.cutoverStart, m.cutoverEnd, "Cutover")
	}
	return metrics
}

func setupC2C(
	ctx context.Context, t test.Test, c cluster.Cluster, srcKVNodes,
	dstKVNodes int,
) (*c2cSetup, func()) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	srcCluster := c.Range(1, srcKVNodes)
	dstCluster := c.Range(srcKVNodes+1, srcKVNodes+dstKVNodes)
	srcTenantNode := srcKVNodes + dstKVNodes + 1
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(srcTenantNode))

	srcStartOps := option.DefaultStartOpts()
	srcStartOps.RoachprodOpts.InitTarget = 1
	srcClusterSetting := install.MakeClusterSettings(install.SecureOption(true))
	c.Start(ctx, t.L(), srcStartOps, srcClusterSetting, srcCluster)

	dstStartOps := option.DefaultStartOpts()
	dstStartOps.RoachprodOpts.InitTarget = srcKVNodes + 1
	dstClusterSetting := install.MakeClusterSettings(install.SecureOption(true))
	c.Start(ctx, t.L(), dstStartOps, dstClusterSetting, dstCluster)

	srcNode := srcCluster.RandNode()
	destNode := dstCluster.RandNode()

	addr, err := c.ExternalPGUrl(ctx, t.L(), srcNode)
	require.NoError(t, err)

	srcDB := c.Conn(ctx, t.L(), srcNode[0])
	srcSQL := sqlutils.MakeSQLRunner(srcDB)
	destDB := c.Conn(ctx, t.L(), destNode[0])
	destSQL := sqlutils.MakeSQLRunner(destDB)

	srcClusterSettings(t, srcSQL)
	destClusterSettings(t, destSQL)

	srcTenantID, destTenantID := 2, 2
	srcTenantName := "src-tenant"
	destTenantName := "destination-tenant"
	srcSQL.Exec(t, fmt.Sprintf(`CREATE TENANT %q`, srcTenantName))

	pgURL, pgCleanup, err := copyPGCertsAndMakeURL(ctx, t, c, srcNode, dstCluster,
		srcClusterSetting.PGUrlCertsDir, addr[0])
	require.NoError(t, err)

	const (
		tenantHTTPPort = 8081
		tenantSQLPort  = 30258
	)
	t.Status("creating tenant node")
	srcTenant := createTenantNode(ctx, t, c, srcCluster, srcTenantID, srcTenantNode, tenantHTTPPort, tenantSQLPort)
	srcTenant.start(ctx, t, c, "./cockroach")
	cleanup := func() {
		srcTenant.stop(ctx, t, c)
		pgCleanup()
	}
	srcTenantInfo := clusterInfo{
		name:    srcTenantName,
		ID:      srcTenantID,
		pgURL:   pgURL,
		tenant:  srcTenant,
		sql:     srcSQL,
		db:      srcDB,
		sqlNode: srcTenantNode,
		kvNodes: srcCluster}
	destTenantInfo := clusterInfo{
		name:    destTenantName,
		ID:      destTenantID,
		sql:     destSQL,
		db:      destDB,
		kvNodes: dstCluster}

	// Currently, a tenant has by default a 10m RU burst limit, which can be
	// reached during these tests. To prevent RU limit throttling, add 10B RUs to
	// the tenant.
	srcTenantInfo.sql.Exec(t, `SELECT crdb_internal.update_tenant_resource_limits($1, 10000000000, 0,
10000000000, now(), 0);`, srcTenantInfo.ID)

	createSystemRole(t, srcTenantInfo.name, srcTenantInfo.sql)
	createSystemRole(t, destTenantInfo.name, destTenantInfo.sql)
	return &c2cSetup{
		src:     srcTenantInfo,
		dst:     destTenantInfo,
		metrics: c2cMetrics{}}, cleanup
}

// createSystemRole creates a role that can be used to log into the cluster's db console
func createSystemRole(t test.Test, name string, sql *sqlutils.SQLRunner) {
	username := "secure"
	password := "roach"
	sql.Exec(t, fmt.Sprintf(`CREATE ROLE %s WITH LOGIN PASSWORD '%s'`, username, password))
	sql.Exec(t, fmt.Sprintf(`GRANT ADMIN TO %s`, username))
	t.L().Printf(`Log into the %s system tenant db console with username "%s" and password "%s"`,
		name, username, password)
}

type streamingWorkload interface {
	// sourceInitCmd returns a command that will populate the src cluster with data before the
	// replication stream begins
	sourceInitCmd(pgURL string) string

	// sourceRunCmd returns a command that will run a workload for the given duration on the src
	// cluster during the replication stream.
	sourceRunCmd(pgURL string, duration time.Duration) string
}

type replicateTPCC struct {
	warehouses int
}

func (tpcc replicateTPCC) sourceInitCmd(pgURL string) string {
	return fmt.Sprintf(`./workload init tpcc --data-loader import --warehouses %d '%s'`,
		tpcc.warehouses, pgURL)
}

func (tpcc replicateTPCC) sourceRunCmd(pgURL string, duration time.Duration) string {
	// added --tolerate-errors flags to prevent test from flaking due to a transaction retry error
	return fmt.Sprintf(`./workload run tpcc --warehouses %d --duration %dm --tolerate-errors '%s'`,
		tpcc.warehouses, int(duration.Minutes()), pgURL)
}

type replicateKV struct {
	readPercent int
}

func (kv replicateKV) sourceInitCmd(pgURL string) string {
	return ""
}

func (kv replicateKV) sourceRunCmd(pgURL string, duration time.Duration) string {
	// added --tolerate-errors flags to prevent test from flaking due to a transaction retry error
	return fmt.Sprintf(`./workload run kv --tolerate-errors --init --duration %dm --read-percent %d '%s'`,
		int(duration.Minutes()),
		kv.readPercent,
		pgURL)
}

type replicationTestSpec struct {
	// name specifies the name of the roachtest
	name string

	// srcKVNodes is the number of kv nodes on the source cluster.
	srcKVNodes int

	// dstKVNodes is the number of kv nodes on the destination cluster.
	dstKVNodes int

	// cpus is the per node cpu count.
	cpus int

	// pdSize specifies the pd-ssd volume (in GB). If set to 0, local ssds are used.
	pdSize int

	// workload specifies the streaming workload.
	workload streamingWorkload

	// additionalDuration specifies how long the workload will run after the initial scan completes.
	additionalDuration time.Duration

	// cutover specifies how soon before the workload ends to choose a cutover timestamp.
	cutover time.Duration

	// timeout specifies when the roachtest should fail due to timeout.
	timeout time.Duration
}

func registerClusterToCluster(r registry.Registry) {
	for _, sp := range []replicationTestSpec{
		{
			name:       "c2c/tpcc",
			srcKVNodes: 4,
			dstKVNodes: 4,
			cpus:       8,
			pdSize:     1000,
			// 500 warehouses adds 30 GB to source
			//
			// TODO(msbutler): increase default test to 1000 warehouses once fingerprinting
			// job speeds up.
			workload:           replicateTPCC{warehouses: 500},
			timeout:            1 * time.Hour,
			additionalDuration: 10 * time.Minute,
			cutover:            5 * time.Minute,
		}, {
			name:               "c2c/kv0",
			srcKVNodes:         3,
			dstKVNodes:         3,
			cpus:               8,
			pdSize:             1000,
			workload:           replicateKV{readPercent: 0},
			timeout:            1 * time.Hour,
			additionalDuration: 10 * time.Minute,
			cutover:            5 * time.Minute,
		},
	} {
		clusterOps := make([]spec.Option, 0)
		clusterOps = append(clusterOps, spec.CPU(sp.cpus))
		if sp.pdSize != 0 {
			clusterOps = append(clusterOps, spec.VolumeSize(sp.pdSize))
		}

		r.Add(registry.TestSpec{
			Name:            sp.name,
			Owner:           registry.OwnerDisasterRecovery,
			Cluster:         r.MakeClusterSpec(sp.dstKVNodes+sp.srcKVNodes+1, clusterOps...),
			RequiresLicense: true,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {

				setup, cleanup := setupC2C(ctx, t, c, sp.srcKVNodes, sp.dstKVNodes)
				defer cleanup()
				m := c.NewMonitor(ctx, setup.src.kvNodes.Merge(setup.dst.kvNodes))
				du, err := NewDiskUsageTracker(c, t.L())
				require.NoError(t, err)
				var initDuration time.Duration
				if initCmd := sp.workload.sourceInitCmd(setup.src.tenant.secureURL()); initCmd != "" {
					t.Status("populating source cluster before replication")
					setup.metrics.start = newSizeTime(ctx, du, setup.src.kvNodes)
					c.Run(ctx, c.Node(setup.src.sqlNode), initCmd)
					setup.metrics.initialScanEnd = newSizeTime(ctx, du, setup.src.kvNodes)

					initDuration = setup.metrics.initialScanEnd.time.Sub(setup.metrics.start.time)
					t.L().Printf("src cluster workload initialization took %d minutes", int(initDuration.Minutes()))
				}

				t.Status("starting replication stream")
				streamReplStmt := fmt.Sprintf("CREATE TENANT %q FROM REPLICATION OF %q ON '%s'",
					setup.dst.name, setup.src.name, setup.src.pgURL)
				var ingestionJobID, streamProducerJobID int
				setup.dst.sql.QueryRow(t, streamReplStmt).Scan(&ingestionJobID, &streamProducerJobID)

				// The replication stream is expected to spend some time conducting an
				// initial scan, ideally on the same order as the `initDuration`, the
				// time taken to initially populate the source cluster. To ensure the
				// latency verifier stabilizes, ensure the workload and the replication
				// stream run for a significant amount of time after the initial scan
				// ends. Explicitly, set the workload to run for the estimated initial scan
				// runtime + the user specified workload duration.
				workloadDuration := initDuration + sp.additionalDuration
				workloadDoneCh := make(chan struct{})
				m.Go(func(ctx context.Context) error {
					defer close(workloadDoneCh)
					cmd := sp.workload.sourceRunCmd(setup.src.tenant.secureURL(), workloadDuration)
					c.Run(ctx, c.Node(setup.src.sqlNode), cmd)
					return nil
				})

				cutoverTime := chooseCutover(t, setup.dst.sql, workloadDuration, sp.cutover)
				t.Status("cutover time chosen: %s", cutoverTime.String())

				// TODO(ssd): The job doesn't record the initial
				// statement time, so we can't correctly measure the
				// initial scan time here.
				lv := makeLatencyVerifier("stream-ingestion", 0, 2*time.Minute, t.L(), getStreamIngestionJobInfo, t.Status, false)
				defer lv.maybeLogLatencyHist()

				m.Go(func(ctx context.Context) error {
					return lv.pollLatency(ctx, setup.dst.db, ingestionJobID, time.Second, workloadDoneCh)
				})

				t.Status("waiting for replication stream to finish ingesting initial scan")
				waitForHighWatermark(t, setup.dst.db, ingestionJobID, sp.timeout/2)

				t.Status("waiting for src cluster workload to complete")
				m.Wait()

				t.Status("waiting for replication stream to cutover")
				setup.metrics.cutoverStart = newSizeTime(ctx, du, setup.dst.kvNodes)
				stopReplicationStream(t, setup.dst.sql, ingestionJobID, cutoverTime)
				setup.metrics.cutoverEnd = newSizeTime(ctx, du, setup.dst.kvNodes)

				t.Status("comparing fingerprints")
				// Currently, it takes about 15 minutes to generate a fingerprint for
				// about 30 GB of data. Once the fingerprinting job is used instead,
				// this should only take about 5 seconds for the same amount of data. At
				// that point, we should increase the number of warehouses in this test.
				//
				// The new fingerprinting job currently OOMs this test. Once it becomes
				// more efficient, it will be used.
				compareTenantFingerprintsAtTimestamp(
					t,
					m,
					setup,
					hlc.Timestamp{WallTime: cutoverTime.UnixNano()})
				lv.assertValid(t)

				// TODO(msbutler): export metrics to roachperf or prom/grafana
				exportedMetrics := setup.metrics.export()
				for key, metric := range exportedMetrics {
					t.L().Printf("%s: %s", key, metric.String())
				}
			},
		})
	}
}

func chooseCutover(
	t test.Test, dstSQL *sqlutils.SQLRunner, workloadDuration time.Duration, cutover time.Duration,
) time.Time {
	var currentTime time.Time
	dstSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&currentTime)
	return currentTime.Add(workloadDuration - cutover)
}

func compareTenantFingerprintsAtTimestamp(
	t test.Test, m cluster.Monitor, setup *c2cSetup, ts hlc.Timestamp,
) {
	fingerprintQuery := fmt.Sprintf(`
SELECT
    xor_agg(
        fnv64(crdb_internal.trim_tenant_prefix(key),
              substring(value from 5))
    ) AS fingerprint
FROM crdb_internal.scan(crdb_internal.tenant_span($1::INT))
AS OF SYSTEM TIME '%s'`, ts.AsOfSystemTime())

	var srcFingerprint int64
	m.Go(func(ctx context.Context) error {
		setup.src.sql.QueryRow(t, fingerprintQuery, setup.src.ID).Scan(&srcFingerprint)
		return nil
	})
	var destFingerprint int64
	setup.dst.sql.QueryRow(t, fingerprintQuery, setup.dst.ID).Scan(&destFingerprint)

	// If the goroutine gets cancelled or fataled, return before comparing fingerprints.
	require.NoError(t, m.WaitE())
	require.Equal(t, srcFingerprint, destFingerprint)
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
		`SELECT status, payload, progress FROM system.jobs WHERE id = $1`, jobID,
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

func waitForHighWatermark(t test.Test, db *gosql.DB, ingestionJobID int, wait time.Duration) {
	testutils.SucceedsWithin(t, func() error {
		info, err := getStreamIngestionJobInfo(db, ingestionJobID)
		if err != nil {
			return err
		}
		if info.GetHighWater().IsZero() {
			return errors.New("no high watermark")
		}
		return nil
	}, wait)
}

func stopReplicationStream(
	t test.Test, destSQL *sqlutils.SQLRunner, ingestionJob int, cutoverTime time.Time,
) {
	destSQL.Exec(t, `SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`, ingestionJob, cutoverTime)
	err := retry.ForDuration(time.Minute*5, func() error {
		var status string
		var payloadBytes []byte
		destSQL.QueryRow(t, `SELECT status, payload FROM system.jobs WHERE id = $1`,
			ingestionJob).Scan(&status, &payloadBytes)
		if jobs.Status(status) == jobs.StatusFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				t.Fatalf("job failed: %s", payload.Error)
			}
			t.Fatalf("job failed")
		}
		if e, a := jobs.StatusSucceeded, jobs.Status(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	})
	require.NoError(t, err)
}

func srcClusterSettings(t test.Test, db *sqlutils.SQLRunner) {
	db.ExecMultiple(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
}

func destClusterSettings(t test.Test, db *sqlutils.SQLRunner) {
	db.ExecMultiple(t, `SET enable_experimental_stream_replication = true;`)
}

func copyPGCertsAndMakeURL(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	srcNode option.NodeListOption,
	destNode option.NodeListOption,
	pgURLDir string,
	urlString string,
) (string, func(), error) {
	cleanup := func() {}
	pgURL, err := url.Parse(urlString)
	if err != nil {
		return "", cleanup, err
	}

	tmpDir, err := os.MkdirTemp("", "certs")
	if err != nil {
		return "", cleanup, err
	}
	cleanup = func() { _ = os.RemoveAll(tmpDir) }

	if err := c.Get(ctx, t.L(), pgURLDir, tmpDir, srcNode); err != nil {
		return "", cleanup, err
	}
	if err := c.PutE(ctx, t.L(), tmpDir, "./src-cluster-certs", destNode); err != nil {
		return "", cleanup, err
	}

	dir := "."
	if c.IsLocal() {
		dir = filepath.Join(local.VMDir(c.Name(), destNode[0]), "src-cluster-certs")
	} else {
		dir = "./src-cluster-certs/certs"
	}
	options := pgURL.Query()
	options.Set("sslmode", "verify-full")
	options.Set("sslrootcert", filepath.Join(dir, "ca.crt"))
	options.Set("sslcert", filepath.Join(dir, "client.root.crt"))
	options.Set("sslkey", filepath.Join(dir, "client.root.key"))
	pgURL.RawQuery = options.Encode()
	return pgURL.String(), cleanup, err
}
