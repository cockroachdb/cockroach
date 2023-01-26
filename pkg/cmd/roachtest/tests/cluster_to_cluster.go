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
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
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

	// sql provides a sql connection to the host cluster
	sql *sqlutils.SQLRunner

	// nodes indicates the roachprod nodes running the cluster's nodes
	nodes option.NodeListOption
}

type c2cSetup struct {
	src          clusterInfo
	dst          clusterInfo
	workloadNode option.NodeListOption
	metrics      c2cMetrics
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

func (em exportedMetric) StringWithUnits() string {
	return fmt.Sprintf("%.2f %s", em.metric, em.unit)
}

func (em exportedMetric) String() string {
	return fmt.Sprintf("%.2f", em.metric)
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

	fingerprintingStart time.Time

	fingerprintingEnd time.Time
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
) *c2cSetup {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	srcCluster := c.Range(1, srcKVNodes)
	dstCluster := c.Range(srcKVNodes+1, srcKVNodes+dstKVNodes)
	workloadNode := c.Node(srcKVNodes + dstKVNodes + 1)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)

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
		name:  srcTenantName,
		ID:    srcTenantID,
		pgURL: pgURL,
		sql:   srcSQL,
		db:    srcDB,
		nodes: srcCluster}
	destTenantInfo := clusterInfo{
		name:  destTenantName,
		ID:    destTenantID,
		sql:   destSQL,
		db:    destDB,
		nodes: dstCluster}

	return &c2cSetup{
		src:          srcTenantInfo,
		dst:          destTenantInfo,
		workloadNode: workloadNode,
		metrics:      c2cMetrics{}}
}

type streamingWorkload interface {
	// sourceInitCmd returns a command that will populate the src cluster with data before the
	// replication stream begins
	sourceInitCmd(tenantName string, nodes option.NodeListOption) string

	// sourceRunCmd returns a command that will run a workload
	sourceRunCmd(tenantName string, nodes option.NodeListOption) string
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

type replicateKV struct {
	readPercent int
}

func (kv replicateKV) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	return ""
}

func (kv replicateKV) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	// added --tolerate-errors flags to prevent test from flaking due to a transaction retry error
	return fmt.Sprintf(`./workload run kv --tolerate-errors --init --read-percent %d {pgurl%s:%s}`,
		kv.readPercent,
		nodes,
		tenantName)
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
	} {
		sp := sp
		clusterOps := make([]spec.Option, 0)
		clusterOps = append(clusterOps, spec.CPU(sp.cpus))
		if sp.pdSize != 0 {
			clusterOps = append(clusterOps, spec.VolumeSize(sp.pdSize))
		}

		r.Add(registry.TestSpec{
			Name:            sp.name,
			Owner:           registry.OwnerDisasterRecovery,
			Cluster:         r.MakeClusterSpec(sp.dstNodes+sp.srcNodes+1, clusterOps...),
			Timeout:         sp.timeout,
			RequiresLicense: true,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				setup := setupC2C(ctx, t, c, sp.srcNodes, sp.dstNodes)
				m := c.NewMonitor(ctx, setup.src.nodes.Merge(setup.dst.nodes))
				du, err := NewDiskUsageTracker(c, t.L())
				require.NoError(t, err)
				var initDuration time.Duration
				if initCmd := sp.workload.sourceInitCmd(setup.src.name, setup.src.nodes); initCmd != "" {
					t.Status("populating source cluster before replication")
					setup.metrics.start = newSizeTime(ctx, du, setup.src.nodes)
					c.Run(ctx, setup.workloadNode, initCmd)
					setup.metrics.initialScanEnd = newSizeTime(ctx, du, setup.src.nodes)

					initDuration = setup.metrics.initialScanEnd.time.Sub(setup.metrics.start.time)
					t.L().Printf("src cluster workload initialization took %s minutes", initDuration)
				}

				t.L().Printf("begin workload on src cluster")
				workloadCtx, workloadCancel := context.WithCancel(ctx)
				defer workloadCancel()

				workloadDoneCh := make(chan struct{})
				m.Go(func(ctx context.Context) error {
					err := c.RunE(workloadCtx, setup.workloadNode,
						sp.workload.sourceRunCmd(setup.src.name, setup.src.nodes))
					// The workload should only stop if the workloadCtx is cancelled once
					// sp.additionalDuration has elapsed after the initial scan completes.
					if workloadCtx.Err() == nil {
						// Implies the workload context was not cancelled and the workload cmd returned on
						// its own.
						return errors.Wrapf(err, `Workload context was not cancelled. Error returned by workload cmd`)
					}
					workloadDoneCh <- struct{}{}
					return nil
				})

				t.Status("starting replication stream")
				streamReplStmt := fmt.Sprintf("CREATE TENANT %q FROM REPLICATION OF %q ON '%s'",
					setup.dst.name, setup.src.name, setup.src.pgURL)
				setup.dst.sql.Exec(t, streamReplStmt)
				ingestionJobID := getIngestionJobID(t, setup.dst.sql, setup.dst.name)

				// TODO(ssd): The job doesn't record the initial
				// statement time, so we can't correctly measure the
				// initial scan time here.
				lv := makeLatencyVerifier("stream-ingestion", 0, 2*time.Minute, t.L(), getStreamIngestionJobInfo, t.Status, false)
				defer lv.maybeLogLatencyHist()

				m.Go(func(ctx context.Context) error {
					return lv.pollLatency(ctx, setup.dst.db, ingestionJobID, time.Second, workloadDoneCh)
				})

				t.L().Printf("waiting for replication stream to finish ingesting initial scan")
				waitForHighWatermark(t, setup.dst.db, ingestionJobID, sp.timeout/2)

				t.Status(fmt.Sprintf(`initial scan complete. run workload and repl. stream for another %s minutes`,
					sp.additionalDuration))

				var currentTime time.Time
				setup.dst.sql.QueryRow(t, "SELECT clock_timestamp()").Scan(&currentTime)
				cutoverTime := currentTime.Add(sp.additionalDuration - sp.cutover)
				t.Status("cutover time chosen: ", cutoverTime.String())

				select {
				case <-time.After(sp.additionalDuration):
					workloadCancel()
					t.L().Printf("workload has finished after %s", sp.additionalDuration)
				case <-ctx.Done():
					t.L().Printf(`roachtest context cancelled while waiting for workload duration to complete`)
					return
				}
				t.Status(fmt.Sprintf("waiting for replication stream to cutover to %s", cutoverTime.String()))
				retainedTime := getReplicationRetainedTime(t, setup.dst.sql, roachpb.TenantName(setup.dst.name))
				setup.metrics.cutoverStart = newSizeTime(ctx, du, setup.dst.nodes)
				stopReplicationStream(t, setup.dst.sql, ingestionJobID, cutoverTime)
				setup.metrics.cutoverEnd = newSizeTime(ctx, du, setup.dst.nodes)

				t.Status("comparing fingerprints")
				compareTenantFingerprintsAtTimestamp(
					t,
					m,
					setup,
					retainedTime,
					cutoverTime,
				)
				lv.assertValid(t)

				// TODO(msbutler): export metrics to roachperf or prom/grafana
				exportedMetrics := setup.metrics.export()
				t.L().Printf(`Initial Scan: Duration, Size, Throughput; Cutover: Duration, Size, Throughput`)
				t.L().Printf(`%s  %s  %s  %s  %s  %s`,
					exportedMetrics["InitialScanDuration"].String(),
					exportedMetrics["InitialScanSize"].String(),
					exportedMetrics["InitialScanThroughput"].String(),
					exportedMetrics["CutoverDuration"].String(),
					exportedMetrics["CutoverSize"].String(),
					exportedMetrics["CutoverThroughput"].String(),
				)
				for key, metric := range exportedMetrics {
					t.L().Printf("%s: %s", key, metric.String())
				}
			},
		})
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

func compareTenantFingerprintsAtTimestamp(
	t test.Test, m cluster.Monitor, setup *c2cSetup, startTime, endTime time.Time,
) {
	t.Status(fmt.Sprintf("comparing tenant fingerprints between start time %s and end time %s", startTime.UTC(), endTime.UTC()))

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
	m.Go(func(ctx context.Context) error {
		setup.src.sql.QueryRow(t, fingerprintQuery, setup.src.ID).Scan(&srcFingerprint)
		return nil
	})
	var destFingerprint int64
	m.Go(func(ctx context.Context) error {
		// TODO(adityamaru): Measure and record fingerprinting throughput.
		setup.metrics.fingerprintingStart = timeutil.Now()
		setup.dst.sql.QueryRow(t, fingerprintQuery, setup.dst.ID).Scan(&destFingerprint)
		setup.metrics.fingerprintingEnd = timeutil.Now()
		fingerprintingDuration := setup.metrics.fingerprintingEnd.Sub(setup.metrics.fingerprintingStart).String()
		t.L().Printf("fingerprinting the destination tenant took %s", fingerprintingDuration)
		return nil
	})

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

// getReplicationRetainedTime returns the `retained_time` of the replication
// job.
func getReplicationRetainedTime(
	t test.Test, destSQL *sqlutils.SQLRunner, destTenantName roachpb.TenantName,
) time.Time {
	var retainedTime time.Time
	destSQL.QueryRow(t, `SELECT retained_time FROM [SHOW TENANT $1 WITH REPLICATION STATUS]`,
		destTenantName).Scan(&retainedTime)
	return retainedTime
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
