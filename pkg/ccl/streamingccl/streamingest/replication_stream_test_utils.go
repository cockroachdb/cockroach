// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type srcInitExecFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner)
type destInitExecFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner) // Tenant is created by the replication stream

type tenantStreamingClustersArgs struct {
	srcTenantName      roachpb.TenantName
	srcTenantID        roachpb.TenantID
	srcInitFunc        srcInitExecFunc
	srcNumNodes        int
	srcClusterSettings map[string]string

	destTenantName      roachpb.TenantName
	destTenantID        roachpb.TenantID
	destInitFunc        destInitExecFunc
	destNumNodes        int
	destClusterSettings map[string]string
	testingKnobs        *sql.StreamingTestingKnobs
}

var defaultTenantStreamingClustersArgs = tenantStreamingClustersArgs{
	srcTenantName: roachpb.TenantName("source"),
	srcTenantID:   roachpb.MustMakeTenantID(10),
	srcInitFunc: func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, `
	CREATE DATABASE d;
	CREATE TABLE d.t1(i int primary key, a string, b string);
	CREATE TABLE d.t2(i int primary key);
	INSERT INTO d.t1 (i) VALUES (42);
	INSERT INTO d.t2 VALUES (2);
	UPDATE d.t1 SET b = 'world' WHERE i = 42;
	`)
	},
	srcNumNodes:         1,
	srcClusterSettings:  defaultSrcClusterSetting,
	destTenantName:      roachpb.TenantName("destination"),
	destTenantID:        roachpb.MustMakeTenantID(2),
	destNumNodes:        1,
	destClusterSettings: defaultDestClusterSetting,
}

type tenantStreamingClusters struct {
	t               *testing.T
	args            tenantStreamingClustersArgs
	srcCluster      *testcluster.TestCluster
	srcTenantConn   *gosql.DB
	srcSysServer    serverutils.TestServerInterface
	srcSysSQL       *sqlutils.SQLRunner
	srcTenantSQL    *sqlutils.SQLRunner
	srcTenantServer serverutils.TestTenantInterface
	srcURL          url.URL
	srcCleanup      func()

	destCluster   *testcluster.TestCluster
	destSysServer serverutils.TestServerInterface
	destSysSQL    *sqlutils.SQLRunner
	destTenantSQL *sqlutils.SQLRunner
}

// Creates a dest tenant SQL runner and returns a cleanup function that shuts
// tenant SQL instance and closes all sessions.
// This function will fail the test if ran prior to the Replication stream
// closing as the tenant will not yet be active
func (c *tenantStreamingClusters) createDestTenantSQL(ctx context.Context) func() error {
	testTenant, destTenantConn := serverutils.StartTenant(c.t, c.destSysServer,
		base.TestTenantArgs{TenantID: c.args.destTenantID, DisableCreateTenant: true, SkipTenantCheck: true})
	c.destTenantSQL = sqlutils.MakeSQLRunner(destTenantConn)
	return func() error {
		if err := destTenantConn.Close(); err != nil {
			return err
		}
		testTenant.Stopper().Stop(ctx)
		return nil
	}
}

// This function has to be called after createTenantSQL.
func (c *tenantStreamingClusters) compareResult(query string) {
	require.NotNil(c.t, c.destTenantSQL,
		"destination tenant SQL runner should be created first")
	sourceData := c.srcTenantSQL.QueryStr(c.t, query)
	destData := c.destTenantSQL.QueryStr(c.t, query)
	require.Equal(c.t, sourceData, destData)
}

// Waits for the ingestion job high watermark to reach the given high watermark.
func (c *tenantStreamingClusters) waitUntilHighWatermark(
	highWatermark hlc.Timestamp, ingestionJobID jobspb.JobID,
) {
	testutils.SucceedsSoon(c.t, func() error {
		progress := jobutils.GetJobProgress(c.t, c.destSysSQL, ingestionJobID)
		if progress.GetHighWater() == nil {
			return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
				highWatermark.String())
		}
		highwater := *progress.GetHighWater()
		if highwater.Less(highWatermark) {
			return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
				highwater.String(), highWatermark.String())
		}
		return nil
	})
}

func (c *tenantStreamingClusters) cutover(
	producerJobID, ingestionJobID int, cutoverTime time.Time,
) {
	// Cut over the ingestion job and the job will stop eventually.
	c.destSysSQL.Exec(c.t, `SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`, ingestionJobID, cutoverTime)
	jobutils.WaitForJobToSucceed(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	jobutils.WaitForJobToSucceed(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
}

// Returns producer job ID and ingestion job ID.
func (c *tenantStreamingClusters) startStreamReplication() (int, int) {
	var ingestionJobID, streamProducerJobID int
	streamReplStmt := fmt.Sprintf("CREATE TENANT %s FROM REPLICATION OF %s ON '%s'", c.args.destTenantName, c.args.srcTenantName, c.srcURL.String())
	c.destSysSQL.QueryRow(c.t, streamReplStmt).Scan(&ingestionJobID, &streamProducerJobID)
	return streamProducerJobID, ingestionJobID
}

func waitForTenantPodsActive(
	t testing.TB, tenantServer serverutils.TestTenantInterface, numPods int,
) {
	testutils.SucceedsWithin(t, func() error {
		status := tenantServer.StatusServer().(serverpb.SQLStatusServer)
		var nodes *serverpb.NodesListResponse
		var err error
		for nodes == nil || len(nodes.Nodes) != numPods {
			nodes, err = status.NodesList(context.Background(), nil)
			if err != nil {
				return err
			}
		}
		return nil
	}, 10*time.Second)
}

func createTenantStreamingClusters(
	ctx context.Context, t *testing.T, args tenantStreamingClustersArgs,
) (*tenantStreamingClusters, func()) {
	serverArgs := base.TestServerArgs{
		// Test fails because it tries to set a cluster setting only accessible
		// to system tenants. Tracked with #76378.
		DisableDefaultTestTenant: true,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				StreamingTestingKnobs: args.testingKnobs,
			},
			Streaming: args.testingKnobs,
		},
	}

	startTestCluster := func(
		ctx context.Context,
		t *testing.T,
		serverArgs base.TestServerArgs,
		numNodes int,
	) (*testcluster.TestCluster, url.URL, func()) {
		params := base.TestClusterArgs{ServerArgs: serverArgs}
		c := testcluster.StartTestCluster(t, numNodes, params)
		c.Server(0).Clock().Now()
		// TODO(casper): support adding splits when we have multiple nodes.
		pgURL, cleanupSinkCert := sqlutils.PGUrl(t, c.Server(0).ServingSQLAddr(), t.Name(), url.User(username.RootUser))
		return c, pgURL, func() {
			c.Stopper().Stop(ctx)
			cleanupSinkCert()
		}
	}

	// Start the source cluster.
	srcCluster, srcURL, srcCleanup := startTestCluster(ctx, t, serverArgs, args.srcNumNodes)

	tenantArgs := base.TestTenantArgs{
		TenantName: args.srcTenantName,
		TenantID:   args.srcTenantID,
		TestingKnobs: base.TestingKnobs{
			TenantTestingKnobs: &sql.TenantTestingKnobs{
				AllowSplitAndScatter: true,
			}},
	}
	srcTenantServer, srcTenantConn := serverutils.StartTenant(t, srcCluster.Server(0), tenantArgs)
	waitForTenantPodsActive(t, srcTenantServer, 1)

	// Start the destination cluster.
	destCluster, _, destCleanup := startTestCluster(ctx, t, serverArgs, args.destNumNodes)

	tsc := &tenantStreamingClusters{
		t:               t,
		args:            args,
		srcCluster:      srcCluster,
		srcTenantConn:   srcTenantConn,
		srcTenantServer: srcTenantServer,
		srcSysSQL:       sqlutils.MakeSQLRunner(srcCluster.ServerConn(0)),
		srcTenantSQL:    sqlutils.MakeSQLRunner(srcTenantConn),
		srcSysServer:    srcCluster.Server(0),
		srcURL:          srcURL,
		srcCleanup:      srcCleanup,
		destCluster:     destCluster,
		destSysSQL:      sqlutils.MakeSQLRunner(destCluster.ServerConn(0)),
		destSysServer:   destCluster.Server(0),
	}

	tsc.srcSysSQL.ExecMultiple(t, configureClusterSettings(args.srcClusterSettings)...)
	if args.srcInitFunc != nil {
		args.srcInitFunc(t, tsc.srcSysSQL, tsc.srcTenantSQL)
	}
	tsc.destSysSQL.ExecMultiple(t, configureClusterSettings(args.destClusterSettings)...)
	if args.destInitFunc != nil {
		args.destInitFunc(t, tsc.destSysSQL)
	}
	// Enable stream replication on dest by default.
	tsc.destSysSQL.Exec(t, `SET enable_experimental_stream_replication = true;`)
	return tsc, func() {
		require.NoError(t, srcTenantConn.Close())
		destCleanup()
		srcCleanup()
	}
}

func (c *tenantStreamingClusters) srcExec(exec srcInitExecFunc) {
	exec(c.t, c.srcSysSQL, c.srcTenantSQL)
}

func createScatteredTable(t *testing.T, c *tenantStreamingClusters, numNodes int) {
	// Create a source table with multiple ranges spread across multiple nodes
	numRanges := 50
	rowsPerRange := 20
	c.srcTenantSQL.Exec(t, fmt.Sprintf(`
  CREATE TABLE d.scattered (key INT PRIMARY KEY);
  INSERT INTO d.scattered (key) SELECT * FROM generate_series(1, %d);
  ALTER TABLE d.scattered SPLIT AT (SELECT * FROM generate_series(%d, %d, %d));
  ALTER TABLE d.scattered SCATTER;
  `, numRanges*rowsPerRange, rowsPerRange, (numRanges-1)*rowsPerRange, rowsPerRange))
	c.srcSysSQL.CheckQueryResultsRetry(t, "SELECT count(distinct lease_holder) from crdb_internal.ranges", [][]string{{fmt.Sprint(numNodes)}})
}

var defaultSrcClusterSetting = map[string]string{
	`kv.rangefeed.enabled`:                `true`,
	`kv.closed_timestamp.target_duration`: `'1s'`,
	// Large timeout makes test to not fail with unexpected timeout failures.
	`stream_replication.job_liveness_timeout`:            `'3m'`,
	`stream_replication.stream_liveness_track_frequency`: `'2s'`,
	`stream_replication.min_checkpoint_frequency`:        `'1s'`,
	// Make all AddSSTable operation to trigger AddSSTable events.
	`kv.bulk_io_write.small_write_size`: `'1'`,
	`jobs.registry.interval.adopt`:      `'1s'`,
}

var defaultDestClusterSetting = map[string]string{
	`stream_replication.consumer_heartbeat_frequency`:      `'1s'`,
	`stream_replication.job_checkpoint_frequency`:          `'100ms'`,
	`bulkio.stream_ingestion.minimum_flush_interval`:       `'10ms'`,
	`bulkio.stream_ingestion.cutover_signal_poll_interval`: `'100ms'`,
	`jobs.registry.interval.adopt`:                         `'1s'`,
}

func configureClusterSettings(setting map[string]string) []string {
	res := make([]string, len(setting))
	for key, val := range setting {
		res = append(res, fmt.Sprintf("SET CLUSTER SETTING %s = %s;", key, val))
	}
	return res
}

func streamIngestionStats(
	t *testing.T, sqlRunner *sqlutils.SQLRunner, ingestionJobID int,
) *streampb.StreamIngestionStats {
	stats, rawStats := &streampb.StreamIngestionStats{}, make([]byte, 0)
	row := sqlRunner.QueryRow(t, "SELECT crdb_internal.stream_ingestion_stats_pb($1)", ingestionJobID)
	row.Scan(&rawStats)
	require.NoError(t, protoutil.Unmarshal(rawStats, stats))
	return stats
}

func runningStatus(t *testing.T, sqlRunner *sqlutils.SQLRunner, ingestionJobID int) string {
	p := jobutils.GetJobProgress(t, sqlRunner, jobspb.JobID(ingestionJobID))
	return p.RunningStatus
}

// streamClientValidatorWrapper wraps a Validator and exposes additional methods
// used by stream ingestion to check for correctness.
type streamClientValidator struct {
	cdctest.StreamValidator
	rekeyer *backupccl.KeyRewriter

	mu syncutil.Mutex
}

// newStreamClientValidator returns a wrapped Validator, that can be used
// to validate the events emitted by the cluster to cluster streaming client.
// The wrapper currently only "wraps" an orderValidator, but can be built out
// to utilize other Validator's.
// The wrapper also allows querying the orderValidator to retrieve streamed
// events from an in-memory store.
func newStreamClientValidator(rekeyer *backupccl.KeyRewriter) *streamClientValidator {
	ov := cdctest.NewStreamOrderValidator()
	return &streamClientValidator{
		StreamValidator: ov,
		rekeyer:         rekeyer,
	}
}

func (sv *streamClientValidator) noteRow(
	partition string, key, value string, updated hlc.Timestamp,
) error {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.NoteRow(partition, key, value, updated)
}

func (sv *streamClientValidator) noteResolved(partition string, resolved hlc.Timestamp) error {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.NoteResolved(partition, resolved)
}

func (sv *streamClientValidator) failures() []string {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.Failures()
}

func (sv *streamClientValidator) getValuesForKeyBelowTimestamp(
	key string, timestamp hlc.Timestamp,
) ([]roachpb.KeyValue, error) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.GetValuesForKeyBelowTimestamp(key, timestamp)
}
