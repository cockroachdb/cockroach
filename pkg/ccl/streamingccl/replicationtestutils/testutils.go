// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package replicationtestutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type srcInitExecFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner)
type destInitExecFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner) // Tenant is created by the replication stream

type TenantStreamingClustersArgs struct {
	SrcTenantName      roachpb.TenantName
	SrcTenantID        roachpb.TenantID
	SrcInitFunc        srcInitExecFunc
	SrcNumNodes        int
	SrcClusterSettings map[string]string

	DestTenantName                 roachpb.TenantName
	DestTenantID                   roachpb.TenantID
	DestInitFunc                   destInitExecFunc
	DestNumNodes                   int
	DestClusterSettings            map[string]string
	RetentionTTLSeconds            int
	TestingKnobs                   *sql.StreamingTestingKnobs
	TenantCapabilitiesTestingKnobs *tenantcapabilities.TestingKnobs
}

var DefaultTenantStreamingClustersArgs = TenantStreamingClustersArgs{
	SrcTenantName: roachpb.TenantName("source"),
	SrcTenantID:   roachpb.MustMakeTenantID(10),
	SrcInitFunc: func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, `
	CREATE DATABASE d;
	CREATE TABLE d.t1(i int primary key, a string, b string);
	CREATE TABLE d.t2(i int primary key);
	INSERT INTO d.t1 (i) VALUES (42);
	INSERT INTO d.t2 VALUES (2);
	UPDATE d.t1 SET b = 'world' WHERE i = 42;
	`)
	},
	SrcNumNodes:         1,
	SrcClusterSettings:  defaultSrcClusterSetting,
	DestTenantName:      roachpb.TenantName("destination"),
	DestTenantID:        roachpb.MustMakeTenantID(2),
	DestNumNodes:        1,
	DestClusterSettings: defaultDestClusterSetting,
}

type TenantStreamingClusters struct {
	T               *testing.T
	Args            TenantStreamingClustersArgs
	SrcCluster      *testcluster.TestCluster
	SrcTenantConn   *gosql.DB
	SrcSysServer    serverutils.TestServerInterface
	SrcSysSQL       *sqlutils.SQLRunner
	SrcTenantSQL    *sqlutils.SQLRunner
	SrcTenantServer serverutils.TestTenantInterface
	SrcURL          url.URL
	SrcCleanup      func()

	DestCluster   *testcluster.TestCluster
	DestSysServer serverutils.TestServerInterface
	DestSysSQL    *sqlutils.SQLRunner
	DestTenantSQL *sqlutils.SQLRunner
}

// CreateDestTenantSQL creates a dest tenant SQL runner and returns a cleanup
// function that shuts tenant SQL instance and closes all sessions.
// This function will fail the test if ran prior to the Replication stream
// closing as the tenant will not yet be active
func (c *TenantStreamingClusters) CreateDestTenantSQL(ctx context.Context) func() error {
	testTenant, destTenantConn := serverutils.StartTenant(c.T, c.DestSysServer,
		base.TestTenantArgs{TenantID: c.Args.DestTenantID, DisableCreateTenant: true, SkipTenantCheck: true})
	c.DestTenantSQL = sqlutils.MakeSQLRunner(destTenantConn)
	return func() error {
		if err := destTenantConn.Close(); err != nil {
			return err
		}
		testTenant.Stopper().Stop(ctx)
		return nil
	}
}

// CompareResult compares the results of query on the primary and standby
// tenants.
func (c *TenantStreamingClusters) CompareResult(query string) {
	require.NotNil(c.T, c.DestTenantSQL,
		"destination tenant SQL runner should be created first")
	sourceData := c.SrcTenantSQL.QueryStr(c.T, query)
	destData := c.DestTenantSQL.QueryStr(c.T, query)
	require.Equal(c.T, sourceData, destData)
}

func (c *TenantStreamingClusters) RequireFingerprintMatchAtTimestamp(timestamp string) string {
	expected := FingerprintTenantAtTimestampNoHistory(c.T, c.SrcSysSQL, c.Args.SrcTenantID.ToUint64(), timestamp)
	actual := FingerprintTenantAtTimestampNoHistory(c.T, c.DestSysSQL, c.Args.DestTenantID.ToUint64(), timestamp)
	require.Equal(c.T, expected, actual)
	return actual
}

func (c *TenantStreamingClusters) RequireDestinationFingerprintAtTimestamp(
	fingerprint string, timestamp string,
) {
	actual := FingerprintTenantAtTimestampNoHistory(c.T, c.DestSysSQL, c.Args.DestTenantID.ToUint64(), timestamp)
	require.Equal(c.T, fingerprint, actual)
}

func FingerprintTenantAtTimestampNoHistory(
	t sqlutils.Fataler, db *sqlutils.SQLRunner, tenantID uint64, timestamp string,
) string {
	fingerprintQuery := fmt.Sprintf("SELECT * FROM crdb_internal.fingerprint(crdb_internal.tenant_span($1::INT), 0::TIMESTAMPTZ, false) AS OF SYSTEM TIME %s", timestamp)
	return db.QueryStr(t, fingerprintQuery, tenantID)[0][0]
}

// WaitUntilHighWatermark waits for the ingestion job high watermark to reach the given high watermark.
func (c *TenantStreamingClusters) WaitUntilHighWatermark(
	highWatermark hlc.Timestamp, ingestionJobID jobspb.JobID,
) {
	testutils.SucceedsSoon(c.T, func() error {
		progress := jobutils.GetJobProgress(c.T, c.DestSysSQL, ingestionJobID)
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

// Cutover sets the cutover timestamp on the replication job causing the job to
// stop eventually.
func (c *TenantStreamingClusters) Cutover(
	producerJobID, ingestionJobID int, cutoverTime time.Time, async bool,
) {
	// Cut over the ingestion job and the job will stop eventually.
	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		c.Args.DestTenantName, cutoverTime).Scan(&cutoverStr)
	cutoverOutput := DecimalTimeToHLC(c.T, cutoverStr)
	require.Equal(c.T, cutoverTime, cutoverOutput.GoTime())
	if !async {
		jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		jobutils.WaitForJobToSucceed(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	}
}

// StartStreamReplication producer job ID and ingestion job ID.
func (c *TenantStreamingClusters) StartStreamReplication(ctx context.Context) (int, int) {
	c.DestSysSQL.Exec(c.T, c.BuildCreateTenantQuery())
	streamProducerJobID, ingestionJobID := GetStreamJobIds(c.T, ctx, c.DestSysSQL, c.Args.DestTenantName)
	return streamProducerJobID, ingestionJobID
}

func (c *TenantStreamingClusters) BuildCreateTenantQuery() string {
	streamReplStmt := fmt.Sprintf("CREATE TENANT %s FROM REPLICATION OF %s ON '%s'",
		c.Args.DestTenantName,
		c.Args.SrcTenantName,
		c.SrcURL.String())
	if c.Args.RetentionTTLSeconds > 0 {
		streamReplStmt = fmt.Sprintf("%s WITH RETENTION = '%ds'", streamReplStmt, c.Args.RetentionTTLSeconds)
	}
	return streamReplStmt
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

func CreateTenantStreamingClusters(
	ctx context.Context, t *testing.T, args TenantStreamingClustersArgs,
) (*TenantStreamingClusters, func()) {
	serverArgs := base.TestServerArgs{
		// Test fails because it tries to set a cluster setting only accessible
		// to system tenants. Tracked with #76378.
		DisableDefaultTestTenant: true,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				StreamingTestingKnobs: args.TestingKnobs,
			},
			Streaming:                      args.TestingKnobs,
			TenantCapabilitiesTestingKnobs: args.TenantCapabilitiesTestingKnobs,
			TenantTestingKnobs: &sql.TenantTestingKnobs{
				// The streaming tests want tenant ID stability. So we want
				// easy-to-predict IDs when we create a tenant after a drop.
				EnableTenantIDReuse: true,
			},
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
	srcCluster, srcURL, srcCleanup := startTestCluster(ctx, t, serverArgs, args.SrcNumNodes)

	clusterSettings := cluster.MakeTestingClusterSettings()
	for _, setting := range []*settings.BoolSetting{
		sql.SecondaryTenantSplitAtEnabled,
		sql.SecondaryTenantScatterEnabled,
	} {
		setting.Override(ctx, &clusterSettings.SV, true)
	}
	tenantArgs := base.TestTenantArgs{
		TenantName: args.SrcTenantName,
		TenantID:   args.SrcTenantID,
		Settings:   clusterSettings,
	}
	srcTenantServer, srcTenantConn := serverutils.StartTenant(t, srcCluster.Server(0), tenantArgs)
	waitForTenantPodsActive(t, srcTenantServer, 1)

	// Start the destination cluster.
	destCluster, _, destCleanup := startTestCluster(ctx, t, serverArgs, args.DestNumNodes)

	tsc := &TenantStreamingClusters{
		T:               t,
		Args:            args,
		SrcCluster:      srcCluster,
		SrcTenantConn:   srcTenantConn,
		SrcTenantServer: srcTenantServer,
		SrcSysSQL:       sqlutils.MakeSQLRunner(srcCluster.ServerConn(0)),
		SrcTenantSQL:    sqlutils.MakeSQLRunner(srcTenantConn),
		SrcSysServer:    srcCluster.Server(0),
		SrcURL:          srcURL,
		SrcCleanup:      srcCleanup,
		DestCluster:     destCluster,
		DestSysSQL:      sqlutils.MakeSQLRunner(destCluster.ServerConn(0)),
		DestSysServer:   destCluster.Server(0),
	}

	tsc.SrcSysSQL.ExecMultiple(t, ConfigureClusterSettings(args.SrcClusterSettings)...)
	if args.SrcInitFunc != nil {
		args.SrcInitFunc(t, tsc.SrcSysSQL, tsc.SrcTenantSQL)
	}
	tsc.DestSysSQL.ExecMultiple(t, ConfigureClusterSettings(args.DestClusterSettings)...)
	if args.DestInitFunc != nil {
		args.DestInitFunc(t, tsc.DestSysSQL)
	}
	// Enable stream replication on dest by default.
	tsc.DestSysSQL.Exec(t, `SET CLUSTER SETTING cross_cluster_replication.enabled = true;`)
	return tsc, func() {
		require.NoError(t, srcTenantConn.Close())
		destCleanup()
		srcCleanup()
	}
}

func (c *TenantStreamingClusters) SrcExec(exec srcInitExecFunc) {
	exec(c.T, c.SrcSysSQL, c.SrcTenantSQL)
}

// WaitUntilStartTimeReached waits for the ingestion job high watermark to reach
// the recorded start time of the job.
func (c *TenantStreamingClusters) WaitUntilStartTimeReached(ingestionJobID jobspb.JobID) {
	WaitUntilStartTimeReached(c.T, c.DestSysSQL, ingestionJobID)
}

func WaitUntilStartTimeReached(t *testing.T, db *sqlutils.SQLRunner, ingestionJobID jobspb.JobID) {
	testutils.SucceedsSoon(t, func() error {
		payload := jobutils.GetJobPayload(t, db, ingestionJobID)
		details, ok := payload.Details.(*jobspb.Payload_StreamIngestion)
		if !ok {
			return errors.New("job does not appear to be a stream ingestion job")
		}
		if details.StreamIngestion == nil {
			return errors.New("no stream ingestion details")
		}
		startTime := details.StreamIngestion.ReplicationStartTime
		if startTime.IsEmpty() {
			return errors.New("ingestion start time not yet recorded")
		}

		progress := jobutils.GetJobProgress(t, db, ingestionJobID)
		if progress.GetHighWater() == nil {
			return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
				startTime.String())
		}
		highwater := *progress.GetHighWater()
		if highwater.Less(startTime) {
			return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
				highwater.String(), startTime.String())
		}
		return nil
	})
}

func CreateScatteredTable(t *testing.T, c *TenantStreamingClusters, numNodes int) {
	// Create a source table with multiple ranges spread across multiple nodes
	numRanges := 50
	rowsPerRange := 20
	c.SrcTenantSQL.Exec(t, fmt.Sprintf(`
  CREATE TABLE d.scattered (key INT PRIMARY KEY);
  INSERT INTO d.scattered (key) SELECT * FROM generate_series(1, %d);
  ALTER TABLE d.scattered SPLIT AT (SELECT * FROM generate_series(%d, %d, %d));
  ALTER TABLE d.scattered SCATTER;
  `, numRanges*rowsPerRange, rowsPerRange, (numRanges-1)*rowsPerRange, rowsPerRange))
	c.SrcSysSQL.CheckQueryResultsRetry(t, "SELECT count(distinct lease_holder) from crdb_internal.ranges", [][]string{{fmt.Sprint(numNodes)}})
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

func ConfigureClusterSettings(setting map[string]string) []string {
	res := make([]string, len(setting))
	for key, val := range setting {
		res = append(res, fmt.Sprintf("SET CLUSTER SETTING %s = %s;", key, val))
	}
	return res
}

func RunningStatus(t *testing.T, sqlRunner *sqlutils.SQLRunner, ingestionJobID int) string {
	p := jobutils.GetJobProgress(t, sqlRunner, jobspb.JobID(ingestionJobID))
	return p.RunningStatus
}

func DecimalTimeToHLC(t *testing.T, s string) hlc.Timestamp {
	t.Helper()
	d, _, err := apd.NewFromString(s)
	require.NoError(t, err)
	ts, err := hlc.DecimalToHLC(d)
	require.NoError(t, err)
	return ts
}

// GetStreamJobIds returns the jod ids of the producer and ingestion jobs.
func GetStreamJobIds(
	t *testing.T,
	ctx context.Context,
	sqlRunner *sqlutils.SQLRunner,
	destTenantName roachpb.TenantName,
) (producer int, consumer int) {
	var tenantInfoBytes []byte
	var tenantInfo mtinfopb.ProtoInfo
	sqlRunner.QueryRow(t, "SELECT info FROM system.tenants WHERE name=$1",
		destTenantName).Scan(&tenantInfoBytes)
	require.NoError(t, protoutil.Unmarshal(tenantInfoBytes, &tenantInfo))

	stats := replicationutils.TestingGetStreamIngestionStatsNoHeartbeatFromReplicationJob(t, ctx, sqlRunner, int(tenantInfo.TenantReplicationJobID))
	return int(stats.IngestionDetails.StreamID), int(tenantInfo.TenantReplicationJobID)
}
