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
	"math/rand"
	"net/url"
	"sort"
	"testing"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const TestingMaxDistSQLRetries = 4

type srcInitExecFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner)
type destInitExecFunc func(t *testing.T, sysSQL *sqlutils.SQLRunner) // Tenant is created by the replication stream

type TenantStreamingClustersArgs struct {
	SrcTenantName         roachpb.TenantName
	SrcTenantID           roachpb.TenantID
	SrcInitFunc           srcInitExecFunc
	SrcNumNodes           int
	SrcClusterSettings    map[string]string
	SrcClusterTestRegions []string

	DestTenantName                 roachpb.TenantName
	DestTenantID                   roachpb.TenantID
	DestInitFunc                   destInitExecFunc
	DestNumNodes                   int
	DestClusterSettings            map[string]string
	DestClusterTestRegions         []string
	RetentionTTLSeconds            int
	TestingKnobs                   *sql.StreamingTestingKnobs
	TenantCapabilitiesTestingKnobs *tenantcapabilities.TestingKnobs

	MultitenantSingleClusterNumNodes    int
	MultiTenantSingleClusterTestRegions []string

	NoMetamorphicExternalConnection bool
	ExternalIODir                   string
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
	SrcSysServer    serverutils.ApplicationLayerInterface
	SrcSysSQL       *sqlutils.SQLRunner
	SrcTenantSQL    *sqlutils.SQLRunner
	SrcTenantServer serverutils.ApplicationLayerInterface
	SrcURL          url.URL
	SrcCleanup      func()

	DestCluster    *testcluster.TestCluster
	DestSysServer  serverutils.ApplicationLayerInterface
	DestSysSQL     *sqlutils.SQLRunner
	DestTenantConn *gosql.DB
	DestTenantSQL  *sqlutils.SQLRunner

	Rng *rand.Rand
}

func (c *TenantStreamingClusters) setupSrcTenant() {
	tenantArgs := base.TestSharedProcessTenantArgs{
		TenantName: c.Args.SrcTenantName,
		TenantID:   c.Args.SrcTenantID,
		Knobs:      DefaultAppTenantTestingKnobs(),
	}
	srcTenantServer, srcTenantConn := serverutils.StartSharedProcessTenant(c.T, c.SrcCluster.Server(0),
		tenantArgs)

	testutils.SucceedsSoon(c.T, func() error {
		return srcTenantConn.Ping()
	})

	c.SrcTenantServer = srcTenantServer
	c.SrcTenantConn = srcTenantConn
	c.SrcTenantSQL = sqlutils.MakeSQLRunner(srcTenantConn)
}

func (c *TenantStreamingClusters) init(ctx context.Context) {
	c.SrcSysSQL.ExecMultiple(c.T, ConfigureClusterSettings(c.Args.SrcClusterSettings)...)
	if !c.Args.SrcTenantID.IsSystem() {
		c.SrcSysSQL.Exec(c.T, `ALTER TENANT $1 SET CLUSTER SETTING sql.virtual_cluster.feature_access.multiregion.enabled=true`, c.Args.SrcTenantName)
		c.SrcSysSQL.Exec(c.T, `ALTER TENANT $1 GRANT CAPABILITY can_use_nodelocal_storage`, c.Args.SrcTenantName)
		require.NoError(c.T, c.SrcCluster.Server(0).TenantController().WaitForTenantCapabilities(ctx, c.Args.SrcTenantID, map[tenantcapabilities.ID]string{
			tenantcapabilities.CanUseNodelocalStorage: "true",
		}, ""))
	}
	if c.Args.SrcInitFunc != nil {
		c.Args.SrcInitFunc(c.T, c.SrcSysSQL, c.SrcTenantSQL)
	}
	c.DestSysSQL.ExecMultiple(c.T, ConfigureClusterSettings(c.Args.DestClusterSettings)...)
	if c.Args.DestInitFunc != nil {
		c.Args.DestInitFunc(c.T, c.DestSysSQL)
	}
}

// StartDestTenant starts the destination tenant and returns a cleanup function
// that shuts tenant SQL instance and closes all sessions. This function will
// fail the test if ran prior to the Replication stream closing as the tenant
// will not yet be active. If the caller passes withTestingKnobs, the
// destination tenant starts up via a testServer.StartSharedProcessTenant().
func (c *TenantStreamingClusters) StartDestTenant(
	ctx context.Context, withTestingKnobs *base.TestingKnobs, server int,
) func() {
	if withTestingKnobs != nil {
		var err error
		_, c.DestTenantConn, err = c.DestCluster.Server(server).TenantController().StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
			TenantID:    c.Args.DestTenantID,
			TenantName:  c.Args.DestTenantName,
			Knobs:       *withTestingKnobs,
			UseDatabase: "defaultdb",
		})
		require.NoError(c.T, err)
	} else {
		c.DestSysSQL.Exec(c.T, `ALTER TENANT $1 START SERVICE SHARED`, c.Args.DestTenantName)
		c.DestTenantConn = c.DestCluster.Server(server).SystemLayer().SQLConn(c.T, serverutils.DBName("cluster:"+string(c.Args.DestTenantName)+"/defaultdb"))
	}

	c.DestTenantSQL = sqlutils.MakeSQLRunner(c.DestTenantConn)
	testutils.SucceedsSoon(c.T, func() error {
		return c.DestTenantConn.Ping()
	})
	// TODO (msbutler): consider granting the new tenant some capabilities.
	c.DestSysSQL.Exec(c.T, `ALTER TENANT $1 GRANT CAPABILITY can_use_nodelocal_storage`, c.Args.DestTenantName)
	require.NoError(c.T, c.DestCluster.Server(server).TenantController().WaitForTenantCapabilities(ctx, c.Args.DestTenantID, map[tenantcapabilities.ID]string{
		tenantcapabilities.CanUseNodelocalStorage: "true",
	}, ""))
	return func() {
		require.NoError(c.T, c.DestTenantConn.Close())
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
	expected := FingerprintTenantAtTimestampNoHistory(c.T, c.SrcSysSQL, c.Args.SrcTenantName, timestamp)
	actual := FingerprintTenantAtTimestampNoHistory(c.T, c.DestSysSQL, c.Args.DestTenantName, timestamp)
	require.Equal(c.T, expected, actual)
	return actual
}

func (c *TenantStreamingClusters) RequireDestinationFingerprintAtTimestamp(
	fingerprint string, timestamp string,
) {
	actual := FingerprintTenantAtTimestampNoHistory(c.T, c.DestSysSQL, c.Args.DestTenantName, timestamp)
	require.Equal(c.T, fingerprint, actual)
}

func FingerprintTenantAtTimestampNoHistory(
	t sqlutils.Fataler, db *sqlutils.SQLRunner, tenantName roachpb.TenantName, timestamp string,
) string {
	fingerprintQuery := fmt.Sprintf(`SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TENANT $1] AS OF SYSTEM TIME %s`, timestamp)
	return db.QueryStr(t, fingerprintQuery, tenantName)[0][0]
}

// WaitUntilReplicatedTime waits for the ingestion job high watermark
// to reach the given target time.
func (c *TenantStreamingClusters) WaitUntilReplicatedTime(
	targetTime hlc.Timestamp, ingestionJobID jobspb.JobID,
) {
	WaitUntilReplicatedTime(c.T, targetTime, c.DestSysSQL, ingestionJobID)
}

// WaitUntilStartTimeReached waits for the ingestion replicated time
// to reach the recorded start time of the job.
func (c *TenantStreamingClusters) WaitUntilStartTimeReached(ingestionJobID jobspb.JobID) {
	WaitUntilStartTimeReached(c.T, c.DestSysSQL, ingestionJobID)
}

// WaitForPostCutoverRetentionJob should be called after cutover completes to
// verify that there exists a new producer job on the newly cutover to tenant. This should be called after the replication job completes.
func (c *TenantStreamingClusters) WaitForPostCutoverRetentionJob() {
	c.DestSysSQL.Exec(c.T, fmt.Sprintf(`ALTER TENANT '%s' SET REPLICATION EXPIRATION WINDOW ='10ms'`, c.Args.DestTenantName))
	var retentionJobID jobspb.JobID
	retentionJobQuery := fmt.Sprintf(`SELECT job_id FROM [SHOW JOBS] 
WHERE description = 'History Retention for Physical Replication of %s'
ORDER BY created DESC LIMIT 1`, c.Args.DestTenantName)
	c.DestSysSQL.QueryRow(c.T, retentionJobQuery).Scan(&retentionJobID)
	testutils.SucceedsSoon(c.T, func() error {
		// Grab the latest producer job on the destination cluster.
		var status string
		c.DestSysSQL.QueryRow(c.T, "SELECT status FROM system.jobs WHERE id = $1", retentionJobID).Scan(&status)
		if jobs.Status(status) == jobs.StatusRunning {
			return nil
		}
		if jobs.Status(status) == jobs.StatusFailed {
			payload := jobutils.GetJobPayload(c.T, c.DestSysSQL, retentionJobID)
			require.Contains(c.T, payload.Error, "replication stream")
			require.Contains(c.T, payload.Error, "timed out")
			return nil
		}
		return errors.Newf("Unexpected status %s", status)
	})
}

// Cutover sets the cutover timestamp on the replication job causing the job to
// stop eventually. If the provided cutover time is the zero value, cutover to
// the latest replicated time.
func (c *TenantStreamingClusters) Cutover(
	producerJobID, ingestionJobID int, cutoverTime time.Time, async bool,
) {
	// Cut over the ingestion job and the job will stop eventually.
	var cutoverStr string
	if cutoverTime.IsZero() {
		c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO LATEST`,
			c.Args.DestTenantName).Scan(&cutoverStr)
	} else {
		c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
			c.Args.DestTenantName, cutoverTime).Scan(&cutoverStr)
		cutoverOutput := DecimalTimeToHLC(c.T, cutoverStr)
		require.Equal(c.T, cutoverTime, cutoverOutput.GoTime())
	}

	if !async {
		jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
		c.WaitForPostCutoverRetentionJob()
	}
}

// StartStreamReplication producer job ID and ingestion job ID.
func (c *TenantStreamingClusters) StartStreamReplication(ctx context.Context) (int, int) {

	// 50% of time, start replication stream via an external connection.
	var externalConnection string
	if c.Rng.Intn(2) == 0 && !c.Args.NoMetamorphicExternalConnection {
		externalConnection = "replication-source-addr"
		c.DestSysSQL.Exec(c.T, fmt.Sprintf(`CREATE EXTERNAL CONNECTION "%s" AS "%s"`,
			externalConnection, c.SrcURL.String()))
	}

	c.DestSysSQL.Exec(c.T, c.BuildCreateTenantQuery(externalConnection))
	streamProducerJobID, ingestionJobID := GetStreamJobIds(c.T, ctx, c.DestSysSQL, c.Args.DestTenantName)
	return streamProducerJobID, ingestionJobID
}

func (c *TenantStreamingClusters) BuildCreateTenantQuery(externalConnection string) string {
	sourceURI := c.SrcURL.String()
	if externalConnection != "" {
		sourceURI = fmt.Sprintf("external://%s", externalConnection)
	}
	streamReplStmt := fmt.Sprintf("CREATE TENANT %s FROM REPLICATION OF %s ON '%s'",
		c.Args.DestTenantName,
		c.Args.SrcTenantName,
		sourceURI)
	if c.Args.RetentionTTLSeconds > 0 {
		streamReplStmt = fmt.Sprintf("%s WITH RETENTION = '%ds'", streamReplStmt, c.Args.RetentionTTLSeconds)
	}
	return streamReplStmt
}

// DefaultAppTenantTestingKnobs returns the default testing knobs for an application tenant.
func DefaultAppTenantTestingKnobs() base.TestingKnobs {
	return base.TestingKnobs{
		JobsTestingKnobs: defaultJobsTestingKnobs(),
	}
}

func defaultJobsTestingKnobs() *jobs.TestingKnobs {
	jobTestingKnobs := jobs.NewTestingKnobsWithShortIntervals()
	jobTestingKnobs.SchedulerDaemonInitialScanDelay = func() time.Duration { return time.Second }
	jobTestingKnobs.SchedulerDaemonScanDelay = func() time.Duration { return time.Second }
	return jobTestingKnobs
}

func CreateServerArgs(args TenantStreamingClustersArgs) base.TestServerArgs {
	if args.TestingKnobs != nil && args.TestingKnobs.DistSQLRetryPolicy == nil {
		args.TestingKnobs.DistSQLRetryPolicy = &retry.Options{
			InitialBackoff: time.Microsecond,
			Multiplier:     2,
			MaxBackoff:     2 * time.Microsecond,
			MaxRetries:     TestingMaxDistSQLRetries,
		}
	}
	return base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: defaultJobsTestingKnobs(),
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
		ExternalIODir: args.ExternalIODir,
	}
}

func startC2CTestCluster(
	ctx context.Context, t *testing.T, serverArgs base.TestServerArgs, numNodes int, regions []string,
) (*testcluster.TestCluster, url.URL, func()) {

	params := base.TestClusterArgs{ServerArgs: serverArgs}

	makeLocality := func(locStr string) roachpb.Locality {
		return roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: locStr}}}
	}
	if len(regions) == 1 {
		params.ServerArgs.Locality = makeLocality(regions[0])
	}
	if len(regions) > 1 {
		require.Equal(t, len(regions), numNodes)
		serverArgsPerNode := make(map[int]base.TestServerArgs)
		for i, locality := range regions {
			param := serverArgs
			param.Locality = makeLocality(locality)
			param.ScanMaxIdleTime = 10 * time.Millisecond
			serverArgsPerNode[i] = param
		}
		params.ServerArgsPerNode = serverArgsPerNode
	}

	c := testcluster.StartTestCluster(t, numNodes, params)

	// TODO(casper): support adding splits when we have multiple nodes.
	pgURL, cleanupSinkCert := sqlutils.PGUrl(t, c.Server(0).SystemLayer().AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	return c, pgURL, func() {
		c.Stopper().Stop(ctx)
		cleanupSinkCert()
	}
}

func CreateMultiTenantStreamingCluster(
	ctx context.Context, t *testing.T, args TenantStreamingClustersArgs,
) (*TenantStreamingClusters, func()) {

	serverArgs := CreateServerArgs(args)
	cluster, url, cleanup := startC2CTestCluster(ctx, t, serverArgs,
		args.MultitenantSingleClusterNumNodes, args.MultiTenantSingleClusterTestRegions)

	rng, _ := randutil.NewPseudoRand()

	destNodeIdx := args.MultitenantSingleClusterNumNodes - 1
	tsc := &TenantStreamingClusters{
		T:             t,
		Args:          args,
		SrcCluster:    cluster,
		SrcSysSQL:     sqlutils.MakeSQLRunner(cluster.ServerConn(0)),
		SrcSysServer:  cluster.Server(0).SystemLayer(),
		SrcURL:        url,
		SrcCleanup:    cleanup,
		DestCluster:   cluster,
		DestSysSQL:    sqlutils.MakeSQLRunner(cluster.ServerConn(destNodeIdx)),
		DestSysServer: cluster.Server(destNodeIdx).SystemLayer(),
		Rng:           rng,
	}
	if args.SrcTenantID.IsSystem() {
		tsc.SrcTenantServer = tsc.SrcSysServer
		tsc.SrcTenantConn = tsc.SrcCluster.ServerConn(0)
		tsc.SrcTenantSQL = tsc.SrcSysSQL
	} else {
		tsc.setupSrcTenant()
	}
	tsc.init(ctx)

	return tsc, func() {
		require.NoError(t, tsc.SrcTenantConn.Close())
		cleanup()
	}
}

func CreateTenantStreamingClusters(
	ctx context.Context, t *testing.T, args TenantStreamingClustersArgs,
) (*TenantStreamingClusters, func()) {
	serverArgs := CreateServerArgs(args)

	g := ctxgroup.WithContext(ctx)

	var srcCluster *testcluster.TestCluster
	var srcURL url.URL
	var srcCleanup func()
	g.GoCtx(func(ctx context.Context) error {
		// Start the source cluster.
		srcCluster, srcURL, srcCleanup = startC2CTestCluster(ctx, t, serverArgs, args.SrcNumNodes, args.SrcClusterTestRegions)
		return nil
	})

	var destCluster *testcluster.TestCluster
	var destCleanup func()
	g.GoCtx(func(ctx context.Context) error {
		// Start the destination cluster.
		destCluster, _, destCleanup = startC2CTestCluster(ctx, t, serverArgs, args.DestNumNodes, args.DestClusterTestRegions)
		return nil
	})

	require.NoError(t, g.Wait())
	rng, _ := randutil.NewPseudoRand()

	tsc := &TenantStreamingClusters{
		T:             t,
		Args:          args,
		SrcCluster:    srcCluster,
		SrcSysSQL:     sqlutils.MakeSQLRunner(srcCluster.ServerConn(0)),
		SrcSysServer:  srcCluster.Server(0).SystemLayer(),
		SrcURL:        srcURL,
		SrcCleanup:    srcCleanup,
		DestCluster:   destCluster,
		DestSysSQL:    sqlutils.MakeSQLRunner(destCluster.ServerConn(0)),
		DestSysServer: destCluster.Server(0).SystemLayer(),
		Rng:           rng,
	}
	tsc.setupSrcTenant()
	tsc.init(ctx)

	return tsc, func() {
		require.NoError(t, tsc.SrcTenantConn.Close())
		srcCleanup()
		destCleanup()
	}
}

func (c *TenantStreamingClusters) SrcExec(exec srcInitExecFunc) {
	exec(c.T, c.SrcSysSQL, c.SrcTenantSQL)
}

func WaitUntilStartTimeReached(t *testing.T, db *sqlutils.SQLRunner, ingestionJobID jobspb.JobID) {
	timeout := 45 * time.Second
	if skip.Stress() || util.RaceEnabled {
		timeout *= 5
	}
	testutils.SucceedsWithin(t, func() error {
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

		return requireReplicatedTime(startTime, jobutils.GetJobProgress(t, db, ingestionJobID))
	}, timeout)
}

func WaitUntilReplicatedTime(
	t *testing.T, targetTime hlc.Timestamp, db *sqlutils.SQLRunner, ingestionJobID jobspb.JobID,
) {
	testutils.SucceedsSoon(t, func() error {
		err := requireReplicatedTime(targetTime, jobutils.GetJobProgress(t, db, ingestionJobID))
		if err == nil {
			return nil
		}
		// Check the job status to see if there is still anything to be waiting for.
		jobStatus := db.QueryStr(t, "SELECT status, error FROM [SHOW JOB $1]", ingestionJobID)
		if len(jobStatus) > 0 {
			// Include job status in the error in case it is useful.
			err = errors.Wrapf(err, "job status %s %s", jobStatus[0][0], jobStatus[0][1])
			// Don't wait for an advance that is never happening if paused or failed.
			if jobStatus[0][0] == string(jobs.StatusPaused) || jobStatus[0][0] == string(jobs.StatusFailed) {
				t.Fatal(err)
			}
		}
		return err
	})
}

func requireReplicatedTime(targetTime hlc.Timestamp, progress *jobspb.Progress) error {
	replicatedTime := replicationutils.ReplicatedTimeFromProgress(progress)
	if replicatedTime.IsEmpty() {
		return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
			targetTime)
	}
	if replicatedTime.Less(targetTime) {
		return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
			replicatedTime, targetTime)
	}
	return nil
}

func CreateScatteredTable(t *testing.T, c *TenantStreamingClusters, numNodes int) {
	// Create a source table with multiple ranges spread across multiple nodes. We
	// need around 50 or more ranges because there are already over 50 system
	// ranges, so if we write just a few ranges those might all be on a single
	// server, which will cause the test to flake.
	numRanges := 50
	rowsPerRange := 20
	c.SrcTenantSQL.Exec(t, "CREATE TABLE d.scattered (key INT PRIMARY KEY)")
	c.SrcTenantSQL.Exec(t, "INSERT INTO d.scattered (key) SELECT * FROM generate_series(1, $1)",
		numRanges*rowsPerRange)
	c.SrcTenantSQL.Exec(t, "ALTER TABLE d.scattered SPLIT AT (SELECT * FROM generate_series($1::INT, $2::INT, $3::INT))",
		rowsPerRange, (numRanges-1)*rowsPerRange, rowsPerRange)
	c.SrcTenantSQL.Exec(t, "ALTER TABLE d.scattered SCATTER")
	timeout := 45 * time.Second
	if skip.Duress() {
		timeout *= 5
	}
	testutils.SucceedsWithin(t, func() error {
		var leaseHolderCount int
		c.SrcTenantSQL.QueryRow(t,
			`SELECT count(DISTINCT lease_holder) FROM [SHOW RANGES FROM DATABASE d WITH DETAILS]`).
			Scan(&leaseHolderCount)
		require.Greater(t, leaseHolderCount, 0)
		if leaseHolderCount < numNodes {
			return errors.New("leaseholders not scattered yet")
		}
		return nil
	}, timeout)
}

var defaultSrcClusterSetting = map[string]string{
	`kv.rangefeed.enabled`: `true`,
	// Speed up the rangefeed. These were set by squinting at the settings set in
	// the changefeed integration tests.
	`kv.closed_timestamp.target_duration`:            `'100ms'`,
	`kv.rangefeed.closed_timestamp_refresh_interval`: `'200ms'`,
	`kv.closed_timestamp.side_transport_interval`:    `'50ms'`,
	// Large timeout makes test to not fail with unexpected timeout failures.
	`stream_replication.stream_liveness_track_frequency`: `'2s'`,
	`stream_replication.min_checkpoint_frequency`:        `'1s'`,
	// Make all AddSSTable operation to trigger AddSSTable events.
	`kv.bulk_io_write.small_write_size`: `'1'`,
	`jobs.registry.interval.adopt`:      `'1s'`,
	// Speed up span reconciliation
	`spanconfig.reconciliation_job.checkpoint_interval`: `'100ms'`,
}

var defaultDestClusterSetting = map[string]string{
	`stream_replication.consumer_heartbeat_frequency`:      `'1s'`,
	`stream_replication.job_checkpoint_frequency`:          `'100ms'`,
	`bulkio.stream_ingestion.minimum_flush_interval`:       `'10ms'`,
	`bulkio.stream_ingestion.cutover_signal_poll_interval`: `'100ms'`,
	`jobs.registry.interval.adopt`:                         `'1s'`,
	`spanconfig.reconciliation_job.checkpoint_interval`:    `'100ms'`,
	`kv.rangefeed.enabled`:                                 `true`,
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

	stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, sqlRunner, int(tenantInfo.PhysicalReplicationConsumerJobID))
	return int(stats.IngestionDetails.StreamID), int(tenantInfo.PhysicalReplicationConsumerJobID)
}

func SSTMaker(t *testing.T, keyValues []roachpb.KeyValue) kvpb.RangeFeedSSTable {
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i].Key.Compare(keyValues[j].Key) < 0
	})
	batchTS := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	kvs := make(storageutils.KVs, 0, len(keyValues))
	for i, keyVal := range keyValues {
		if i > 0 && keyVal.Key.Equal(keyValues[i-1].Key) {
			continue
		}
		kvs = append(kvs, storage.MVCCKeyValue{
			Key: storage.MVCCKey{
				Key:       keyVal.Key,
				Timestamp: batchTS,
			},
			Value: keyVal.Value.RawBytes,
		})
	}
	data, start, end := storageutils.MakeSST(t, cluster.MakeTestingClusterSettings(), kvs)
	return kvpb.RangeFeedSSTable{
		Data: data,
		Span: roachpb.Span{
			Key:    start,
			EndKey: end,
		},
		WriteTS: batchTS,
	}
}
