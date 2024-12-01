// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var localityCfgs = map[string]roachpb.Locality{
	"us-east-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-east1"},
		},
	},
	"us-west-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-west-1"},
			{Key: "availability-zone", Value: "us-west1"},
		},
	},
	"eu-central-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "eu-central-1"},
			{Key: "availability-zone", Value: "eu-central-1"},
		},
	},
	"eu-north-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "eu-north-1"},
			{Key: "availability-zone", Value: "eu-north-1"},
		},
	},
}

var clusterVersionKeys = map[string]clusterversion.Key{
	"latest":           clusterversion.Latest,
	"previous-release": clusterversion.PreviousRelease,
}

type sqlDBKey struct {
	name string
	vc   string
	user string
}

type datadrivenTestState struct {
	// cluster maps the user defined cluster name to its cluster
	clusters map[string]serverutils.TestClusterInterface

	// firstNode maps the cluster name to the first node in the cluster
	firstNode         map[string]serverutils.TestServerInterface
	dataDirs          map[string]string
	sqlDBs            map[sqlDBKey]*gosql.DB
	jobTags           map[string]jobspb.JobID
	clusterTimestamps map[string]string
	noticeBuffer      []string
	cleanupFns        []func()
	vars              map[string]string
}

func newDatadrivenTestState() datadrivenTestState {
	return datadrivenTestState{
		clusters:          make(map[string]serverutils.TestClusterInterface),
		firstNode:         make(map[string]serverutils.TestServerInterface),
		dataDirs:          make(map[string]string),
		sqlDBs:            make(map[sqlDBKey]*gosql.DB),
		jobTags:           make(map[string]jobspb.JobID),
		clusterTimestamps: make(map[string]string),
		vars:              make(map[string]string),
	}
}

func (d *datadrivenTestState) cleanup(ctx context.Context, t *testing.T) {
	// While the testCluster cleanupFns would close the dbConn and clusters, close
	// them manually to ensure all queries finish on tests that share these
	// resources.
	for _, db := range d.sqlDBs {
		backuptestutils.CheckForInvalidDescriptors(t, db)
		db.Close()
	}
	for _, s := range d.firstNode {
		s.Stopper().Stop(ctx)
	}
	for _, f := range d.cleanupFns {
		f()
	}
	d.noticeBuffer = nil
}

type clusterCfg struct {
	name              string
	iodir             string
	nodes             int
	splits            int
	ioConf            base.ExternalIODirConfig
	localities        string
	beforeVersion     string
	testingKnobCfg    string
	defaultTestTenant base.DefaultTestTenantOptions
	randomTxnRetries  bool
}

func (d *datadrivenTestState) addCluster(t *testing.T, cfg clusterCfg) error {
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODirConfig = cfg.ioConf

	params.ServerArgs.DefaultTestTenant = cfg.defaultTestTenant
	var transactionRetryFilter func(roachpb.Transaction) bool
	if cfg.randomTxnRetries {
		transactionRetryFilter = kvclientutils.RandomTransactionRetryFilter()
	}
	params.ServerArgs.Knobs = base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		TenantTestingKnobs: &sql.TenantTestingKnobs{
			// The tests in this package are particular about the tenant IDs
			// they get in CREATE TENANT.
			EnableTenantIDReuse: true,
		},
		KVClient: &kvcoord.ClientTestingKnobs{
			TransactionRetryFilter: transactionRetryFilter,
		},
	}

	settings := cluster.MakeTestingClusterSettings()
	if cfg.beforeVersion != "" {
		settings = cluster.MakeClusterSettings()
		beforeKey, ok := clusterVersionKeys[cfg.beforeVersion]
		if !ok {
			t.Fatalf("clusterVersion %s does not exist in data driven global map", cfg.beforeVersion)
		}
		params.ServerArgs.Knobs.Server = &server.TestingKnobs{
			ClusterVersionOverride:         beforeKey.Version(),
			DisableAutomaticVersionUpgrade: make(chan struct{}),
		}
	}

	closedts.TargetDuration.Override(context.Background(), &settings.SV, 10*time.Millisecond)
	closedts.SideTransportCloseInterval.Override(context.Background(), &settings.SV, 10*time.Millisecond)
	kvserver.RangeFeedRefreshInterval.Override(context.Background(), &settings.SV, 10*time.Millisecond)
	sql.TempObjectWaitInterval.Override(context.Background(), &settings.SV, time.Millisecond)
	params.ServerArgs.Settings = settings

	clusterSize := cfg.nodes

	if cfg.localities != "" {
		cfgs := strings.Split(cfg.localities, ",")
		clusterSize = len(cfgs)
		serverArgsPerNode := make(map[int]base.TestServerArgs)
		for i, cfg := range cfgs {
			param := params.ServerArgs
			param.Locality = localityCfgs[cfg]
			serverArgsPerNode[i] = param
		}
		params.ServerArgsPerNode = serverArgsPerNode
	}
	if cfg.testingKnobCfg != "" {
		switch cfg.testingKnobCfg {
		default:
			t.Fatalf("TestingKnobCfg %s not found", cfg.testingKnobCfg)
		}
	}

	opts := []backuptestutils.BackupTestArg{
		backuptestutils.WithParams(params),
		backuptestutils.WithTempDir(cfg.iodir),
		// We can skip the check for invalid descriptors because we do it in
		// datadrivenTestState.cleanup explicitly, and having it in the cleanup
		// function returned by StartBackupRestoreTestCluster fails anyway since
		// we stop the first node before executing cleanupFns.
		backuptestutils.WithSkipInvalidDescriptorCheck(),
	}
	if cfg.iodir == "" {
		opts = append(opts, backuptestutils.WithBank(cfg.splits))
	}

	var tc serverutils.TestClusterInterface
	var cleanup func()
	tc, _, cfg.iodir, cleanup = backuptestutils.StartBackupRestoreTestCluster(t, clusterSize, opts...)
	d.clusters[cfg.name] = tc
	d.firstNode[cfg.name] = tc.Server(0)
	d.dataDirs[cfg.name] = cfg.iodir
	d.cleanupFns = append(d.cleanupFns, cleanup)

	return nil
}

func (d *datadrivenTestState) getIODir(t *testing.T, name string) string {
	dir, ok := d.dataDirs[name]
	if !ok {
		t.Fatalf("cluster %s does not exist", name)
	}
	return dir
}

func (d *datadrivenTestState) clearConnCache() {
	for _, db := range d.sqlDBs {
		db.Close()
	}
	d.sqlDBs = make(map[sqlDBKey]*gosql.DB)
}

func (d *datadrivenTestState) getSQLDB(t *testing.T, name string, user string) *gosql.DB {
	return d.getSQLDBForVC(t, name, "default", user)
}

func (d *datadrivenTestState) getSQLDBForVC(
	t *testing.T, name string, vc string, user string,
) *gosql.DB {
	key := sqlDBKey{name, vc, user}
	if db, ok := d.sqlDBs[key]; ok {
		return db
	}

	opts := []serverutils.SQLConnOption{
		serverutils.CertsDirPrefix("TestBackupRestoreDataDriven"),
		serverutils.User(user),
	}

	s := d.firstNode[name].ApplicationLayer()
	switch vc {
	case "default":
		// Nothing to do.
	case "system":
		// We use the system layer since in the case of
		// external SQL server's the application layer can't
		// route to the system tenant.
		s = d.firstNode[name].SystemLayer()
	default:
		opts = append(opts, serverutils.DBName("cluster:"+vc))
	}

	pgURL, cleanup := s.PGUrl(t, opts...)
	d.cleanupFns = append(d.cleanupFns, cleanup)

	base, err := pq.NewConnector(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	connector := pq.ConnectorWithNoticeHandler(base, func(notice *pq.Error) {
		d.noticeBuffer = append(d.noticeBuffer, notice.Severity+": "+notice.Message)
		if notice.Detail != "" {
			d.noticeBuffer = append(d.noticeBuffer, "DETAIL: "+notice.Detail)
		}
		if notice.Hint != "" {
			d.noticeBuffer = append(d.noticeBuffer, "HINT: "+notice.Hint)
		}
	})
	d.sqlDBs[key] = gosql.OpenDB(connector)
	return d.sqlDBs[key]
}

// TestDataDriven is a datadriven test to test standard backup/restore
// interactions involving setting up clusters and running different SQL
// commands. The test files are in testdata/backup-restore. The following
// syntax is provided:
//
//   - "new-cluster name=<name> [args]"
//     Create a new cluster with the input name.
//
//     Supported arguments:
//
//   - share-io-dir: can be specified to share an IO directory with an existing
//     cluster. This is useful when restoring from a backup taken in another
//     cluster.
//
//   - allow-implicit-access: can be specified to set
//     `EnableNonAdminImplicitAndArbitraryOutbound` to true
//
//   - disable-http: disables use of external HTTP endpoints.
//
//   - localities: specifies the localities that will be used when starting up
//     the test cluster. The cluster will have len(localities) nodes, with each
//     node assigned a locality config corresponding to the locality. Please
//     update the `localityCfgs` map when adding new localities.
//
//   - nodes: specifies the number of nodes in the test cluster.
//
//   - splits: specifies the number of ranges the bank table is split into.
//
//   - before-version=<beforeVersion>: bootstraps the test cluster and upgrades
//     it to a cluster version that is one version before the passed in
//     <beforeVersion> key. See cockroach_versions.go for possible values.
//
//   - testingKnobCfg: specifies a key to a hardcoded testingKnob configuration
//
//   - disable-tenant : ensures the test is never run in a multitenant environment by
//     setting testserverargs.DefaultTestTenant to base.TODOTestTenantDisabled.
//
//   - "upgrade-cluster version=<version>"
//     Upgrade the cluster version of the active cluster to the passed in
//     clusterVersion key. See cockroach_versions.go for possible values.
//
//   - "exec-sql [cluster=<name>] [user=<name>] [args]"
//     Executes the input SQL query on the target cluster. By default, cluster is
//     the last created cluster.
//
//     Supported arguments:
//
//   - expect-error-regex=<regex>: expects the query to return an error with a string
//     matching the provided regex
//
//   - expect-error-ignore: expects the query to return an error, but we will
//     ignore it.
//
//   - ignore-notice: does not print out the notice that is buffered during
//     query execution.
//
//   - "query-sql [cluster=<name>] [user=<name>] [regex=<regex pattern>]"
//     Executes the input SQL query and print the results.
//
//     Supported arguments:
//
//   - regex: return true if the query result matches the regex pattern and
//     false otherwise.
//
//   - "set-cluster-setting setting=<name> value=<name>"
//     Sets the cluster setting on all nodes and ensures all nodes in the test cluster
//     have seen the update.
//
//   - "reset"
//     Clear all state associated with the test.
//
//   - "job" [cluster=<name>] [user=<name>] [args]
//     Executes job specific operations.
//
//     Supported arguments:
//
//   - resume=<tag>: resumes the job referenced by the tag, use in conjunction
//     with wait-for-state.
//
//   - cancel=<tag>: cancels the job referenced by the tag and waits for it to
//     reach a CANCELED state.
//
//   - wait-for-state=<succeeded|paused|failed|cancelled> tag=<tag>: wait for
//     the job referenced by the tag to reach the specified state.
//
//   - "let" [args]
//     Assigns the returned value of the SQL query to the provided args as variables.
//
//   - "save-cluster-ts" tag=<tag>
//     Saves the `SELECT cluster_logical_timestamp()` with the tag. Can be used
//     in the future with intstructions such as `aost`.
//
//   - "backup" [args]
//     Executes backup specific operations.
//
//     Supported arguments:
//
//   - tag=<tag>: tag the backup job to reference it in the future.
//
//   - expect-pausepoint: expects the backup job to end up in a paused state because
//     of a pausepoint error.
//
//   - "restore" [args]
//     Executes restore specific operations.
//
//     Supported arguments:
//
//   - tag=<tag>: tag the restore job to reference it in the future.
//
//   - expect-pausepoint: expects the restore job to end up in a paused state because
//     of a pausepoint error.
//
//   - aost: expects a tag referencing a previously saved cluster timestamp
//     using `save-cluster-ts`. It then runs the restore as of the saved cluster
//     timestamp.
//
//   - "schema" [args]
//     Executes schema change specific operations.
//
//     Supported arguments:
//
//   - tag=<tag>: tag the schema change job to reference it in the future.
//
//   - expect-pausepoint: expects the schema change job to end up in a paused state because
//     of a pausepoint error.
//
//   - "kv" [args]
//     Issues a kv request
//
//     Supported arguments:
//
//   - type: kv request type. Currently, only DeleteRange is supported
//
//   - target: SQL target. Currently, only table names are supported.
//
//   - "corrupt-backup" uri=<collectionUri>
//     Finds the latest backup in the provided collection uri an flips a bit in one SST in the backup
//
//   - "link-backup" cluster=<cluster> src-path=<testDataPathRelative> dest-path=<fileIO path relative>
//     Creates a symlink from the testdata path to the file IO path, so that we
//     can restore precreated backup. src-path and dest-path are comma seperated
//     paths that will be joined.
//
//   - "sleep ms=TIME"
//     Sleep for TIME milliseconds.
//
//lint:ignore U1000 unused
func runTestDataDriven(t *testing.T, testFilePathFromWorkspace string) {
	// This test uses this mock HTTP server to pass the backup files between tenants.
	httpAddr, httpServerCleanup := makeInsecureHTTPServer(t)
	defer httpServerCleanup()

	ctx := context.Background()
	var path string
	// Runfile can't be generally implemented outside of Bazel - for testdata scripts, they will always
	// be relative to the test binary.
	if bazel.BuiltWithBazel() {
		var err error
		path, err = bazel.Runfile(testFilePathFromWorkspace)
		require.NoError(t, err)
	} else {
		idx := strings.Index(testFilePathFromWorkspace, "testdata")
		if idx == -1 {
			t.Fatalf("%q doesn't contain 'testdata' - can't run outside of Bazel", testFilePathFromWorkspace)
		}
		path = testFilePathFromWorkspace[idx:]
	}

	var lastCreatedCluster string
	ds := newDatadrivenTestState()
	defer ds.cleanup(ctx, t)
	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
		execWithTagAndPausePoint := func(jobType jobspb.Type) string {
			ds.noticeBuffer = nil
			const user = "root"
			sqlDB := ds.getSQLDB(t, lastCreatedCluster, user)

			_, err := sqlDB.Exec(d.Input)

			var jobID jobspb.JobID
			{
				const query = `SELECT id FROM system.jobs WHERE job_type = $1 ORDER BY created DESC LIMIT 1`
				errJob := sqlDB.QueryRow(query, jobType.String()).Scan(&jobID)
				if !errors.Is(errJob, gosql.ErrNoRows) {
					require.NoError(t, errJob)
				}
				require.NotZerof(t, jobID, "job not found for %q: %+v", d.Input, err)
			}

			// Tag the job.
			if d.HasArg("tag") {
				var jobTag string
				d.ScanArgs(t, "tag", &jobTag)
				if _, exists := ds.jobTags[jobTag]; exists {
					t.Fatalf("failed to `tag`, job with tag %s already exists", jobTag)
				}
				ds.jobTags[jobTag] = jobID
			}

			// Check if we expect a pausepoint error.
			if d.HasArg("expect-pausepoint") {
				// Check if we are expecting a pausepoint error.
				require.NotNilf(t, err, "expected pause point error")
				require.Regexp(t, "pause point .* hit$", err.Error())
				jobutils.WaitForJobToPause(t, sqlutils.MakeSQLRunner(sqlDB), jobID)
				ret := append(ds.noticeBuffer, "job paused at pausepoint")
				return strings.Join(ret, "\n")
			}

			// All other errors are bad.
			require.NoError(t, err)
			return ""
		}

		for v := range ds.vars {
			d.Input = strings.Replace(d.Input, v, ds.vars[v], -1)
			d.Expected = strings.Replace(d.Expected, v, ds.vars[v], -1)
		}
		switch d.Cmd {
		case "skip":
			var issue int
			d.ScanArgs(t, "issue-num", &issue)
			skip.WithIssue(t, issue)
			return ""

		case "skip-under-duress":
			skip.UnderDuress(t)
			return ""

		case "reset":
			ds.cleanup(ctx, t)
			ds = newDatadrivenTestState()
			if d.HasArg("test-nodelocal") {
				nodelocalCleanup := nodelocal.ReplaceNodeLocalForTesting(t.TempDir())
				ds.cleanupFns = append(ds.cleanupFns, nodelocalCleanup)
			}
			return ""
		case "clear-conn-cache":
			ds.clearConnCache()
			return ""
		case "new-cluster":
			var name, shareDirWith, iodir, localities, beforeVersion, testingKnobCfg string
			var splits int
			var defaultTestTenant base.DefaultTestTenantOptions
			nodes := singleNode
			var io base.ExternalIODirConfig
			d.ScanArgs(t, "name", &name)
			if d.HasArg("share-io-dir") {
				d.ScanArgs(t, "share-io-dir", &shareDirWith)
			}
			if shareDirWith != "" {
				iodir = ds.getIODir(t, shareDirWith)
			}
			if d.HasArg("allow-implicit-access") {
				io.EnableNonAdminImplicitAndArbitraryOutbound = true
			}
			if d.HasArg("disable-http") {
				io.DisableHTTP = true
			}
			if d.HasArg("localities") {
				d.ScanArgs(t, "localities", &localities)
			}
			if d.HasArg("nodes") {
				d.ScanArgs(t, "nodes", &nodes)
			}
			if d.HasArg("splits") {
				d.ScanArgs(t, "splits", &splits)
			}
			if d.HasArg("before-version") {
				d.ScanArgs(t, "before-version", &beforeVersion)
				if !d.HasArg("disable-tenant") {
					// TODO(msbutler): figure out why test tenants don't mix with version testing
					t.Fatal("tests that use before-version must use disable-tenant")
				}
			}
			if d.HasArg("testingKnobCfg") {
				d.ScanArgs(t, "testingKnobCfg", &testingKnobCfg)
			}
			if d.HasArg("disable-tenant") {
				defaultTestTenant = base.TODOTestTenantDisabled
			}

			// TODO(ssd): Once TestServer starts up reliably enough:
			// randomTxnRetries := !d.HasArg("disable-txn-retries")
			randomTxnRetries := false
			lastCreatedCluster = name
			cfg := clusterCfg{
				name:              name,
				iodir:             iodir,
				nodes:             nodes,
				splits:            splits,
				ioConf:            io,
				localities:        localities,
				beforeVersion:     beforeVersion,
				testingKnobCfg:    testingKnobCfg,
				defaultTestTenant: defaultTestTenant,
				randomTxnRetries:  randomTxnRetries,
			}
			err := ds.addCluster(t, cfg)
			if err != nil {
				return err.Error()
			}
			return ""

		case "switch-cluster":
			var name string
			d.ScanArgs(t, "name", &name)
			lastCreatedCluster = name
			return ""

		case "upgrade-cluster":
			cluster := lastCreatedCluster
			user := "root"

			var version string
			if d.HasArg("version") {
				d.ScanArgs(t, "version", &version)
			}
			key, ok := clusterVersionKeys[version]
			if !ok {
				t.Fatalf("clusterVersion %s does not exist in data driven global map", version)
			}
			clusterVersion := key.Version()
			_, err := ds.getSQLDB(t, cluster, user).Exec("SET CLUSTER SETTING version = $1", clusterVersion.String())
			require.NoError(t, err)
			return ""

		case "exec-sql":
			cluster := lastCreatedCluster
			vc := "default"
			user := "root"
			if d.HasArg("cluster") {
				d.ScanArgs(t, "cluster", &cluster)
			}
			if d.HasArg("user") {
				d.ScanArgs(t, "user", &user)
			}
			if d.HasArg("vc") {
				d.ScanArgs(t, "vc", &vc)
			}

			ds.noticeBuffer = nil
			checkForClusterSetting(t, d.Input, ds.clusters[cluster].NumServers())
			d.Input = strings.ReplaceAll(d.Input, "http://COCKROACH_TEST_HTTP_server/", httpAddr)
			_, err := ds.getSQLDBForVC(t, cluster, vc, user).Exec(d.Input)
			ret := ds.noticeBuffer

			if d.HasArg("ignore-notice") {
				ret = nil
			}

			// Check if we are expecting a pausepoint error.
			if d.HasArg("expect-pausepoint") {
				require.NotNilf(t, err, "expected pause point error")
				require.True(t, strings.Contains(err.Error(), "job requested it be paused"))

				// Find job ID of the pausepoint job.
				var jobID jobspb.JobID
				require.NoError(t,
					ds.getSQLDB(t, cluster, user).QueryRow(
						`SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID))
				fmt.Printf("expecting pausepoint, found job ID %d\n\n\n", jobID)

				runner := sqlutils.MakeSQLRunner(ds.getSQLDB(t, cluster, user))
				jobutils.WaitForJobToPause(t, runner, jobID)
				ret = append(ds.noticeBuffer, "job paused at pausepoint")
				ret = append(ret, "")
				return strings.Join(ret, "\n")
			}

			// Check if we are expecting an error, and want to ignore outputting it.
			if d.HasArg("expect-error-ignore") {
				require.NotNilf(t, err, "expected error")
				ret = append(ret, "ignoring expected error")
				return strings.Join(ret, "\n")
			}

			// Check if we are expecting an error, and want to match it against a
			// regex.
			if d.HasArg("expect-error-regex") {
				require.NotNilf(t, err, "expected error")
				var expectErrorRegex string
				d.ScanArgs(t, "expect-error-regex", &expectErrorRegex)
				require.True(t,
					testutils.IsError(err, expectErrorRegex),
					"Regex `%s` did not match `%s`",
					expectErrorRegex,
					err)
				ret = append(ret, "regex matches error")
				return strings.Join(ret, "\n")
			}

			// Check for other errors.
			if err != nil {
				if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
					ret = append(ds.noticeBuffer, err.Error())
					if pqErr.Detail != "" {
						ret = append(ret, "DETAIL: "+pqErr.Detail)
					}
					if pqErr.Hint != "" {
						ret = append(ret, "HINT: "+pqErr.Hint)
					}
				} else {
					t.Errorf("failed to execute stmt %s due to %s", d.Input, err.Error())
				}
			}
			return strings.Join(ret, "\n")

		case "query-sql":
			cluster := lastCreatedCluster
			user := "root"
			if d.HasArg("cluster") {
				d.ScanArgs(t, "cluster", &cluster)
			}
			if d.HasArg("user") {
				d.ScanArgs(t, "user", &user)
			}
			checkForClusterSetting(t, d.Input, ds.clusters[cluster].NumServers())
			if d.HasArg("retry") {
				var eventualOutput string
				testutils.SucceedsSoon(t, func() error {
					rows, err := ds.getSQLDB(t, cluster, user).Query(d.Input)
					if err != nil {
						return err
					}
					output, err := sqlutils.RowsToDataDrivenOutput(rows)
					require.NoError(t, err)
					if output != d.Expected {
						return errors.Newf("latest output: %s\n expected: %s", output, d.Expected)
					}
					eventualOutput = output
					return nil
				})
				return eventualOutput
			}
			rows, err := ds.getSQLDB(t, cluster, user).Query(d.Input)
			if err != nil {
				return err.Error()
			}
			output, err := sqlutils.RowsToDataDrivenOutput(rows)
			require.NoError(t, err)
			if d.HasArg("regex") {
				var pattern string
				d.ScanArgs(t, "regex", &pattern)
				matched, err := regexp.MatchString(pattern, output)
				require.NoError(t, err)
				if matched {
					return "true"
				}
				return "false"
			}
			return output
		case "set-cluster-setting":
			var setting, value string
			d.ScanArgs(t, "setting", &setting)
			d.ScanArgs(t, "value", &value)
			cluster := lastCreatedCluster
			serverutils.SetClusterSetting(t, ds.clusters[cluster], setting, value)
			return ""

		case "let":
			cluster := lastCreatedCluster
			user := "root"
			if len(d.CmdArgs) == 0 {
				t.Fatalf("Must specify at least one variable name.")
			}
			rows, err := ds.getSQLDB(t, cluster, user).Query(d.Input)
			if err != nil {
				return err.Error()
			}
			output, err := sqlutils.RowsToDataDrivenOutput(rows)
			output = strings.TrimSpace(output)
			values := strings.Split(output, "\n")
			if len(values) != len(d.CmdArgs) {
				t.Fatalf("Expecting %d vars, found %d", len(d.CmdArgs), len(values))
			}
			for i := range values {
				key := d.CmdArgs[i].Key
				if !strings.HasPrefix(key, "$") {
					t.Fatalf("Vars must start with `$`.")
				}
				ds.vars[key] = values[i]
			}
			require.NoError(t, err)

			return ""

		case "sleep":
			var msStr string
			if d.HasArg("ms") {
				d.ScanArgs(t, "ms", &msStr)
			} else {
				t.Fatalf("must specify sleep time in ms")
			}
			ms, err := strconv.ParseInt(msStr, 10, 64)
			if err != nil {
				t.Fatalf("invalid sleep time: %v", err)
			}
			time.Sleep(time.Duration(ms) * time.Millisecond)
			return ""

		case "backup":
			return execWithTagAndPausePoint(jobspb.TypeBackup)

		case "import":
			return execWithTagAndPausePoint(jobspb.TypeImport)

		case "restore":
			if d.HasArg("aost") {
				var aost string
				d.ScanArgs(t, "aost", &aost)
				var ts string
				var ok bool
				if ts, ok = ds.clusterTimestamps[aost]; !ok {
					t.Fatalf("no cluster timestamp found for %s", aost)
				}

				// Replace the ts tag with the actual timestamp.
				d.Input = strings.Replace(d.Input, aost,
					fmt.Sprintf("'%s'", ts), 1)
			}
			return execWithTagAndPausePoint(jobspb.TypeRestore)

		case "new-schema-change":
			return execWithTagAndPausePoint(jobspb.TypeNewSchemaChange)

		case "schema-change":
			return execWithTagAndPausePoint(jobspb.TypeSchemaChange)

		case "job":
			cluster := lastCreatedCluster
			const user = "root"

			if d.HasArg("cancel") {
				var cancelJobTag string
				d.ScanArgs(t, "cancel", &cancelJobTag)
				var jobID jobspb.JobID
				var ok bool
				if jobID, ok = ds.jobTags[cancelJobTag]; !ok {
					t.Fatalf("could not find job with tag %s", cancelJobTag)
				}
				runner := sqlutils.MakeSQLRunner(ds.getSQLDB(t, cluster, user))
				runner.Exec(t, `CANCEL JOB $1`, jobID)
				jobutils.WaitForJobToCancel(t, runner, jobID)
			} else if d.HasArg("resume") {
				var resumeJobTag string
				d.ScanArgs(t, "resume", &resumeJobTag)
				var jobID jobspb.JobID
				var ok bool
				if jobID, ok = ds.jobTags[resumeJobTag]; !ok {
					t.Fatalf("could not find job with tag %s", resumeJobTag)
				}
				runner := sqlutils.MakeSQLRunner(ds.getSQLDB(t, cluster, user))
				runner.Exec(t, `RESUME JOB $1`, jobID)
			} else if d.HasArg("wait-for-state") {
				var tag string
				d.ScanArgs(t, "tag", &tag)
				var jobID jobspb.JobID
				var ok bool
				if jobID, ok = ds.jobTags[tag]; !ok {
					t.Fatalf("could not find job with tag %s", tag)
				}
				runner := sqlutils.MakeSQLRunner(ds.getSQLDB(t, cluster, user))
				var state string
				d.ScanArgs(t, "wait-for-state", &state)
				switch state {
				case "succeeded":
					jobutils.WaitForJobToSucceed(t, runner, jobID)
				case "cancelled":
					jobutils.WaitForJobToCancel(t, runner, jobID)
				case "paused":
					jobutils.WaitForJobToPause(t, runner, jobID)
				case "failed":
					jobutils.WaitForJobToFail(t, runner, jobID)
				case "reverting":
					jobutils.WaitForJobReverting(t, runner, jobID)
				default:
					t.Fatalf("unknown state %s", state)
				}
			}
			return ""

		case "kv":
			var request string
			d.ScanArgs(t, "request", &request)

			var target string
			d.ScanArgs(t, "target", &target)
			handleKVRequest(ctx, t, lastCreatedCluster, ds, request, target)
			return ""

		case "save-cluster-ts":
			cluster := lastCreatedCluster
			const user = "root"
			var timestampTag string
			d.ScanArgs(t, "tag", &timestampTag)
			if _, ok := ds.clusterTimestamps[timestampTag]; ok {
				t.Fatalf("cannot reuse cluster ts tag %s", timestampTag)
			}
			var ts string
			err := ds.getSQLDB(t, cluster, user).QueryRow(`SELECT cluster_logical_timestamp()`).Scan(&ts)
			require.NoError(t, err)
			ds.clusterTimestamps[timestampTag] = ts
			return ""

		case "create-dummy-system-table":
			al := ds.firstNode[lastCreatedCluster].ApplicationLayer()
			db := al.DB()
			execCfg := al.ExecutorConfig().(sql.ExecutorConfig)
			codec := execCfg.Codec
			dummyTable := systemschema.SettingsTable
			err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				id, err := execCfg.DescIDGenerator.GenerateUniqueDescID(ctx)
				if err != nil {
					return err
				}
				mut := dummyTable.NewBuilder().BuildCreatedMutable().(*tabledesc.Mutable)
				mut.ID = id
				mut.Name = fmt.Sprintf("%s_%d", "crdb_internal_copy", id)
				tKey := catalogkeys.EncodeNameKey(codec, mut)
				b := txn.NewBatch()
				b.CPut(tKey, mut.GetID(), nil)
				b.CPut(catalogkeys.MakeDescMetadataKey(codec, mut.GetID()), mut.DescriptorProto(), nil)
				return txn.Run(ctx, b)
			})
			require.NoError(t, err)
			return ""

		case "corrupt-backup":
			cluster := lastCreatedCluster
			const user = "root"
			var uri string
			d.ScanArgs(t, "uri", &uri)
			parsedURI, err := url.Parse(strings.Replace(uri, "'", "", -1))
			require.NoError(t, err)
			var filePath string
			filePathQuery := fmt.Sprintf("SELECT path FROM [SHOW BACKUP FILES FROM LATEST IN %s] LIMIT 1", uri)
			err = ds.getSQLDB(t, cluster, user).QueryRow(filePathQuery).Scan(&filePath)
			require.NoError(t, err)
			fullPath := filepath.Join(ds.getIODir(t, cluster), parsedURI.Path, filePath)
			data, err := os.ReadFile(fullPath)
			require.NoError(t, err)
			data[20] ^= 1
			if err := os.WriteFile(fullPath, data, 0644 /* perm */); err != nil {
				t.Fatal(err)
			}
			return ""
		case "link-backup":
			cluster := lastCreatedCluster
			sourceRelativePath := ""
			destRelativePath := ""
			ioDir := ds.getIODir(t, cluster)
			d.ScanArgs(t, "cluster", &cluster)
			d.ScanArgs(t, "src-path", &sourceRelativePath)
			d.ScanArgs(t, "dest-path", &destRelativePath)
			splitSrcPath := strings.Split(sourceRelativePath, ",")
			sourcePath, err := filepath.Abs(datapathutils.TestDataPath(t, splitSrcPath...))
			require.NoError(t, err)
			splitDestPath := strings.Split(destRelativePath, ",")
			destPath := filepath.Join(ioDir, filepath.Join(splitDestPath...))
			require.NoError(t, err)
			require.NoError(t, os.Symlink(sourcePath, destPath))
			return ""
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func handleKVRequest(
	ctx context.Context, t *testing.T, cluster string, ds datadrivenTestState, request, target string,
) {
	user := "root"
	if request == "DeleteRange" {
		var tableID uint32
		err := ds.getSQLDB(t, cluster, user).QueryRow(`SELECT id FROM system.namespace WHERE name = $1`,
			target).Scan(&tableID)
		require.NoError(t, err)
		bankSpan := makeTableSpan(keys.SystemSQLCodec, tableID)
		dr := kvpb.DeleteRangeRequest{
			// Bogus span to make it a valid request.
			RequestHeader: kvpb.RequestHeader{
				Key:    bankSpan.Key,
				EndKey: bankSpan.EndKey,
			},
			UseRangeTombstone: true,
		}
		if _, err := kv.SendWrapped(ctx, ds.firstNode[cluster].SystemLayer().DistSenderI().(*kvcoord.DistSender), &dr); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatalf("Unknown kv request")
	}
}

func checkForClusterSetting(t *testing.T, stmt string, numNodes int) {
	if numNodes != 1 && strings.Contains(stmt, "SET CLUSTER SETTING") {
		t.Fatal("You are attempting to set a cluster setting in a multi node cluster. " +
			"Use the 'set-cluster-setting' dd cmd instead to ensure the setting propagates to all nodes" +
			" during the cmd.")
	}
}
