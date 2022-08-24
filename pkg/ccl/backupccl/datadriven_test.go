// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	"Start22_2": clusterversion.Start22_2,
}

type sqlDBKey struct {
	server string
	user   string
}

type datadrivenTestState struct {
	servers           map[string]serverutils.TestServerInterface
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
		servers:           make(map[string]serverutils.TestServerInterface),
		dataDirs:          make(map[string]string),
		sqlDBs:            make(map[sqlDBKey]*gosql.DB),
		jobTags:           make(map[string]jobspb.JobID),
		clusterTimestamps: make(map[string]string),
		vars:              make(map[string]string),
	}
}

func (d *datadrivenTestState) cleanup(ctx context.Context) {
	for _, db := range d.sqlDBs {
		db.Close()
	}
	for _, s := range d.servers {
		s.Stopper().Stop(ctx)
	}
	for _, f := range d.cleanupFns {
		f()
	}
	d.noticeBuffer = nil
}

type serverCfg struct {
	name           string
	iodir          string
	nodes          int
	splits         int
	ioConf         base.ExternalIODirConfig
	localities     string
	beforeVersion  string
	testingKnobCfg string
}

func (d *datadrivenTestState) addServer(t *testing.T, cfg serverCfg) error {
	var tc serverutils.TestClusterInterface
	var cleanup func()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODirConfig = cfg.ioConf
	params.ServerArgs.Knobs = base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	settings := cluster.MakeTestingClusterSettings()

	if cfg.beforeVersion != "" {
		beforeKey, ok := clusterVersionKeys[cfg.beforeVersion]
		if !ok {
			t.Fatalf("clusterVersion %s does not exist in data driven global map", cfg.beforeVersion)
		}
		beforeKey--
		params.ServerArgs.Knobs.Server = &server.TestingKnobs{
			BinaryVersionOverride:          clusterversion.ByKey(beforeKey),
			DisableAutomaticVersionUpgrade: make(chan struct{})}
		settings = cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.TestingBinaryVersion,
			clusterversion.ByKey(beforeKey),
			false,
		)
	}

	closedts.TargetDuration.Override(context.Background(), &settings.SV, 10*time.Millisecond)
	closedts.SideTransportCloseInterval.Override(context.Background(), &settings.SV, 10*time.Millisecond)
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
		case "RecoverFromIterPanic":
			params.ServerArgs.Knobs.DistSQL = &execinfra.TestingKnobs{
				BackupRestoreTestingKnobs: &sql.BackupRestoreTestingKnobs{
					RecoverFromIterPanic: true,
				},
			}
		default:
			t.Fatalf("TestingKnobCfg %s not found", cfg.testingKnobCfg)
		}
	}
	if cfg.iodir == "" {
		tc, _, cfg.iodir, cleanup = backupRestoreTestSetupWithParams(t, clusterSize, cfg.splits,
			InitManualReplication, params)
	} else {
		tc, _, cleanup = backupRestoreTestSetupEmptyWithParams(t, clusterSize, cfg.iodir,
			InitManualReplication, params)
	}
	cleanupFn := func() {
		cleanup()
	}
	d.servers[cfg.name] = tc.Server(0)
	d.dataDirs[cfg.name] = cfg.iodir
	d.cleanupFns = append(d.cleanupFns, cleanupFn)

	return nil
}

func (d *datadrivenTestState) getIODir(t *testing.T, server string) string {
	dir, ok := d.dataDirs[server]
	if !ok {
		t.Fatalf("server %s does not exist", server)
	}
	return dir
}

func (d *datadrivenTestState) getSQLDB(t *testing.T, server string, user string) *gosql.DB {
	key := sqlDBKey{server, user}
	if db, ok := d.sqlDBs[key]; ok {
		return db
	}
	addr := d.servers[server].ServingSQLAddr()
	pgURL, cleanup := sqlutils.PGUrl(t, addr, "TestBackupRestoreDataDriven", url.User(user))
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
// - "new-server name=<name> [args]"
//   Create a new server with the input name.
//
//   Supported arguments:
//
//   + share-io-dir: can be specified to share an IO directory with an existing
//   server. This is useful when restoring from a backup taken in another
//   server.
//
//   + allow-implicit-access: can be specified to set
//   `EnableNonAdminImplicitAndArbitraryOutbound` to true
//
//   + disable-http: disables use of external HTTP endpoints.
//
//   + localities: specifies the localities that will be used when starting up
//   the test cluster. The cluster will have len(localities) nodes, with each
//   node assigned a locality config corresponding to the locality. Please
//   update the `localityCfgs` map when adding new localities.
//
//   + nodes: specifies the number of nodes in the test cluster.
//
//   + splits: specifies the number of ranges the bank table is split into.
//
//   + before-version=<beforeVersion>: creates a mixed version cluster where all
//   nodes running the test server binary think the clusterVersion is one
//   version before the passed in <beforeVersion> key. See cockroach_versions.go
//   for possible values.
//
//   + testingKnobCfg: specifies a key to a hardcoded testingKnob configuration
//
//
// - "upgrade-server version=<version>"
//    Upgrade the cluster version of the active server to the passed in
//    clusterVersion key. See cockroach_versions.go for possible values.
//
//
// - "exec-sql [server=<name>] [user=<name>] [args]"
//   Executes the input SQL query on the target server. By default, server is
//   the last created server.
//
//   Supported arguments:
//
//   + expect-error-regex=<regex>: expects the query to return an error with a string
//   matching the provided regex
//
//   + expect-error-ignore: expects the query to return an error, but we will
//   ignore it.
//
//   + ignore-notice: does not print out the notice that is buffered during
//   query execution.
//
// - "query-sql [server=<name>] [user=<name>] [regex=<regex pattern>]"
//   Executes the input SQL query and print the results.
//
//   + regex: return true if the query result matches the regex pattern and
//   false otherwise.
//
// - "reset"
//    Clear all state associated with the test.
//
// - "job" [server=<name>] [user=<name>] [args]
//   Executes job specific operations.
//
//   Supported arguments:
//
//   + resume=<tag>: resumes the job referenced by the tag, use in conjunction
//   with wait-for-state.
//
//   + cancel=<tag>: cancels the job referenced by the tag and waits for it to
//   reach a CANCELED state.
//
//   + wait-for-state=<succeeded|paused|failed|cancelled> tag=<tag>: wait for
//   the job referenced by the tag to reach the specified state.
//
// - "let" [args]
//   Assigns the returned value of the SQL query to the provided args as variables.
//
// - "save-cluster-ts" tag=<tag>
//   Saves the `SELECT cluster_logical_timestamp()` with the tag. Can be used
//   in the future with intstructions such as `aost`.
//
// - "backup" [args]
//   Executes backup specific operations.
//
//   Supported arguments:
//
//   + tag=<tag>: tag the backup job to reference it in the future.
//
//   + expect-pausepoint: expects the backup job to end up in a paused state because
//   of a pausepoint error.
//
// - "restore" [args]
//   Executes restore specific operations.
//
//   Supported arguments:
//
//   + tag=<tag>: tag the restore job to reference it in the future.
//
//   + expect-pausepoint: expects the restore job to end up in a paused state because
//   of a pausepoint error.
//
//   + aost: expects a tag referencing a previously saved cluster timestamp
//   using `save-cluster-ts`. It then runs the restore as of the saved cluster
//   timestamp.
//
// - "schema" [args]
//   Executes schema change specific operations.
//
//   Supported arguments:
//
//   + tag=<tag>: tag the schema change job to reference it in the future.
//
//   + expect-pausepoint: expects the schema change job to end up in a paused state because
//   of a pausepoint error.
//
// - "kv" [args]
//   Issues a kv request
//
//   Supported arguments:
//
//   + type: kv request type. Currently, only DeleteRange is supported
//
//   + target: SQL target. Currently, only table names are supported.
//
//
// - "corrupt-backup" uri=<collectionUri>
//   Finds the latest backup in the provided collection uri an flips a bit in one SST in the backup
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes ~3mins to run")

	// This test uses this mock HTTP server to pass the backup files between tenants.
	httpAddr, httpServerCleanup := makeInsecureHTTPServer(t)
	defer httpServerCleanup()

	ctx := context.Background()
	datadriven.Walk(t, testutils.TestDataPath(t, "backup-restore"), func(t *testing.T, path string) {
		var lastCreatedServer string
		ds := newDatadrivenTestState()
		defer ds.cleanup(ctx)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {

			for v := range ds.vars {
				d.Input = strings.Replace(d.Input, v, ds.vars[v], -1)
				d.Expected = strings.Replace(d.Expected, v, ds.vars[v], -1)
			}
			switch d.Cmd {
			case "reset":
				ds.cleanup(ctx)
				ds = newDatadrivenTestState()
				return ""

			case "new-server":
				var name, shareDirWith, iodir, localities, beforeVersion, testingKnobCfg string
				var splits int
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
				if d.HasArg("beforeVersion") {
					d.ScanArgs(t, "beforeVersion", &beforeVersion)
				}
				if d.HasArg("testingKnobCfg") {
					d.ScanArgs(t, "testingKnobCfg", &testingKnobCfg)
				}

				lastCreatedServer = name
				cfg := serverCfg{
					name:           name,
					iodir:          iodir,
					nodes:          nodes,
					splits:         splits,
					ioConf:         io,
					localities:     localities,
					beforeVersion:  beforeVersion,
					testingKnobCfg: testingKnobCfg,
				}
				err := ds.addServer(t, cfg)
				if err != nil {
					return err.Error()
				}
				return ""

			case "switch-server":
				var name string
				d.ScanArgs(t, "name", &name)
				lastCreatedServer = name
				return ""

			case "upgrade-server":
				server := lastCreatedServer
				user := "root"

				var version string
				if d.HasArg("version") {
					d.ScanArgs(t, "version", &version)
				}
				key, ok := clusterVersionKeys[version]
				if !ok {
					t.Fatalf("clusterVersion %s does not exist in data driven global map", version)
				}
				clusterVersion := clusterversion.ByKey(key)
				_, err := ds.getSQLDB(t, server, user).Exec("SET CLUSTER SETTING version = $1", clusterVersion.String())
				require.NoError(t, err)
				return ""

			case "exec-sql":
				server := lastCreatedServer
				user := "root"
				if d.HasArg("server") {
					d.ScanArgs(t, "server", &server)
				}
				if d.HasArg("user") {
					d.ScanArgs(t, "user", &user)
				}
				ds.noticeBuffer = nil
				d.Input = strings.ReplaceAll(d.Input, "http://COCKROACH_TEST_HTTP_SERVER/", httpAddr)
				_, err := ds.getSQLDB(t, server, user).Exec(d.Input)
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
						ds.getSQLDB(t, server, user).QueryRow(
							`SELECT job_id FROM [SHOW JOBS] ORDER BY created DESC LIMIT 1`).Scan(&jobID))
					fmt.Printf("expecting pausepoint, found job ID %d\n\n\n", jobID)

					runner := sqlutils.MakeSQLRunner(ds.getSQLDB(t, server, user))
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
					testutils.IsError(err, expectErrorRegex)
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
				server := lastCreatedServer
				user := "root"
				if d.HasArg("server") {
					d.ScanArgs(t, "server", &server)
				}
				if d.HasArg("user") {
					d.ScanArgs(t, "user", &user)
				}
				rows, err := ds.getSQLDB(t, server, user).Query(d.Input)
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

			case "let":
				server := lastCreatedServer
				user := "root"
				if len(d.CmdArgs) == 0 {
					t.Fatalf("Must specify at least one variable name.")
				}
				rows, err := ds.getSQLDB(t, server, user).Query(d.Input)
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

			case "backup":
				server := lastCreatedServer
				user := "root"
				jobType := "BACKUP"

				// First, run the backup.
				_, err := ds.getSQLDB(t, server, user).Exec(d.Input)

				// Tag the job.
				if d.HasArg("tag") {
					tagJob(t, server, user, jobType, ds, d)
				}

				// Check if we expect a pausepoint error.
				if d.HasArg("expect-pausepoint") {
					expectPausepoint(t, err, jobType, server, user, ds)
					ret := append(ds.noticeBuffer, "job paused at pausepoint")
					return strings.Join(ret, "\n")
				}

				// All other errors are bad.
				require.NoError(t, err)
				return ""

			case "import":
				server := lastCreatedServer
				user := "root"
				jobType := "IMPORT"

				// First, run the backup.
				_, err := ds.getSQLDB(t, server, user).Exec(d.Input)

				// Tag the job.
				if d.HasArg("tag") {
					tagJob(t, server, user, jobType, ds, d)
				}

				// Check if we expect a pausepoint error.
				if d.HasArg("expect-pausepoint") {
					expectPausepoint(t, err, jobType, server, user, ds)
					ret := append(ds.noticeBuffer, "job paused at pausepoint")
					return strings.Join(ret, "\n")
				}

				// All other errors are bad.
				require.NoError(t, err)
				return ""

			case "restore":
				server := lastCreatedServer
				user := "root"
				jobType := "RESTORE"

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

				// First, run the restore.
				_, err := ds.getSQLDB(t, server, user).Exec(d.Input)

				// Tag the job.
				if d.HasArg("tag") {
					tagJob(t, server, user, jobType, ds, d)
				}

				// Check if the job must be run aost.
				if d.HasArg("aost") {
					var aost string
					d.ScanArgs(t, "aost", &aost)
				}

				// Check if we expect a pausepoint error.
				if d.HasArg("expect-pausepoint") {
					expectPausepoint(t, err, jobType, server, user, ds)
					ret := append(ds.noticeBuffer, "job paused at pausepoint")
					return strings.Join(ret, "\n")
				}

				// All other errors are bad.
				require.NoError(t, err)
				return ""

			case "new-schema-change":
				server := lastCreatedServer
				user := "root"
				jobType := "NEW SCHEMA CHANGE"

				// First, run the schema change.
				_, err := ds.getSQLDB(t, server, user).Exec(d.Input)

				// Tag the job.
				if d.HasArg("tag") {
					tagJob(t, server, user, jobType, ds, d)
				}

				// Check if the job must be run aost.
				if d.HasArg("aost") {
					var aost string
					d.ScanArgs(t, "aost", &aost)
				}

				// Check if we expect a pausepoint error.
				if d.HasArg("expect-pausepoint") {
					expectPausepoint(t, err, jobType, server, user, ds)
					ret := append(ds.noticeBuffer, "job paused at pausepoint")
					return strings.Join(ret, "\n")
				}

				// All other errors are bad.
				require.NoError(t, err)
				return ""

			case "schema-change":
				server := lastCreatedServer
				user := "root"
				jobType := "SCHEMA CHANGE"

				// First, run the schema change.
				_, err := ds.getSQLDB(t, server, user).Exec(d.Input)

				// Tag the job.
				if d.HasArg("tag") {
					tagJob(t, server, user, jobType, ds, d)
				}

				// Check if the job must be run aost.
				if d.HasArg("aost") {
					var aost string
					d.ScanArgs(t, "aost", &aost)
				}

				// Check if we expect a pausepoint error.
				if d.HasArg("expect-pausepoint") {
					expectPausepoint(t, err, jobType, server, user, ds)
					ret := append(ds.noticeBuffer, "job paused at pausepoint")
					return strings.Join(ret, "\n")
				}

				// All other errors are bad.
				require.NoError(t, err)
				return ""

			case "job":
				server := lastCreatedServer
				user := "root"

				if d.HasArg("cancel") {
					var cancelJobTag string
					d.ScanArgs(t, "cancel", &cancelJobTag)
					var jobID jobspb.JobID
					var ok bool
					if jobID, ok = ds.jobTags[cancelJobTag]; !ok {
						t.Fatalf("could not find job with tag %s", cancelJobTag)
					}
					runner := sqlutils.MakeSQLRunner(ds.getSQLDB(t, server, user))
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
					runner := sqlutils.MakeSQLRunner(ds.getSQLDB(t, server, user))
					runner.Exec(t, `RESUME JOB $1`, jobID)
				} else if d.HasArg("wait-for-state") {
					var tag string
					d.ScanArgs(t, "tag", &tag)
					var jobID jobspb.JobID
					var ok bool
					if jobID, ok = ds.jobTags[tag]; !ok {
						t.Fatalf("could not find job with tag %s", tag)
					}
					runner := sqlutils.MakeSQLRunner(ds.getSQLDB(t, server, user))
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
				handleKVRequest(ctx, t, lastCreatedServer, ds, request, target)
				return ""

			case "save-cluster-ts":
				server := lastCreatedServer
				user := "root"
				var timestampTag string
				d.ScanArgs(t, "tag", &timestampTag)
				if _, ok := ds.clusterTimestamps[timestampTag]; ok {
					t.Fatalf("cannot reuse cluster ts tag %s", timestampTag)
				}
				var ts string
				err := ds.getSQLDB(t, server, user).QueryRow(`SELECT cluster_logical_timestamp()`).Scan(&ts)
				require.NoError(t, err)
				ds.clusterTimestamps[timestampTag] = ts
				return ""

			case "create-dummy-system-table":
				db := ds.servers[lastCreatedServer].DB()
				codec := ds.servers[lastCreatedServer].ExecutorConfig().(sql.ExecutorConfig).Codec
				dummyTable := systemschema.SettingsTable
				err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					id, err := ds.servers[lastCreatedServer].ExecutorConfig().(sql.ExecutorConfig).
						DescIDGenerator.GenerateUniqueDescID(ctx)
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
				server := lastCreatedServer
				user := "root"
				var uri string
				d.ScanArgs(t, "uri", &uri)
				parsedURI, err := url.Parse(strings.Replace(uri, "'", "", -1))
				require.NoError(t, err)
				var filePath string
				filePathQuery := fmt.Sprintf("SELECT path FROM [SHOW BACKUP FILES FROM LATEST IN %s] LIMIT 1", uri)
				err = ds.getSQLDB(t, server, user).QueryRow(filePathQuery).Scan(&filePath)
				require.NoError(t, err)
				fullPath := filepath.Join(ds.getIODir(t, server), parsedURI.Path, filePath)
				print(fullPath)
				data, err := ioutil.ReadFile(fullPath)
				require.NoError(t, err)
				data[20] ^= 1
				if err := ioutil.WriteFile(fullPath, data, 0644 /* perm */); err != nil {
					t.Fatal(err)
				}
				return ""

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func handleKVRequest(
	ctx context.Context, t *testing.T, server string, ds datadrivenTestState, request, target string,
) {
	user := "root"
	if request == "DeleteRange" {
		var tableID uint32
		err := ds.getSQLDB(t, server, user).QueryRow(`SELECT id FROM system.namespace WHERE name = $1`,
			target).Scan(&tableID)
		require.NoError(t, err)
		bankSpan := makeTableSpan(tableID)
		dr := roachpb.DeleteRangeRequest{
			// Bogus span to make it a valid request.
			RequestHeader: roachpb.RequestHeader{
				Key:    bankSpan.Key,
				EndKey: bankSpan.EndKey,
			},
			UseRangeTombstone: true,
		}
		if _, err := kv.SendWrapped(ctx, ds.servers[server].DistSenderI().(*kvcoord.DistSender), &dr); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatalf("Unknown kv request")
	}
}

// findMostRecentJobWithType returns the most recently created job of `job_type`
// jobType.
func findMostRecentJobWithType(
	t *testing.T, ds datadrivenTestState, server, user string, jobType string,
) jobspb.JobID {
	var jobID jobspb.JobID
	require.NoError(
		t, ds.getSQLDB(t, server, user).QueryRow(
			fmt.Sprintf(
				`SELECT job_id FROM [SHOW JOBS] WHERE job_type = '%s' ORDER BY created DESC LIMIT 1`,
				jobType)).Scan(&jobID))
	return jobID
}

// expectPausepoint waits for the job to hit a pausepoint and enter a paused
// state.
func expectPausepoint(
	t *testing.T, err error, jobType, server, user string, ds datadrivenTestState,
) {
	// Check if we are expecting a pausepoint error.
	require.NotNilf(t, err, "expected pause point error")

	runner := sqlutils.MakeSQLRunner(ds.getSQLDB(t, server, user))
	jobutils.WaitForJobToPause(t, runner,
		findMostRecentJobWithType(t, ds, server, user, jobType))
}

// tagJob stores the jobID of the most recent job of `jobType`. Users can use
// the tag to refer to the job in the future.
func tagJob(
	t *testing.T, server, user, jobType string, ds datadrivenTestState, d *datadriven.TestData,
) {
	var jobTag string
	d.ScanArgs(t, "tag", &jobTag)
	if _, exists := ds.jobTags[jobTag]; exists {
		t.Fatalf("failed to `tag`, job with tag %s already exists", jobTag)
	}
	ds.jobTags[jobTag] = findMostRecentJobWithType(t, ds, server, user, jobType)
}
