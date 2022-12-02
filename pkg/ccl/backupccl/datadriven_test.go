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
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

var customKnobs = map[string]sql.BackupRestoreTestingKnobs{
	"skip-descriptor-change-intro": {SkipDescriptorChangeIntroduction: true},
}

type sqlDBKey struct {
	server string
	user   string
}

type datadrivenTestState struct {
	// clusters maps a name to its cluster
	clusters map[string]serverutils.TestClusterInterface

	// servers maps a name to the first server in the cluster
	servers map[string]serverutils.TestServerInterface
	// tempObjectCleanupAndWait is a mapping from server name to a method that can
	// be used to nudge and wait for temporary object cleanup.
	tempObjectCleanupAndWait map[string]func()
	dataDirs                 map[string]string
	sqlDBs                   map[sqlDBKey]*gosql.DB
	jobTags                  map[string]jobspb.JobID
	clusterTimestamps        map[string]string
	noticeBuffer             []string
	cleanupFns               []func()
}

func newDatadrivenTestState() datadrivenTestState {
	return datadrivenTestState{
		clusters:                 make(map[string]serverutils.TestClusterInterface),
		servers:                  make(map[string]serverutils.TestServerInterface),
		tempObjectCleanupAndWait: make(map[string]func()),
		dataDirs:                 make(map[string]string),
		sqlDBs:                   make(map[sqlDBKey]*gosql.DB),
		jobTags:                  make(map[string]jobspb.JobID),
		clusterTimestamps:        make(map[string]string),
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
	name  string
	iodir string
	// nudgeTempObjectsCleanup is a channel used to nudge the temporary object
	// reconciliation job to run.
	nudgeTempObjectsCleanup chan time.Time
	// tempObjectCleanupDone is the channel used by the temporary object
	// reconciliation job to signal it is done cleaning up.
	tempObjectCleanupDone chan struct{}
	nodes                 int
	splits                int
	ioConf                base.ExternalIODirConfig
	localities            string
	customTestingKnobs    string
}

func (d *datadrivenTestState) addServer(t *testing.T, cfg serverCfg) error {
	var tc serverutils.TestClusterInterface
	var cleanup func()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODirConfig = cfg.ioConf
	params.ServerArgs.Knobs = base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	if cfg.customTestingKnobs != "" {
		knobs := customKnobs[cfg.customTestingKnobs]
		params.ServerArgs.Knobs.BackupRestore = &knobs
	}

	// If the server needs to control temporary object cleanup, let us set that up
	// now.
	if cfg.nudgeTempObjectsCleanup != nil && cfg.tempObjectCleanupDone != nil {
		params.ServerArgs.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
			OnTempObjectsCleanupDone: func() {
				cfg.tempObjectCleanupDone <- struct{}{}
			},
			TempObjectsCleanupCh: cfg.nudgeTempObjectsCleanup,
		}
	}

	settings := cluster.MakeTestingClusterSettings()
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
	if cfg.iodir == "" {
		tc, _, cfg.iodir, cleanup = backupRestoreTestSetupWithParams(t, clusterSize, cfg.splits,
			InitManualReplication, params)
	} else {
		tc, _, cleanup = backupRestoreTestSetupEmptyWithParams(t, clusterSize, cfg.iodir,
			InitManualReplication, params)
	}
	cleanupFn := func() {
		if cfg.nudgeTempObjectsCleanup != nil {
			close(cfg.nudgeTempObjectsCleanup)
		}
		if cfg.tempObjectCleanupDone != nil {
			close(cfg.tempObjectCleanupDone)
		}
		cleanup()
	}
	d.clusters[cfg.name] = tc
	d.servers[cfg.name] = tc.Server(0)
	d.dataDirs[cfg.name] = cfg.iodir
	d.cleanupFns = append(d.cleanupFns, cleanupFn)

	if cfg.nudgeTempObjectsCleanup != nil && cfg.tempObjectCleanupDone != nil {
		d.tempObjectCleanupAndWait[cfg.name] = func() {
			cfg.nudgeTempObjectsCleanup <- timeutil.Now()
			<-cfg.tempObjectCleanupDone
		}
	}

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
//   - "new-server name=<name> [args]"
//     Create a new server with the input name.
//
//     Supported arguments:
//
//   - share-io-dir: can be specified to share an IO directory with an existing
//     server. This is useful when restoring from a backup taken in another
//     server.
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
//   - control-temp-object-cleanup: sets up the server in a way that the test
//     can control when to run the temporary object reconciliation loop using
//     nudge-and-wait-for-temp-cleanup
//
//   - knobs: specificies a custom debug testing knob setup
//
//   - "exec-sql [server=<name>] [user=<name>] [args]"
//     Executes the input SQL query on the target server. By default, server is
//     the last created server.
//
//     Supported arguments:
//
//   - expect-error-regex=<regex>: expects the query to return an error with a string
//     matching the provided regex
//
//   - expect-error-ignore: expects the query to return an error, but we will
//     ignore it.
//
//   - "query-sql [server=<name>] [user=<name>]"
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
//   - "job" [server=<name>] [user=<name>] [args]
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
//   - "nudge-and-wait-for-temp-cleanup"
//     Nudges the temporary object reconciliation loop to run, and waits for completion.
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

			switch d.Cmd {
			case "reset":
				ds.cleanup(ctx)
				ds = newDatadrivenTestState()
				return ""

			case "new-server":
				var name, shareDirWith, iodir, localities, knobs string
				var splits int
				var nudgeTempObjectCleanup chan time.Time
				var tempObjectCleanupDone chan struct{}
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
				if d.HasArg("control-temp-object-cleanup") {
					nudgeTempObjectCleanup = make(chan time.Time)
					tempObjectCleanupDone = make(chan struct{})
				}

				if d.HasArg("knobs") {
					d.ScanArgs(t, "knobs", &knobs)
				}

				lastCreatedServer = name
				cfg := serverCfg{
					name:                    name,
					iodir:                   iodir,
					nudgeTempObjectsCleanup: nudgeTempObjectCleanup,
					tempObjectCleanupDone:   tempObjectCleanupDone,
					nodes:                   nodes,
					splits:                  splits,
					ioConf:                  io,
					localities:              localities,
					customTestingKnobs:      knobs,
				}
				err := ds.addServer(t, cfg)
				if err != nil {
					return err.Error()
				}
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
				checkForClusterSetting(t, d.Input, ds.clusters[server].NumServers())
				d.Input = strings.ReplaceAll(d.Input, "http://COCKROACH_TEST_HTTP_SERVER/", httpAddr)
				_, err := ds.getSQLDB(t, server, user).Exec(d.Input)
				ret := ds.noticeBuffer

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
				checkForClusterSetting(t, d.Input, ds.clusters[server].NumServers())
				rows, err := ds.getSQLDB(t, server, user).Query(d.Input)
				if err != nil {
					return err.Error()
				}
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output
			case "set-cluster-setting":
				var setting, value string
				d.ScanArgs(t, "setting", &setting)
				d.ScanArgs(t, "value", &value)
				server := lastCreatedServer
				serverutils.SetClusterSetting(t, ds.clusters[server], setting, value)
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

			case "nudge-and-wait-for-temp-cleanup":
				server := lastCreatedServer
				if nudgeAndWait, ok := ds.tempObjectCleanupAndWait[server]; !ok {
					t.Fatalf("server %s was not configured with `control-temp-object-cleanup`", server)
				} else {
					nudgeAndWait()
				}
				return ""

			case "create-dummy-system-table":
				db := ds.servers[lastCreatedServer].DB()
				codec := ds.servers[lastCreatedServer].ExecutorConfig().(sql.ExecutorConfig).Codec
				dummyTable := systemschema.SettingsTable
				err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					id, err := descidgen.GenerateUniqueDescID(ctx, db, codec)
					if err != nil {
						return err
					}
					mut := dummyTable.NewBuilder().BuildCreatedMutable().(*tabledesc.Mutable)
					mut.ID = id
					mut.Name = fmt.Sprintf("%s_%d",
						catprivilege.RestoreCopySystemTablePrefix, id)
					tKey := catalogkeys.EncodeNameKey(codec, mut)
					b := txn.NewBatch()
					b.CPut(tKey, mut.GetID(), nil)
					b.CPut(catalogkeys.MakeDescMetadataKey(codec, mut.GetID()), mut.DescriptorProto(), nil)
					return txn.Run(ctx, b)
				})
				require.NoError(t, err)
				return ""
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
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

func checkForClusterSetting(t *testing.T, stmt string, numNodes int) {
	if numNodes != 1 && strings.Contains(stmt, "SET CLUSTER SETTING") {
		t.Fatal("You are attempting to set a cluster setting in a multi node cluster. " +
			"Use the 'set-cluster-setting' data driven cmd instead to ensure the setting propagates to" +
			" all nodes during the cmd.")
	}
}
