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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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

type sqlDBKey struct {
	server string
	user   string
}

type datadrivenTestState struct {
	servers      map[string]serverutils.TestServerInterface
	dataDirs     map[string]string
	sqlDBs       map[sqlDBKey]*gosql.DB
	noticeBuffer []string
	cleanupFns   []func()
}

func newDatadrivenTestState() datadrivenTestState {
	return datadrivenTestState{
		servers:  make(map[string]serverutils.TestServerInterface),
		dataDirs: make(map[string]string),
		sqlDBs:   make(map[sqlDBKey]*gosql.DB),
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

func (d *datadrivenTestState) addServer(
	t *testing.T,
	name, iodir, tempCleanupFrequency string,
	ioConf base.ExternalIODirConfig,
	localities string,
) error {
	var tc serverutils.TestClusterInterface
	var cleanup func()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODirConfig = ioConf
	params.ServerArgs.Knobs = base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	settings := cluster.MakeTestingClusterSettings()
	closedts.TargetDuration.Override(context.Background(), &settings.SV, 10*time.Millisecond)
	closedts.SideTransportCloseInterval.Override(context.Background(), &settings.SV, 10*time.Millisecond)
	if tempCleanupFrequency != "" {
		duration, err := time.ParseDuration(tempCleanupFrequency)
		if err != nil {
			return errors.New("unable to parse tempCleanupFrequency during server creation")
		}
		sql.TempObjectCleanupInterval.Override(context.Background(), &settings.SV, duration)
		sql.TempObjectWaitInterval.Override(context.Background(), &settings.SV, time.Millisecond)
	}
	params.ServerArgs.Settings = settings

	clusterSize := singleNode

	if localities != "" {
		cfgs := strings.Split(localities, ",")
		clusterSize = len(cfgs)
		serverArgsPerNode := make(map[int]base.TestServerArgs)
		for i, cfg := range cfgs {
			param := params.ServerArgs
			param.Locality = localityCfgs[cfg]
			serverArgsPerNode[i] = param
		}
		params.ServerArgsPerNode = serverArgsPerNode
	}
	if iodir == "" {
		tc, _, iodir, cleanup = backupRestoreTestSetupWithParams(t, clusterSize, 0, InitManualReplication, params)
	} else {
		tc, _, cleanup = backupRestoreTestSetupEmptyWithParams(t, clusterSize, iodir, InitManualReplication, params)
	}
	d.servers[name] = tc.Server(0)
	d.dataDirs[name] = iodir
	d.cleanupFns = append(d.cleanupFns, cleanup)

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
// - "new-server name=<name> [share-io-dir=<name>]"
//   Create a new server with the input name. It takes in an optional
//   share-io-dir argument to share an IO directory with an existing server.
//   This is useful when restoring from a backup taken in another server.
//
// - "exec-sql server=<name>"
//   Executes the input SQL query on the target server. By default, server is
//   the last created server.
//
//   Supported options:
//
// - "query-sql server=<name>"
//   Executes the input SQL query and print the results.
//
// - "reset"
//    Clear all state associated with the test.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1 min under race")

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
			case "sleep":
				var sleepDuration string
				d.ScanArgs(t, "time", &sleepDuration)
				duration, err := time.ParseDuration(sleepDuration)
				if err != nil {
					return err.Error()
				}
				time.Sleep(duration)
				return ""
			case "reset":
				ds.cleanup(ctx)
				ds = newDatadrivenTestState()
				return ""
			case "new-server":
				var name, shareDirWith, iodir, tempCleanupFrequency, localities string
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
				if d.HasArg("temp-cleanup-freq") {
					d.ScanArgs(t, "temp-cleanup-freq", &tempCleanupFrequency)
				}
				if d.HasArg("localities") {
					d.ScanArgs(t, "localities", &localities)
				}
				lastCreatedServer = name
				err := ds.addServer(t, name, iodir, tempCleanupFrequency, io, localities)
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
				d.Input = strings.ReplaceAll(d.Input, "http://COCKROACH_TEST_HTTP_SERVER/", httpAddr)
				_, err := ds.getSQLDB(t, server, user).Exec(d.Input)
				ret := ds.noticeBuffer
				if err != nil {
					// pausepoint errors have the job ID in them, and datadriven tests
					// don't seem to support regex matching. Clean the error up to not
					// include the job ID.
					if i := strings.Index(err.Error(), "paused before it completed with reason"); i != -1 {
						ret = append(ds.noticeBuffer, err.Error()[i:])
					} else if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
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
				return output
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}
