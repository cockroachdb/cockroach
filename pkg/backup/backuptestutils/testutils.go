// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backuptestutils

import (
	"context"
	gosql "database/sql"
	"reflect"
	"strings"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/backup/backupbase" // imported for cluster settings.
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func IsOnlineRestoreSupported() bool {
	// TODO(jeffswenson): relax this check once online restore is in preview.
	return clusterversion.DevelopmentBranch
}

const (
	// SingleNode is the size of a single node test cluster.
	SingleNode = 1
	// MultiNode is the size of a multi node test cluster.
	MultiNode = 3
)

// smallEngineBlocks configures Pebble with a block size of 1 byte, to provoke
// bugs in time-bound iterators. We disable this in race builds, which can
// be too slow.
var smallEngineBlocks = !util.RaceEnabled && metamorphic.ConstantWithTestBool("small-engine-blocks", false)

// InitManualReplication calls tc.ToggleReplicateQueues(false).
//
// Note that the test harnesses that use this typically call
// tc.WaitForFullReplication before calling this method,
// so up-replication has usually already taken place.
func InitManualReplication(tc *testcluster.TestCluster) {
	tc.ToggleReplicateQueues(false)
}

type BackupTestArg func(*backupTestOptions)

type bankWorkloadArgs struct {
	numAccounts int
}

type backupTestOptions struct {
	bankArgs                   *bankWorkloadArgs
	dataDir                    string
	testClusterArgs            base.TestClusterArgs
	initFunc                   func(*testcluster.TestCluster)
	skipInvalidDescriptorCheck bool
}

func WithParams(p base.TestClusterArgs) BackupTestArg {
	return func(o *backupTestOptions) {
		o.testClusterArgs = p
	}
}

func WithBank(numAccounts int) BackupTestArg {
	return func(o *backupTestOptions) {
		o.bankArgs = &bankWorkloadArgs{
			numAccounts: numAccounts,
		}
	}
}

func WithInitFunc(f func(*testcluster.TestCluster)) BackupTestArg {
	return func(o *backupTestOptions) {
		o.initFunc = f
	}
}

func WithTempDir(dir string) BackupTestArg {
	return func(o *backupTestOptions) {
		o.dataDir = dir
	}
}

func WithSkipInvalidDescriptorCheck() BackupTestArg {
	return func(o *backupTestOptions) {
		o.skipInvalidDescriptorCheck = true
	}
}

func StartBackupRestoreTestCluster(
	t testing.TB, clusterSize int, args ...BackupTestArg,
) (*testcluster.TestCluster, *sqlutils.SQLRunner, string, func()) {
	ctx := logtags.AddTag(context.Background(), "start-backup-restore-test-cluster", nil)
	opts := backupTestOptions{}
	for _, a := range args {
		a(&opts)
	}

	dirCleanupFunc := func() {}
	if opts.dataDir == "" {
		opts.dataDir, dirCleanupFunc = testutils.TempDir(t)
	}

	if opts.initFunc == nil {
		// TODO(ssd): Is anything actually using a custom init
		// func?
		opts.initFunc = InitManualReplication
	}

	useDatabase := ""
	if opts.bankArgs != nil {
		useDatabase = "data"
	}

	setTestClusterDefaults(&opts.testClusterArgs, opts.dataDir, useDatabase)
	tc := testcluster.StartTestCluster(t, clusterSize, opts.testClusterArgs)
	opts.initFunc(tc)

	// Disable autocommit before DDLs in order to make schema changes during test
	// setup faster. Becuase the cluster setting only enacts on new conns in
	// the pool, temprorarily reduce the number of max open conns to 1, and apply
	// the session var to the existing conn.
	for i := 0; i < clusterSize; i++ {
		tc.Conns[i].SetMaxOpenConns(1)
		_, err := tc.Conns[i].Exec("SET autocommit_before_ddl = false")
		require.NoError(t, err)
		tc.Conns[i].SetMaxOpenConns(0)
	}
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `SET CLUSTER SETTING sql.defaults.autocommit_before_ddl.enabled = 'false'`)

	if opts.bankArgs != nil {
		const payloadSize = 100
		splits := 10
		numAccounts := opts.bankArgs.numAccounts
		if numAccounts == 0 {
			splits = 0
		}
		bankData := bank.FromConfig(numAccounts, numAccounts, payloadSize, splits)

		// Lower the initial buffering adder ingest size to allow
		// concurrent import jobs to run without borking the memory
		// monitor.
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.pk_buffer_size = '16MiB'`)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.index_buffer_size = '16MiB'`)

		sqlDB.Exec(t, `CREATE DATABASE data`)
		l := workloadsql.InsertsDataLoader{BatchSize: 1000, Concurrency: 4}
		if _, err := workloadsql.Setup(ctx, sqlDB.DB.(*gosql.DB), bankData, l); err != nil {
			t.Fatal(err)
		}

		if err := tc.WaitForFullReplication(); err != nil {
			t.Fatal(err)
		}
	}

	return tc, sqlDB, opts.dataDir, func() {
		if !opts.skipInvalidDescriptorCheck {
			CheckForInvalidDescriptors(t, tc.Conns[0])
		}
		tc.Stopper().Stop(ctx) // cleans up in memory storage's auxiliary dirs
		dirCleanupFunc()
	}
}

func setTestClusterDefaults(params *base.TestClusterArgs, dataDir string, useDatabase string) {
	if useDatabase != "" {
		params.ServerArgs.UseDatabase = "data"
		for i := range params.ServerArgsPerNode {
			param := params.ServerArgsPerNode[i]
			param.UseDatabase = "data"
			params.ServerArgsPerNode[i] = param
		}
	}

	params.ServerArgs.ExternalIODir = dataDir
	for i := range params.ServerArgsPerNode {
		param := params.ServerArgsPerNode[i]
		param.ExternalIODir = dataDir + param.ExternalIODir
		params.ServerArgsPerNode[i] = param
	}

	if smallEngineBlocks {
		if params.ServerArgs.Knobs.Store == nil {
			params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{}
		}
		params.ServerArgs.Knobs.Store.(*kvserver.StoreTestingKnobs).SmallEngineBlocks = true
	}

	if params.ServerArgs.Knobs.JobsTestingKnobs == nil {
		params.ServerArgs.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	}

	// TODO(ssd): How many tests actually need these?
	if params.ServerArgs.Knobs.KeyVisualizer == nil {
		params.ServerArgs.Knobs.KeyVisualizer = &keyvisualizer.TestingKnobs{
			SkipJobBootstrap:        true,
			SkipZoneConfigBootstrap: true,
		}
	}
	if params.ServerArgs.Knobs.SQLStatsKnobs == nil {
		params.ServerArgs.Knobs.SQLStatsKnobs = &sqlstats.TestingKnobs{
			SkipZoneConfigBootstrap: true,
		}
	}
}

// VerifyBackupRestoreStatementResult conducts a Backup or Restore and verifies
// it was properly written to the jobs table. Note, does not verify online restores
func VerifyBackupRestoreStatementResult(
	t *testing.T, sqlDB *sqlutils.SQLRunner, query string, args ...interface{},
) error {
	t.Helper()
	rows := sqlDB.Query(t, query, args...)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if a, e := columns, []string{
		"job_id", "status", "fraction_completed", "rows",
	}; !reflect.DeepEqual(e, a) {
		return errors.Errorf("unexpected columns:\n%s", strings.Join(pretty.Diff(e, a), "\n"))
	}

	type job struct {
		id                int64
		status            string
		fractionCompleted float32
	}

	var expectedJob job
	var actualJob job
	var unused int64

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return errors.New("zero rows in result")
	}
	if err := rows.Scan(
		&actualJob.id, &actualJob.status, &actualJob.fractionCompleted, &unused,
	); err != nil {
		return err
	}
	if rows.Next() {
		return errors.New("more than one row in result")
	}

	sqlDB.QueryRow(t,
		`SELECT job_id, status, fraction_completed FROM crdb_internal.jobs WHERE job_id = $1`, actualJob.id,
	).Scan(
		&expectedJob.id, &expectedJob.status, &expectedJob.fractionCompleted,
	)
	require.Equal(t, expectedJob, actualJob, "result does not match system.jobs")

	return nil
}

// CheckForInvalidDescriptors returns an error if there exists any descriptors in
// the crdb_internal.invalid_objects virtual table.
func CheckForInvalidDescriptors(t testing.TB, sqlDB *gosql.DB) {
	// Ensure the connection to the database is still open.
	if err := sqlDB.Ping(); err != nil {
		t.Logf("Warning: Could not check for invalid descriptors: %v", err)
		return
	}
	// Because crdb_internal.invalid_objects is a virtual table, by default, the
	// query will take a lease on the database sqlDB is connected to and only run
	// the query on the given database. The "" prefix prevents this lease
	// acquisition and allows the query to fetch all descriptors in the cluster.
	rows, err := sqlDB.Query(`SELECT id, obj_name, error FROM "".crdb_internal.invalid_objects`)
	if err != nil {
		if testutils.IsError(err, "role .* was concurrently dropped") {
			// Some tests do not restore users, so the user who owned this session may
			// no longer exist.
			return
		}
		t.Fatal(err)
	}
	invalidIDs, err := sqlutils.RowsToDataDrivenOutput(rows)
	if err != nil {
		t.Error(err)
	}
	if invalidIDs != "" {
		t.Fatalf("the following descriptor ids are invalid\n%v", invalidIDs)
	}
	t.Log("no Invalid Descriptors")
}
