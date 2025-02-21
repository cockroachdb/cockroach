// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTenantBackupWithCanceledImport tests a scenario known to fail
// without range_tombstones in place.
//
// - START IMPORT
// - PAUSE IMPORT after some data has been written
// - BACKUP TENANT (full)
// - CANCEL IMPORT
// - BACKUP TENANT (incremental)
// - RESTORE
//
// Without range tombstones, this results in data from the canceled
// import being erroneously restored into the table.
func TestTenantBackupWithCanceledImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tempDir, tempDirCleanupFn := testutils.TempDir(t)
	defer tempDirCleanupFn()

	tc, hostSQLDB, hostClusterCleanupFn := backupRestoreTestSetupEmpty(
		t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				},
			},
		},
	)
	defer hostClusterCleanupFn()

	tenant10, err := tc.Servers[0].TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID: roachpb.MustMakeTenantID(10),
		TestingKnobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	require.NoError(t, err)
	tenant10Conn := tenant10.SQLConn(t, serverutils.DBName("defaultdb"))
	tenant10DB := sqlutils.MakeSQLRunner(tenant10Conn)

	tenant10DB.Exec(t, "CREATE DATABASE bank")
	tenant10DB.Exec(t, "USE bank")

	tableName := "import_cancel"
	tenant10DB.Exec(t, fmt.Sprintf(`CREATE TABLE "%s" (id INT PRIMARY KEY, n INT, s STRING)`, tableName))

	tenant10DB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'import.after_ingest'")
	importStmt := fmt.Sprintf(`IMPORT INTO "%s" CSV DATA ('workload:///csv/bank/bank?payload-bytes=100&row-end=1&row-start=0&rows=1000&seed=1&version=1.0.0')`, tableName)
	tenant10DB.ExpectErr(t, "pause", importStmt)

	hostSQLDB.Exec(t, "BACKUP TENANT 10 INTO 'nodelocal://1/tenant-backup'")

	tenant10DB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
	tenant10DB.Exec(t, "CANCEL JOB (SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT' AND status = 'paused')")
	tenant10DB.Exec(t, "SHOW JOBS WHEN COMPLETE (SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT')")

	hostSQLDB.Exec(t, "BACKUP TENANT 10 INTO LATEST IN 'nodelocal://1/tenant-backup'")
	hostSQLDB.Exec(t, "RESTORE TENANT 10 FROM LATEST IN 'nodelocal://1/tenant-backup' WITH virtual_cluster_name = 'cluster-11'")

	tenant11, err := tc.Servers[0].TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantName:          "cluster-11",
		DisableCreateTenant: true,
	})
	require.NoError(t, err)

	tenant11Conn := tenant11.SQLConn(t, serverutils.DBName("bank"))
	tenant11DB := sqlutils.MakeSQLRunner(tenant11Conn)
	countQuery := fmt.Sprintf(`SELECT count(1) FROM bank."%s"`, tableName)
	assertEqualQueries(t, tenant10DB, tenant11DB, countQuery)
	require.Equal(t, [][]string{{"0"}}, tenant11DB.QueryStr(t, countQuery))
}

// TestTenantBackupNemesis runs a seriest of incremental tenant
// backups while other operations are happening inside the tenant.
func TestTenantBackupNemesis(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "slow test") // take >1mn under race

	ctx := context.Background()
	tempDir, tempDirCleanupFn := testutils.TempDir(t)
	defer func() {
		if !t.Failed() {
			tempDirCleanupFn()
		} else {
			t.Logf("leaving %s for inspection", tempDir)
		}
	}()

	tc, hostSQLDB, hostClusterCleanupFn := backupRestoreTestSetupEmpty(
		t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				},
			},
		},
	)
	defer hostClusterCleanupFn()

	tenant10, err := tc.Servers[0].TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID:   roachpb.MustMakeTenantID(10),
		TenantName: "tenant-10",
		TestingKnobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	require.NoError(t, err)
	tenant10Conn := tenant10.SQLConn(t, serverutils.DBName("defaultdb"))
	_, err = tenant10Conn.Exec("CREATE DATABASE bank")
	require.NoError(t, err)
	tenant10Conn = tenant10.SQLConn(t, serverutils.DBName("bank"))

	// Import initial bank data. The bank workload runs against
	// the bank table while we concurrently run other operations
	// against other tables.
	const (
		numAccounts = 1000
		payloadSize = 100
		splits      = 0
	)
	bankData := bank.FromConfig(numAccounts, numAccounts, payloadSize, splits)
	l := workloadsql.InsertsDataLoader{BatchSize: 1000, Concurrency: 4}
	_, err = workloadsql.Setup(ctx, tenant10Conn, bankData, l)
	require.NoError(t, err)

	backupDone := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		pgURL, cleanupGoDB, err := pgurlutils.PGUrlE(
			tenant10.AdvSQLAddr(), "workload-worker" /* prefix */, url.User(username.RootUser))
		if err != nil {
			return err
		}
		defer cleanupGoDB()
		pgURL.Path = bankData.Meta().Name
		reg := histogram.NewRegistry(20*time.Second, bankData.Meta().Name)
		ops, err := bankData.(workload.Opser).Ops(ctx, []string{pgURL.String()}, reg)
		if err != nil {
			return err
		}
		defer func() {
			if ops.Close != nil {
				_ = ops.Close(ctx)
			}
		}()
		fn := ops.WorkerFns[0]
		for {
			if err := fn(ctx); err != nil {
				return err
			}
			select {
			case <-backupDone:
				return nil
			default:
			}
		}
	})

	hostURL, cleanup, err := pgurlutils.PGUrlE(
		tc.SystemLayer(0).AdvSQLAddr(), "backup-nemesis", url.User(username.RootUser))
	require.NoError(t, err)
	defer cleanup()

	// Effectively reserve tenant 11 for restore. We care about
	// the ID here because the IDs are built into the test
	// certificates.
	hostSQLDB.Exec(t, "SELECT crdb_internal.create_tenant(12)")
	require.NoError(t, err)

	rng, _ := randutil.NewPseudoRand()
	nemesisRunner := newBackupNemesis(t, rng, tenant10Conn, hostSQLDB.DB.(*gosql.DB), hostURL.String())
	nemesisRunner.Start(ctx)

	var tablesToCheck []string
	var aost string
	g.GoCtx(func(ctx context.Context) error {
		defer close(backupDone)
		defer nemesisRunner.Stop()

		if err := nemesisRunner.FullBackup(); err != nil {
			return err
		}
		numIncrementals := 20
		if util.RaceEnabled {
			numIncrementals = 10
		}
		for i := 0; i < numIncrementals; i++ {
			// This seems to catch errors more often if we
			// don't try to coordinate between the backups
			// and the nemesis runner.
			tablesToCheck = nemesisRunner.TablesToCheck()
			var err error
			aost, err = nemesisRunner.IncrementalBackup()
			if err != nil {
				return err
			}
		}
		return nil
	})

	require.NoError(t, g.Wait())
	restoreQuery := fmt.Sprintf("RESTORE TENANT 10 FROM LATEST IN '%s' AS OF SYSTEM TIME %s WITH virtual_cluster = '11', virtual_cluster_name = 'restored-tenant-10'", nemesisRunner.BackupLocation(), aost)
	t.Logf("backup-nemesis: restoring tenant 10 into restored-tenant-10: %s", restoreQuery)
	hostSQLDB.Exec(t, restoreQuery)

	// Validation
	//
	// We check bank.bank which has had the workload running against it
	// and any table from a completed nemesis.
	restoredTenant, err := tc.Servers[0].TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantName:          "restored-tenant-10",
		DisableCreateTenant: true,
	})
	require.NoError(t, err)

	restoreTenantConn := restoredTenant.SQLConn(t, serverutils.DBName("bank"))

	tenant10SQLDB := sqlutils.MakeSQLRunner(tenant10Conn)
	restoredTenantSQLDB := sqlutils.MakeSQLRunner(restoreTenantConn)

	validateTableAcrossTenantRestore(t, tenant10SQLDB, restoredTenantSQLDB, "bank.bank", aost)
	for _, name := range tablesToCheck {
		validateTableAcrossTenantRestore(t, tenant10SQLDB, restoredTenantSQLDB, fmt.Sprintf(`bank."%s"`, name), aost)
	}
}

func validateTableAcrossTenantRestore(
	t *testing.T,
	oldTenant *sqlutils.SQLRunner,
	newTenant *sqlutils.SQLRunner,
	tableName string,
	endTime string,
) {
	t.Logf("backup-nemesis: checking table %s at %s", tableName, endTime)
	assertEqualQueriesAtTime(t, oldTenant, newTenant,
		fmt.Sprintf("SELECT count(*) FROM %s", tableName), endTime)
	assertEqualQueriesAtTime(t, oldTenant, newTenant,
		fmt.Sprintf("SELECT * FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s]", tableName), endTime)
}

func assertEqualQueriesAtTime(
	t *testing.T,
	oldTenant *sqlutils.SQLRunner,
	newTenant *sqlutils.SQLRunner,
	queryBase string,
	endTime string,
) {
	t.Helper()
	assertEqualQueries(t, oldTenant, newTenant, fmt.Sprintf("%s AS OF SYSTEM TIME %s", queryBase, endTime))
}

func assertEqualQueries(
	t *testing.T, db1 *sqlutils.SQLRunner, db2 *sqlutils.SQLRunner, query string, args ...interface{},
) {
	t.Helper()
	out1 := db1.QueryStr(t, query, args...)
	out2 := db2.QueryStr(t, query, args...)
	require.Equal(t, out1, out2)
}

type randomBackupNemesis struct {
	t        *testing.T
	tenantDB *gosql.DB
	hostDB   *gosql.DB
	hostURL  string

	grp    ctxgroup.Group
	cancel context.CancelFunc
	rng    *rand.Rand

	useClusterBackup bool
	// We keep some state for the replication nemesis to avoid
	// having hundreds of running replication streams slowing down
	// the test.
	replCount atomic.Int32
	nemeses   []nemesis

	mu struct {
		syncutil.Mutex
		tablesToCheck []string

		otherTenants []string
	}
}

type nemesis struct {
	name string
	impl func(context.Context, *randomBackupNemesis, *gosql.DB, *gosql.DB) error
}

func newBackupNemesis(
	t *testing.T, rng *rand.Rand, tenantDB *gosql.DB, hostDB *gosql.DB, hostURL string,
) *randomBackupNemesis {
	return &randomBackupNemesis{
		t:        t,
		rng:      rng,
		tenantDB: tenantDB,
		hostDB:   hostDB,
		hostURL:  hostURL,
		// Prefer cluster backups most of the time since those
		// are what we use more often at the moment.
		useClusterBackup: rng.Float64() > 0.2,

		nemeses: []nemesis{
			{name: "CREATE TENANT",
				impl: func(ctx context.Context, n *randomBackupNemesis, _, hostDB *gosql.DB) error {
					tenantName := fmt.Sprintf("test-tenant-%d", n.rng.Int63())
					if _, err := hostDB.Exec("CREATE TENANT $1", tenantName); err != nil {
						return err
					}
					n.addTenant(tenantName)
					return nil
				},
			},
			{name: "DELETE TENANT",
				impl: func(ctx context.Context, n *randomBackupNemesis, _, hostDB *gosql.DB) error {
					tenant := n.popTenant()
					if tenant == "" {
						n.t.Log("no tenants to delete")
						return nil
					}
					if _, err := hostDB.Exec("DROP TENANT $1", tenant); err != nil {
						return err
					}
					return nil
				},
			},
			{name: "RENAME TENANT",
				impl: func(ctx context.Context, n *randomBackupNemesis, _, hostDB *gosql.DB) error {
					tenant := n.popTenant()
					if tenant == "" {
						n.t.Log("no tenants to rename")
						return nil
					}
					newName := fmt.Sprintf("test-tenant-%d", n.rng.Int63())
					if _, err := hostDB.Exec("ALTER TENANT $1 RENAME TO $2", tenant, newName); err != nil {
						return err
					}
					n.addTenant(newName)
					return nil
				},
			},
			{name: "REPLICATE TENANT",
				impl: func(ctx context.Context, n *randomBackupNemesis, _, hostDB *gosql.DB) error {
					if n.replCount.Load() > 3 {
						n.t.Log("already hit replication limit")
						return nil
					}
					if _, err := hostDB.Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
						return err
					}
					newName := fmt.Sprintf("repl-tenant-%d", n.rng.Int63())
					if _, err := hostDB.Exec("CREATE TENANT $1 FROM REPLICATION OF $2 ON $3", newName, "tenant-10", n.hostURL); err != nil {
						return err
					}
					n.addTenant(newName)
					n.replCount.Add(1)
					return nil
				},
			},
			{name: "CANCELED IMPORT INTO",
				impl: func(ctx context.Context, n *randomBackupNemesis, db, _ *gosql.DB) error {
					if _, err := db.Exec("SET CLUSTER SETTING jobs.debug.pausepoints = 'import.after_ingest'"); err != nil {
						return err
					}
					tableName, err := n.makeRandomBankTable("import_into_cancel")
					if !testutils.IsError(err, "pause") {
						return err
					}
					if _, err := db.Exec("SET CLUSTER SETTING jobs.debug.pausepoints = ''"); err != nil {
						return err
					}
					for {
						row := db.QueryRow("SELECT count(1) FROM [SHOW JOBS] WHERE job_type = 'IMPORT' AND status = 'paused'")
						var count int
						if err := row.Scan(&count); err != nil {
							return err
						}
						if count > 0 {
							break
						}
						t.Log("waiting for paused import job")
						time.Sleep(time.Second)
					}
					if _, err := db.Exec("CANCEL JOB (SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT' AND status = 'paused')"); err != nil {
						return err
					}
					if _, err := db.Exec("SHOW JOBS WHEN COMPLETE (SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT')"); err != nil {
						return err
					}
					n.addTable(tableName)
					return nil
				},
			},
			{name: "IMPORT INTO",
				impl: func(ctx context.Context, n *randomBackupNemesis, db, _ *gosql.DB) error {
					tableName, err := n.makeRandomBankTable("import_into")
					if err != nil {
						return err
					}
					n.addTable(tableName)
					return nil
				},
			},
			{name: "CREATE INDEX",
				impl: func(ctx context.Context, n *randomBackupNemesis, db, _ *gosql.DB) error {
					tableName, err := n.makeRandomBankTable("create_index")
					if err != nil {
						return err
					}
					if _, err := db.Exec(fmt.Sprintf(`CREATE INDEX ON "%s"(n)`, tableName)); err != nil {
						return err
					}
					n.addTable(tableName)
					return nil
				},
			},
			{name: "CREATE UNIQUE INDEX (will fail)",
				impl: func(ctx context.Context, n *randomBackupNemesis, db, _ *gosql.DB) error {
					tableName, err := n.makeRandomBankTable("create_unique_index")
					if err != nil {
						return err
					}
					// Add a conflict that ensures the unique index creation will fail.
					mkConflict := fmt.Sprintf(`UPDATE "%[1]s" SET n = 5 WHERE id IN (SELECT id FROM "%[1]s" ORDER BY random() LIMIT 5)`, tableName)
					if _, err := db.Exec(mkConflict); err != nil {
						return err
					}

					if _, err := db.Exec(fmt.Sprintf(`CREATE UNIQUE INDEX ON "%s"(n)`, tableName)); err == nil {
						return errors.New("expected error but found none")
					}
					n.addTable(tableName)
					return nil
				},
			},
		},
	}
}

func (r *randomBackupNemesis) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)

	r.cancel = cancel
	r.grp = ctxgroup.WithContext(ctx)

	r.grp.GoCtx(r.runNemesis)
}

func (r *randomBackupNemesis) Stop() {
	r.cancel()
	_ = r.grp.Wait()
}

// TablesToCheck returns the tables that are should be online in any
// backup taken after it returns. Tables for in-progress nemesis
// operations aren't returned until the operation has reached some
// state where we expect the table to be online.
func (r *randomBackupNemesis) TablesToCheck() []string {
	r.mu.Lock()
	ret := append([]string(nil), r.mu.tablesToCheck...)
	r.mu.Unlock()
	return ret
}

func (r *randomBackupNemesis) BackupLocation() string {
	if r.useClusterBackup {
		return "nodelocal://1/cluster-backup"
	}
	return "nodelocal://1/tenant-backup"
}

func (r *randomBackupNemesis) FullBackup() error {
	backupQuery := "BACKUP TENANT 10 INTO $1"
	if r.useClusterBackup {
		backupQuery = "BACKUP INTO $1 WITH include_all_secondary_tenants"
	}
	r.t.Logf("backup-nemesis: full backup started: %s", backupQuery)
	if _, err := r.hostDB.Exec(backupQuery, r.BackupLocation()); err != nil {
		return err
	}
	r.t.Log("backup-nemesis: full backup finished")
	return nil
}

func (r *randomBackupNemesis) IncrementalBackup() (string, error) {
	var aost string
	if err := r.hostDB.QueryRow("SELECT cluster_logical_timestamp()").Scan(&aost); err != nil {
		return aost, err
	}
	backupQuery := fmt.Sprintf("BACKUP TENANT 10 INTO LATEST IN '%s' AS OF SYSTEM TIME %s", r.BackupLocation(), aost)
	if r.useClusterBackup {
		backupQuery = fmt.Sprintf("BACKUP INTO LATEST IN '%s' AS OF SYSTEM TIME %s WITH include_all_secondary_tenants", r.BackupLocation(), aost)
	}

	r.t.Logf("backup-nemesis: incremental backup started: %s", backupQuery)
	if _, err := r.hostDB.Exec(backupQuery); err != nil {
		return aost, err
	}
	r.t.Logf("backup-nemesis: incremental backup finished")
	return aost, nil

}

func (r *randomBackupNemesis) addTable(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.tablesToCheck = append(r.mu.tablesToCheck, name)

}

func (r *randomBackupNemesis) addTenant(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.otherTenants = append(r.mu.otherTenants, name)

}

func (r *randomBackupNemesis) popTenant() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var tenant string
	if len(r.mu.otherTenants) < 1 {
		return tenant
	}
	tenant, r.mu.otherTenants = r.mu.otherTenants[0], r.mu.otherTenants[1:]
	return tenant
}

func (r *randomBackupNemesis) runNemesis(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		n := r.nemeses[r.rng.Intn(len(r.nemeses))]
		r.t.Logf("backup-nemesis: %s started", n.name)
		if err := n.impl(ctx, r, r.tenantDB, r.hostDB); err != nil {
			r.t.Logf("backup-nemesis: %s failed: %s", n.name, err)
			return err
		}
		r.t.Logf("backup-nemesis: %s finished", n.name)
	}
}

func (r *randomBackupNemesis) makeRandomBankTable(prefix string) (string, error) {
	tableName := fmt.Sprintf("%s_%s", prefix, uuid.MakeV4().String())
	if _, err := r.tenantDB.Exec(fmt.Sprintf(`CREATE TABLE "%s" (id INT PRIMARY KEY, n INT, s STRING)`, tableName)); err != nil {
		return "", err
	}
	importStmt := fmt.Sprintf(`IMPORT INTO "%s" CSV DATA ('workload:///csv/bank/bank?payload-bytes=100&row-end=1&row-start=0&rows=1000&seed=1&version=1.0.0')`, tableName)
	if _, err := r.tenantDB.Exec(importStmt); err != nil {
		return tableName, err
	}
	return tableName, nil
}
