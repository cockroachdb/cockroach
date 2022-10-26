package backupccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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
// import being in the table.
func TestTenantBackupWithCanceledImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tempDir, tempDirCleanupFn := testutils.TempDir(t)
	defer tempDirCleanupFn()

	tc, hostSQLDB, hostClusterCleanupFn := backupRestoreTestSetupEmpty(
		t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DisableDefaultTestTenant: true,
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				},
			},
		},
	)
	defer hostClusterCleanupFn()

	hostSQLDB.Exec(t, "SET CLUSTER SETTING storage.mvcc.range_tombstones.enabled = true")
	hostSQLDB.Exec(t, "ALTER TENANT ALL SET CLUSTER SETTING storage.mvcc.range_tombstones.enabled = true")

	tenant10, err := tc.Servers[0].StartTenant(ctx, base.TestTenantArgs{
		TenantID: roachpb.MustMakeTenantID(10),
		TestingKnobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	require.NoError(t, err)
	tenant10Conn, err := serverutils.OpenDBConnE(tenant10.SQLAddr(), "defaultdb", false, tenant10.Stopper())
	require.NoError(t, err)
	tenant10DB := sqlutils.MakeSQLRunner(tenant10Conn)

	tenant10DB.Exec(t, "CREATE DATABASE bank")
	tenant10DB.Exec(t, "USE bank")

	tableName := "import_cancel"
	tenant10DB.Exec(t, fmt.Sprintf(`CREATE TABLE "%s" (id INT PRIMARY KEY, n INT, s STRING)`, tableName))

	tenant10DB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'import.after_ingest'")
	importStmt := fmt.Sprintf(`IMPORT INTO "%s" CSV DATA ('workload:///csv/bank/bank?payload-bytes=100&row-end=1&row-start=0&rows=1000&seed=1&version=1.0.0')`, tableName)
	tenant10DB.ExpectErr(t, "pause", importStmt)

	hostSQLDB.Exec(t, "BACKUP TENANT 10 INTO 'nodelocal://0/tenant-backup'")
	s := hostSQLDB.QueryStr(t, "SHOW BACKUP LATEST IN 'nodelocal://0/tenant-backup'")
	t.Logf("%v", s)

	tenant10DB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
	tenant10DB.Exec(t, "CANCEL JOB (SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT' AND status = 'paused')")
	tenant10DB.Exec(t, "SHOW JOBS WHEN COMPLETE (SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT')")
	hostSQLDB.Exec(t, "BACKUP TENANT 10 INTO LATEST IN 'nodelocal://0/tenant-backup'")
	t.Logf("%v", hostSQLDB.QueryStr(t, "SHOW BACKUP LATEST IN 'nodelocal://0/tenant-backup'"))

	hostSQLDB.Exec(t, "RESTORE TENANT 10 FROM LATEST IN 'nodelocal://0/tenant-backup' WITH tenant = '11'")
	tenant11, err := tc.Servers[0].StartTenant(ctx, base.TestTenantArgs{
		TenantID:            roachpb.MustMakeTenantID(11),
		DisableCreateTenant: true,
	})
	require.NoError(t, err)

	tenant11Conn, err := serverutils.OpenDBConnE(tenant11.SQLAddr(), "bank", false, tenant11.Stopper())
	require.NoError(t, err)
	tenant11DB := sqlutils.MakeSQLRunner(tenant11Conn)
	assertEqualQueries(t, tenant10DB, tenant11DB, fmt.Sprintf(`SELECT * FROM bank."%s"`, tableName))
}

func TestTenantBackupNemesis(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tempDir, tempDirCleanupFn := testutils.TempDir(t)
	defer tempDirCleanupFn()

	tc, hostSQLDB, hostClusterCleanupFn := backupRestoreTestSetupEmpty(
		t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DisableDefaultTestTenant: true,
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				},
			},
		},
	)
	defer hostClusterCleanupFn()

	hostSQLDB.Exec(t, "SET CLUSTER SETTING storage.mvcc.range_tombstones.enabled = true")
	hostSQLDB.Exec(t, "ALTER TENANT ALL SET CLUSTER SETTING storage.mvcc.range_tombstones.enabled = true")

	tenant10, err := tc.Servers[0].StartTenant(ctx, base.TestTenantArgs{
		TenantID: roachpb.MustMakeTenantID(10),
		TestingKnobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	require.NoError(t, err)
	tenant10Conn, err := serverutils.OpenDBConnE(
		tenant10.SQLAddr(), "defaultdb", false, tenant10.Stopper())
	require.NoError(t, err)
	_, err = tenant10Conn.Exec("CREATE DATABASE bank")
	require.NoError(t, err)
	_, err = tenant10Conn.Exec("USE bank")
	require.NoError(t, err)

	// Import initial data
	const numAccounts = 1000
	const payloadSize = 100
	const splits = 0
	bankData := bank.FromConfig(numAccounts, numAccounts, payloadSize, splits)
	l := workloadsql.InsertsDataLoader{BatchSize: 1000, Concurrency: 4}
	_, err = workloadsql.Setup(ctx, tenant10Conn, bankData, l)
	require.NoError(t, err)

	backupLoc := "nodelocal://0/tenant-backup"

	backupDone := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		pgURL, cleanupGoDB, err := sqlutils.PGUrlE(
			tenant10.SQLAddr(), "workload-worker" /* prefix */, url.User(username.RootUser))
		if err != nil {
			return err
		}
		defer cleanupGoDB()
		reg := histogram.NewRegistry(20*time.Second, "bank")
		ops, err := bankData.(workload.Opser).Ops(ctx, []string{pgURL.String()}, reg)
		if err != nil {
			return err
		}
		defer func() {
			if ops.Close != nil {
				ops.Close(ctx)
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

	rng, _ := randutil.NewPseudoRand()
	nemesisRunner := newBackupNemesis(t, rng, tenant10Conn)
	nemesisRunner.Start(ctx)

	var tablesToCheck []string
	g.GoCtx(func(ctx context.Context) error {
		defer close(backupDone)
		defer nemesisRunner.Stop()

		// Full Backup
		done := nemesisRunner.RequireStart()
		t.Logf("backup-nemesis: full backup started")
		hostSQLDB.Exec(t, fmt.Sprintf("BACKUP TENANT 10 INTO '%s'", backupLoc))
		t.Logf("backup-nemesis: full backup finished")
		<-done

		numIncrementals := 2 // rng.Intn(5) + 1
		for i := 0; i < numIncrementals; i++ {
			<-nemesisRunner.RequireStart()
			tablesToCheck = nemesisRunner.TablesToCheck()

			t.Logf("backup-nemesis: incremental backup started")
			hostSQLDB.Exec(t, fmt.Sprintf("BACKUP TENANT 10 INTO LATEST IN '%s'", backupLoc))
			t.Logf("backup-nemesis: incremental backup finished")
		}
		return nil
	})

	require.NoError(t, g.Wait())
	hostSQLDB.Exec(t, fmt.Sprintf("RESTORE TENANT 10 FROM LATEST IN '%s' WITH tenant = '11'", backupLoc))

	tenant11, err := tc.Servers[0].StartTenant(ctx, base.TestTenantArgs{
		TenantID:            roachpb.MustMakeTenantID(11),
		DisableCreateTenant: true,
	})
	require.NoError(t, err)

	tenant11Conn, err := serverutils.OpenDBConnE(
		tenant11.SQLAddr(), "bank", false, tenant11.Stopper())
	require.NoError(t, err)

	tenant10SQLDB := sqlutils.MakeSQLRunner(tenant10Conn)
	tenant11SQLDB := sqlutils.MakeSQLRunner(tenant11Conn)
	endTimeQuery := fmt.Sprintf(`SELECT max(end_time)::string FROM [SHOW BACKUP LATEST IN '%s']`, backupLoc)
	endTime := hostSQLDB.QueryStr(t, endTimeQuery)[0][0]
	assertEqualAcrossTenantRestore(t, tenant10SQLDB, tenant11SQLDB, "SELECT * FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank.bank]", endTime)
	for _, name := range tablesToCheck {
		t.Logf("backup-nemesis: checking table %s at %s", name, endTime)
		assertEqualAcrossTenantRestore(t, tenant10SQLDB, tenant11SQLDB, fmt.Sprintf(`SELECT count(*) FROM bank."%s"`, name), endTime)
		assertEqualAcrossTenantRestore(t, tenant10SQLDB, tenant11SQLDB,
			fmt.Sprintf(`SELECT * FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank."%s"]`, name), endTime)
	}

}

func assertEqualAcrossTenantRestore(
	t *testing.T,
	oldTenant *sqlutils.SQLRunner,
	newTenant *sqlutils.SQLRunner,
	queryBase string,
	endTime string,
) {
	oldTenantQuery := fmt.Sprintf("%s AS OF SYSTEM TIME '%s'", queryBase, endTime)
	newTenantQuery := queryBase

	t.Helper()
	out1 := oldTenant.QueryStr(t, oldTenantQuery)
	out2 := newTenant.QueryStr(t, newTenantQuery)
	require.Equal(t, out1, out2)
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
	t  *testing.T
	db *gosql.DB

	grp    ctxgroup.Group
	cancel context.CancelFunc
	rng    *rand.Rand

	nemeses []nemesis

	mu struct {
		syncutil.Mutex
		oneTimeListeners []chan nemesisNotification
		tablesToCheck    []string
	}
}

type nemesis struct {
	name string
	impl func(context.Context, *randomBackupNemesis, *gosql.DB) error
}

type nemesisNotification struct {
	name string
	done chan struct{}
}

func newBackupNemesis(t *testing.T, rng *rand.Rand, db *gosql.DB) *randomBackupNemesis {
	return &randomBackupNemesis{
		t:   t,
		rng: rng,
		db:  db,

		nemeses: []nemesis{
			{name: "CANCELED IMPORT INTO",
				impl: func(ctx context.Context, n *randomBackupNemesis, db *gosql.DB) error {
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
				impl: func(ctx context.Context, n *randomBackupNemesis, db *gosql.DB) error {
					tableName, err := n.makeRandomBankTable("import_into")
					if err != nil {
						return err
					}
					n.addTable(tableName)
					return nil
				},
			},
			{name: "CREATE INDEX",
				impl: func(ctx context.Context, n *randomBackupNemesis, db *gosql.DB) error {
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
				impl: func(ctx context.Context, n *randomBackupNemesis, db *gosql.DB) error {
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

func (r *randomBackupNemesis) RequireStart() chan struct{} {
	notifyCh := make(chan nemesisNotification)
	r.registerOneTimeListener(notifyCh)
	newNemesis := <-notifyCh
	return newNemesis.done
}

func (r *randomBackupNemesis) TablesToCheck() []string {
	r.mu.Lock()
	ret := append([]string(nil), r.mu.tablesToCheck...)
	r.mu.Unlock()
	return ret
}

func (r *randomBackupNemesis) addTable(name string) {
	r.mu.Lock()
	r.mu.tablesToCheck = append(r.mu.tablesToCheck, name)
	r.mu.Unlock()
	r.t.Logf("backup-nemesis: adding %s to table list", name)
}

func (r *randomBackupNemesis) registerOneTimeListener(ch chan nemesisNotification) {
	r.mu.Lock()
	r.mu.oneTimeListeners = append(r.mu.oneTimeListeners, ch)
	r.mu.Unlock()
}

func (r *randomBackupNemesis) notifyOneTimeListeners(done chan struct{}) {
	r.mu.Lock()
	for _, l := range r.mu.oneTimeListeners {
		l <- nemesisNotification{
			done: done,
		}
	}
	r.mu.oneTimeListeners = nil
	r.mu.Unlock()
}

func (r *randomBackupNemesis) runNemesis(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		n := r.nemeses[r.rng.Intn(len(r.nemeses))]
		doneCh := make(chan struct{})
		r.notifyOneTimeListeners(doneCh)
		r.t.Logf("backup-nemesis: %s started", n.name)
		if err := n.impl(ctx, r, r.db); err != nil {
			r.t.Logf("backup-nemesis: %s failed: %s", n.name, err)
			close(doneCh)
			return err
		}
		r.t.Logf("backup-nemesis: %s finished", n.name)
		close(doneCh)
	}
}

func (r *randomBackupNemesis) makeRandomBankTable(prefix string) (string, error) {
	tableName := fmt.Sprintf("%s_%s", prefix, uuid.FastMakeV4().String())
	if _, err := r.db.Exec(fmt.Sprintf(`CREATE TABLE "%s" (id INT PRIMARY KEY, n INT, s STRING)`, tableName)); err != nil {
		return "", err
	}
	importStmt := fmt.Sprintf(`IMPORT INTO "%s" CSV DATA ('workload:///csv/bank/bank?payload-bytes=100&row-end=1&row-start=0&rows=1000&seed=1&version=1.0.0')`, tableName)
	if _, err := r.db.Exec(importStmt); err != nil {
		return tableName, err
	}
	return tableName, nil
}
