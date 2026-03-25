// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/blobfixture"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Maps a fixture database name to the expected number of tables in the
// database, useful for verifying that the fingerprint of the fixture is as
// expected.
var expectedNumTables = map[string]int{
	"tpcc": 9,
}

type BackupFixture interface {
	Kind() string
	// The database that is backed up.
	DatabaseName() string
}

type TpccFixture struct {
	Name                 string
	ImportWarehouses     int
	WorkloadWarehouses   int
	ChainLength          int
	NumChains            int
	CompactionThreshold  int
	CompactionWindow     int
	FullFrequency        string
	IncrementalFrequency string
	RestoredSizeEstimate string
}

var _ BackupFixture = TpccFixture{}

func (f TpccFixture) Kind() string {
	return f.Name
}

func (f TpccFixture) DatabaseName() string {
	return "tpcc"
}

// TinyFixture is a TPCC fixture that is intended for smoke tests, local
// testing, and continous testing of the fixture generation logic.
var TinyFixture = TpccFixture{
	Name:                 "tpcc-10",
	ImportWarehouses:     10,
	WorkloadWarehouses:   10,
	ChainLength:          4,
	CompactionThreshold:  4,
	CompactionWindow:     3,
	RestoredSizeEstimate: "700MiB",
}

// SmallFixture is a TPCC fixture that is intended to be quick to restore and
// cheap to generate for continous testing of the fixture generation logic.
var SmallFixture = TpccFixture{
	Name:                 "tpcc-5k",
	ImportWarehouses:     5000,
	WorkloadWarehouses:   1000,
	ChainLength:          48,
	CompactionThreshold:  4,
	CompactionWindow:     3,
	RestoredSizeEstimate: "350GiB",
}

// MediumFixture is a TPCC fixture sized so that it is a tight fit in 3 nodes
// with the smallest supported node size of 4 VCPU per node.
var MediumFixture = TpccFixture{
	Name:                 "tpcc-30k",
	ImportWarehouses:     30000,
	WorkloadWarehouses:   5000,
	ChainLength:          400,
	CompactionThreshold:  4,
	CompactionWindow:     3,
	RestoredSizeEstimate: "2TiB",
}

// LargeFixture is a TPCC fixture sized so that it is a tight fit in 3 nodes
// with the maximum supported node density of 8 TiB storage per node. If the
// node storage density increases, then the size of this fixture should be
// increased.
var LargeFixture = TpccFixture{
	Name:                 "tpcc-300k",
	ImportWarehouses:     300000,
	WorkloadWarehouses:   7500,
	ChainLength:          400,
	CompactionThreshold:  4,
	CompactionWindow:     3,
	RestoredSizeEstimate: "20TiB",
}

type backupFixtureSpecs struct {
	// hardware specifies the roachprod specs to create the fixture on.
	hardware hardwareSpecs

	fixture TpccFixture

	timeout time.Duration

	suites registry.SuiteSet

	clouds []spec.Cloud

	// If non-empty, the test will be skipped with the supplied reason.
	skip string

	// If set, the fixture will not be fingerprinted after the backup. Used for
	// larger fixtures where fingerprinting is too expensive.
	skipFingerprint bool
}

const scheduleLabel = "tpcc_backup"

func CreateScheduleStatement(fixture TpccFixture, uri url.URL) string {
	// This backup schedule will first run a full backup immediately and then
	// the incremental backups at the configured frequency. The full backup
	// frequency defaults to @weekly (single chain) but can be overridden via
	// FullFrequency for multi-full tests.

	fullFreq := fixture.FullFrequency
	if fullFreq == "" {
		fullFreq = "@weekly"
	}
	incFreq := fixture.IncrementalFrequency
	if incFreq == "" {
		incFreq = "* * * * *"
	}

	statement := fmt.Sprintf(
		`CREATE SCHEDULE IF NOT EXISTS "%s"
FOR BACKUP DATABASE %s
INTO '%s'
RECURRING '%s'
FULL BACKUP '%s'
WITH SCHEDULE OPTIONS first_run = 'now';
`, scheduleLabel, fixture.DatabaseName(), uri.String(), incFreq, fullFreq)
	return statement
}

type backupDriver struct {
	sp       backupFixtureSpecs
	t        test.Test
	c        cluster.Cluster
	fixture  blobfixture.FixtureMetadata
	registry *blobfixture.Registry
}

func (bd *backupDriver) prepareCluster(ctx context.Context) {
	startOptions := option.NewStartOpts(option.NoBackupSchedule)
	startOptions.RoachprodOpts.ExtraArgs = append(
		startOptions.RoachprodOpts.ExtraArgs,
		"--vmodule=cloud_logging_transport=1")
	bd.c.Start(
		ctx, bd.t.L(), startOptions,
		install.MakeClusterSettings(
			install.ClusterSettingsOption{
				// Large imports can run into a death spiral where splits fail because
				// there is a snapshot backlog, which makes the snapshot backlog worse
				// because add sst causes ranges to fall behind and need recovery snapshots
				// to catch up.
				"kv.snapshot_rebalance.max_rate":                    "256 MiB",
				"server.debug.default_vmodule":                      "s3_storage=2",
				"cloudstorage.s3.client_retry_token_bucket.enabled": "false",
			},
		))
}

func (bd *backupDriver) initWorkload(ctx context.Context) {
	bd.t.L().Printf("importing tpcc with %d warehouses", bd.sp.fixture.ImportWarehouses)

	urls, err := bd.c.InternalPGUrl(ctx, bd.t.L(), bd.c.Node(1), roachprod.PGURLOptions{})
	require.NoError(bd.t, err)

	cmd := roachtestutil.NewCommand("./cockroach workload fixtures import tpcc").
		Arg("%q", urls[0]).
		Option("checks=false").
		Flag("warehouses", bd.sp.fixture.ImportWarehouses).
		String()

	bd.c.Run(ctx, option.WithNodes(bd.c.WorkloadNode()), cmd)
}

func (bd *backupDriver) runWorkload(ctx context.Context) (func(), error) {
	bd.t.L().Printf("starting tpcc workload against %d", bd.sp.fixture.WorkloadWarehouses)

	workloadCtx, workloadCancel := context.WithCancel(ctx)
	m := bd.c.NewDeprecatedMonitor(workloadCtx)
	m.Go(func(ctx context.Context) error {
		cmd := roachtestutil.NewCommand("./cockroach workload run tpcc").
			Arg("{pgurl%s}", bd.c.CRDBNodes()).
			Option("tolerate-errors=true").
			// Increase the ramp time to prevent the initial connection spike from
			// the workload starting from overloading the cluster. Connection set up
			// is not yet integreated with admission control. Starting the full
			// backup and workload at the same time caused the cluster to hiccup and
			// a few nodes shed leaseholders
			Flag("ramp", "10m").
			// Limit the number of connections to limit total concurrency. 2
			// connections per warehouse was fine in the steady state, but lead to
			// periodic spikes in the number of connections.
			Flag("conns", int(float64(bd.sp.fixture.WorkloadWarehouses)*0.5)).
			// Increase the lifetime of connections to prevent connection churn from
			// causing unecessary load.
			Flag("max-conn-lifetime", 2*time.Hour).
			Flag("max-conn-lifetime-jitter", time.Hour).
			Flag("max-conn-idle-time", 2*time.Hour).
			Flag("warehouses", bd.sp.fixture.WorkloadWarehouses).
			String()
		err := bd.c.RunE(ctx, option.WithNodes(bd.c.WorkloadNode()), cmd)
		if err != nil && ctx.Err() == nil {
			return err
		}
		// We expect the workload to return a context cancelled error because
		// the roachtest driver cancels the monitor's context after the backup
		// schedule completes.
		if err != nil && ctx.Err() == nil {
			// Implies the workload context was not cancelled and the workload cmd returned a
			// different error.
			return errors.Wrapf(err, `Workload context was not cancelled. Error returned by workload cmd`)
		}
		bd.t.L().Printf("workload successfully finished")
		return nil
	})

	return func() {
		workloadCancel()
		m.Wait()
	}, nil
}

// scheduleBackups begins the backup schedule.
func (bd *backupDriver) scheduleBackups(ctx context.Context) {
	bd.t.L().Printf("creating backup schedule %d", bd.sp.fixture.WorkloadWarehouses)
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	defer conn.Close()
	if bd.sp.fixture.CompactionThreshold > 0 {
		bd.t.L().Printf(
			"enabling compaction with threshold %d and window size %d",
			bd.sp.fixture.CompactionThreshold, bd.sp.fixture.CompactionWindow,
		)
		_, err := conn.Exec(fmt.Sprintf(
			"SET CLUSTER SETTING backup.compaction.threshold = %d", bd.sp.fixture.CompactionThreshold,
		))
		require.NoError(bd.t, err)
		_, err = conn.Exec(fmt.Sprintf(
			"SET CLUSTER SETTING backup.compaction.window_size = %d", bd.sp.fixture.CompactionWindow,
		))
		require.NoError(bd.t, err)
	}
	createScheduleStatement := CreateScheduleStatement(bd.sp.fixture, bd.registry.URI(bd.fixture.DataPath))
	_, err := conn.Exec(createScheduleStatement)
	require.NoError(bd.t, err)
}

// monitorBackups pauses the schedule once the target number of backups have
// been taken across all chains and waits for in-flight jobs to drain.
func (bd *backupDriver) monitorBackups(ctx context.Context) error {
	db := bd.c.Conn(ctx, bd.t.L(), 1)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return errors.Wrap(err, "pinning single connection")
	}
	defer func() { _ = conn.Close() }()
	sql := sqlutils.MakeSQLRunner(conn)
	sql.Exec(bd.t, "SET use_backups_with_ids = true")

	fixtureURI := bd.registry.URI(bd.fixture.DataPath)

	targetNumChains := 1
	if bd.sp.fixture.NumChains > 0 {
		targetNumChains = bd.sp.fixture.NumChains
	}

	schedulePaused := false
	for {
		time.Sleep(1 * time.Minute)

		compSuccess, compRunning, compFailed, err := bd.compactionJobStates(sql)
		if err != nil {
			return err
		}
		_, backupRunning, backupFailed, err := bd.backupJobStates(sql)
		if err != nil {
			return err
		}
		if len(backupFailed) > 0 {
			return errors.Newf("backup jobs failed: %v", backupFailed)
		}
		if bd.sp.fixture.CompactionThreshold > 0 {
			bd.t.L().Printf(
				"%d compaction jobs succeeded, %d running",
				len(compSuccess), len(compRunning),
			)
			if len(compFailed) > 0 {
				bd.t.L().Errorf("compaction jobs failed: %v", compFailed)
			}
		}

		if schedulePaused {
			if len(backupRunning) > 0 {
				bd.t.L().Printf("waiting for %d backup jobs to finish", len(backupRunning))
			} else if len(compRunning) > 0 {
				bd.t.L().Printf("waiting for %d compaction jobs to finish", len(compRunning))
			} else {
				break
			}
			continue
		}

		var numChains int
		sql.QueryRow(bd.t,
			"SELECT count(*) FROM [SHOW BACKUPS IN $1 WITH DEBUG] WHERE start_time IS NULL",
			fixtureURI.String()).Scan(&numChains)
		var totalBackups int
		sql.QueryRow(bd.t,
			"SELECT count(*) FROM [SHOW BACKUPS IN $1]",
			fixtureURI.String()).Scan(&totalBackups)
		bd.t.L().Printf(
			"%d scheduled backups taken (%d fulls + %d incrementals)",
			totalBackups, numChains, totalBackups-numChains,
		)

		if numChains >= targetNumChains &&
			totalBackups >= bd.sp.fixture.ChainLength*targetNumChains {
			pauseSchedulesQuery := fmt.Sprintf(
				`PAUSE SCHEDULES WITH x AS (SHOW SCHEDULES) SELECT id FROM x WHERE label = '%s'`,
				scheduleLabel,
			)
			sql.Exec(bd.t, pauseSchedulesQuery)
			schedulePaused = true
		}
	}

	// Do a final accounting of compactions to determine if the test as a whole should fail.
	compSuccess, _, compFailed, err := bd.compactionJobStates(sql)
	if err != nil {
		return err
	}
	if len(compSuccess) < len(compFailed) {
		return errors.Newf(
			"more compaction jobs failed than succeeded (%d failed, %d succeeded), failures: %v",
			len(compFailed), len(compSuccess), compFailed,
		)
	}

	return nil
}

type jobMeta struct {
	jobID jobspb.JobID
	state jobs.State
	error string
}

// compactionJobStates returns the state of the compaction jobs, returning
// a partition of jobs that succeeded, are running, and failed.
func (bd *backupDriver) compactionJobStates(
	sql *sqlutils.SQLRunner,
) ([]jobMeta, []jobMeta, []jobMeta, error) {
	if bd.sp.fixture.CompactionThreshold == 0 {
		return nil, nil, nil, nil
	}
	s, r, f, err := bd.queryJobStates(
		sql, "job_type = 'BACKUP' AND description ILIKE 'COMPACT BACKUPS%'",
	)
	return s, r, f, errors.Wrapf(err, "error querying compaction job states")
}

// backupJobStates returns the state of the backup jobs, returning
// a partition of jobs that succeeded, are running, and failed.
func (bd *backupDriver) backupJobStates(
	sql *sqlutils.SQLRunner,
) ([]jobMeta, []jobMeta, []jobMeta, error) {
	s, r, f, err := bd.queryJobStates(
		sql, "job_type = 'BACKUP' AND description ILIKE 'BACKUP %'",
	)
	return s, r, f, errors.Wrapf(err, "error querying backup job states")
}

// queryJobStates queries the job table and returns a partition of jobs that
// succeeded, are running, and failed. The filter is applied to the query to
// limit the jobs searched. If the filter is empty, all jobs are searched.
func (bd *backupDriver) queryJobStates(
	sql *sqlutils.SQLRunner, filter string,
) ([]jobMeta, []jobMeta, []jobMeta, error) {
	query := "SELECT job_id, status, error FROM [SHOW JOBS]"
	if filter != "" {
		query += fmt.Sprintf(" WHERE %s", filter)
	}
	rows := sql.Query(bd.t, query)
	defer rows.Close()
	var jobMetas []jobMeta
	for rows.Next() {
		var job jobMeta
		if err := rows.Scan(&job.jobID, &job.state, &job.error); err != nil {
			return nil, nil, nil, errors.Wrapf(err, "error scanning job")
		}
		jobMetas = append(jobMetas, job)
	}
	var successes, running, failures []jobMeta
	for _, job := range jobMetas {
		switch job.state {
		case jobs.StateSucceeded:
			successes = append(successes, job)
		case jobs.StateRunning:
			running = append(running, job)
		case jobs.StateFailed:
			failures = append(failures, job)
		default:
			bd.t.L().Printf(`unexpected job %d in state %s`, job.jobID, job.state)
		}
	}
	return successes, running, failures, nil
}

// fingerprintFixture computes fingerprints for the fixture as of the time of
// its last incremental backup. It maps the fully qualified name of each table
// to its fingerprint.
func (bd *backupDriver) fingerprintFixture(ctx context.Context, asOfTime string) map[string]string {
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	defer conn.Close()
	return fingerprintDatabase(
		bd.t, conn, bd.sp.fixture.DatabaseName(), asOfTime)
}

func (bd *backupDriver) checkRestorability(ctx context.Context) error {
	bd.t.L().Printf("running schema-only online restore to verify restorability of fixture")
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	defer conn.Close()

	uri := bd.registry.URI(bd.fixture.DataPath)
	var restoreJobID jobspb.JobID
	err := conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`RESTORE DATABASE %s FROM LATEST IN '%s'
			WITH detached, new_db_name='%s_restored', schema_only, experimental deferred copy`,
			bd.sp.fixture.DatabaseName(),
			uri.String(),
			bd.sp.fixture.DatabaseName(),
		),
	).Scan(&restoreJobID)
	if err != nil {
		return errors.Wrapf(err, "error starting restore job")
	}

	bd.t.L().Printf("waiting for restore job %d to complete", restoreJobID)
	if err := WaitForSucceeded(ctx, conn, restoreJobID, 5*time.Minute); err != nil {
		return errors.Wrapf(err, "error waiting for restore job %d to complete", restoreJobID)
	}
	bd.t.L().Printf("restore job %d completed successfully", restoreJobID)
	return nil
}

// getLatestAOST returns the end time as seen in SHOW BACKUP of the latest
// backup in the fixture.
func (bd *backupDriver) getLatestAOST(ctx context.Context) string {
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	defer conn.Close()
	sql := sqlutils.MakeSQLRunner(conn)
	sql.Exec(bd.t, "SET use_backups_with_ids = true")
	uri := bd.registry.URI(bd.fixture.DataPath)
	query := fmt.Sprintf(
		`SELECT end_time FROM
		[SHOW BACKUP FROM LATEST IN '%s']
		LIMIT 1`,
		uri.String(),
	)
	var endTime string
	sql.QueryRow(bd.t, query).Scan(&endTime)
	return endTime
}

// fingerprintDatabase fingerprints all of the tables in the provided database
// and returns a map of fully qualified table names to their fingerprints.
// If AOST is not provided, the current time is used as the AOST.
func fingerprintDatabase(
	t test.Test, conn *gosql.DB, dbName string, aost string,
) map[string]string {
	sql := sqlutils.MakeSQLRunner(conn)
	tables := getDatabaseTables(t, sql, dbName)
	if len(tables) == 0 {
		t.L().Printf("no tables found in database %s", dbName)
		return nil
	}
	require.Len(t, tables, expectedNumTables[dbName], "unexpected number of tables in database %s", dbName)
	t.L().Printf("fingerprinting %d tables in database %s", len(tables), dbName)

	fingerprints := make(map[string]string)
	var mu syncutil.Mutex
	start := timeutil.Now()
	group := t.NewErrorGroup()
	for _, table := range tables {
		group.Go(func(ctx context.Context, log *logger.Logger) error {
			fpContents := newFingerprintContents(conn, table)
			if err := fpContents.Load(
				ctx, log, aost, nil, /* tableContents */
			); err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			fingerprints[table] = fpContents.fingerprints
			return nil
		})
	}
	require.NoError(t, group.WaitE(), "error fingerprinting tables in database %s", dbName)
	t.L().Printf(
		"fingerprinted %d tables in %s in %s",
		len(tables), dbName, timeutil.Since(start),
	)
	require.Len(
		t, fingerprints, expectedNumTables[dbName],
		"unexpected number of fingerprints for database %s", dbName,
	)
	return fingerprints
}

// getDatabaseTables returns the fully qualified name of every table in the
// fixture.
// Note: This assumes there aren't any funky characters in the identifiers, so
// nothing is SQL-escaped.
func getDatabaseTables(t test.Test, sql *sqlutils.SQLRunner, db string) []string {
	tablesQuery := fmt.Sprintf(`SELECT schema_name, table_name FROM [SHOW TABLES FROM %s]`, db)
	rows := sql.Query(t, tablesQuery)
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			require.NoError(t, err, "error scanning table name")
		}
		tables = append(tables, fmt.Sprintf(`%s.%s.%s`, db, schemaName, tableName))
	}
	require.NoError(t, rows.Err(), "error iterating over tables in database %s", db)
	return tables
}

func fixtureDirectory() string {
	if clusterversion.DevelopmentBranch {
		return "roachtest/master"
	}
	version := clusterupgrade.CurrentVersion()
	return version.Format("roachtest/v%X.%Y")
}

// GetFixtureRegistry returns the backup fixture registry for the given cloud provider.
func GetFixtureRegistry(ctx context.Context, t test.Test, cloud spec.Cloud) *blobfixture.Registry {
	var uri url.URL
	switch cloud {
	case spec.AWS:
		uri = url.URL{
			Scheme:   "s3",
			Host:     "cockroach-fixtures-us-east-2",
			RawQuery: "AUTH=implicit",
		}
	case spec.Azure:
		uri = url.URL{
			Scheme:   "azure-blob",
			Host:     "cockroachdb-fixtures-eastus",
			RawQuery: "AUTH=implicit&AZURE_ACCOUNT_NAME=roachtest",
		}
	case spec.GCE, spec.Local:
		account, err := vm.Providers["gce"].FindActiveAccount(t.L())
		require.NoError(t, err)
		t.L().Printf("using GCE account %s", account)

		uri = url.URL{
			Scheme:   "gs",
			Host:     "cockroach-fixtures-us-east1",
			RawQuery: "AUTH=implicit",
		}
	default:
		t.Fatalf("fixtures not supported on %s", cloud)
	}

	uri.Path = path.Join(uri.Path, fixtureDirectory())

	registry, err := blobfixture.NewRegistry(ctx, uri)
	require.NoError(t, err)

	return registry
}

func registerBackupFixtures(r registry.Registry) {
	specs := []backupFixtureSpecs{
		{
			fixture: TinyFixture,
			hardware: makeHardwareSpecs(hardwareSpecs{
				workloadNode: true,
			}),
			timeout: 30 * time.Minute,
			suites:  registry.Suites(registry.Nightly),
			clouds:  []spec.Cloud{spec.AWS, spec.Azure, spec.GCE, spec.Local},
		},
		{
			fixture: SmallFixture,
			hardware: makeHardwareSpecs(hardwareSpecs{
				workloadNode: true,
			}),
			// Fingerprinting is measured to take about 40 minutes, and occasionally longer than an hour,
			// on a 350 GB fixture on top of the allocated 2 hours for the test.
			timeout: 4 * time.Hour,
			suites:  registry.Suites(registry.Nightly),
			clouds:  []spec.Cloud{spec.AWS, spec.Azure, spec.GCE},
		},
		{
			fixture: MediumFixture,
			hardware: makeHardwareSpecs(hardwareSpecs{
				workloadNode: true,
				nodes:        9,
				cpus:         16,
			}),
			timeout: 12 * time.Hour,
			suites:  registry.Suites(registry.Weekly),
			clouds:  []spec.Cloud{spec.AWS, spec.Azure, spec.GCE},
			// The fixture takes an estimated 3.5 hours to fingerprint, so we skip it.
			skipFingerprint: true,
		},
		{
			fixture: LargeFixture,
			hardware: makeHardwareSpecs(hardwareSpecs{
				workloadNode: true,
				nodes:        9,
				cpus:         32,
				volumeSize:   4000,
			}),
			timeout: 40 * time.Hour,
			suites:  registry.Suites(registry.Weekly),
			// The large fixture is only generated on GCE to reduce the cost of
			// storing the fixtures.
			clouds: []spec.Cloud{spec.GCE},
			// Well medium fixture takes 3.5 hours to fingerprint, so we dare not
			// consider fingerprinting the large fixture.
			skipFingerprint: true,
		},
	}
	for _, bf := range specs {
		bf := bf
		clusterSpec := bf.hardware.makeClusterSpecs(r)
		r.Add(registry.TestSpec{
			Name: fmt.Sprintf(
				"backupFixture/tpcc/warehouses=%d/incrementals=%d",
				bf.fixture.ImportWarehouses, bf.fixture.ChainLength,
			),
			Owner:             registry.OwnerDisasterRecovery,
			Cluster:           clusterSpec,
			Timeout:           bf.timeout,
			EncryptionSupport: registry.EncryptionMetamorphic,
			CompatibleClouds:  registry.Clouds(bf.clouds...),
			Suites:            bf.suites,
			Skip:              bf.skip,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				registry := GetFixtureRegistry(ctx, t, c.Cloud())

				handle, err := registry.Create(ctx, bf.fixture.Name, t.L())
				require.NoError(t, err)

				bd := backupDriver{
					t:        t,
					c:        c,
					sp:       bf,
					fixture:  handle.Metadata(),
					registry: registry,
				}
				bd.prepareCluster(ctx)

				bd.initWorkload(ctx)

				stopWorkload, err := bd.runWorkload(ctx)
				require.NoError(t, err)

				bd.scheduleBackups(ctx)
				require.NoError(t, bd.monitorBackups(ctx))

				stopWorkload()

				if !bf.skipFingerprint {
					fingerprintTime := bd.getLatestAOST(ctx)
					fingerprint := bd.fingerprintFixture(ctx, fingerprintTime)
					require.NoError(t, handle.SetFingerprint(ctx, fingerprint, fingerprintTime))
				}

				require.NoError(t, bd.checkRestorability(ctx))

				require.NoError(t, handle.SetReadyAt(ctx))
			},
		})
	}
}

const (
	// scheduleChainingDefaultGCTTLSeconds is the GC TTL set for the default range when running a
	// schedule chaining test. This is set to 50 seconds in order to be less than the default 1 minute
	// incremental backup frequency for these tests.
	scheduleChainingDefaultGCTTLSeconds = 50

	// scheduleChainingNotIncludedGCTTLSeconds is the GC TTL set for spans which should not be included
	// in backups during these tests. It is set to an extremely low value in order to cause heavy stress
	// in situations where we are attempting to protect or backup spans that we should not be touching.
	scheduleChainingNotIncludedGCTTLSeconds = 10

	// scheduleChainingDefaultFullFrequency is the scheduled frequency for full backups in a schedule
	// chaining test.
	scheduleChainingDefaultFullFrequency = "*/5 * * * *"

	// scheduleChainingDefaultNumChains is the number of full backups to take in a schedule chaining test.
	// This value should be at least 2 in order to test the PTS chaining mechanism against situations
	// where we transition between backup chains.
	scheduleChainingDefaultNumChains = 2
)

func registerBackupScheduleChaining(r registry.Registry) {
	specs := []backupFixtureSpecs{
		{
			fixture: TinyFixture,
			hardware: makeHardwareSpecs(hardwareSpecs{
				workloadNode: true,
			}),
			timeout: 30 * time.Minute,
			suites:  registry.Suites(registry.Nightly),
			clouds:  []spec.Cloud{spec.AWS, spec.Azure, spec.GCE, spec.Local},
		},
		{
			fixture: MediumFixture,
			hardware: makeHardwareSpecs(hardwareSpecs{
				workloadNode: true,
				nodes:        9,
				cpus:         16,
			}),
			timeout:         16 * time.Hour,
			suites:          registry.Suites(registry.Weekly),
			clouds:          []spec.Cloud{spec.AWS, spec.Azure, spec.GCE},
			skipFingerprint: true,
		},
	}
	for _, spec := range specs {
		spec := spec
		clusterSpec := spec.hardware.makeClusterSpecs(r)
		spec.fixture.NumChains = scheduleChainingDefaultNumChains
		spec.fixture.FullFrequency = scheduleChainingDefaultFullFrequency
		r.Add(registry.TestSpec{
			Name:              fmt.Sprintf("backupFixture/schedule-chaining/%s", spec.fixture.Name),
			Owner:             registry.OwnerDisasterRecovery,
			Cluster:           clusterSpec,
			Timeout:           spec.timeout,
			EncryptionSupport: registry.EncryptionMetamorphic,
			CompatibleClouds:  registry.Clouds(spec.clouds...),
			Suites:            spec.suites,
			Skip:              spec.skip,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				registry := GetFixtureRegistry(ctx, t, c.Cloud())

				handle, err := registry.Create(ctx, fmt.Sprintf(
					"%s-schedule-chaining", spec.fixture.Name,
				), t.L())
				require.NoError(t, err)

				bd := backupDriver{
					t:        t,
					c:        c,
					sp:       spec,
					fixture:  handle.Metadata(),
					registry: registry,
				}
				bd.prepareCluster(ctx)

				// Shorten the GC TTL to be less than the frequency of the backup schedule.
				conn := bd.c.Conn(ctx, t.L(), 1 /* node */)
				defer conn.Close()
				_, err = conn.Exec(
					"ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = $1",
					scheduleChainingDefaultGCTTLSeconds,
				)
				require.NoError(t, err)
				// Shorten the GC TTL of the system database, which should not be included in the backup.
				_, err = conn.Exec(
					"ALTER DATABASE system CONFIGURE ZONE USING gc.ttlseconds = $1",
					scheduleChainingNotIncludedGCTTLSeconds,
				)
				require.NoError(t, err)

				bd.initWorkload(ctx)
				stopWorkload, err := bd.runWorkload(ctx)
				require.NoError(t, err)

				bd.scheduleBackups(ctx)
				// TODO(art): not sure about this one, but let's just try it and see what happens for now
				time.Sleep(3 * time.Minute)
				_, err = conn.Exec(`DROP DATABASE "tpcc"`)
				require.NoError(t, err)
				_, err = conn.Exec(`CREATE DATABASE IF NOT EXISTS "tpcc"`)
				require.NoError(t, err)
				require.NoError(t, bd.monitorBackups(ctx))

				stopWorkload()

				if !spec.skipFingerprint {
					fingerprintTime := bd.getLatestAOST(ctx)
					fingerprint := bd.fingerprintFixture(ctx, fingerprintTime)
					require.NoError(t, handle.SetFingerprint(ctx, fingerprint, fingerprintTime))
				}

				require.NoError(t, bd.checkRestorability(ctx))

				require.NoError(t, handle.SetReadyAt(ctx))
			},
		})
	}
}

func registerBlobFixtureGC(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "blobfixture/gc",
		Owner:            registry.OwnerDisasterRecovery,
		Cluster:          r.MakeClusterSpec(1, spec.CPU(2)),
		CompatibleClouds: registry.Clouds(spec.GCE, spec.AWS),
		Timeout:          1 * time.Hour,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// TODO(jeffswenson): ideally we would run the GC on the scheduled node
			// so that it is close to the fixture repository.
			registry := GetFixtureRegistry(ctx, t, c.Cloud())
			require.NoError(t, registry.GC(ctx, t.L()))
		},
	})
}
