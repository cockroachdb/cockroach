// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type BackupFixture interface {
	Kind() string
	// The database that is backed up.
	DatabaseName() string
}

type TpccFixture struct {
	Name                   string
	ImportWarehouses       int
	WorkloadWarehouses     int
	IncrementalChainLength int
	CompactionThreshold    int
	CompactionWindow       int
	RestoredSizeEstimate   string
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
	Name:                   "tpcc-10",
	ImportWarehouses:       10,
	WorkloadWarehouses:     10,
	IncrementalChainLength: 4,
	CompactionThreshold:    4,
	CompactionWindow:       3,
	RestoredSizeEstimate:   "700MiB",
}

// SmallFixture is a TPCC fixture that is intended to be quick to restore and
// cheap to generate for continous testing of the fixture generation logic.
var SmallFixture = TpccFixture{
	Name:                   "tpcc-5k",
	ImportWarehouses:       5000,
	WorkloadWarehouses:     1000,
	IncrementalChainLength: 48,
	CompactionThreshold:    4,
	CompactionWindow:       3,
	RestoredSizeEstimate:   "350GiB",
}

// MediumFixture is a TPCC fixture sized so that it is a tight fit in 3 nodes
// with the smallest supported node size of 4 VCPU per node.
var MediumFixture = TpccFixture{
	Name:                   "tpcc-30k",
	ImportWarehouses:       30000,
	WorkloadWarehouses:     5000,
	IncrementalChainLength: 400,
	CompactionThreshold:    4,
	CompactionWindow:       3,
	RestoredSizeEstimate:   "2TiB",
}

// LargeFixture is a TPCC fixture sized so that it is a tight fit in 3 nodes
// with the maximum supported node density of 8 TiB storage per node. If the
// node storage density increases, then the size of this fixture should be
// increased.
var LargeFixture = TpccFixture{
	Name:                   "tpcc-300k",
	ImportWarehouses:       300000,
	WorkloadWarehouses:     7500,
	IncrementalChainLength: 400,
	CompactionThreshold:    4,
	CompactionWindow:       3,
	RestoredSizeEstimate:   "20TiB",
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
}

const scheduleLabel = "tpcc_backup"

func CreateScheduleStatement(uri url.URL) string {
	// This backup schedule will first run a full backup immediately and then the
	// incremental backups every minute until the user cancels the backup
	// schedules. To ensure that only one full backup chain gets created,
	// schedule the full back up on backup will get created on Sunday at Midnight
	// ;)
	statement := fmt.Sprintf(
		`CREATE SCHEDULE IF NOT EXISTS "%s"
FOR BACKUP DATABASE tpcc
INTO '%s'
RECURRING '* * * * *'
FULL BACKUP '@weekly'
WITH SCHEDULE OPTIONS first_run = 'now';
`, scheduleLabel, uri.String())
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
	bd.c.Start(ctx, bd.t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings(install.ClusterSettingsOption{
		// Large imports can run into a death spiral where splits fail because
		// there is a snapshot backlog, which makes the snapshot backlog worse
		// because add sst causes ranges to fall behind and need recovery snapshots
		// to catch up.
		"kv.snapshot_rebalance.max_rate": "256 MiB",
	}))
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
	m := bd.c.NewMonitor(workloadCtx)
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
	bd.t.L().Printf("creating backup schedule", bd.sp.fixture.WorkloadWarehouses)
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
	createScheduleStatement := CreateScheduleStatement(bd.registry.URI(bd.fixture.DataPath))
	_, err := conn.Exec(createScheduleStatement)
	require.NoError(bd.t, err)
}

// monitorBackups pauses the schedule once the target number of backups in the
// chain have been taken.
func (bd *backupDriver) monitorBackups(ctx context.Context) error {
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	defer conn.Close()
	sql := sqlutils.MakeSQLRunner(conn)
	fixtureURI := bd.registry.URI(bd.fixture.DataPath)
	const (
		WaitingFirstFull = iota
		RunningIncrementals
		WaitingCompactions
		Done
	)
	state := WaitingFirstFull
	for state != Done {
		time.Sleep(1 * time.Minute)
		compSuccess, compRunning, compFailed, err := bd.compactionJobStates(sql)
		if err != nil {
			return err
		}
		switch state {
		case WaitingFirstFull:
			var activeScheduleCount int
			scheduleCountQuery := fmt.Sprintf(
				`SELECT count(*) FROM [SHOW SCHEDULES] WHERE label='%s' AND schedule_status='ACTIVE'`, scheduleLabel,
			)
			sql.QueryRow(bd.t, scheduleCountQuery).Scan(&activeScheduleCount)
			if activeScheduleCount < 2 {
				bd.t.L().Printf(`First full backup still running`)
			} else {
				state = RunningIncrementals
			}
		case RunningIncrementals:
			var backupCount int
			backupCountQuery := fmt.Sprintf(
				`SELECT count(DISTINCT end_time) FROM [SHOW BACKUP FROM LATEST IN '%s']`, fixtureURI.String(),
			)
			sql.QueryRow(bd.t, backupCountQuery).Scan(&backupCount)
			bd.t.L().Printf(`%d scheduled backups taken`, backupCount)

			if bd.sp.fixture.CompactionThreshold > 0 {
				bd.t.L().Printf("%d compaction jobs succeeded, %d running", len(compSuccess), len(compRunning))
				if len(compFailed) > 0 {
					return errors.Newf("compaction jobs failed: %v", compFailed)
				}
			}

			if backupCount >= bd.sp.fixture.IncrementalChainLength {
				pauseSchedulesQuery := fmt.Sprintf(
					`PAUSE SCHEDULES WITH x AS (SHOW SCHEDULES) SELECT id FROM x WHERE label = '%s'`, scheduleLabel,
				)
				sql.Exec(bd.t, pauseSchedulesQuery)
				if len(compRunning) > 0 {
					state = WaitingCompactions
				} else {
					state = Done
				}
			}
		case WaitingCompactions:
			if len(compFailed) > 0 {
				return errors.Newf("compaction jobs failed: %v", compFailed)
			} else if len(compRunning) > 0 {
				bd.t.L().Printf("waiting for %d compaction jobs to finish", len(compRunning))
			} else {
				state = Done
			}
		}
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
	compactionQuery := `SELECT job_id, status, error FROM [SHOW JOBS] WHERE job_type = 'BACKUP' AND
	description ILIKE 'COMPACT BACKUPS%'`
	rows := sql.Query(bd.t, compactionQuery)
	defer rows.Close()
	var compJobs []jobMeta
	for rows.Next() {
		var job jobMeta
		if err := rows.Scan(&job.jobID, &job.state, &job.error); err != nil {
			return nil, nil, nil, errors.Wrapf(err, "error scanning compaction job")
		}
		compJobs = append(compJobs, job)
	}
	var successes, running, failures []jobMeta
	for _, job := range compJobs {
		switch job.state {
		case jobs.StateSucceeded:
			successes = append(successes, job)
		case jobs.StateRunning:
			running = append(running, job)
		case jobs.StateFailed:
			failures = append(failures, job)
		default:
			bd.t.L().Printf(`unexpected compaction job %d in state %s`, job.jobID, job.state)
		}
	}
	return successes, running, failures, nil
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
	case spec.GCE, spec.Local:
		account, err := vm.Providers["gce"].FindActiveAccount(t.L())
		require.NoError(t, err)
		t.L().Printf("using GCE account", account)

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
			clouds:  []spec.Cloud{spec.AWS, spec.GCE, spec.Local},
		},
		{
			fixture: SmallFixture,
			hardware: makeHardwareSpecs(hardwareSpecs{
				workloadNode: true,
			}),
			timeout: 2 * time.Hour,
			suites:  registry.Suites(registry.Nightly),
			clouds:  []spec.Cloud{spec.AWS, spec.GCE}},
		{
			fixture: MediumFixture,
			hardware: makeHardwareSpecs(hardwareSpecs{
				workloadNode: true,
				nodes:        9,
				cpus:         16,
			}),
			timeout: 12 * time.Hour,
			suites:  registry.Suites(registry.Weekly),
			clouds:  []spec.Cloud{spec.AWS, spec.GCE},
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
		},
	}
	for _, bf := range specs {
		bf := bf
		clusterSpec := bf.hardware.makeClusterSpecs(r)
		r.Add(registry.TestSpec{
			Name: fmt.Sprintf(
				"backupFixture/tpcc/warehouses=%d/incrementals=%d",
				bf.fixture.ImportWarehouses, bf.fixture.IncrementalChainLength,
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
