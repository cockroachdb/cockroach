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
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/blobfixture"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type TpccFixture struct {
	Name                   string
	ImportWarehouses       int
	WorkloadWarehouses     int
	MinutesPerIncremental  int
	IncrementalChainLength int
	RestoredSizeEstimate   string
}

// TinyFixture is a TPCC fixture that is intended for smoke tests, local
// testing, and continous testing of the fixture generation logic.
var TinyFixture = TpccFixture{
	Name:                   "tpcc-10",
	ImportWarehouses:       10,
	WorkloadWarehouses:     10,
	IncrementalChainLength: 4,
	RestoredSizeEstimate:   "700MiB",
}

// SmallFixture is a TPCC fixture that is intended to be quick to restore and
// cheap to generate for continous testing of the fixture generation logic.
var SmallFixture = TpccFixture{
	Name:                   "tpcc-5k",
	ImportWarehouses:       5000,
	WorkloadWarehouses:     1000,
	IncrementalChainLength: 48,
	RestoredSizeEstimate:   "350GiB",
}

// MediumFixture is a TPCC fixture sized so that it is a tight fit in 3 nodes
// with the smallest supported node size of 4 VCPU per node.
var MediumFixture = TpccFixture{
	Name:                   "tpcc-30k",
	ImportWarehouses:       30000,
	WorkloadWarehouses:     5000,
	IncrementalChainLength: 400,
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

// fixtureFromMasterVersion should be used in the backupSpecs version field to
// create a fixture using the bleeding edge of master. In the backup fixture
// path on external storage, the {version} subdirectory will be equal to this
// value.
const fixtureFromMasterVersion = "latest"

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

	createScheduleStatement := CreateScheduleStatement(bd.registry.URI(bd.fixture.DataPath))
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	_, err := conn.Exec(createScheduleStatement)
	require.NoError(bd.t, err)
}

// monitorBackups pauses the schedule once the target number of backups in the
// chain have been taken.
func (bd *backupDriver) monitorBackups(ctx context.Context) {
	sql := sqlutils.MakeSQLRunner(bd.c.Conn(ctx, bd.t.L(), 1))
	fixtureURI := bd.registry.URI(bd.fixture.DataPath)
	for {
		time.Sleep(1 * time.Minute)
		var activeScheduleCount int
		scheduleCountQuery := fmt.Sprintf(`SELECT count(*) FROM [SHOW SCHEDULES] WHERE label='%s' AND schedule_status='ACTIVE'`, scheduleLabel)
		sql.QueryRow(bd.t, scheduleCountQuery).Scan(&activeScheduleCount)
		if activeScheduleCount < 2 {
			bd.t.L().Printf(`First full backup still running`)
			continue
		}
		var backupCount int
		backupCountQuery := fmt.Sprintf(`SELECT count(DISTINCT end_time) FROM [SHOW BACKUP FROM LATEST IN '%s']`, fixtureURI.String())
		sql.QueryRow(bd.t, backupCountQuery).Scan(&backupCount)
		bd.t.L().Printf(`%d scheduled backups taken`, backupCount)
		if backupCount >= bd.sp.fixture.IncrementalChainLength {
			pauseSchedulesQuery := fmt.Sprintf(`PAUSE SCHEDULES WITH x AS (SHOW SCHEDULES) SELECT id FROM x WHERE label = '%s'`, scheduleLabel)
			sql.Exec(bd.t, pauseSchedulesQuery)
			break
		}
	}
}

func fixtureDirectory() string {
	if clusterversion.DevelopmentBranch {
		return "roachtest/master"
	}
	version := clusterupgrade.CurrentVersion()
	return fmt.Sprintf("roachtest/v%d.%d", version.Major(), version.Minor())
}

func newFixtureRegistry(ctx context.Context, t test.Test, c cluster.Cluster) *blobfixture.Registry {
	var uri url.URL
	switch c.Cloud() {
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
		t.Fatalf("fixtures not supported on %s", c.Cloud())
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
				registry := newFixtureRegistry(ctx, t, c)

				// Piggy back on fixture generation to run the GC. Run the GC first to
				// bound the number of fixtures leaked if fixture creation is broken.
				// TODO(jeffswenson): run GC as its own test. GC'ing a huge fixture can
				// take more time than small fixture creation because the teamcity
				// worker is not colocated with the fixture repository, so there is a
				// large amount of network latency.
				// require.NoError(t, registry.GC(ctx, t.L()))

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
				bd.monitorBackups(ctx)

				stopWorkload()

				require.NoError(t, handle.SetReadyAt(ctx))
			},
		})
	}
}
