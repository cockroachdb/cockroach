// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

const (
	// scheduleChainingDefaultGCTTL is the GC TTL set for the default range when running a schedule chaining test.
	// It is set to 1 minute in order to be less than the 2 minute incremental frequency used in the tests.
	scheduleChainingDefaultGCTTL = time.Minute

	// scheduleChainingNotIncludedGCTTL is the GC TTL set for spans which should not be included
	// in backups during these tests. It is set to an extremely low value in order to cause heavy stress
	// in situations where we are attempting to protect or backup spans that we should not be touching.
	scheduleChainingNotIncludedGCTTL = 10 * time.Second

	// scheduleChainingTenantName is the name of the shared-process virtual
	// cluster used when isTenant is true.
	scheduleChainingTenantName = "apptenant"
)

func registerBackupScheduleChaining(r registry.Registry) {
	specs := []backupFixtureSpecs{
		{
			fixture: TinyFixture.WithSchedule(BackupSchedule{
				NumChains:            3,
				ChainLength:          6,
				FullFrequency:        "*/16 * * * *",
				IncrementalFrequency: "*/2 * * * *",
				CompactionThreshold:  4,
				CompactionWindow:     3,
			}),
			hardware: makeHardwareSpecs(hardwareSpecs{
				workloadNode: true,
			}),
			timeout: time.Hour,
			suites:  registry.Suites(registry.Nightly),
			clouds:  []spec.Cloud{spec.GCE, spec.Local},
		},
	}
	for _, spec := range specs {
		spec := spec
		clusterSpec := spec.hardware.makeClusterSpecs(r)
		r.Add(registry.TestSpec{
			Name:              fmt.Sprintf("backup/schedule-chaining/%s", spec.fixture.Name),
			Owner:             registry.OwnerDisasterRecovery,
			Cluster:           clusterSpec,
			Timeout:           spec.timeout,
			EncryptionSupport: registry.EncryptionMetamorphic,
			CompatibleClouds:  registry.Clouds(spec.clouds...),
			Suites:            spec.suites,
			Skip:              spec.skip,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				testRNG, seed := randutil.NewLockedPseudoRand()
				t.L().Printf("random seed: %d", seed)
				// isTenant := testRNG.Intn(2) == 0
				isTenant := true

				fixtureName := fmt.Sprintf("%s-schedule-chaining", spec.fixture.Name)
				if isTenant {
					fixtureName += "-tenant"
				}
				registry := GetFixtureRegistry(ctx, t, c.Cloud())
				handle, err := registry.Create(ctx, fixtureName, t.L())
				require.NoError(t, err)

				if isTenant {
					spec.fixture.Schedule.CompactionThreshold = 0
					spec.fixture.Schedule.CompactionWindow = 0
				}
				bd := backupDriver{
					t:        t,
					c:        c,
					sp:       spec,
					fixture:  handle.Metadata(),
					registry: registry,
				}
				bd.prepareCluster(ctx)

				systemConn := bd.c.Conn(
					ctx, t.L(), 1, /* node */
					option.VirtualClusterName(install.SystemInterfaceName),
				)
				defer systemConn.Close()

				var sqlConn *gosql.DB
				if isTenant {
					bd.tenantName = scheduleChainingTenantName
					c.StartServiceForVirtualCluster(
						ctx, t.L(),
						option.StartSharedVirtualClusterOpts(scheduleChainingTenantName),
						install.MakeClusterSettings(),
					)
					sqlConn = bd.c.Conn(
						ctx, t.L(), 1, /* node */
						option.VirtualClusterName(scheduleChainingTenantName),
					)
					defer sqlConn.Close()
					require.NoError(t, roachtestutil.WaitForSQLReady(ctx, sqlConn))
					// BACKUP/RESTORE VIRTUAL CLUSTER only accepts an integer tenant ID,
					// so resolve the name to an ID for the driver to use.
					require.NoError(t, systemConn.QueryRowContext(
						ctx, "SELECT id FROM system.tenants WHERE name = $1",
						scheduleChainingTenantName,
					).Scan(&bd.tenantID))
				} else {
					sqlConn = systemConn
				}

				// Shorten the GC TTL to be less than the frequency of the backup schedule.
				_, err = sqlConn.Exec(
					"ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = $1",
					int(scheduleChainingDefaultGCTTL.Seconds()),
				)
				require.NoError(t, err)
				// Shorten the GC TTL of the system database, which should not be included in the backup.
				_, err = sqlConn.Exec(
					"ALTER DATABASE system CONFIGURE ZONE USING gc.ttlseconds = $1",
					int(scheduleChainingNotIncludedGCTTL.Seconds()),
				)
				require.NoError(t, err)

				bd.initWorkload(ctx)
				stopWorkload, err := bd.runWorkload(ctx)
				require.NoError(t, err)
				if isTenant {
					stopExcludedWorkload := t.GoWithCancel(func(ctx context.Context, l *logger.Logger) error {
						runExcludedTablesWorkload(t, ctx, sqlConn, testRNG)
						return nil
					}, task.Name("excluded-tables-workload"))
					defer stopExcludedWorkload()
				}

				bd.scheduleBackups(ctx)
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

func runExcludedTablesWorkload(t test.Test, ctx context.Context, db *gosql.DB, rng *rand.Rand) {
	var numCreatedTables int
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Create or drop a table.
			if rng.Intn(2) == 0 {
				_, err := db.Exec(fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS maybe_excluded_table_%d (id INT PRIMARY KEY)",
					numCreatedTables,
				))
				require.NoError(t, err)
				numCreatedTables += 1
			} else {
				if numCreatedTables == 0 {
					continue
				}
				toDrop := rng.Intn(numCreatedTables)
				_, err := db.Exec(fmt.Sprintf(
					"DROP TABLE IF EXISTS maybe_excluded_table_%d",
					toDrop,
				))
				require.NoError(t, err)
			}

			// Set or remove excluded flag.
			_, err := db.Exec(fmt.Sprintf(
				"ALTER TABLE IF EXISTS maybe_excluded_table_%d SET (exclude_data_from_backup = %t)",
				rng.Intn(numCreatedTables), rng.Intn(2) == 0,
			))
			require.NoError(t, err)
		}
	}
}
