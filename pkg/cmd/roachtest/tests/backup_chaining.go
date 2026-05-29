// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
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
					int(scheduleChainingDefaultGCTTL.Seconds()),
				)
				require.NoError(t, err)
				// Shorten the GC TTL of the system database, which should not be included in the backup.
				_, err = conn.Exec(
					"ALTER DATABASE system CONFIGURE ZONE USING gc.ttlseconds = $1",
					int(scheduleChainingNotIncludedGCTTL.Seconds()),
				)
				require.NoError(t, err)

				bd.initWorkload(ctx)
				stopWorkload, err := bd.runWorkload(ctx)
				require.NoError(t, err)

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
