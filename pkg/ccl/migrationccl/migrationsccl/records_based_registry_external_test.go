// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package migrationsccl_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/baseccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestRecordsBasedRegistryMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		stickyEngineId = "engine1"
		key128         = "111111111111111111111111111111111234567890123456"
		keyFile128     = "a.key"
	)

	type encryptionTiming bool
	const (
		beforeMigration encryptionTiming = false
		afterMigration  encryptionTiming = true
	)

	testCases := []struct {
		name                string
		encryptionStartTime encryptionTiming
	}{
		{
			name:                "enable-encryption-before-migration",
			encryptionStartTime: beforeMigration,
		},
		{
			name:                "enable-encryption-after-migration",
			encryptionStartTime: afterMigration,
		},
	}

	for _, c := range testCases {
		ctx := context.Background()
		registry := server.NewStickyInMemEnginesRegistry()

		storeSpec := base.DefaultTestStoreSpec
		storeSpec.StickyInMemoryEngineID = stickyEngineId

		clusterArgs := base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						DisableAutomaticVersionUpgrade: 1,
						BinaryVersionOverride:          clusterversion.ByKey(clusterversion.RecordsBasedRegistry - 1),
						StickyEngineRegistry:           registry,
					},
				},
				StoreSpecs: []base.StoreSpec{storeSpec},
			},
		}

		// Start and stop the cluster in order to create a sticky engine.
		tc := testcluster.StartTestCluster(t, 1 /* nodes */, clusterArgs)
		if c.encryptionStartTime == afterMigration {
			tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
			tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
				clusterversion.ByKey(clusterversion.RecordsBasedRegistry).String())
		}
		tc.Stopper().Stop(ctx)

		// Write the keyfile to the sticky engine's underlying FS.
		memFS, err := registry.GetUnderlyingFS(storeSpec)
		require.NoError(t, err)
		f, err := memFS.Create(keyFile128)
		require.NoError(t, err)
		_, err = f.Write([]byte(key128))
		require.NoError(t, err)

		// Add the keyfile details to the store spec.
		encOpts := &baseccl.EncryptionOptions{
			KeySource: baseccl.EncryptionKeySource_KeyFiles,
			KeyFiles: &baseccl.EncryptionKeyFiles{
				OldKey:     "plain",
				CurrentKey: keyFile128,
			},
			DataKeyRotationPeriod: 1000,
		}
		b, err := protoutil.Marshal(encOpts)
		require.NoError(t, err)
		storeSpec.EncryptionOptions = b
		storeSpec.UseFileRegistry = true

		// Restart the cluster with encryption-at-rest turned on.
		clusterArgs.ServerArgs.StoreSpecs = []base.StoreSpec{storeSpec}
		tc = testcluster.StartTestCluster(t, 1 /* nodes */, clusterArgs)

		// Create a SQL table, write some rows, and start migration.
		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		tdb.Exec(t, `CREATE TABLE test (id int, abc string)`)
		tdb.Exec(t, `INSERT INTO test ( VALUES (1, 'hi'), (2, 'bye'))`)
		if c.encryptionStartTime == beforeMigration {
			tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
				clusterversion.ByKey(clusterversion.RecordsBasedRegistry).String())
		}

		// Check that all engines are using the new records based registry and verify
		// that we can still retrieve the rows from the table.
		svr := tc.Server(0)
		for _, eng := range svr.Engines() {
			target := clusterversion.ByKey(clusterversion.RecordsBasedRegistry)
			ok, err := eng.MinVersionIsAtLeastTargetVersion(&target)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = eng.UsingRecordsEncryptionRegistry()
			require.NoError(t, err)
			require.True(t, ok)
		}
		tdb.Exec(t, `SELECT * FROM test`)
		tc.Stopper().Stop(ctx)

		// Restart the cluster to ensure there are no problems with loading the new
		// records based registries.
		tc = testcluster.StartTestCluster(t, 1 /* nodes */, clusterArgs)
		tdb = sqlutils.MakeSQLRunner(tc.ServerConn(0))
		tdb.Exec(t, `SELECT * FROM test`)
		tc.Stopper().Stop(ctx)

		// NB: We cannot defer this without leaking resources since a new sticky
		// engine registry is created in each loop iteration.
		registry.CloseAllStickyInMemEngines()
	}
}
