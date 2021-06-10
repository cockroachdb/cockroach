// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDeleteDeprecatedNamespaceDescriptorMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.DeleteDeprecatedNamespaceTableDescriptorMigration - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Inject deprecated namespace table descriptor and namespace entries.
	err := tc.Servers[0].DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		codec := keys.SystemSQLCodec
		deprecated := *systemschema.NamespaceTable.TableDesc()
		deprecated.ID = keys.DeprecatedNamespaceTableID
		descProto := &descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: &deprecated}}
		b := txn.NewBatch()
		b.Put(catalogkeys.MakeDescMetadataKey(codec, keys.DeprecatedNamespaceTableID), descProto)
		namespaceKey := catalogkeys.MakePublicObjectNameKey(codec, keys.SystemDatabaseID, `namespace`)
		b.Put(namespaceKey, keys.DeprecatedNamespaceTableID)
		namespace2Key := catalogkeys.MakePublicObjectNameKey(codec, keys.SystemDatabaseID, `namespace2`)
		b.Put(namespace2Key, keys.NamespaceTableID)
		return txn.Run(ctx, b)
	})
	require.NoError(t, err)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	require.True(t, isTableDescThere(t, tdb, keys.DeprecatedNamespaceTableID))
	require.True(t, isTableDescThere(t, tdb, keys.NamespaceTableID))
	require.Equal(t, keys.DeprecatedNamespaceTableID, lookupNamespaceEntry(t, tdb, `namespace`))
	require.Equal(t, keys.NamespaceTableID, lookupNamespaceEntry(t, tdb, `namespace2`))

	runMigrationAndCheckState(t, tdb)
}

func TestDeleteDeprecatedNamespaceDescriptorMigrationOnlyNamespace2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.DeleteDeprecatedNamespaceTableDescriptorMigration - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Inject deprecated namespace table descriptor and namespace2 entry.
	err := tc.Servers[0].DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		codec := keys.SystemSQLCodec
		deprecated := *systemschema.NamespaceTable.TableDesc()
		deprecated.ID = keys.DeprecatedNamespaceTableID
		descProto := &descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: &deprecated}}
		b := txn.NewBatch()
		b.Put(catalogkeys.MakeDescMetadataKey(codec, keys.DeprecatedNamespaceTableID), descProto)
		b.Del(catalogkeys.MakePublicObjectNameKey(codec, keys.SystemDatabaseID, `namespace`))
		b.Put(catalogkeys.MakePublicObjectNameKey(codec, keys.SystemDatabaseID, `namespace2`), keys.NamespaceTableID)
		return txn.Run(ctx, b)
	})
	require.NoError(t, err)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	require.True(t, isTableDescThere(t, tdb, keys.DeprecatedNamespaceTableID))
	require.True(t, isTableDescThere(t, tdb, keys.NamespaceTableID))
	require.Zero(t, lookupNamespaceEntry(t, tdb, `namespace`))
	require.Equal(t, keys.NamespaceTableID, lookupNamespaceEntry(t, tdb, `namespace2`))
	require.Equal(t, keys.NamespaceTableID, lookupNamespaceEntry(t, tdb, `namespace2`))

	runMigrationAndCheckState(t, tdb)
}

// TestDeleteDeprecatedNamespaceDescriptorMigrationNoOp tests that the migration is idempotent.
func TestDeleteDeprecatedNamespaceDescriptorMigrationNoOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.DeleteDeprecatedNamespaceTableDescriptorMigration - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	require.False(t, isTableDescThere(t, tdb, keys.DeprecatedNamespaceTableID))
	require.True(t, isTableDescThere(t, tdb, keys.NamespaceTableID))
	require.Equal(t, keys.NamespaceTableID, lookupNamespaceEntry(t, tdb, `namespace`))
	require.Zero(t, lookupNamespaceEntry(t, tdb, `namespace2`))

	runMigrationAndCheckState(t, tdb)
}

func isTableDescThere(t *testing.T, tdb *sqlutils.SQLRunner, id descpb.ID) bool {
	var n int
	tdb.QueryRow(t, `SELECT count(*) FROM crdb_internal.tables WHERE table_id = $1`, id).Scan(&n)
	return n > 0
}

func lookupNamespaceEntry(t *testing.T, tdb *sqlutils.SQLRunner, systemTable string) (id int) {
	rows := tdb.Query(
		t,
		`SELECT id FROM system.namespace WHERE "parentID" = $1 AND "parentSchemaID" = $2 AND name = $3`,
		keys.SystemDatabaseID,
		keys.PublicSchemaID,
		systemTable,
	)
	defer rows.Close()
	if !rows.Next() {
		return 0
	}
	err := rows.Scan(&id)
	require.NoError(t, err)
	if rows.Next() {
		t.Fatal("Expected not more than 1 result")
	}
	return id
}

func runMigrationAndCheckState(t *testing.T, tdb *sqlutils.SQLRunner) {
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.DeleteDeprecatedNamespaceTableDescriptorMigration).String())
	require.False(t, isTableDescThere(t, tdb, keys.DeprecatedNamespaceTableID))
	require.True(t, isTableDescThere(t, tdb, keys.NamespaceTableID))
	require.Equal(t, keys.NamespaceTableID, lookupNamespaceEntry(t, tdb, `namespace`))
	require.Zero(t, lookupNamespaceEntry(t, tdb, `namespace2`))
}

// TestCanReadSystemNamespaceWhenNamedNamespace2 tests that the name resolution
// code for the namespace table does not break when upgrading from an earlier
// version in which the descriptor in question (30) carries the name
// "namespace2".
func TestCanReadSystemNamespaceWhenNamedNamespace2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const N = 3
	registry := server.NewStickyInMemEnginesRegistry()
	defer registry.CloseAllStickyInMemEngines()
	perServerArgs := make(map[int]base.TestServerArgs, N)
	for i := 0; i < N; i++ {
		settings := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.TestingBinaryVersion,
			clusterversion.TestingBinaryMinSupportedVersion,
			false, // initializeVersion
		)
		// Initialize the version to the BinaryMinSupportedVersion.
		require.NoError(t, clusterversion.Initialize(ctx,
			clusterversion.TestingBinaryMinSupportedVersion, &settings.SV))
		// Make the in-memory store sticky so that we can restart the server.
		storeSpec := base.DefaultTestStoreSpec
		storeSpec.StickyInMemoryEngineID = strconv.Itoa(i)
		args := base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.TestingBinaryMinSupportedVersion,
					StickyEngineRegistry:           registry,
				},
			},
			StoreSpecs: []base.StoreSpec{storeSpec},
		}
		args.RaftElectionTimeoutTicks = 1000 // make this robust under stress
		perServerArgs[i] = args
	}
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: perServerArgs,
	})
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(2))
	// Validate that we can query the namespace table.
	tdb.Exec(t, "SELECT * FROM system.namespace")
	// Update the descriptor to be named "namespace2".
	tdb.Exec(t, `
  WITH ns_table AS (
					SELECT id,
					       crdb_internal.pb_to_json(
							'cockroach.sql.sqlbase.Descriptor',
							descriptor,
							false
					       ) AS d
					  FROM system.descriptor
					 WHERE id = $1
                ),
       updated AS (
				SELECT id,
				       crdb_internal.json_to_pb(
						'cockroach.sql.sqlbase.Descriptor',
						json_set(d, ARRAY['table', 'name'], '"namespace2"'::jsonb)
				       ) AS d
				  FROM ns_table
               )
SELECT crdb_internal.unsafe_upsert_descriptor(id, d)
  FROM updated;
`, keys.NamespaceTableID)

	// Validate that that got injected.
	checkNamespaceTableName := func(t *testing.T, exp string) {
		tdb.CheckQueryResults(t,
			fmt.Sprintf(`
SELECT crdb_internal.pb_to_json(
		'cockroach.sql.sqlbase.Descriptor',
		descriptor
       )->'table'->>'name'
  FROM system.descriptor
 WHERE id = %d`, keys.NamespaceTableID),
			[][]string{{exp}})
	}
	checkNamespaceTableName(t, "namespace2")

	// Validate that we can still query the namespace table.
	tdb.Exec(t, "SELECT * FROM system.namespace")

	// Restart the server and then validate that we can still query the
	// namespace table.
	tc.StopServer(2)
	require.NoError(t, tc.RestartServer(2))
	tdb = sqlutils.MakeSQLRunner(tc.ServerConn(2))
	tdb.Exec(t, "SELECT * FROM system.namespace")

	// Upgrade the cluster.
	tdb.Exec(t, "SET CLUSTER SETTING version = crdb_internal.node_executable_version()")

	// Validate that the name got upgraded.
	checkNamespaceTableName(t, "namespace")

	// Validate that we can still query the namespace table.
	tdb.Exec(t, "SELECT * FROM system.namespace")
}
