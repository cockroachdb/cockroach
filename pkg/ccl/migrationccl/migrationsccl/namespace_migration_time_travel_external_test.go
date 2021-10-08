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
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestDeprecatedNamespaceTableMigrationWithTimeTravel tests that the
// DeleteDeprecatedNamespaceTableDescriptorMigration doesn't interfere with
// backup/changefeed descriptor resolution when time-traveling to before
// the migration.
// At that point in time, there may still exist two namespace table descriptors.
// See issue #71301 for more details.
func TestDeprecatedNamespaceTableMigrationWithTimeTravel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.DeleteDeprecatedNamespaceTableDescriptorMigration - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	registry := tc.Servers[0].JobRegistry().(*jobs.Registry)
	registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
		jobspb.TypeChangefeed: func(raw jobs.Resumer) jobs.Resumer {
			return &fakeResumer{}
		},
	}

	// Inject deprecated namespace table descriptors and namespace entries.
	// This partially replicates the state of a system database created prior to
	// the introduction of system.namespace2 several major versions ago:
	// - There is a deprecated namespace table descriptor at ID 2, for the
	//   deprecated namespace table prior to the introduction of user-defined
	//   schemas. The particulars of that descriptor are not important other than
	//   the name and ID fields.
	// - The namespace table descriptor at ID 30 is the same except for the name
	//   which would have been `namespace2`.
	//
	// This code was taken as-is from non-CCL test
	// TestDeleteDeprecatedNamespaceDescriptorMigration
	err := tc.Servers[0].DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		codec := keys.SystemSQLCodec
		deprecated := *systemschema.NamespaceTable.TableDesc()
		deprecated.ID = keys.DeprecatedNamespaceTableID
		deprecatedDescProto := &descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: &deprecated}}
		namespace2 := *systemschema.NamespaceTable.TableDesc()
		namespace2.ID = keys.NamespaceTableID
		namespace2.Name = `namespace2`
		ns2DescProto := &descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: &namespace2}}
		b := txn.NewBatch()
		b.Put(catalogkeys.MakeDescMetadataKey(codec, keys.DeprecatedNamespaceTableID), deprecatedDescProto)
		namespaceKey := catalogkeys.MakePublicObjectNameKey(codec, keys.SystemDatabaseID, `namespace`)
		b.Put(namespaceKey, keys.DeprecatedNamespaceTableID)
		b.Put(catalogkeys.MakeDescMetadataKey(codec, keys.NamespaceTableID), ns2DescProto)
		namespace2Key := catalogkeys.MakePublicObjectNameKey(codec, keys.SystemDatabaseID, `namespace2`)
		b.Put(namespace2Key, keys.NamespaceTableID)
		return txn.Run(ctx, b)
	})
	require.NoError(t, err)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.ExecSucceedsSoon(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	// Create a dummy table to create a dummy changefeed on.
	tdb.Exec(t, "CREATE TABLE t (a INT)")

	// Insert a record and query its MVCC timestamp to later use as a changefeed
	// cursor, which will be after table creation but before before running the
	// migration.
	tdb.Query(t, `INSERT INTO t (a) VALUES (1)`)
	results := tdb.QueryStr(t, `SELECT crdb_internal_mvcc_timestamp FROM t`)
	require.NotEmpty(t, results)
	require.NotEmpty(t, results[0])
	ts := results[0][0]

	// Run the migration.
	tdb.ExecSucceedsSoon(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.DeleteDeprecatedNamespaceTableDescriptorMigration).String())
	tdb.CheckQueryResultsRetry(t, "SELECT count(*) FROM system.descriptor WHERE id = 2", [][]string{{"0"}})

	// Create a dummy changefeed to exercise the time-traveling descriptor
	// resolution code.
	tdb.Exec(t, `CREATE CHANGEFEED FOR TABLE t INTO 'webhook-https://fake-http-sink:8081'
		WITH webhook_auth_header='Basic Zm9v', CURSOR=$1`, ts)
}

type fakeResumer struct{}

var _ jobs.Resumer = (*fakeResumer)(nil)

func (d *fakeResumer) Resume(ctx context.Context, _ interface{}) error {
	return nil
}

func (d *fakeResumer) OnFailOrCancel(ctx context.Context, _ interface{}) error {
	return nil
}
