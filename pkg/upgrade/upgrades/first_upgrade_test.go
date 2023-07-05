// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestFirstUpgrade tests the correct behavior of upgrade steps which are
// implicitly defined for each V[0-9]+_[0-9]+Start cluster version key.
func TestFirstUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.TestingBinaryMinSupportedVersion
		v1 = clusterversion.ByKey(clusterversion.BinaryVersionKey)
	)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v1, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))
	testServer, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          v0,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer testServer.Stopper().Stop(ctx)

	// Set up the test cluster schema.
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	execStmts := func(t *testing.T, stmts ...string) {
		for _, stmt := range stmts {
			tdb.Exec(t, stmt)
		}
	}
	execStmts(t,
		"CREATE DATABASE test",
		"USE test",
		"CREATE TABLE foo (i INT PRIMARY KEY, j INT, INDEX idx(j))",
	)

	// Corrupt the table descriptor in an unrecoverable manner.
	tbl := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "foo")
	descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tbl.GetID())
	require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		mut := tabledesc.NewBuilder(tbl.TableDesc()).BuildExistingMutableTable()
		mut.NextIndexID = 1
		return txn.Put(ctx, descKey, mut.DescriptorProto())
	}))

	// Wait long enough for precondition check to be effective.
	execStmts(t, "CREATE DATABASE test2")
	const qWaitForAOST = "SELECT count(*) FROM [SHOW DATABASES] AS OF SYSTEM TIME '-10s'"
	tdb.CheckQueryResultsRetry(t, qWaitForAOST, [][]string{{"5"}})

	// Try upgrading the cluster version, precondition check should fail.
	const qUpgrade = "SET CLUSTER SETTING version = crdb_internal.node_executable_version()"
	tdb.ExpectErr(
		t, `verifying precondition for version .*invalid_objects is not empty`, qUpgrade,
	)

	// Unbreak the table descriptor, but unset its modification time.
	// Post-deserialization, this will be set to the MVCC timestamp.
	require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		mut := tabledesc.NewBuilder(tbl.TableDesc()).BuildExistingMutableTable()
		mut.ModificationTime = hlc.Timestamp{}
		return txn.Put(ctx, descKey, mut.DescriptorProto())
	}))

	// Check that the descriptor protobuf will undergo changes when read.
	readDescFromStorage := func() catalog.Descriptor {
		var b catalog.DescriptorBuilder
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			v, err := txn.Get(ctx, descKey)
			if err != nil {
				return err
			}
			b, err = descbuilder.FromSerializedValue(v.Value)
			return err
		}))
		return b.BuildImmutable()
	}
	require.False(t, readDescFromStorage().GetModificationTime().IsEmpty())
	require.True(t, readDescFromStorage().GetPostDeserializationChanges().HasChanges())

	// Wait long enough for precondition check to see the unbroken table descriptor.
	execStmts(t, "CREATE DATABASE test3")
	tdb.CheckQueryResultsRetry(t, qWaitForAOST, [][]string{{"6"}})

	// Upgrade the cluster version.
	tdb.Exec(t, qUpgrade)

	// The table descriptor protobuf should have the modification time set.
	require.False(t, readDescFromStorage().GetModificationTime().IsEmpty())
	require.False(t, readDescFromStorage().GetPostDeserializationChanges().HasChanges())
}

// TestFirstUpgradeRepair tests the correct repair behavior of upgrade
// steps which are implicitly defined for each V[0-9]+_[0-9]+Start cluster
// version key.
func TestFirstUpgradeRepair(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.TestingBinaryMinSupportedVersion
		v1 = clusterversion.ByKey(clusterversion.BinaryVersionKey)
	)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v1, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))
	testServer, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          v0,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer testServer.Stopper().Stop(ctx)

	// Set up the test cluster schema.
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	execStmts := func(t *testing.T, stmts ...string) {
		for _, stmt := range stmts {
			tdb.Exec(t, stmt)
		}
	}

	// Create a table and a function for this test.
	execStmts(t,
		"CREATE DATABASE test",
		"USE test",
		"CREATE TABLE foo (i INT PRIMARY KEY, j INT, INDEX idx(j))",
		"INSERT INTO foo VALUES (1, 2)",
		"CREATE FUNCTION test.public.f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
	)

	// Corrupt FK back references in the test table descriptor.
	codec := keys.SystemSQLCodec
	tbl := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", "foo")
	fn := desctestutils.TestingGetFunctionDescriptor(kvDB, codec, "test", "public", "f")
	require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		tbl := tabledesc.NewBuilder(tbl.TableDesc()).BuildExistingMutableTable()
		tbl.InboundFKs = []descpb.ForeignKeyConstraint{{
			OriginTableID:       123456789,
			OriginColumnIDs:     []descpb.ColumnID{1},
			ReferencedColumnIDs: tbl.PublicColumnIDs(),
			ReferencedTableID:   tbl.GetID(),
			Name:                "corrupt_fk",
			Validity:            descpb.ConstraintValidity_Validated,
			ConstraintID:        tbl.NextConstraintID,
		}}
		tbl.NextConstraintID++
		b.Put(catalogkeys.MakeDescMetadataKey(codec, tbl.GetID()), tbl.DescriptorProto())
		fn := funcdesc.NewBuilder(fn.FuncDesc()).BuildExistingMutableFunction()
		fn.DependedOnBy = []descpb.FunctionDescriptor_Reference{{
			ID:        123456789,
			ColumnIDs: []descpb.ColumnID{1},
		}}
		b.Put(catalogkeys.MakeDescMetadataKey(codec, fn.GetID()), fn.DescriptorProto())
		return txn.Run(ctx, b)
	}))

	// The corruption should remain undetected for DML queries.
	tdb.CheckQueryResults(t, "SELECT * FROM test.public.foo", [][]string{{"1", "2"}})
	tdb.CheckQueryResults(t, "SELECT test.public.f()", [][]string{{"1"}})

	// The corruption should interfere with DDL statements.
	const errRE = "referenced table ID 123456789: referenced descriptor not found"
	tdb.ExpectErr(t, errRE, "ALTER TABLE test.public.foo RENAME TO bar")
	tdb.ExpectErr(t, errRE, "ALTER FUNCTION test.public.f RENAME TO g")

	// Check that the corruption is detected by invalid_objects.
	const qDetectCorruption = `SELECT count(*) FROM "".crdb_internal.invalid_objects`
	tdb.CheckQueryResults(t, qDetectCorruption, [][]string{{"2"}})

	// Check that the corruption is detected by kv_repairable_catalog_corruptions.
	const qDetectRepairableCorruption = `
		SELECT count(*) FROM "".crdb_internal.kv_repairable_catalog_corruptions`
	tdb.CheckQueryResults(t, qDetectRepairableCorruption, [][]string{{"2"}})

	// Wait long enough for precondition check to be effective.
	tdb.Exec(t, "CREATE DATABASE test2")
	const qWaitForAOST = "SELECT count(*) FROM [SHOW DATABASES] AS OF SYSTEM TIME '-10s'"
	tdb.CheckQueryResultsRetry(t, qWaitForAOST, [][]string{{"5"}})

	// Try upgrading the cluster version.
	// Precondition check should repair all corruptions and upgrade should succeed.
	const qUpgrade = "SET CLUSTER SETTING version = crdb_internal.node_executable_version()"
	tdb.Exec(t, qUpgrade)
	tdb.CheckQueryResults(t, qDetectCorruption, [][]string{{"0"}})
	tdb.CheckQueryResults(t, qDetectRepairableCorruption, [][]string{{"0"}})

	// Check that the table and function are OK.
	tdb.CheckQueryResults(t, "SELECT * FROM test.public.foo", [][]string{{"1", "2"}})
	tdb.Exec(t, "ALTER TABLE test.foo ADD COLUMN k INT DEFAULT 42")
	tdb.CheckQueryResults(t, "SELECT * FROM test.public.foo", [][]string{{"1", "2", "42"}})
	tdb.CheckQueryResults(t, "SELECT test.public.f()", [][]string{{"1"}})
}
