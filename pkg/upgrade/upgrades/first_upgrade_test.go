// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
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
		v0 = clusterversion.MinSupported.Version()
		v1 = clusterversion.Latest.Version()
	)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v1, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))
	testServer, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         v0,
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

	// Corrupt the table descriptor in an unrecoverable manner. We are not able to automatically repair this
	// descriptor.
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

	// Confirm that the only change undergone is the modification time being set to
	// the MVCC timestamp.
	require.False(t, readDescFromStorage().GetModificationTime().IsEmpty())
	changes := readDescFromStorage().GetPostDeserializationChanges()
	require.Equal(t, changes.Len(), 1)
	require.True(t, changes.Contains(catalog.SetModTimeToMVCCTimestamp))

	// Wait long enough for precondition check to see the unbroken table descriptor.
	execStmts(t, "CREATE DATABASE test3")
	tdb.CheckQueryResultsRetry(t, qWaitForAOST, [][]string{{"6"}})

	// Upgrade the cluster version.
	tdb.Exec(t, qUpgrade)

	// The table descriptor protobuf should still have the modification time set;
	// the only post-deserialization change should be SetModTimeToMVCCTimestamp.
	require.False(t, readDescFromStorage().GetModificationTime().IsEmpty())
	changes = readDescFromStorage().GetPostDeserializationChanges()
	require.Equal(t, changes.Len(), 1)
	require.True(t, changes.Contains(catalog.SetModTimeToMVCCTimestamp))
}

// TestFirstUpgradeRepair tests the correct repair behavior of upgrade
// steps which are implicitly defined for each V[0-9]+_[0-9]+Start cluster
// version key.
func TestFirstUpgradeRepair(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.MinSupported.Version()
		v1 = clusterversion.Latest.Version()
	)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v1, v0, false /* initializeVersion */)
	// We intentionally create contention on the system.descriptor table by having
	// a high priority txn that intentionally updates rows in this table. This txn
	// also holds a lease on the system database descriptor, which we will wait to
	// be released. Reducing the lease duration makes this part of the test speed
	// up.
	lease.LeaseDuration.Override(ctx, &settings.SV, time.Second*30)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))
	upgradePausePoint := make(chan struct{})
	upgradeResumePoint := make(chan struct{})
	upgradeCompleted := make(chan struct{})
	testServer, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			UpgradeManager: &upgradebase.TestingKnobs{
				InterlockPausePoint:               upgradebase.AfterMigration,
				InterlockReachedPausePointChannel: &upgradePausePoint,
				InterlockResumeChannel:            &upgradeResumePoint,
			},
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         v0,
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
		// Create a table and function that we will corrupt for this test.
		"CREATE TABLE foo (i INT PRIMARY KEY, j INT, INDEX idx(j))",
		"INSERT INTO foo VALUES (1, 2)",
		"CREATE FUNCTION test.public.f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
		// Create the following to cover more descriptor types - ensure that said descriptors do not get repaired.
		"CREATE TABLE bar (i INT PRIMARY KEY, j INT, INDEX idx(j))",
		"CREATE SCHEMA bar",
		"CREATE TYPE bar.bar AS ENUM ('hello')",
		"CREATE FUNCTION bar.bar(a INT) RETURNS INT AS 'SELECT a*a' LANGUAGE SQL",
	)

	dbDesc := desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "test")
	tblDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "bar")
	schemaDesc := desctestutils.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), "bar")
	typDesc := desctestutils.TestingGetTypeDescriptor(kvDB, keys.SystemSQLCodec, "test", "bar", "bar")
	fnDesc := desctestutils.TestingGetFunctionDescriptor(kvDB, keys.SystemSQLCodec, "test", "bar", "bar")
	nonCorruptDescs := []catalog.Descriptor{dbDesc, tblDesc, schemaDesc, typDesc, fnDesc}

	// Corrupt FK back references in the test table descriptor, foo.
	codec := keys.SystemSQLCodec
	fooTbl := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", "foo")
	fooFn := desctestutils.TestingGetFunctionDescriptor(kvDB, codec, "test", "public", "f")
	corruptDescs := []catalog.Descriptor{fooTbl, fooFn}
	require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		tbl := tabledesc.NewBuilder(fooTbl.TableDesc()).BuildExistingMutableTable()
		tbl.InboundFKs = []descpb.ForeignKeyConstraint{{
			OriginTableID:       123456789,
			OriginColumnIDs:     tbl.PublicColumnIDs(), // Used such that len(OriginColumnIDs) == len(PublicColumnIDs)
			ReferencedColumnIDs: tbl.PublicColumnIDs(),
			ReferencedTableID:   tbl.GetID(),
			Name:                "corrupt_fk",
			Validity:            descpb.ConstraintValidity_Validated,
			ConstraintID:        tbl.NextConstraintID,
		}}
		tbl.NextConstraintID++
		b.Put(catalogkeys.MakeDescMetadataKey(codec, tbl.GetID()), tbl.DescriptorProto())
		fn := funcdesc.NewBuilder(fooFn.FuncDesc()).BuildExistingMutableFunction()
		fn.DependedOnBy = []descpb.FunctionDescriptor_Reference{{
			ID:        123456789,
			ColumnIDs: []descpb.ColumnID{1},
		}}
		b.Put(catalogkeys.MakeDescMetadataKey(codec, fn.GetID()), fn.DescriptorProto())
		return txn.Run(ctx, b)
	}))

	readDescFromStorage := func(descID descpb.ID) catalog.Descriptor {
		descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, descID)
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

	descOldVersionMap := make(map[descpb.ID]descpb.DescriptorVersion)

	for _, desc := range append(nonCorruptDescs, corruptDescs...) {
		descId := desc.GetID()
		descOldVersionMap[descId] = readDescFromStorage(descId).GetVersion()
	}

	// The corruption should remain undetected for DML queries.
	tdb.CheckQueryResults(t, "SELECT * FROM test.public.foo", [][]string{{"1", "2"}})
	tdb.CheckQueryResults(t, "SELECT test.public.f()", [][]string{{"1"}})

	// The corruption should interfere with DDL statements.
	const errRE = "relation \"foo\" \\(106\\): invalid foreign key backreference: missing table=123456789: referenced table ID 123456789: referenced descriptor not found"
	tdb.ExpectErr(t, errRE, "ALTER TABLE test.public.foo RENAME TO bar")
	const errReForFunction = " function \"f\" \\(107\\): referenced descriptor ID 123456789: referenced descriptor not found"
	tdb.ExpectErr(t, errReForFunction, "ALTER FUNCTION test.public.f RENAME TO g")

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

	// Intentionally block any attempt to upgrade by holding the descriptors
	// with another txn.
	tdb.Exec(t, "CREATE TABLE t1(n int)")
	grp := ctxgroup.WithContext(ctx)
	locksHeld := make(chan struct{})
	grp.GoCtx(func(ctx context.Context) error {
		tx, err := sqlDB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for _, corruptDesc := range corruptDescs {
			_, err = tx.Exec("SELECT crdb_internal.unsafe_upsert_descriptor($1, descriptor, true) FROM system.descriptor WHERE id=$1", corruptDesc.GetID())
			if err != nil {
				return err
			}
			_, err = tx.Exec("INSERT INTO t1 VALUES(5)")
			if err != nil {
				return err
			}

		}
		close(locksHeld)
		<-upgradePausePoint
		doneUpgrade := false
		for !doneUpgrade {
			select {
			case <-upgradePausePoint:
			case <-upgradeCompleted:
				doneUpgrade = true
			case upgradeResumePoint <- struct{}{}:
			}
		}
		return tx.Rollback()
	})

	// Try upgrading the cluster version.
	// Precondition check should repair all corruptions and upgrade should succeed.
	const qUpgrade = "SET CLUSTER SETTING version = crdb_internal.node_executable_version()"
	<-locksHeld
	tdb.Exec(t, qUpgrade)
	tdb.CheckQueryResults(t, qDetectCorruption, [][]string{{"0"}})
	tdb.CheckQueryResults(t, qDetectRepairableCorruption, [][]string{{"0"}})
	close(upgradeCompleted)
	require.NoError(t, grp.Wait())
	// Assert that a version upgrade is reflected for repaired descriptors (stricly one version upgrade).
	for _, d := range corruptDescs {
		descId := d.GetID()
		desc := readDescFromStorage(descId)
		require.Equalf(t, descOldVersionMap[descId]+1, desc.GetVersion(), desc.GetName())
	}

	// Assert that no version upgrade is reflected for non-repaired descriptors.
	for _, d := range nonCorruptDescs {
		descId := d.GetID()
		desc := readDescFromStorage(descId)
		require.Equalf(t, descOldVersionMap[descId], desc.GetVersion(), desc.GetName())
	}

	// Check that the repaired table and function are OK.
	tdb.CheckQueryResults(t, "SELECT * FROM test.public.foo", [][]string{{"1", "2"}})
	tdb.Exec(t, "ALTER TABLE test.foo ADD COLUMN k INT DEFAULT 42")
	tdb.CheckQueryResults(t, "SELECT * FROM test.public.foo", [][]string{{"1", "2", "42"}})
	tdb.CheckQueryResults(t, "SELECT test.public.f()", [][]string{{"1"}})
}

// TestFirstUpgradeRepairBatchSize validates that the maximum batch size
// used by the repair upgrade is sustainable.
func TestFirstUpgradeRepairBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is fairly slow because of the large number of
	// descriptors.
	skip.UnderDuress(t)

	var (
		v0 = clusterversion.MinSupported.Version()
		v1 = clusterversion.Latest.Version()
	)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v1, v0, false /* initializeVersion */)
	// We intentionally create contention on the system.descriptor table by having
	// a high priority txn that intentionally updates rows in this table. This txn
	// also holds a lease on the system database descriptor, which we will wait to
	// be released. Reducing the lease duration makes this part of the test speed
	// up.
	lease.LeaseDuration.Override(ctx, &settings.SV, time.Second*30)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))
	testServer, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         v0,
			},
			SQLEvalContext: &eval.TestingKnobs{
				ForceProductionValues: true,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer testServer.Stopper().Stop(ctx)

	const totalDescriptorsToTest = 2500
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	idb := testServer.InternalDB().(*sql.InternalDB)
	tx := sqlRunner.Begin(t)
	_, err := tx.Exec("SET LOCAL autocommit_before_ddl = false")
	require.NoError(t, err)
	const batchSize = 100
	lastCommit := 0
	commitFn := func(startIdx int) {
		require.NoError(t, tx.Commit())
		// Execute a txn to inject corruption
		require.NoError(t,
			idb.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
				for i := lastCommit; i < startIdx; i++ {
					db, err := txn.Descriptors().MutableByName(txn.KV()).Database(ctx, "defaultdb")
					if err != nil {
						return err
					}
					schema, err := txn.Descriptors().MutableByName(txn.KV()).Schema(ctx, db, "public")
					if err != nil {
						return err
					}
					tbl, err := txn.Descriptors().MutableByName(txn.KV()).Table(ctx, db, schema, fmt.Sprintf("t_%d", i))
					if err != nil {
						return err
					}
					nonExistentRole := i%2 == 0
					if nonExistentRole {
						// Forces an invalid_object error.
						tbl.Privileges.Users = append(tbl.Privileges.Users, catpb.UserPrivileges{UserProto: "bobby"})
					} else {
						// Forces a post deserialization change.
						tbl.Indexes[0].Invisibility = 0.0
						tbl.Indexes[0].NotVisible = true
					}
					txn.Descriptors().SkipValidationOnWrite()
					err = txn.Descriptors().WriteDesc(ctx, false, tbl, txn.KV())
					if err != nil {
						return err
					}
				}
				return nil
			}))

		lastCommit = startIdx
		if startIdx == totalDescriptorsToTest {
			return
		}
		tx = sqlRunner.Begin(t)
		_, err = tx.Exec("SET LOCAL autocommit_before_ddl = false")
		require.NoError(t, err)
	}
	for i := 0; i < totalDescriptorsToTest; i++ {
		if i%batchSize == 0 {
			commitFn(i)
		}
		_, err := tx.Exec(fmt.Sprintf("CREATE TABLE t_%d(n int, j int, index(j))", i))
		require.NoError(t, err)
		_, err = tx.Exec(fmt.Sprintf("SELECT 't_%d'::REGCLASS::OID", i))
		require.NoError(t, err)
	}
	commitFn(totalDescriptorsToTest)
	// AOST query is run 10 seconds in the past below so wait for the corruption
	// to appear.
	time.Sleep(time.Second * 10)

	// Confirm the repairable descriptors exist, we will avoid scanning all of
	// them avoid hitting the memory limit in testing.
	require.Equal(t,
		[][]string{{"true"}},
		sqlRunner.QueryStr(t, "SELECT count(*)>0 FROM \"\".crdb_internal.kv_repairable_catalog_corruptions"))
	// Upgrade to the latest version.
	sqlRunner.Exec(t, "SET CLUSTER SETTING version = crdb_internal.node_executable_version()")
	// Confirm that all descriptors are repaired.
	require.Equal(t,
		[][]string{{"0"}},
		sqlRunner.QueryStr(t, "SELECT count(*) FROM \"\".crdb_internal.kv_repairable_catalog_corruptions"),
	)

}
