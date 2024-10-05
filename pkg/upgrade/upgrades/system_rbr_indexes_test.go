// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBackfillRegionalByRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "times out under deadlock")

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	const rowCount = 10000

	tDB.Exec(t, `CREATE DATABASE test`)
	tDB.Exec(t, `
	CREATE TABLE test.public.migration_test (
		key   INT8 NOT NULL PRIMARY KEY,
		value INT8 NOT NULL
	)`)
	originalDesc := desctestutils.TestingGetTableDescriptor(kvDB, s.Codec(), "test", "public", "migration_test")

	tDB.Exec(t, `
		INSERT INTO test.public.migration_test (key, value)
		SELECT 
			generate_series AS key,
			crc32c(generate_series::string::bytes) as value
		FROM generate_series(1, $1);`, int(rowCount))

	newDesc := patchDescriptor(originalDesc)
	migration := makeMigration(s, newDesc)
	deps := makeDeps(s)

	readRows := func() map[string]string {
		result := map[string]string{}
		rows := tDB.QueryStr(t, `SELECT key, value FROM test.public.migration_test`)
		for _, row := range rows {
			result[row[0]] = row[1]
		}
		return result
	}

	// The original index should have all the rows and the new index should be
	// empty.
	require.Equal(t, rowCount, countRowsWithKv(t, s, originalDesc))
	require.Equal(t, 0, countRowsWithKv(t, s, newDesc))
	rowsBefore := readRows()

	require.NoError(t, migrateTableToRbrIndex(ctx, migration, deps))

	// After running the first part of the migration, the new index should have
	// everything in the old index.
	require.Equal(t, rowCount, countRowsWithKv(t, s, originalDesc))
	require.Equal(t, rowCount, countRowsWithKv(t, s, newDesc))
	rowsBetween := readRows()

	// Decode a whole row. Make sure there is a crdb_region column in the
	// table.
	var region []byte
	var key, value int
	row := tDB.QueryRow(t, `SELECT crdb_region, key, value FROM test.public.migration_test LIMIT 1`)
	row.Scan(&region, &key, &value)
	require.Equal(t, region, enum.One)

	require.NoError(t, deleteOldIndex(ctx, migration, deps))

	// deleteOldIndex is expected to remove all kvs from the original index and
	// leave the new index untouched.
	require.Equal(t, 0, countRowsWithKv(t, s, originalDesc))
	require.Equal(t, rowCount, countRowsWithKv(t, s, newDesc))
	rowsAfter := readRows()

	// Verify that the state seen from sql is the same for each phase of the
	// migration.
	require.Equal(t, rowsBefore, rowsBetween)
	require.Equal(t, rowsBetween, rowsAfter)
	require.Len(t, rowsAfter, rowCount)
}

func countRowsWithKv(
	t *testing.T, s serverutils.TestServerInterface, desc catalog.TableDescriptor,
) (count int) {
	require.NoError(t, s.DB().Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		count = 0
		index := s.Codec().IndexPrefix(uint32(desc.GetID()), uint32(desc.GetPrimaryIndexID()))
		return txn.Iterate(ctx, index, index.PrefixEnd(), 1024, func(rows []kv.KeyValue) error {
			count += len(rows)
			return nil
		})
	}))
	return count
}

func makeDeps(s serverutils.TestServerInterface) upgrade.TenantDeps {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	return upgrade.TenantDeps{
		DB:           execCfg.InternalDB,
		KVDB:         s.DB(),
		Codec:        s.Codec(),
		LeaseManager: s.LeaseManager().(*lease.Manager),
	}
}

func patchDescriptor(desc catalog.TableDescriptor) catalog.TableDescriptor {
	table := tabledesc.NewBuilder(desc.TableDesc()).BuildExistingMutableTable()
	crdbRegionColumn := &descpb.ColumnDescriptor{
		Name: "crdb_region",
		ID:   table.GetNextColumnID(),
		Type: types.Bytes,
	}
	table.AddColumn(crdbRegionColumn)
	table.NextColumnID += 1

	originalIndex := table.TableDesc().PrimaryIndex

	// Add crdb_region to the primary key
	table.SetPrimaryIndex(descpb.IndexDescriptor{
		Name:                "primary",
		ID:                  table.GetNextIndexID(),
		Unique:              true,
		KeyColumnNames:      append([]string{"crdb_region"}, originalIndex.KeyColumnNames...),
		KeyColumnIDs:        append([]catid.ColumnID{crdbRegionColumn.ID}, originalIndex.KeyColumnIDs...),
		KeyColumnDirections: append([]catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC}, originalIndex.KeyColumnDirections...),
		StoreColumnIDs:      append([]catid.ColumnID{}, originalIndex.StoreColumnIDs...),
		StoreColumnNames:    append([]string{}, originalIndex.StoreColumnNames...),
		ConstraintID:        table.NextConstraintID,
		Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
		EncodingType:        catenumpb.PrimaryIndexEncoding,
	})
	table.NextIndexID += 1
	table.NextConstraintID += 1

	// Add crdb_region to the column family
	family := &table.Families[0]
	family.ColumnNames = append(family.ColumnNames, crdbRegionColumn.Name)
	family.ColumnIDs = append(family.ColumnIDs, crdbRegionColumn.ID)

	return table.MakePublic()
}

func makeMigration(s serverutils.TestServerInterface, desc catalog.TableDescriptor) rbrMigration {
	return rbrMigration{
		tableName:       desc.GetName(),
		keyMapper:       makeKeyMapper(s.Codec(), desc.TableDesc(), uint32(1)),
		finalDescriptor: desc.TableDesc(),
	}
}

func TestPrefixKeyMapper(t *testing.T) {
	const tableID = 12
	const sourceIndexID = 13
	const destIndexID = 14
	tenantID, err := roachpb.MakeTenantID(1337)
	require.NoError(t, err)
	codec := keys.MakeSQLCodec(tenantID)

	tableDesc := descpb.TableDescriptor{
		ID: tableID,
		PrimaryIndex: descpb.IndexDescriptor{
			ID: destIndexID,
		},
	}
	desc := tabledesc.NewBuilder(&tableDesc).BuildImmutableTable()

	keyMapper := makeKeyMapper(codec, desc.TableDesc(), sourceIndexID)

	sourceKey := codec.IndexPrefix(tableID, sourceIndexID)
	sourceKey = encoding.EncodeVarintAscending(sourceKey, 1234)
	sourceKey = keys.MakeFamilyKey(sourceKey, 0)
	require.Equal(t, sourceKey.String(), `/Tenant/1337/Table/12/13/1234/0`)
	require.Equal(t, keyMapper.OldPrefix().String(), `/Tenant/1337/Table/12/13`)

	destKey, err := keyMapper.OldToNew(sourceKey)
	require.NoError(t, err)
	require.Equal(t, destKey.String(), `/Tenant/1337/Table/12/14/"\x80"/1234/0`)
	require.Equal(t, keyMapper.NewPrefix().String(), `/Tenant/1337/Table/12/14/"\x80"`)
}

func TestRbrMigrationDescriptors(t *testing.T) {
	// Note: If you are making a change that breaks this test after the 23.1
	// release is shipped, it is probably safe to delete this test. You will
	// need to write an upgrade for the change, since this test will only break
	// if the bootstrap schema changes.
	codec := keys.SystemSQLCodec
	require.Equal(t, sqlLivenessMigration(codec).finalDescriptor, systemschema.SqllivenessTable().TableDesc())
	require.Equal(t, sqlInstanceMigration(codec).finalDescriptor, systemschema.SQLInstancesTable().TableDesc())
	require.Equal(t, leaseMigration(codec).finalDescriptor, systemschema.LeaseTable().TableDesc())
}
