// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// alterCrdbRegionType converts the crdb_region []byte column in a system
// database table into the system database's enum type.
func alterCrdbRegionType(
	ctx context.Context, tableID descpb.ID, db *kv.DB, executor descs.TxnManager,
) error {
	flags := tree.CommonLookupFlags{
		Required:    true,
		AvoidLeased: true,
	}
	objFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: flags,
	}

	getRegionEnum := func(systemDB catalog.DatabaseDescriptor, txn *kv.Txn, collection *descs.Collection) (*typedesc.Mutable, *types.T, error) {
		enumID, err := systemDB.MultiRegionEnumID()
		if err != nil {
			return nil, nil, err
		}
		enumTypeDesc, err := collection.GetMutableTypeByID(ctx, txn, enumID, objFlags)
		if err != nil {
			return nil, nil, err
		}
		schema, err := collection.GetImmutableSchemaByID(ctx, txn, enumTypeDesc.GetParentSchemaID(), flags)
		if err != nil {
			return nil, nil, err
		}
		enumName := tree.MakeQualifiedTypeName(systemDB.GetName(), schema.GetName(), enumTypeDesc.GetName())
		enumType, err := enumTypeDesc.MakeTypesT(ctx, &enumName, nil)
		if err != nil {
			return nil, nil, err
		}
		return enumTypeDesc, enumType, nil
	}

	getMutableColumn := func(table *tabledesc.Mutable, name string) (*descpb.ColumnDescriptor, error) {
		for i := range table.Columns {
			if table.Columns[i].Name == name {
				return &table.Columns[i], nil
			}
		}
		return nil, errors.New("crdb_region column not found")
	}

	err := executor.DescsTxn(ctx, db, func(ctx context.Context, txn *kv.Txn, collection *descs.Collection) error {
		_, systemDB, err := collection.GetImmutableDatabaseByID(ctx, txn, keys.SystemDatabaseID, flags)
		if err != nil {
			return err
		}

		enumTypeDesc, enumType, err := getRegionEnum(systemDB, txn, collection)
		if err != nil {
			return err
		}

		// Change the crdb_region column's type to the enum
		tableDesc, err := collection.GetMutableTableByID(ctx, txn, tableID, objFlags)
		if err != nil {
			return err
		}
		column, err := getMutableColumn(tableDesc, "crdb_region")
		if err != nil {
			return err
		}
		column.Type = enumType
		if err := collection.WriteDesc(ctx, false, tableDesc, txn); err != nil {
			return err
		}

		// Add a back reference to the enum
		enumTypeDesc.AddReferencingDescriptorID(tableID)
		return collection.WriteDesc(ctx, false, enumTypeDesc, txn)
	})
	if err != nil {
		return errors.Wrapf(err, "unable to change crdb_region from []byte to the multi-region enum for table %d", tableID)
	}
	return err
}

func TestMrSystemDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer envutil.TestSetEnv(t, "COCKROACH_MR_SYSTEM_DATABASE", "1")()

	// Enable settings required for configuring a tenant's system database as multi-region.
	cs := cluster.MakeTestingClusterSettings()
	sql.SecondaryTenantsMultiRegionAbstractionsEnabled.Override(context.Background(), &cs.SV, true)
	sql.SecondaryTenantZoneConfigsEnabled.Override(context.Background(), &cs.SV, true)

	cluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3, base.TestingKnobs{}, multiregionccltestutils.WithSettings(cs))
	defer cleanup()

	id, err := roachpb.MakeTenantID(11)
	require.NoError(t, err)

	tenantArgs := base.TestTenantArgs{
		Settings: cs,
		TenantID: id,
		Locality: *cluster.Servers[0].Locality(),
	}
	tenantServer, tenantSQL := serverutils.StartTenant(t, cluster.Servers[0], tenantArgs)

	tDB := sqlutils.MakeSQLRunner(tenantSQL)

	tDB.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1"`)
	tDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east2"`)
	tDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east3"`)

	ctx := context.Background()
	executor := tenantServer.ExecutorConfig().(sql.ExecutorConfig)

	// Changing the type of the crdb_region field is required to modify the
	// types with SET LOCALITY REGIONAL BY ROW.
	require.NoError(t, alterCrdbRegionType(ctx, keys.SqllivenessID, executor.DB, executor.InternalExecutorFactory))
	require.NoError(t, alterCrdbRegionType(ctx, keys.SQLInstancesTableID, executor.DB, executor.InternalExecutorFactory))

	// Run schema validations to ensure the manual descriptor modifications are
	// okay.
	tDB.CheckQueryResults(t, `SELECT * FROM crdb_internal.invalid_objects`, [][]string{})

	t.Run("Sqlliveness", func(t *testing.T) {
		// TODO(jeffswenson): Setting the locality does not work because it causes the schema
		// changer to rewrite the primary key index.
		// tDB.Exec(t, `ALTER TABLE system.sqlliveness SET LOCALITY REGIONAL BY ROW`)
		row := tDB.QueryRow(t, `SELECT crdb_region, session_uuid, expiration FROM system.sqlliveness LIMIT 1`)
		var sessionUUID string
		var crdbRegion string
		var rawExpiration apd.Decimal
		row.Scan(&crdbRegion, &sessionUUID, &rawExpiration)
		require.Equal(t, "us-east1", crdbRegion)
	})

	t.Run("Sqlinstances", func(t *testing.T) {
		// TODO(jeffswenson): Setting the locality does not work because it causes the schema
		// changer to rewrite the primary key index.
		// tDB.Exec(t, `ALTER TABLE system.sql_instances SET LOCALITY REGIONAL BY ROW`)

		t.Run("InUse", func(t *testing.T) {
			query := `
				SELECT id, addr, session_id, locality, crdb_region
				FROM system.sql_instances
				WHERE session_id IS NOT NULL
			`
			rows := tDB.Query(t, query)
			require.True(t, rows.Next())
			for {
				var id base.SQLInstanceID
				var addr, locality string
				var crdb_region string
				var session sqlliveness.SessionID

				require.NoError(t, rows.Scan(&id, &addr, &session, &locality, &crdb_region))

				require.True(t, 0 < id)
				require.NotEmpty(t, addr)
				require.NotEmpty(t, locality)
				require.NotEmpty(t, session)
				require.NotEmpty(t, crdb_region)

				require.Equal(t, "us-east1", crdb_region)

				if !rows.Next() {
					break
				}
			}
			require.NoError(t, rows.Close())
		})

		t.Run("Preallocated", func(t *testing.T) {
			query := `
				SELECT id, addr, session_id, locality, crdb_region
				FROM system.sql_instances
				WHERE session_id IS NULL
			`
			rows := tDB.Query(t, query)
			require.True(t, rows.Next())
			for {
				var id base.SQLInstanceID
				var addr, locality, session gosql.NullString
				var crdb_region string

				require.NoError(t, rows.Scan(&id, &addr, &session, &locality, &crdb_region))

				require.True(t, 0 < id)
				require.False(t, addr.Valid)
				require.False(t, locality.Valid)
				require.False(t, session.Valid)
				require.NotEmpty(t, crdb_region)

				if !rows.Next() {
					break
				}
			}
			require.NoError(t, rows.Close())
		})
	})
}
