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
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as multi-region.
	cs := cluster.MakeTestingClusterSettings()
	sql.SecondaryTenantsMultiRegionAbstractionsEnabled.Override(ctx, &cs.SV, true)
	sql.SecondaryTenantZoneConfigsEnabled.Override(ctx, &cs.SV, true)
	instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)

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

			query = `
				SELECT COUNT(id), crdb_region
				FROM system.sql_instances
				WHERE session_id IS NULL GROUP BY crdb_region
			`
			preallocatedCount := instancestorage.PreallocatedCount.Get(&cs.SV)
			testutils.SucceedsSoon(t, func() error {
				rows := tDB.Query(t, query)
				require.True(t, rows.Next())

				countMap := map[string]int{}
				for {
					var count int
					var crdb_region string

					require.NoError(t, rows.Scan(&count, &crdb_region))
					countMap[crdb_region] = count

					if !rows.Next() {
						break
					}
				}
				require.NoError(t, rows.Close())
				if len(countMap) != 3 {
					return errors.New("some regions have not been preallocated")
				}
				for _, r := range []string{"us-east1", "us-east2", "us-east3"} {
					c, ok := countMap[r]
					require.True(t, ok)
					if c != int(preallocatedCount) {
						return errors.Newf("require %d, but got %d", preallocatedCount, c)
					}
				}
				return nil
			})
		})

		t.Run("Reclaim", func(t *testing.T) {
			id := uuid.MakeV4()
			s1, err := slstorage.MakeSessionID(make([]byte, 100), id)
			require.NoError(t, err)
			s2, err := slstorage.MakeSessionID(make([]byte, 200), id)
			require.NoError(t, err)

			// Insert expired entries into sql_instances.
			tDB.Exec(t, `INSERT INTO system.sql_instances (id, addr, session_id, locality, crdb_region) VALUES
		   		(100, NULL, $1, NULL, 'us-east2'),
		   		(200, NULL, $2, NULL, 'us-east3')`, s1.UnsafeBytes(), s2.UnsafeBytes())

			query := `SELECT count(*) FROM system.sql_instances WHERE id = 42`

			// Wait until expired entries get removed.
			testutils.SucceedsSoon(t, func() error {
				var rowCount int
				tDB.QueryRow(t, query).Scan(&rowCount)
				if rowCount != 0 {
					return errors.New("some regions have not been reclaimed")
				}
				return nil
			})
		})
	})
}

func TestTenantStartupWithMultiRegionEnum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer envutil.TestSetEnv(t, "COCKROACH_MR_SYSTEM_DATABASE", "1")()

	// Enable settings required for configuring a tenant's system database as multi-region.
	cs := cluster.MakeTestingClusterSettings()
	sql.SecondaryTenantsMultiRegionAbstractionsEnabled.Override(context.Background(), &cs.SV, true)
	sql.SecondaryTenantZoneConfigsEnabled.Override(context.Background(), &cs.SV, true)

	tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /*numServers*/, base.TestingKnobs{}, multiregionccltestutils.WithSettings(cs),
	)
	defer cleanup()

	tenID := roachpb.MustMakeTenantID(10)
	ten, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		Settings: cs,
		TenantID: tenID,
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
			},
		},
	})
	defer tSQL.Close()
	tenSQLDB := sqlutils.MakeSQLRunner(tSQL)

	// Update system database with regions.
	tenSQLDB.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1"`)
	tenSQLDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east2"`)
	tenSQLDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east3"`)

	ten2, tSQL2 := serverutils.StartTenant(t, tc.Server(2), base.TestTenantArgs{
		Settings: cs,
		TenantID: tenID,
		Existing: true,
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east3"},
			},
		},
	})
	defer tSQL2.Close()
	tenSQLDB2 := sqlutils.MakeSQLRunner(tSQL2)

	// The sqlliveness entry created by the first SQL server has enum.One as the
	// region as the system database hasn't been updated when it first started.
	var sessionID string
	tenSQLDB2.QueryRow(t, `SELECT session_id FROM system.sql_instances WHERE id = $1`,
		ten.SQLInstanceID()).Scan(&sessionID)
	region, id, err := slstorage.UnsafeDecodeSessionID(sqlliveness.SessionID(sessionID))
	require.NoError(t, err)
	require.NotNil(t, id)
	require.Equal(t, enum.One, region)

	// Ensure that the sqlliveness entry created by the second SQL server has
	// the right region and session UUID.
	tenSQLDB2.QueryRow(t, `SELECT session_id FROM system.sql_instances WHERE id = $1`,
		ten2.SQLInstanceID()).Scan(&sessionID)
	region, id, err = slstorage.UnsafeDecodeSessionID(sqlliveness.SessionID(sessionID))
	require.NoError(t, err)
	require.NotNil(t, id)
	require.NotEqual(t, enum.One, region)

	rows := tenSQLDB2.Query(t, `SELECT crdb_region, session_uuid FROM system.sqlliveness`)
	defer rows.Close()
	livenessMap := map[string][]byte{}
	for rows.Next() {
		var region, sessionUUID string
		require.NoError(t, rows.Scan(&region, &sessionUUID))
		livenessMap[sessionUUID] = []byte(region)
	}
	require.NoError(t, rows.Err())
	r, ok := livenessMap[string(id)]
	require.True(t, ok)
	require.Equal(t, r, region)
}
