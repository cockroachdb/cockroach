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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func createSqllivenessTable(
	t *testing.T, db *sqlutils.SQLRunner, dbName string,
) (tableID descpb.ID) {
	t.Helper()
	db.Exec(t, fmt.Sprintf(`
		CREATE DATABASE IF NOT EXISTS "%s" 
		WITH PRIMARY REGION "us-east1"
		REGIONS "us-east1", "us-east2", "us-east3"
	`, dbName))

	// expiration needs to be column 2. slstorage.Table assumes the column id.
	// session_uuid and crdb_region are identified by their location in the
	// primary key.
	db.Exec(t, fmt.Sprintf(`
		CREATE TABLE "%s".sqlliveness (
			session_uuid BYTES NOT NULL,
			expiration DECIMAL NOT NULL,
			crdb_region "%s".public.crdb_internal_region,
			PRIMARY KEY(crdb_region, session_uuid)
		) LOCALITY REGIONAL BY ROW;
	`, dbName, dbName))
	db.QueryRow(t, `
		select u.id
		from system.namespace t
		join system.namespace u 
		  on t.id = u."parentID" 
		where t.name = $1 and u.name = $2`,
		dbName, "sqlliveness").Scan(&tableID)
	return tableID
}

func TestMrSystemDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer envutil.TestSetEnv(t, "COCKROACH_MR_SYSTEM_DATABASE", "1")()

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3, base.TestingKnobs{})
	defer cleanup()

	tDB := sqlutils.MakeSQLRunner(sqlDB)

	t.Run("Sqlliveness", func(t *testing.T) {
		row := tDB.QueryRow(t, `SELECT crdb_region, session_uuid, expiration FROM system.sqlliveness LIMIT 1`)
		var sessionUUID string
		var crdbRegion string
		var rawExpiration apd.Decimal
		row.Scan(&crdbRegion, &sessionUUID, &rawExpiration)
	})
}

func TestRbrSqllivenessTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer envutil.TestSetEnv(t, "COCKROACH_MR_SYSTEM_DATABASE", "1")()

	ctx := context.Background()

	cluster, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3, base.TestingKnobs{})
	defer cleanup()
	kvDB := cluster.Servers[0].DB()
	settings := cluster.Servers[0].Cfg.Settings
	stopper := cluster.Servers[0].Stopper()

	tDB := sqlutils.MakeSQLRunner(sqlDB)

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)
	clock := hlc.NewClock(timeSource, base.DefaultMaxClockOffset)

	setup := func(t *testing.T) *slstorage.Storage {
		dbName := t.Name()
		tableID := createSqllivenessTable(t, tDB, dbName)
		var ambientCtx log.AmbientContext
		// rbrIndexID is the index id used to access the regional by row index in
		// tests. In production it will be index 2, but the freshly created test table
		// will have index 1.
		const rbrIndexID = 1
		return slstorage.NewTestingStorage(ambientCtx, stopper, clock, kvDB, keys.SystemSQLCodec, settings,
			tableID, rbrIndexID, timeSource.NewTimer)
	}

	t.Run("SqlRead", func(t *testing.T) {
		storage := setup(t)

		initialUUID := uuid.MakeV4()
		session, err := slstorage.MakeSessionID(enum.One, initialUUID)
		require.NoError(t, err)

		writeExpiration := clock.Now().Add(10, 00)
		require.NoError(t, storage.Insert(ctx, session, writeExpiration))

		var sessionUUID string
		var crdbRegion string
		var rawExpiration apd.Decimal

		row := tDB.QueryRow(t, fmt.Sprintf(`SELECT crdb_region, session_uuid, expiration FROM "%s".sqlliveness`, t.Name()))
		row.Scan(&crdbRegion, &sessionUUID, &rawExpiration)

		require.Contains(t, []string{"us-east1", "us-east2", "us-east3"}, crdbRegion)
		require.Equal(t, sessionUUID, string(initialUUID.GetBytes()))

		readExpiration, err := hlc.DecimalToHLC(&rawExpiration)
		require.NoError(t, err)

		require.Equal(t, writeExpiration, readExpiration)
	})
}

func TestTenantStartupWithMultiRegionEnum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer envutil.TestSetEnv(t, "COCKROACH_MR_SYSTEM_DATABASE", "1")()

	tc, db, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /*numServers*/, base.TestingKnobs{},
	)
	defer cleanup()
	sqlDB := sqlutils.MakeSQLRunner(db)

	tenID := roachpb.MakeTenantID(10)
	ten, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID: tenID,
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
			},
		},
	})
	defer tSQL.Close()
	tenSQLDB := sqlutils.MakeSQLRunner(tSQL)

	// Set cluster setting override on the host, and wait for it to propagate.
	sqlDB.Exec(t, fmt.Sprintf("ALTER TENANT $1 SET CLUSTER SETTING %s = 'true'",
		sql.SecondaryTenantsMultiRegionAbstractionsEnabledSettingName), tenID.ToUint64())
	testutils.SucceedsSoon(t, func() error {
		var currentVal string
		tenSQLDB.QueryRow(t, fmt.Sprintf("SHOW CLUSTER SETTING %s",
			sql.SecondaryTenantsMultiRegionAbstractionsEnabledSettingName)).Scan(&currentVal)
		if currentVal != "true" {
			return errors.New("waiting for cluster setting to be set to true")
		}
		return nil
	})

	// Update system database with regions.
	tenSQLDB.Exec(t, `SET descriptor_validation = read_only`)
	tenSQLDB.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1"`)
	tenSQLDB.Exec(t, `SET descriptor_validation = on`)
	tenSQLDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east2"`)
	tenSQLDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east3"`)

	ten2, tSQL2 := serverutils.StartTenant(t, tc.Server(2), base.TestTenantArgs{
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
