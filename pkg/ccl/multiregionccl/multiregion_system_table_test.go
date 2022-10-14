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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

func TestRbrSqllivenessTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
		clock := hlc.NewClock(timeSource, base.DefaultMaxClockOffset)
		var ambientCtx log.AmbientContext
		return slstorage.NewTestingStorage(ambientCtx, stopper, clock, kvDB, keys.SystemSQLCodec, settings,
			tableID, timeSource.NewTimer)
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
