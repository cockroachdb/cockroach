// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccl_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// REGIONAL BY ROW tests are defined in partitionccl as REGIONAL BY ROW
// requires CCL to operate.

// TestAlterTableLocalityToRegionalByRowError tests an alteration to REGIONAL
// BY ROW which gets interrupted.
func TestAlterTableLocalityToRegionalByRowError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

	var chunkSize int64 = 100
	var maxValue = 4000
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		maxValue = 200
		chunkSize = 5
	}
	// BulkInsertIntoTable adds from 0 to maxValue inclusive, so
	// we round (maxValue + 1) / chunkSize to the nearest int.
	// To round up x / y using integers, we do (x + y - 1) / y.
	// In this case, since x=maxValue+1, we do (maxValue + chunkSize) / chunkSize.
	var chunksPerBackfill = (maxValue + int(chunkSize)) / int(chunkSize)
	ctx := context.Background()

	checkGlobal := func(sqlDB *gosql.DB, tableDesc catalog.TableDescriptor) error {
		if !tableDesc.IsLocalityGlobal() {
			return errors.Errorf("expected locality to be global")
		}

		zoneConfigRow := sqlDB.QueryRow("SHOW ZONE CONFIGURATION FROM TABLE t.test")
		var target string
		var rawZoneSQL string
		require.NoError(t, zoneConfigRow.Scan(&target, &rawZoneSQL))
		const expectedRawZoneSQL = `ALTER TABLE t.public.test CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 90000,
	global_reads = true,
	num_replicas = 3,
	num_voters = 3,
	constraints = '{+region=ajstorm-1: 1}',
	voter_constraints = '[+region=ajstorm-1]',
	lease_preferences = '[[+region=ajstorm-1]]'`
		if !(target == "TABLE t.public.test" && rawZoneSQL == expectedRawZoneSQL) {
			return errors.Errorf(
				"expected zone configuration to not have changed, got %s, sql %s",
				target,
				rawZoneSQL,
			)
		}
		return nil
	}

	checkRegionalByTable := func(sqlDB *gosql.DB, tableDesc catalog.TableDescriptor) error {
		if !tableDesc.IsLocalityRegionalByTable() {
			return errors.Errorf("expected locality to be regional by table")
		}

		zoneConfigRow := sqlDB.QueryRow("SHOW ZONE CONFIGURATION FROM TABLE t.test")
		var target string
		var rawZoneSQL string
		require.NoError(t, zoneConfigRow.Scan(&target, &rawZoneSQL))
		const expectedRawZoneSQL = `ALTER DATABASE t CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 90000,
	num_replicas = 3,
	num_voters = 3,
	constraints = '{+region=ajstorm-1: 1}',
	voter_constraints = '[+region=ajstorm-1]',
	lease_preferences = '[[+region=ajstorm-1]]'`
		if !(target == "DATABASE t" && rawZoneSQL == expectedRawZoneSQL) {
			return errors.Errorf(
				"expected zone configuration to not have changed, got %s, sql %s",
				target,
				rawZoneSQL,
			)
		}
		return nil
	}
	const showCreateTableGlobal = `CREATE TABLE public.test (
	k INT8 NOT NULL,
	v INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	cr public.crdb_internal_region NOT NULL DEFAULT gateway_region()::public.crdb_internal_region,
	CONSTRAINT "primary" PRIMARY KEY (rowid ASC),
	FAMILY "primary" (k, v, rowid, cr)
) LOCALITY GLOBAL`
	const showCreateTableRegionalByRow = `CREATE TABLE public.test (
	k INT8 NOT NULL,
	v INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	cr public.crdb_internal_region NOT NULL DEFAULT gateway_region()::public.crdb_internal_region,
	CONSTRAINT "primary" PRIMARY KEY (rowid ASC),
	FAMILY "primary" (k, v, rowid, cr)
) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION`

	testCases := []struct {
		desc                  string
		setupQuery            string
		toRegionalByRowQuery  string
		checkAfterCancel      func(*gosql.DB, catalog.TableDescriptor) error
		showCreateTableOutput string
		// cancelOnBackfillChunk on which chunk the cancel query should run.
		cancelOnBackfillChunk int
	}{
		{
			desc:                  "cancel after backfilling one chunk from GLOBAL to REGIONAL BY ROW AS",
			setupQuery:            `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY GLOBAL`,
			toRegionalByRowQuery:  `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW AS cr`,
			checkAfterCancel:      checkGlobal,
			cancelOnBackfillChunk: 1,
			showCreateTableOutput: showCreateTableGlobal,
		},
		{
			desc:                  "cancel during ADD COLUMN stage of from GLOBAL to REGIONAL BY ROW",
			setupQuery:            `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY GLOBAL`,
			toRegionalByRowQuery:  `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
			checkAfterCancel:      checkGlobal,
			cancelOnBackfillChunk: 1,
			showCreateTableOutput: showCreateTableGlobal,
		},
		{
			desc:                  "cancel during ALTER PRIMARY KEY stage from GLOBAL to REGIONAL BY ROW",
			setupQuery:            `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY GLOBAL`,
			toRegionalByRowQuery:  `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
			checkAfterCancel:      checkGlobal,
			cancelOnBackfillChunk: chunksPerBackfill + 1,
			showCreateTableOutput: showCreateTableGlobal,
		},

		{
			desc:                  "cancel after backfilling one chunk from REGIONAL BY TABLE to REGIONAL BY ROW AS",
			setupQuery:            `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY TABLE`,
			toRegionalByRowQuery:  `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW AS cr`,
			checkAfterCancel:      checkRegionalByTable,
			cancelOnBackfillChunk: 1,
			showCreateTableOutput: showCreateTableRegionalByRow,
		},
		{
			desc:                  "cancel during ADD COLUMN stage of from REGIONAL BY TABLE to REGIONAL BY ROW",
			setupQuery:            `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY TABLE`,
			toRegionalByRowQuery:  `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
			checkAfterCancel:      checkRegionalByTable,
			cancelOnBackfillChunk: 1,
			showCreateTableOutput: showCreateTableRegionalByRow,
		},
		{
			desc:                  "cancel during ALTER PRIMARY KEY stage from REGIONAL BY TABLE to REGIONAL BY ROW",
			setupQuery:            `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY TABLE`,
			toRegionalByRowQuery:  `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
			checkAfterCancel:      checkRegionalByTable,
			cancelOnBackfillChunk: chunksPerBackfill + 1,
			showCreateTableOutput: showCreateTableRegionalByRow,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			for _, errorMode := range []struct {
				desc          string
				runOnChunk    func(db *gosql.DB) error
				errorContains string
			}{
				{
					desc: "cancel",
					runOnChunk: func(db *gosql.DB) error {
						_, err := db.Exec(`CANCEL JOB (
					SELECT job_id FROM [SHOW JOBS]
					WHERE
						job_type = 'SCHEMA CHANGE' AND
						status = $1 AND
						description NOT LIKE 'ROLL BACK%'
				)`, jobs.StatusRunning)
						return err
					},
					errorContains: "job canceled by user",
				},
				{
					desc: "arbitrary error",
					runOnChunk: func(db *gosql.DB) error {
						return errors.Newf("arbitrary error during backfill")
					},
					errorContains: "arbitrary error during backfill",
				},
			} {
				t.Run(errorMode.desc, func(t *testing.T) {
					var db *gosql.DB
					// set backfill chunk to -chunksPerBackfill, to allow the ALTER TABLE ... ADD COLUMN
					// to backfill successfully.
					currentBackfillChunk := -(chunksPerBackfill + 1)
					params, _ := tests.CreateTestServerParams()
					params.Locality.Tiers = []roachpb.Tier{
						{Key: "region", Value: "ajstorm-1"},
					}
					params.Knobs = base.TestingKnobs{
						SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
							BackfillChunkSize: chunkSize,
						},
						DistSQL: &execinfra.TestingKnobs{
							RunBeforeBackfillChunk: func(sp roachpb.Span) error {
								currentBackfillChunk += 1
								if currentBackfillChunk != tc.cancelOnBackfillChunk {
									return nil
								}
								return errorMode.runOnChunk(db)
							},
						},
					}
					s, sqlDB, kvDB := serverutils.StartServer(t, params)
					db = sqlDB
					defer s.Stopper().Stop(ctx)

					// Disable strict GC TTL enforcement because we're going to shove a zero-value
					// TTL into the system with AddImmediateGCZoneConfig.
					defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

					if _, err := sqlDB.Exec(fmt.Sprintf(`
SET experimental_enable_implicit_column_partitioning = true;
CREATE DATABASE t PRIMARY REGION "ajstorm-1";
%s;
`, tc.setupQuery)); err != nil {
						t.Fatal(err)
					}

					if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
						t.Fatal(err)
					}

					// We add the "cr" column, which can be used for REGIONAL BY ROW AS.
					if _, err := sqlDB.Exec(`
		ALTER TABLE t.test ADD COLUMN cr t.crdb_internal_region
		NOT NULL
		DEFAULT gateway_region()::t.crdb_internal_region
	`); err != nil {
						t.Fatal(err)
					}
					// This will fail, so we don't want to check the error.
					_, err := sqlDB.Exec(tc.toRegionalByRowQuery)
					require.Error(t, err)
					require.Contains(t, err.Error(), errorMode.errorContains)

					// Ensure that the mutations corresponding to the primary key change are cleaned up and
					// that the job did not succeed even though it was canceled.
					testutils.SucceedsSoon(t, func() error {
						tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
						if len(tableDesc.GetMutations()) != 0 {
							return errors.Errorf("expected 0 mutations after cancellation, found %d", len(tableDesc.GetMutations()))
						}
						if tableDesc.GetPrimaryIndex().NumColumns() != 1 || tableDesc.GetPrimaryIndex().GetColumnName(0) != "rowid" {
							return errors.Errorf("expected primary key change to not succeed after cancellation")
						}

						// Ensure SHOW CREATE TABLE for the table has not changed.
						createTableRow := sqlDB.QueryRow(
							"SELECT create_statement FROM [SHOW CREATE TABLE t.test]",
						)
						var createTableString string
						if err := createTableRow.Scan(&createTableString); err != nil {
							return err
						}
						if createTableString != tc.showCreateTableOutput {
							return errors.Errorf(
								"expected SHOW CREATE TABLE to be %s\ngot %s",
								tc.showCreateTableOutput,
								createTableString,
							)
						}

						return tc.checkAfterCancel(sqlDB, tableDesc)
					})

					tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
					if _, err := sqltestutils.AddImmediateGCZoneConfig(db, tableDesc.GetID()); err != nil {
						t.Fatal(err)
					}
					// Ensure that the writes from the partial new indexes are cleaned up.
					testutils.SucceedsSoon(t, func() error {
						return sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue)
					})
				})
			}
		})
	}
}
