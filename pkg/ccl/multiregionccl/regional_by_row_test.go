// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

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

// REGIONAL BY ROW tests are defined in multiregionccl as REGIONAL BY ROW
// requires CCL to operate.

// TestAlterTableLocalityRegionalByRowError tests an alteration involving
// REGIONAL BY ROW which gets its async job interrupted by some sort of
// error or cancellation. After this, we expect the table to retain
// its original form with no extra columns or implicit partitioning added.
func TestAlterTableLocalityRegionalByRowError(t *testing.T) {
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
	// BulkInsertIntoTable adds testCase 0 to maxValue inclusive, so
	// we round (maxValue + 1) / chunkSize to the nearest int.
	// To round up x / y using integers, we do (x + y - 1) / y.
	// In this case, since x=maxValue+1, we do (maxValue + chunkSize) / chunkSize.
	var chunksPerBackfill = (maxValue + int(chunkSize)) / int(chunkSize)
	ctx := context.Background()

	const showCreateTableStringSQL = `SELECT create_statement FROM [SHOW CREATE TABLE t.test]`
	const showZoneConfigurationSQL = `SHOW ZONE CONFIGURATION FROM TABLE t.test`

	// alterState is a struct that contains an action for a base test case
	// to execute ALTER TABLE t.test SET LOCALITY <locality> against.
	type alterState struct {
		desc       string
		alterQuery string
		// cancelOnBackfillChunk on which chunk the cancel query should run.
		cancelOnBackfillChunk int
	}

	// nonRegionalByRowAlterStates contains SET LOCALITY operations that exercise
	// the async ALTER PRIMARY KEY path for non-REGIONAL BY ROW base test cases.
	nonRegionalByRowAlterStates := []alterState{
		{
			desc:                  "alter to REGIONAL BY ROW AS cr",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW AS cr`,
			cancelOnBackfillChunk: 1,
		},
		{
			desc:                  "alter to REGIONAL BY ROW, interrupt during add column",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
			cancelOnBackfillChunk: 1,
		},
		{
			desc:                  "alter to REGIONAL BY ROW, interrupt during pk swap",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
			cancelOnBackfillChunk: chunksPerBackfill + 1,
		},
	}
	// nonRegionalByRowAlterStates contains SET LOCALITY operations that exercise
	// the async ALTER PRIMARY KEY path for REGIONAL BY ROW base test cases.
	regionalByRowAlterStates := []alterState{
		{
			desc:                  "alter to GLOBAL",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY GLOBAL`,
			cancelOnBackfillChunk: 1,
		},
		{
			desc:                  "alter to REGIONAL BY TABLE",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY TABLE`,
			cancelOnBackfillChunk: 1,
		},
		{
			desc:                  "alter to REGIONAL BY TABLE IN ajstorm-1",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY TABLE IN "ajstorm-1"`,
			cancelOnBackfillChunk: 1,
		},
	}

	// testCases contain a base table structure to start off as.
	// For each of these test cases, we pick either the alter state corresponding
	// to the base table's locality -- for REGIONAL BY ROW tables we pick
	// regionalyByRowAlterStates and for GLOBAL/REGIONAL BY TABLE tables we pick
	// nonRegionalByRowAlterStates.
	testCases := []struct {
		desc           string
		setupQuery     string
		rowidIdx       int
		alterStates    []alterState
		originalPKCols []string
	}{
		{
			desc:           "GLOBAL",
			setupQuery:     `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY GLOBAL`,
			originalPKCols: []string{"rowid"},
			alterStates:    nonRegionalByRowAlterStates,
		},
		{
			desc:           "REGIONAL BY TABLE",
			setupQuery:     `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY TABLE`,
			originalPKCols: []string{"rowid"},
			alterStates:    nonRegionalByRowAlterStates,
		},
		{
			desc:           "REGIONAL BY TABLE IN ajstorm-1",
			setupQuery:     `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY TABLE IN "ajstorm-1"`,
			originalPKCols: []string{"rowid"},
			alterStates:    nonRegionalByRowAlterStates,
		},
		{
			desc:           "REGIONAL BY ROW",
			setupQuery:     `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY ROW`,
			originalPKCols: []string{"crdb_region", "rowid"},
			alterStates: append(
				regionalByRowAlterStates,
				alterState{
					desc:                  "REGIONAL BY ROW AS cr",
					alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW AS cr`,
					cancelOnBackfillChunk: 1,
				},
			),
		},
		{
			desc: "REGIONAL BY ROW AS",
			setupQuery: `CREATE TABLE t.test (
				k INT NOT NULL, v INT, cr2 t.public.crdb_internal_region NOT NULL DEFAULT 'ajstorm-1'
			) LOCALITY REGIONAL BY ROW AS cr2`,
			originalPKCols: []string{"cr2", "rowid"},
			alterStates: append(
				regionalByRowAlterStates,
				alterState{
					desc:                  "REGIONAL BY ROW, cancel during column addition",
					alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
					cancelOnBackfillChunk: 1,
				},
				alterState{
					desc:                  "REGIONAL BY ROW, cancel during PK swap",
					alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
					cancelOnBackfillChunk: chunksPerBackfill + 1,
				},
			),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			for _, alterState := range testCase.alterStates {
				t.Run(alterState.desc, func(t *testing.T) {
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
										if currentBackfillChunk != alterState.cancelOnBackfillChunk {
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
CREATE DATABASE t PRIMARY REGION "ajstorm-1";
USE t;
%s;
`, testCase.setupQuery)); err != nil {
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
							_, err := sqlDB.Exec(alterState.alterQuery)
							require.Error(t, err)
							require.Contains(t, err.Error(), errorMode.errorContains)

							// Grab a copy of SHOW CREATE TABLE and SHOW ZONE CONFIGURATION before we run
							// any ALTER query. The result should match if the operation fails.
							var originalCreateTableOutput string
							require.NoError(
								t,
								sqlDB.QueryRow(showCreateTableStringSQL).Scan(&originalCreateTableOutput),
							)

							var originalTarget, originalZoneConfig string
							require.NoError(
								t,
								sqlDB.QueryRow(showZoneConfigurationSQL).Scan(&originalTarget, &originalZoneConfig),
							)

							// Ensure that the mutations corresponding to the primary key change are cleaned up and
							// that the job did not succeed even though it was canceled.
							testutils.SucceedsSoon(t, func() error {
								tableDesc := catalogkv.TestingGetTableDescriptor(
									kvDB, keys.SystemSQLCodec, "t", "test",
								)
								if len(tableDesc.GetMutations()) != 0 {
									return errors.Errorf(
										"expected 0 mutations after cancellation, found %d",
										len(tableDesc.GetMutations()),
									)
								}
								if tableDesc.GetPrimaryIndex().NumColumns() != len(testCase.originalPKCols) {
									return errors.Errorf("expected primary key change to not succeed after cancellation")
								}
								for i, name := range testCase.originalPKCols {
									if tableDesc.GetPrimaryIndex().GetColumnName(i) != name {
										return errors.Errorf(
											"expected primary key change to not succeed after cancellation\nmismatch idx %d: exp %s, got %s",
											i,
											name,
											tableDesc.GetPrimaryIndex().GetColumnName(i),
										)
									}
								}

								// Ensure SHOW CREATE TABLE for the table has not changed.
								var createTableString string
								if err := sqlDB.QueryRow(showCreateTableStringSQL).Scan(&createTableString); err != nil {
									return err
								}
								if createTableString != originalCreateTableOutput {
									return errors.Errorf(
										"expected SHOW CREATE TABLE to be %s\ngot %s",
										originalCreateTableOutput,
										createTableString,
									)
								}

								// Ensure SHOW ZONE CONFIGURATION has not changed.
								var target, zoneConfig string
								require.NoError(
									t,
									sqlDB.QueryRow(showZoneConfigurationSQL).Scan(&target, &zoneConfig),
								)
								if !(target == originalTarget && zoneConfig == originalZoneConfig) {
									return errors.Errorf(
										"expected zone configuration to not have changed, got %s/%s, sql %s/%s",
										originalTarget,
										target,
										originalZoneConfig,
										zoneConfig,
									)
								}

								return nil
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
		})
	}
}
