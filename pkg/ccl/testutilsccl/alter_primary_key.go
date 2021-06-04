// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package testutilsccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/stretchr/testify/require"
)

// AlterPrimaryKeyCorrectZoneConfigIntermediateZoneConfig is an expected
// intermediate zone configuration in the AlterPrimaryKeyCorrectZoneConfigTestCase.
type AlterPrimaryKeyCorrectZoneConfigIntermediateZoneConfig struct {
	ShowConfigStatement string
	ExpectedTarget      string
	ExpectedSQL         string
}

// AlterPrimaryKeyCorrectZoneConfigTestCase is a test case for
// AlterPrimaryKeyCorrectZoneConfigTest.
type AlterPrimaryKeyCorrectZoneConfigTestCase struct {
	Desc                            string
	SetupQuery                      string
	AlterQuery                      string
	ExpectedIntermediateZoneConfigs []AlterPrimaryKeyCorrectZoneConfigIntermediateZoneConfig
}

// AlterPrimaryKeyCorrectZoneConfigTest tests that zone configurations
// are correctly set before the backfill of a PRIMARY KEY.
func AlterPrimaryKeyCorrectZoneConfigTest(
	t *testing.T, createDBStatement string, testCases []AlterPrimaryKeyCorrectZoneConfigTestCase,
) {
	chunkSize := int64(100)
	maxValue := 4000

	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		maxValue = 200
		chunkSize = 5
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.Desc, func(t *testing.T) {
			var db *gosql.DB
			params, _ := tests.CreateTestServerParams()
			params.Locality.Tiers = []roachpb.Tier{
				{Key: "region", Value: "ajstorm-1"},
			}

			runCheck := false
			params.Knobs = base.TestingKnobs{
				SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
					BackfillChunkSize: chunkSize,
				},
				DistSQL: &execinfra.TestingKnobs{
					RunBeforeBackfillChunk: func(sp roachpb.Span) error {
						if runCheck {
							for _, subTC := range tc.ExpectedIntermediateZoneConfigs {
								t.Run(subTC.ShowConfigStatement, func(t *testing.T) {
									var target, sql string
									require.NoError(
										t,
										db.QueryRow(subTC.ShowConfigStatement).Scan(&target, &sql),
									)
									require.Equal(t, subTC.ExpectedTarget, target)
									require.Equal(t, subTC.ExpectedSQL, sql)
								})
							}
							runCheck = false
						}
						return nil
					},
				},
				// Decrease the adopt loop interval so that retries happen quickly.
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			}
			s, sqlDB, _ := serverutils.StartServer(t, params)
			db = sqlDB
			defer s.Stopper().Stop(ctx)

			if _, err := sqlDB.Exec(fmt.Sprintf(`
%s;
USE t;
%s
`, createDBStatement, tc.SetupQuery)); err != nil {
				t.Fatal(err)
			}

			// Insert some rows so we can interrupt inspect state during backfill.
			require.NoError(t, sqltestutils.BulkInsertIntoTable(sqlDB, maxValue))

			runCheck = true
			_, err := sqlDB.Exec(tc.AlterQuery)
			require.NoError(t, err)
		})
	}

}
