// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAutoHoming(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name             string
		createStatement  string
		expectedLocality string
	}{
		{
			name: "auto homing",
			createStatement: `
SET CLUSTER SETTING sql.defaults.experimental_auto_rehoming.enabled = true;
SET experimental_enable_auto_rehoming = true;
CREATE DATABASE test PRIMARY REGION "us-east2" REGIONS "us-east1", "us-east3";
CREATE TABLE test.rbr (p INT PRIMARY KEY, s STRING) LOCALITY REGIONAL BY ROW`,
			expectedLocality: "us-east3",
		},
		{
			name: "auto homing with altered table",
			createStatement: `
SET CLUSTER SETTING sql.defaults.experimental_auto_rehoming.enabled = true;
SET experimental_enable_auto_rehoming = true;
CREATE DATABASE test PRIMARY REGION "us-east2" REGIONS "us-east1", "us-east3";
CREATE TABLE test.rbr (p INT PRIMARY KEY, s STRING) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
ALTER TABLE test.rbr SET LOCALITY REGIONAL BY ROW`,
			expectedLocality: "us-east3",
		},
		{
			name: "auto homing disabled",
			createStatement: `
CREATE DATABASE test PRIMARY REGION "us-east2" REGIONS "us-east1", "us-east3";
CREATE TABLE test.rbr (p INT PRIMARY KEY, s STRING) LOCALITY REGIONAL BY ROW`,
			expectedLocality: "us-east1",
		},
		{
			name: "auto homing outside region",
			createStatement: `
SET CLUSTER SETTING sql.defaults.experimental_auto_rehoming.enabled = true;
SET experimental_enable_auto_rehoming = true;
CREATE DATABASE test PRIMARY REGION "us-east2" REGIONS "us-east1";
CREATE TABLE test.rbr (p INT PRIMARY KEY, s STRING) LOCALITY REGIONAL BY ROW`,
			expectedLocality: "us-east2",
		},
	}

	for _, test := range tests {
		updateTypes := []struct {
			name string
			stmt string
		}{
			{
				name: "update",
				stmt: `UPDATE rbr SET (s) = ('whaddup') WHERE p = 1`,
			},
			{
				name: "upsert",
				stmt: `UPSERT INTO rbr (p, s) VALUES (1, 'whaddup')`,
			},
		}
		for _, updateType := range updateTypes {
			t.Run(test.name+"-"+updateType.name, func(t *testing.T) {
				tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
					t,
					3,
					base.TestingKnobs{},
				)
				defer cleanup()

				// Create a separate bootstrapping connection so new connections have the
				// cluster setting applied if necessary.
				bootstrapCon := sqlutils.MakeSQLRunner(tc.Conns[0])
				bootstrapCon.Exec(t, test.createStatement)

				sql0 := sqlutils.MakeSQLRunner(tc.Conns[0])
				sql2 := sqlutils.MakeSQLRunner(tc.Conns[2])

				sql0.Exec(t, "USE test")
				sql2.Exec(t, "USE test")

				var crdbRegion string

				sql0.Exec(t, `
INSERT INTO rbr (p, s) VALUES (1, 'hi');
`)

				row := sql0.QueryRow(t, `
SELECT crdb_region FROM rbr WHERE p = 1`)
				row.Scan(&crdbRegion)

				if crdbRegion != "us-east1" {
					t.Fatalf(
						"expected initial crdbRegion to be us-east1 but got %s",
						crdbRegion,
					)
				}

				sql2.Exec(t, updateType.stmt)

				row = sql0.QueryRow(t, `
SELECT crdb_region FROM rbr WHERE p = 1`)
				row.Scan(&crdbRegion)

				if crdbRegion != test.expectedLocality {
					t.Fatalf(
						"expected crdbRegion after update to be %s but got %s",
						test.expectedLocality,
						crdbRegion,
					)
				}
			})
		}
	}
}
