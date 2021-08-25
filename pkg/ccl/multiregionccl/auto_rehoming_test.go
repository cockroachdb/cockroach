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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAutoHoming(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name               string
		createStatement    string
		expectedLocalities []string
	}{
		{
			"auto homing",
			`
SET CLUSTER SETTING sql.defaults.auto_rehoming.enabled = true;
SET enable_auto_rehoming = true;
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
CREATE TABLE test.rbr (p INT PRIMARY KEY, s STRING) LOCALITY REGIONAL BY ROW`,
			[]string{"us-east1", "us-east2", "us-east3", "us-east2"},
		},
		{
			"auto homing with altered table",
			`
SET CLUSTER SETTING sql.defaults.auto_rehoming.enabled = true;
SET enable_auto_rehoming = true;
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
CREATE TABLE test.rbr (p INT PRIMARY KEY, s STRING) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
ALTER TABLE test.rbr SET LOCALITY REGIONAL BY ROW`,
			[]string{"us-east1", "us-east2", "us-east3", "us-east2"},
		},
		{
			"auto homing disabled",
			`
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
CREATE TABLE test.rbr (p INT PRIMARY KEY, s STRING) LOCALITY REGIONAL BY ROW`,
			[]string{"us-east1", "us-east1", "us-east1", "us-east1"},
		},
		{
			"auto homing outside region",
			`
SET CLUSTER SETTING sql.defaults.auto_rehoming.enabled = true;
SET enable_auto_rehoming = true;
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2";
CREATE TABLE test.rbr (p INT PRIMARY KEY, s STRING) LOCALITY REGIONAL BY ROW`,
			[]string{"us-east1", "us-east2", "us-east1", "us-east2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
			sql1 := sqlutils.MakeSQLRunner(tc.Conns[1])
			sql2 := sqlutils.MakeSQLRunner(tc.Conns[2])

			sql0.Exec(t, "USE test")
			sql1.Exec(t, "USE test")
			sql2.Exec(t, "USE test")

			var p int
			var s string
			var crdbRegion string

			sql0.Exec(t, `
INSERT INTO rbr (p, s) VALUES (1, 'hi');
`)

			row := sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
			row.Scan(&p, &s, &crdbRegion)

			if crdbRegion != test.expectedLocalities[0] {
				t.Fatalf(
					"expected crdbRegion after first update to be %s but got %s",
					test.expectedLocalities[0],
					crdbRegion,
				)
			}

			sql1.Exec(t, `
UPDATE rbr SET (s) = ('whaddup') WHERE p = 1;`)

			row = sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
			row.Scan(&p, &s, &crdbRegion)

			if crdbRegion != test.expectedLocalities[1] {
				t.Fatalf(
					"expected crdbRegion after second update to be %s but got %s",
					test.expectedLocalities[1],
					crdbRegion,
				)
			}

			sql2.Exec(t, `
UPDATE rbr SET (s) = ('anothaone') WHERE p = 1;`)

			row = sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
			row.Scan(&p, &s, &crdbRegion)

			if crdbRegion != test.expectedLocalities[2] {
				t.Fatalf(
					"expected crdbRegion after third update to be %s but got %s",
					test.expectedLocalities[2],
					crdbRegion,
				)
			}

			sql1.Exec(t, `
UPSERT INTO rbr (p, s) VALUES (1, 'last_try')`)

			row = sql0.QueryRow(t, `
SELECT p, s, crdb_region FROM rbr WHERE p = 1`)
			row.Scan(&p, &s, &crdbRegion)

			if crdbRegion != test.expectedLocalities[3] {
				t.Fatalf(
					"expected crdbRegion after third update to be %s but got %s",
					test.expectedLocalities[3],
					crdbRegion,
				)
			}
		})
	}
}

func TestRehomingVersionGating(t *testing.T) {
	tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t,
		3,
		base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: 1,
				BinaryVersionOverride: clusterversion.ByKey(
					clusterversion.OnUpdateExpressions - 1,
				),
			},
		},
	)
	defer cleanup()

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	tdb.Exec(t, `
SET CLUSTER SETTING sql.defaults.auto_rehoming.enabled = true;
SET enable_auto_rehoming = true;
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
CREATE TABLE test.rbr (p INT PRIMARY KEY, s STRING) LOCALITY REGIONAL BY ROW`)

	createRow := tdb.QueryRow(t, `
SHOW CREATE TABLE test.rbr`)
	var tableName string
	var createStatement string
	createRow.Scan(&tableName, &createStatement)
	expectedCreateStatement := `CREATE TABLE public.rbr (
	p INT8 NOT NULL,
	s STRING NULL,
	crdb_region public.crdb_internal_region NOT VISIBLE NOT NULL DEFAULT default_to_database_primary_region(gateway_region())::public.crdb_internal_region,
	CONSTRAINT "primary" PRIMARY KEY (p ASC),
	FAMILY "primary" (p, s, crdb_region)
) LOCALITY REGIONAL BY ROW`

	if createStatement != expectedCreateStatement {
		t.Fatalf(
			"expected not to have an ON UPDATE statement after create but got:\n%s",
			createStatement,
		)
	}

	tdb.Exec(t, `
DROP TABLE test.rbr;
CREATE TABLE test.rbr (p INT PRIMARY KEY, s STRING) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
ALTER TABLE test.rbr SET LOCALITY REGIONAL BY ROW`)

	createRow = tdb.QueryRow(t, `
SHOW CREATE TABLE test.rbr`)
	createRow.Scan(&tableName, &createStatement)

	if createStatement != expectedCreateStatement {
		t.Fatalf(
			"expected not to have an ON UPDATE statement after alter but got:\n%s",
			createStatement,
		)
	}
}
