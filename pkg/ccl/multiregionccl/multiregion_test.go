// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

const multiRegionNoEnterpriseContains = "use of multi-region features requires an enterprise license"

func TestMultiRegionNoLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingDisableEnterprise()()

	_, sqlDB, cleanup := createTestMultiRegionCluster(t, 3, base.TestingKnobs{})
	defer cleanup()

	_, err := sqlDB.Exec(`CREATE DATABASE test`)
	require.NoError(t, err)

	for _, errorStmt := range []string{
		`CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2"`,
		`ALTER DATABASE test PRIMARY REGION "us-east2"`,
	} {
		t.Run(errorStmt, func(t *testing.T) {
			_, err := sqlDB.Exec(errorStmt)
			require.Error(t, err)
			require.Contains(t, err.Error(), multiRegionNoEnterpriseContains)
		})
	}
}

func TestMultiRegionAfterEnterpriseDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	skip.UnderRace(t, "#61163")

	_, sqlDB, cleanup := createTestMultiRegionCluster(t, 3, base.TestingKnobs{})
	defer cleanup()

	_, err := sqlDB.Exec(`
CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2";
USE test;
CREATE TABLE t1 () LOCALITY GLOBAL;
CREATE TABLE t2 () LOCALITY REGIONAL BY TABLE;
CREATE TABLE t3 () LOCALITY REGIONAL BY TABLE IN "us-east2";
CREATE TABLE t4 () LOCALITY REGIONAL BY ROW
	`)
	require.NoError(t, err)

	defer utilccl.TestingDisableEnterprise()()

	// Test certain commands are no longer usable.
	t.Run("no new multi-region items", func(t *testing.T) {
		for _, errorStmt := range []string{
			`CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2"`,
		} {
			t.Run(errorStmt, func(t *testing.T) {
				_, err := sqlDB.Exec(errorStmt)
				require.Error(t, err)
				require.Contains(t, err.Error(), multiRegionNoEnterpriseContains)
			})
		}
	})

	// Test we can still drop multi-region functionality.
	t.Run("drop multi-region", func(t *testing.T) {
		for _, tc := range []struct {
			stmt string
		}{
			{stmt: `ALTER DATABASE test PRIMARY REGION "us-east2"`},
			{stmt: `ALTER TABLE t2 SET LOCALITY REGIONAL BY TABLE`},
			{stmt: `ALTER TABLE t3 SET LOCALITY REGIONAL BY TABLE`},
			{stmt: `ALTER TABLE t4 SET LOCALITY REGIONAL BY TABLE`},
			{stmt: `ALTER TABLE t4 DROP COLUMN crdb_region`},
			{stmt: `ALTER DATABASE test DROP REGION "us-east1"`},
			{stmt: `ALTER DATABASE test DROP REGION "us-east2"`},
		} {
			t.Run(tc.stmt, func(t *testing.T) {
				_, err := sqlDB.Exec(tc.stmt)
				require.NoError(t, err)
			})
		}
	})
}
