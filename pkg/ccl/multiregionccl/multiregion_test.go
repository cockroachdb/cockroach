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
	// Blank import ccl so that we have all CCL features enabled.
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

const multiRegionNoEnterpriseContains = "use of multi-region features requires an enterprise license"

func TestMultiRegionNoLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingDisableEnterprise()()

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{},
	)
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

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{},
	)
	defer cleanup()

	for _, setupQuery := range []string{
		`CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2"`,
		`USE test`,
		`CREATE TABLE t1 () LOCALITY GLOBAL`,
		`CREATE TABLE t2 () LOCALITY REGIONAL BY TABLE`,
		`CREATE TABLE t3 () LOCALITY REGIONAL BY TABLE IN "us-east2"`,
		`CREATE TABLE t4 () LOCALITY REGIONAL BY ROW`,
	} {
		t.Run(setupQuery, func(t *testing.T) {
			_, err := sqlDB.Exec(setupQuery)
			require.NoError(t, err)
		})
	}

	defer utilccl.TestingDisableEnterprise()()

	// Test certain commands are no longer usable.
	t.Run("no new multi-region items", func(t *testing.T) {
		for _, tc := range []struct {
			stmt             string
			expectedContains string
		}{
			{
				stmt:             `CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2"`,
				expectedContains: multiRegionNoEnterpriseContains,
			},
			{
				stmt:             `ALTER DATABASE test ADD REGION "us-east3"`,
				expectedContains: "use of ADD REGION requires an enterprise license",
			},
		} {
			t.Run(tc.stmt, func(t *testing.T) {
				_, err := sqlDB.Exec(tc.stmt)
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedContains)
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

func TestGlobalReadsAfterEnterpriseDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 1 /* numServers */, base.TestingKnobs{},
	)
	defer cleanup()

	for _, setupQuery := range []string{
		`CREATE DATABASE test`,
		`USE test`,
		`CREATE TABLE t1 ()`,
		`CREATE TABLE t2 ()`,
	} {
		_, err := sqlDB.Exec(setupQuery)
		require.NoError(t, err)
	}

	// Can set global_reads with enterprise license enabled.
	_, err := sqlDB.Exec(`ALTER TABLE t1 CONFIGURE ZONE USING global_reads = true`)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`ALTER TABLE t2 CONFIGURE ZONE USING global_reads = true`)
	require.NoError(t, err)

	// Can unset global_reads with enterprise license enabled.
	_, err = sqlDB.Exec(`ALTER TABLE t1 CONFIGURE ZONE USING global_reads = false`)
	require.NoError(t, err)

	defer utilccl.TestingDisableEnterprise()()

	// Cannot set global_reads with enterprise license disabled.
	_, err = sqlDB.Exec(`ALTER TABLE t1 CONFIGURE ZONE USING global_reads = true`)
	require.Error(t, err)
	require.Regexp(t, "use of global_reads requires an enterprise license", err)

	// Can unset global_reads with enterprise license disabled.
	_, err = sqlDB.Exec(`ALTER TABLE t2 CONFIGURE ZONE USING global_reads = false`)
	require.NoError(t, err)
}
