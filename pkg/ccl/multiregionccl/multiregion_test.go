// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMultiRegionNoLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ccl.TestingDisableEnterprise()()

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
			require.NoError(t, err)
		})
	}
}

func TestMultiRegionAfterEnterpriseDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ccl.TestingEnableEnterprise()()

	skip.UnderRace(t, "times out under race")

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

	defer ccl.TestingDisableEnterprise()()

	// Test certain commands are still supported with enterprise disabled
	t.Run("no new multi-region items", func(t *testing.T) {
		for _, tc := range []struct {
			stmt string
		}{
			{
				stmt: `CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2"`,
			},
			{
				stmt: `ALTER DATABASE db ADD REGION "us-east3"`,
			},
		} {
			t.Run(tc.stmt, func(t *testing.T) {
				_, err := sqlDB.Exec(tc.stmt)
				require.NoError(t, err)
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
	defer ccl.TestingEnableEnterprise()()

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

	defer ccl.TestingDisableEnterprise()()

	// Can set global_reads with enterprise license disabled.
	_, err = sqlDB.Exec(`ALTER TABLE t1 CONFIGURE ZONE USING global_reads = true`)
	require.NoError(t, err)

	// Can unset global_reads with enterprise license disabled.
	_, err = sqlDB.Exec(`ALTER TABLE t2 CONFIGURE ZONE USING global_reads = false`)
	require.NoError(t, err)
}
