// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestParallelCheckEnumVersionMismatch is a deterministic reproduction of a
// rare nightly CI failure where parallel constraint checks produce a "comparison
// of two different versions of enum" assertion error.
//
// The root cause: when parallel checks run, the worker goroutine gets a fresh
// descs.Collection which may resolve a different version of the
// crdb_internal_region type descriptor (due to advancing lease timestamps). The
// buffer DEnum values (from the INSERT's source scan) carry version V, while
// the lookup join's DEnum values (hydrated via the fresh Collection) carry
// version V', and the DEnum.Compare method asserts V == V'.
//
// This test uses ForceTypeVersionMismatchInParallelChecks to deterministically
// bump the type version on the worker goroutine's join reader rightTypes,
// simulating the version skew without relying on timing.
func TestParallelCheckEnumVersionMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	knobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			ForceTypeVersionMismatchInParallelChecks: true,
		},
	}

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, knobs,
	)
	defer cleanup()

	r := sqlutils.MakeSQLRunner(sqlDB)

	// Set up the multiregion database and tables matching the original
	// failing test case (regional_by_row_foreign_key).
	r.Exec(t, `CREATE DATABASE db PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3"`)
	r.Exec(t, `USE db`)
	r.Exec(t, `SET experimental_enable_unique_without_index_constraints = true`)

	// Ensure parallel checks are enabled.
	r.Exec(t, `SET CLUSTER SETTING sql.distsql.parallelize_checks.enabled = true`)

	// Create the source table with an explicit region column.
	r.Exec(t, `
		CREATE TABLE parent_default (
			k INT PRIMARY KEY,
			v INT,
			region crdb_internal_region,
			UNIQUE WITHOUT INDEX (k, region)
		)
	`)

	// Create the target REGIONAL BY ROW table with two UNIQUE constraints
	// to trigger parallel checks (one UNIQUE index + one UNIQUE WITHOUT INDEX).
	r.Exec(t, `
		CREATE TABLE parent_unique_nullable (
			x INT NOT NULL,
			y INT,
			UNIQUE (x, y),
			UNIQUE WITHOUT INDEX (x, y, crdb_region),
			FAMILY (x, y, crdb_region)
		) LOCALITY REGIONAL BY ROW
	`)

	// Insert source data with various regions.
	r.Exec(t, `
		INSERT INTO parent_default (k, v, region) VALUES
			(1, 10, 'us-east1'),
			(2, 20, 'us-east2'),
			(3, 30, 'us-east3')
	`)

	// Verify the INSERT plan generates constraint checks. We need at least
	// 2 check plans to trigger parallel execution.
	rows := r.QueryStr(t, `EXPLAIN INSERT INTO parent_unique_nullable (x, y, crdb_region) (SELECT k, v, region FROM parent_default)`)
	var hasConstraintCheck int
	for _, row := range rows {
		for _, col := range row {
			if col == "constraint-check" {
				hasConstraintCheck++
			}
		}
	}
	t.Logf("found %d constraint checks in plan", hasConstraintCheck)
	for _, row := range rows {
		t.Logf("EXPLAIN: %s", row)
	}

	// This INSERT triggers two constraint checks in parallel:
	// 1. Cross-region primary key uniqueness via parent_unique_nullable_pkey
	// 2. Cross-region UNIQUE (x, y) uniqueness via parent_unique_nullable_x_y_key
	//    (this also covers the UNIQUE WITHOUT INDEX (x, y, crdb_region) constraint)
	//
	// Both checks use lookup joins whose ON predicate compares the buffer's
	// region column against the lookup's crdb_region column. With the testing
	// knob, the worker goroutine's join reader bumps the type version on its
	// rightTypes in-place, causing DEnum.Compare to fail with "comparison of
	// two different versions of enum".
	_, err := sqlDB.Exec(`
		INSERT INTO parent_unique_nullable (x, y, crdb_region)
		(SELECT k, v, region FROM parent_default)
	`)
	if err != nil {
		t.Logf("INSERT returned error (expected): %v", err)
		require.Contains(t, err.Error(), "comparison of two different versions of enum")
	} else {
		t.Fatal("INSERT succeeded but expected 'comparison of two different versions of enum' error")
	}
}
