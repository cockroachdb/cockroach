// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	gosql "database/sql"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

// loadTPCHDataset loads a TPC-H dataset for the specific benchmark spec on the
// provided roachNodes. The function is idempotent and first checks whether a
// compatible dataset exists (compatible is defined as a tpch dataset with a
// scale factor at least as large as the provided scale factor), performing an
// expensive dataset restore only if it doesn't.
func loadTPCHDataset(
	ctx context.Context, t *test, c *cluster, sf int, m *monitor, roachNodes nodeListOption,
) error {
	db := c.Conn(ctx, roachNodes[0])
	defer db.Close()

	if _, err := db.ExecContext(ctx, `USE tpch`); err == nil {
		t.l.Printf("found existing tpch dataset, verifying scale factor\n")

		var supplierCardinality int
		if err := db.QueryRowContext(
			ctx, `SELECT count(*) FROM tpch.supplier`,
		).Scan(&supplierCardinality); err != nil {
			if pqErr := (*pq.Error)(nil); !(errors.As(err, &pqErr) && pgcode.MakeCode(string(pqErr.Code)) == pgcode.UndefinedTable) {
				return err
			}
			// Table does not exist. Set cardinality to 0.
			supplierCardinality = 0
		}

		// Check if a tpch database with the required scale factor exists.
		// 10000 is the number of rows in the supplier table at scale factor 1.
		// supplier is the smallest table whose cardinality scales with the scale
		// factor.
		expectedSupplierCardinality := 10000 * sf
		if supplierCardinality >= expectedSupplierCardinality {
			t.l.Printf("dataset is at least of scale factor %d, continuing", sf)
			return nil
		}

		// If the scale factor was smaller than the required scale factor, wipe the
		// cluster and restore.
		m.ExpectDeaths(int32(c.spec.NodeCount))
		c.Wipe(ctx, roachNodes)
		c.Start(ctx, t, roachNodes)
		m.ResetDeaths()
	} else if pqErr := (*pq.Error)(nil); !(errors.As(err, &pqErr) &&
		pgcode.MakeCode(string(pqErr.Code)) == pgcode.InvalidCatalogName) {
		return err
	}

	t.l.Printf("restoring tpch scale factor %d\n", sf)
	tpchURL := fmt.Sprintf("gs://cockroach-fixtures/workload/tpch/scalefactor=%d/backup", sf)
	query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS tpch; RESTORE tpch.* FROM '%s' WITH into_db = 'tpch';`, tpchURL)
	_, err := db.ExecContext(ctx, query)
	return err
}

// scatterTables runs "ALTER TABLE ... SCATTER" statement for every table in
// tableNames. It assumes that conn is already using the target database. If an
// error is encountered, the test is failed.
func scatterTables(t *test, conn *gosql.DB, tableNames []string) {
	t.Status("scattering the data")
	for _, table := range tableNames {
		scatter := fmt.Sprintf("ALTER TABLE %s SCATTER;", table)
		if _, err := conn.Exec(scatter); err != nil {
			t.Fatal(err)
		}
	}
}

// disableAutoStats disables automatic collection of statistics on the cluster.
func disableAutoStats(t *test, conn *gosql.DB) {
	t.Status("disabling automatic collection of stats")
	if _, err := conn.Exec(
		`SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false;`,
	); err != nil {
		t.Fatal(err)
	}
}

// createStatsFromTables runs "CREATE STATISTICS" statement for every table in
// tableNames. It assumes that conn is already using the target database. If an
// error is encountered, the test is failed.
func createStatsFromTables(t *test, conn *gosql.DB, tableNames []string) {
	t.Status("collecting stats")
	for _, tableName := range tableNames {
		t.Status(fmt.Sprintf("creating statistics from table %q", tableName))
		if _, err := conn.Exec(
			fmt.Sprintf(`CREATE STATISTICS %s FROM %s;`, tableName, tableName),
		); err != nil {
			t.Fatal(err)
		}
	}
}

// disableVectorizeRowCountThresholdHeuristic sets
// 'vectorize_row_count_threshold' cluster setting to zero so that the test
// would use the vectorized engine with 'vectorize=on' regardless of the
// fact whether the stats are present or not (if we don't set it, then when
// the stats are not present, we fallback to row-by-row engine even with
// `vectorize=on` set).
func disableVectorizeRowCountThresholdHeuristic(t *test, conn *gosql.DB) {
	if _, err := conn.Exec("SET CLUSTER SETTING sql.defaults.vectorize_row_count_threshold=0"); err != nil {
		t.Fatal(err)
	}
}
