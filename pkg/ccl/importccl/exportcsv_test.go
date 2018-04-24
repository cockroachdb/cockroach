// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
)

func setupExportableBank(t *testing.T, nodes, rows int) (*sqlutils.SQLRunner, func()) {
	ctx := context.Background()
	dir, cleanupDir := testutils.TempDir(t)

	tc := testcluster.StartTestCluster(t, nodes,
		base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir, UseDatabase: "test"}},
	)
	db := sqlutils.MakeSQLRunner(tc.Conns[0])
	db.Exec(t, "CREATE DATABASE test")

	wk := bank.FromRows(rows)
	if _, err := workload.Setup(ctx, db.DB, wk, 100, 3); err != nil {
		t.Fatal(err)
	}

	config.TestingSetupZoneConfigHook(tc.Stopper())
	v, err := tc.Servers[0].DB().Get(context.TODO(), keys.DescIDGenerator)
	if err != nil {
		t.Fatal(err)
	}
	last := uint32(v.ValueInt())
	config.TestingSetZoneConfig(last+1, config.ZoneConfig{RangeMaxBytes: 5000})
	if err := bank.Split(db.DB, wk); err != nil {
		t.Fatal(err)
	}
	db.Exec(t, "ALTER TABLE bank SCATTER")
	db.Exec(t, "SELECT 'force a scan to repopulate range cache' FROM [SELECT COUNT(*) FROM bank]")

	return db, func() {
		tc.Stopper().Stop(ctx)
		cleanupDir()
	}
}

func TestExportImportBank(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, cleanup := setupExportableBank(t, 3, 100)
	defer cleanup()

	chunkSize := 13
	var files []string
	for _, row := range db.QueryStr(t,
		`EXPORT INTO CSV 'nodelocal:///t' WITH chunk_rows = $1, delimiter = '|' FROM TABLE bank`, chunkSize,
	) {
		files = append(files, row[0])
	}

	schema := bank.FromRows(1).Tables()[0].Schema
	fileList := "'nodelocal:///t/" + strings.Join(files, "', 'nodelocal:///t/") + "'"
	db.Exec(t, fmt.Sprintf(`IMPORT TABLE bank2 %s CSV DATA (%s) WITH delimiter = '|'`, schema, fileList))

	db.CheckQueryResults(t,
		`SELECT * FROM bank ORDER BY id`, db.QueryStr(t, `SELECT * FROM bank2 ORDER BY id`),
	)
	db.CheckQueryResults(t,
		`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank`, db.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank2`),
	)
}

func TestMultiNodeExportStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	nodes := 5
	exportRows := 100
	db, cleanup := setupExportableBank(t, nodes, exportRows*2)
	defer cleanup()

	chunkSize := 13
	rows := db.Query(t,
		`EXPORT INTO CSV 'nodelocal:///t' WITH chunk_rows = $3 FROM SELECT * FROM bank WHERE id >= $1 and id < $2`,
		10, 10+exportRows, chunkSize,
	)

	files, totalRows, totalBytes := 0, 0, 0
	nodesSeen := make(map[string]bool)
	for rows.Next() {
		filename, count, bytes := "", 0, 0
		if err := rows.Scan(&filename, &count, &bytes); err != nil {
			t.Fatal(err)
		}
		files++
		if count > chunkSize {
			t.Fatalf("expected no chunk larger than %d, got %d", chunkSize, count)
		}
		totalRows += count
		totalBytes += bytes
		nodesSeen[strings.SplitN(filename, ".", 2)[0]] = true
	}
	if totalRows != exportRows {
		t.Fatalf("Expected %d rows, got %d", exportRows, totalRows)
	}
	if expected := exportRows / chunkSize; files < expected {
		t.Fatalf("expected at least %d files, got %d", expected, files)
	}
	if len(nodesSeen) < 2 {
		t.Fatalf("expected files from %d nodes, got %d: %v", 2, len(nodesSeen), nodesSeen)
	}
}

func TestExportNonTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{ServerArgs: base.TestServerArgs{UseDatabase: "test"}},
	)
	defer tc.Stopper().Stop(context.Background())
	db := sqlutils.MakeSQLRunner(tc.Conns[0])
	db.Exec(t, "CREATE DATABASE test")

	if _, err := db.DB.Exec(
		`EXPORT INTO CSV 'nodelocal:///series' WITH chunk_rows = '10' FROM SELECT generate_series(1, 100)`,
	); !testutils.IsError(err, "unsupported EXPORT query") {
		t.Fatal(err)
	}
}
