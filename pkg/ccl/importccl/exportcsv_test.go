// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl_test

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func setupExportableBank(t *testing.T, nodes, rows int) (*sqlutils.SQLRunner, string, func()) {
	ctx := context.Background()
	dir, cleanupDir := testutils.TempDir(t)

	tc := testcluster.StartTestCluster(t, nodes,
		base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir, UseDatabase: "test"}},
	)
	conn := tc.Conns[0]
	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, "CREATE DATABASE test")

	wk := bank.FromRows(rows)
	l := workloadsql.InsertsDataLoader{BatchSize: 100, Concurrency: 3}
	if _, err := workloadsql.Setup(ctx, conn, wk, l); err != nil {
		t.Fatal(err)
	}

	config.TestingSetupZoneConfigHook(tc.Stopper())
	v, err := tc.Servers[0].DB().Get(context.Background(), keys.SystemSQLCodec.DescIDSequenceKey())
	if err != nil {
		t.Fatal(err)
	}
	last := config.SystemTenantObjectID(v.ValueInt())
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.RangeMaxBytes = proto.Int64(5000)
	config.TestingSetZoneConfig(last+1, zoneConfig)
	db.Exec(t, "ALTER TABLE bank SCATTER")
	db.Exec(t, "SELECT 'force a scan to repopulate range cache' FROM [SELECT count(*) FROM bank]")

	return db, dir, func() {
		tc.Stopper().Stop(ctx)
		cleanupDir()
	}
}

func TestExportImportBank(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, dir, cleanup := setupExportableBank(t, 3, 100)
	defer cleanup()

	// Add some unicode to prove FmtExport works as advertised.
	db.Exec(t, "UPDATE bank SET payload = payload || '✅' WHERE id = 5")
	db.Exec(t, "UPDATE bank SET payload = NULL WHERE id % 2 = 0")

	chunkSize := 13
	for _, null := range []string{"", "NULL"} {
		nullAs, nullIf := "", ", nullif = ''"
		if null != "" {
			nullAs = fmt.Sprintf(", nullas = '%s'", null)
			nullIf = fmt.Sprintf(", nullif = '%s'", null)
		}
		t.Run("null="+null, func(t *testing.T) {
			var files []string

			var asOf string
			db.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&asOf)

			for _, row := range db.QueryStr(t,
				fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://0/t'
					WITH chunk_rows = $1, delimiter = '|' %s
					FROM SELECT * FROM bank AS OF SYSTEM TIME %s`, nullAs, asOf), chunkSize,
			) {
				files = append(files, row[0])
				f, err := ioutil.ReadFile(filepath.Join(dir, "t", row[0]))
				if err != nil {
					t.Fatal(err)
				}
				t.Log(string(f))
			}

			schema := bank.FromRows(1).Tables()[0].Schema
			fileList := "'nodelocal://0/t/" + strings.Join(files, "', 'nodelocal://0/t/") + "'"
			db.Exec(t, fmt.Sprintf(`IMPORT TABLE bank2 %s CSV DATA (%s) WITH delimiter = '|'%s`, schema, fileList, nullIf))

			db.CheckQueryResults(t,
				fmt.Sprintf(`SELECT * FROM bank AS OF SYSTEM TIME %s ORDER BY id`, asOf), db.QueryStr(t, `SELECT * FROM bank2 ORDER BY id`),
			)
			db.CheckQueryResults(t,
				`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank2`, db.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank`),
			)
			db.Exec(t, "DROP TABLE bank2")
		})
	}
}

func TestMultiNodeExportStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	nodes := 5
	exportRows := 100
	db, _, cleanup := setupExportableBank(t, nodes, exportRows*2)
	defer cleanup()

	maxTries := 10
	// we might need to retry if our table didn't actually scatter enough.
	for tries := 0; tries < maxTries; tries++ {
		chunkSize := 13
		rows := db.Query(t,
			`EXPORT INTO CSV 'nodelocal://0/t' WITH chunk_rows = $3 FROM SELECT * FROM bank WHERE id >= $1 and id < $2`,
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
			// table isn't as scattered as we expected, but we can try again.
			if tries < maxTries {
				continue
			}
			t.Fatalf("expected files from %d nodes, got %d: %v", 2, len(nodesSeen), nodesSeen)
		}
		break
	}
}

func TestExportJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t AS VALUES (1, 2)`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/join' FROM SELECT * FROM t, t as u`)
}

func TestExportOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/order' from select * from foo order by y asc limit 2`)
	content, err := ioutil.ReadFile(filepath.Join(dir, "order", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if expected, got := "3,32,1,34\n2,22,2,24\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestExportUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)
	// Set up some initial state for the tests.
	sqlDB.Exec(t, `
SET experimental_enable_enums = true;
CREATE TYPE greeting AS ENUM ('hello', 'hi');
CREATE TABLE greeting_table (x greeting, y greeting);
INSERT INTO greeting_table VALUES ('hello', 'hello'), ('hi', 'hi');
`)
	tests := []struct {
		stmt     string
		expected string
	}{
		{
			stmt:     "EXPORT INTO CSV 'nodelocal://0/test/' FROM (SELECT 'hello':::greeting, 'hi':::greeting)",
			expected: "hello,hi\n",
		},
		{
			stmt:     "EXPORT INTO CSV 'nodelocal://0/test/' FROM TABLE greeting_table",
			expected: "hello,hello\nhi,hi\n",
		},
		{
			stmt:     "EXPORT INTO CSV 'nodelocal://0/test/' FROM (SELECT x, y, enum_first(x) FROM greeting_table)",
			expected: "hello,hello,hello\nhi,hi,hello\n",
		},
	}
	for _, test := range tests {
		sqlDB.Exec(t, test.stmt)
		// Read the dumped file.
		contents, err := ioutil.ReadFile(filepath.Join(baseDir, "test", "n1.0.csv"))
		require.NoError(t, err)
		require.Equal(t, test.expected, string(contents))
	}
}

func TestExportOrderCompressed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	var close = func(c io.Closer) {
		if err := c.Close(); err != nil {
			t.Fatalf("failed to close stream, got error %s", err)
		}
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/order' with compression = gzip from select * from foo order by y asc limit 2`)
	fi, err := os.Open(filepath.Join(dir, "order", "n1.0.csv.gz"))
	defer close(fi)

	if err != nil {
		t.Fatal(err)
	}

	gzipReader, err := gzip.NewReader(fi)
	defer close(gzipReader)

	if err != nil {
		t.Fatal(err)
	}

	content, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		t.Fatal(err)
	}

	if expected, got := "3,32,1,34\n2,22,2,24\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestExportShow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/show' FROM SELECT * FROM [SHOW DATABASES] ORDER BY database_name`)
	content, err := ioutil.ReadFile(filepath.Join(dir, "show", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if expected, got := "defaultdb\npostgres\nsystem\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

// TestExportVectorized makes sure that SupportsVectorized check doesn't panic
// on CSVWriter processor.
func TestExportVectorized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t(a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `SET vectorize_row_count_threshold=0`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'http://0.1:37957/exp_1' FROM TABLE t`)
}
