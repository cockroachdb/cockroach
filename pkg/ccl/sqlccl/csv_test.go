// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const testSSTMaxSize = 1024 * 1024 * 50

func TestLoadCSV(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	tmp, tmpCleanup := testutils.TempDir(t)
	defer tmpCleanup()

	const (
		tableName   = "t"
		csvName     = tableName + ".dat"
		tableCreate = `
			CREATE TABLE ` + tableName + ` (
				i int,
				s string,
				f float,
				d decimal,
				primary key (d, s),
				index idx_f (f)
			)
		`
		tableCSV = `1,1,1,1
2,two,2.0,2.00
1234,a string,12.34e56,123456.78e90
0,0,,0
`
	)

	tablePath := filepath.Join(tmp, tableName)
	dataPath := filepath.Join(tmp, csvName)

	// Make tablePath and dataPath relative to ensure relative directories work.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	tablePath, err = filepath.Rel(cwd, tablePath)
	if err != nil {
		t.Fatal(err)
	}
	dataPath, err = filepath.Rel(cwd, dataPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := ioutil.WriteFile(tablePath, []byte(tableCreate), 0666); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(dataPath, []byte(tableCSV), 0666); err != nil {
		t.Fatal(err)
	}

	null := ""
	if _, _, _, err := sqlccl.LoadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, &null, testSSTMaxSize, tmp); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("CREATE DATABASE csv"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`RESTORE csv.* FROM $1`, fmt.Sprintf("nodelocal://%s", tmp)); err != nil {
		t.Fatal(err)
	}

	// Test the primary key.
	var i int
	if err := db.QueryRow("SELECT count(*) FROM csv. " + tableName + "@primary WHERE f = 2").Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != 1 {
		t.Fatalf("expected 1 row, got %v", i)
	}
	// Test the secondary index.
	if err := db.QueryRow("SELECT count(*) FROM csv. " + tableName + "@idx_f WHERE f = 2").Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != 1 {
		t.Fatalf("expected 1 row, got %v", i)
	}
	// Test the NULL was created correctly in row 4.
	if err := db.QueryRow("SELECT count(f) FROM csv. " + tableName).Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != 3 {
		t.Fatalf("expected 3, got %v", i)
	}
}

func TestLoadCSVUniqueDuplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmp, tmpCleanup := testutils.TempDir(t)
	defer tmpCleanup()
	ctx := context.Background()

	const (
		tableName   = "t"
		csvName     = tableName + ".dat"
		tableCreate = `
			CREATE TABLE ` + tableName + ` (
				i int,
				unique index idx_f (i)
			)
		`
		tableCSV = `1
2
3
3
4
`
	)

	tablePath := filepath.Join(tmp, tableName)
	dataPath := filepath.Join(tmp, csvName)

	if err := ioutil.WriteFile(tablePath, []byte(tableCreate), 0666); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(dataPath, []byte(tableCSV), 0666); err != nil {
		t.Fatal(err)
	}

	_, _, _, err := sqlccl.LoadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, testSSTMaxSize, tmp)
	if !testutils.IsError(err, "duplicate key") {
		t.Fatalf("unexpected error: %+v", err)
	}
}

func TestLoadCSVPrimaryDuplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmp, tmpCleanup := testutils.TempDir(t)
	defer tmpCleanup()
	ctx := context.Background()

	const (
		tableName   = "t"
		csvName     = tableName + ".dat"
		tableCreate = `
			CREATE TABLE ` + tableName + ` (
				i int primary key
			)
		`
		tableCSV = `1
2
3
3
4
`
	)

	tablePath := filepath.Join(tmp, tableName)
	dataPath := filepath.Join(tmp, csvName)

	if err := ioutil.WriteFile(tablePath, []byte(tableCreate), 0666); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(dataPath, []byte(tableCSV), 0666); err != nil {
		t.Fatal(err)
	}

	_, _, _, err := sqlccl.LoadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, testSSTMaxSize, tmp)
	if !testutils.IsError(err, "duplicate key") {
		t.Fatalf("unexpected error: %+v", err)
	}
}

// TestLoadCSVPrimaryDuplicateSSTBoundary tests that duplicate keys at
// SST boundaries are detected.
func TestLoadCSVPrimaryDuplicateSSTBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmp, tmpCleanup := testutils.TempDir(t)
	defer tmpCleanup()
	ctx := context.Background()

	const (
		tableName   = "t"
		csvName     = tableName + ".dat"
		tableCreate = `
			CREATE TABLE ` + tableName + ` (
				i int primary key,
				s string
			)
		`
	)

	const sstMaxSize = 10

	tablePath := filepath.Join(tmp, tableName)
	dataPath := filepath.Join(tmp, csvName)

	s := strings.Repeat("0", sstMaxSize)
	tableCSV := fmt.Sprintf("1,%s\n1,%s\n", s, s)

	if err := ioutil.WriteFile(tablePath, []byte(tableCreate), 0666); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(dataPath, []byte(tableCSV), 0666); err != nil {
		t.Fatal(err)
	}

	_, _, _, err := sqlccl.LoadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, sstMaxSize, tmp)
	if !testutils.IsError(err, "duplicate key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestLoadCSVOptions tests sqlccl.LoadCSV with the delimiter, comment, and nullif
// options set.
func TestLoadCSVOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmp, tmpCleanup := testutils.TempDir(t)
	defer tmpCleanup()
	ctx := context.Background()

	const (
		tableName   = "t"
		csvName     = tableName + ".dat"
		tableCreate = `
			CREATE TABLE ` + tableName + ` (
				i int,
				s string,
				index (s)
			)
		`
		tableCSV = `1|2
# second value should be null
2|N
# delimiter at EOL is allowed
3|blah|
4|"quoted "" line"
5|"quoted "" line
"
6|"quoted "" line
"|
7|"quoted "" line
# with comment
"
8|lazy " quotes|
9|"lazy "quotes"|
N|N
10|"|"|
`
	)

	tablePath := filepath.Join(tmp, tableName)
	dataPath := filepath.Join(tmp, csvName)

	if err := ioutil.WriteFile(tablePath, []byte(tableCreate), 0666); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(dataPath, []byte(tableCSV), 0666); err != nil {
		t.Fatal(err)
	}
	null := "N"
	csv, kv, sst, err := sqlccl.LoadCSV(ctx, tablePath, []string{dataPath}, tmp, '|' /* comma */, '#' /* comment */, &null /* nullif */, 500, tmp)
	if err != nil {
		t.Fatal(err)
	}
	if csv != 11 {
		t.Fatalf("read %d rows, expected %d", csv, 11)
	}
	if kv != 22 {
		t.Fatalf("created %d KVs, expected %d", kv, 22)
	}
	if sst != 2 {
		t.Fatalf("created %d SSTs, expected %d", sst, 2)
	}
}

// TestLoadCSVSplit ensures that a split cannot happen in the middle of a row.
func TestLoadCSVSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmp, tmpCleanup := testutils.TempDir(t)
	defer tmpCleanup()
	ctx := context.Background()

	const (
		tableName   = "t"
		csvName     = tableName + ".dat"
		tableCreate = `
			CREATE TABLE ` + tableName + ` (
				i int primary key,
				s string,
				b int,
				c int,
				index (s),
				index (i, s),
				family (i, b),
				family (s, c)
			)
		`
		tableCSV = `5,STRING,7,9`
	)

	tablePath := filepath.Join(tmp, tableName)
	dataPath := filepath.Join(tmp, csvName)

	if err := ioutil.WriteFile(tablePath, []byte(tableCreate), 0666); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(dataPath, []byte(tableCSV), 0666); err != nil {
		t.Fatal(err)
	}
	// sstMaxSize = 1 should put each index (could be more than one KV due to
	// column families) in its own SST.
	csv, kv, sst, err := sqlccl.LoadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, 1 /* sstMaxSize */, tmp)
	if err != nil {
		t.Fatal(err)
	}
	// Only a single input row.
	if csv != 1 {
		t.Fatalf("read %d rows, expected %d", csv, 1)
	}
	// Should produce 4 kvs: 2 families on PK + 2 indexes.
	if kv != 4 {
		t.Fatalf("created %d KVs, expected %d", kv, 4)
	}
	// But only 3 SSTs because the first 2 KVs should be in same SST.
	if sst != 3 {
		t.Fatalf("created %d SSTs, expected %d", sst, 3)
	}
}

// TODO(dt): switch to a helper in sampledataccl.
func makeCSVData(
	t testing.TB, in string, numFiles, rowsPerFile int,
) (files []string, filesWithOpts []string, filesWithDups []string) {
	csvPath := filepath.Join(in, "csv")
	if err := os.Mkdir(csvPath, 0777); err != nil {
		t.Fatal(err)
	}
	for fn := 0; fn < numFiles; fn++ {
		path := filepath.Join(csvPath, fmt.Sprintf("data-%d", fn))
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		pathWithOpts := filepath.Join(csvPath, fmt.Sprintf("data-%d-opts", fn))
		fWithOpts, err := os.Create(pathWithOpts)
		if err != nil {
			t.Fatal(err)
		}
		pathDup := filepath.Join(csvPath, fmt.Sprintf("data-%d-dup", fn))
		fDup, err := os.Create(pathDup)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < rowsPerFile; i++ {
			x := fn*rowsPerFile + i
			if _, err := fmt.Fprintf(f, "%d,%c\n", x, 'A'+x%26); err != nil {
				t.Fatal(err)
			}
			if _, err := fmt.Fprintf(fDup, "1,%c\n", 'A'+x%26); err != nil {
				t.Fatal(err)
			}

			// Write a comment.
			if _, err := fmt.Fprintf(fWithOpts, "# %d\n", x); err != nil {
				t.Fatal(err)
			}
			// Write a pipe-delim line with trailing delim.
			if x%4 == 0 { // 1/4 of rows have blank val for b
				if _, err := fmt.Fprintf(fWithOpts, "%d||\n", x); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := fmt.Fprintf(fWithOpts, "%d|%c|\n", x, 'A'+x%26); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if err := fWithOpts.Close(); err != nil {
			t.Fatal(err)
		}
		files = append(files, fmt.Sprintf(`'nodelocal://%s'`, path))
		filesWithOpts = append(filesWithOpts, fmt.Sprintf(`'nodelocal://%s'`, pathWithOpts))
		filesWithDups = append(filesWithDups, fmt.Sprintf(`'nodelocal://%s'`, pathDup))
	}
	return files, filesWithOpts, filesWithDups
}

func TestImportStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes       = 3
		numFiles    = nodes + 2
		rowsPerFile = 1000
	)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]

	if _, err := conn.Exec(`SET CLUSTER SETTING experimental.importcsv.enabled = true`); err != nil {
		t.Fatal(err)
	}

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tablePath := filepath.Join(dir, "table")
	if err := ioutil.WriteFile(tablePath, []byte(`
		CREATE TABLE t (
			a int primary key,
			b string,
			index (b),
			index (a, b)
		)
	`), 0666); err != nil {
		t.Fatal(err)
	}

	files, filesWithOpts, dups := makeCSVData(t, dir, numFiles, rowsPerFile)
	expectedRows := numFiles * rowsPerFile

	for i, tc := range []struct {
		name    string
		query   string        // must have one `%s` for the files list.
		args    []interface{} // will have backupPath appended
		files   []string
		jobOpts string
		err     string
	}{
		{
			"schema-in-file",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH temp = $2`,
			[]interface{}{fmt.Sprintf("nodelocal://%s", tablePath)},
			files,
			`WITH temp = %s, transform_only`,
			"",
		},
		{
			"schema-in-query",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH temp = $1`,
			nil,
			files,
			`WITH temp = %s, transform_only`,
			"",
		},
		{
			"schema-in-query-opts",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH temp = $1, delimiter = '|', comment = '#', nullif=''`,
			nil,
			filesWithOpts,
			`WITH comment = '#', delimiter = '|', "nullif" = '', temp = %s, transform_only`,
			"",
		},
		{
			"schema-in-file-dist",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH temp = $2, distributed`,
			[]interface{}{fmt.Sprintf("nodelocal://%s", tablePath)},
			files,
			`WITH distributed, temp = %s, transform_only`,
			"",
		},
		{
			"schema-in-query-dist",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH temp = $1, distributed, transform_only`,
			nil,
			files,
			`WITH distributed, temp = %s, transform_only`,
			"",
		},
		{
			"schema-in-query-dist-opts",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH temp = $1, distributed, delimiter = '|', comment = '#', nullif=''`,
			nil,
			filesWithOpts,
			`WITH comment = '#', delimiter = '|', distributed, "nullif" = '', temp = %s, transform_only`,
			"",
		},
		{
			"schema-in-query-transform-only",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH temp = $1, distributed, delimiter = '|', comment = '#', nullif='', transform_only`,
			nil,
			filesWithOpts,
			`WITH comment = '#', delimiter = '|', distributed, "nullif" = '', temp = %s, transform_only`,
			"",
		},
		// NB: successes above, failures below, because we check the i-th job.
		{
			"missing-temp",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s)`,
			nil,
			files,
			``,
			"must provide a temporary storage location",
		},
		{
			"bad-opt-name",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH foo = 'bar'`,
			nil,
			files,
			``,
			"invalid option \"foo\"",
		},
		{
			"bad-opt-arg",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH distributed = 'bar'`,
			nil,
			files,
			``,
			"option \"distributed\" does not take a value",
		},
		{
			"bad-opt-no-arg",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH temp`,
			nil,
			files,
			``,
			"option \"temp\" requires a value",
		},
		{
			"primary-key-dup",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH temp = $2, distributed`,
			[]interface{}{fmt.Sprintf("nodelocal://%s", tablePath)},
			dups,
			``,
			"primary or unique index has duplicate keys",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(t, conn)
			sqlDB.Exec(fmt.Sprintf(`CREATE DATABASE csv%d`, i))
			sqlDB.Exec(fmt.Sprintf(`SET DATABASE = csv%d`, i))

			var unused string
			var restored struct {
				rows, idx, sys, bytes int
			}

			backupPath := fmt.Sprintf("nodelocal://%s", filepath.Join(dir, t.Name()))
			if strings.Contains(tc.query, "temp = $") {
				tc.args = append(tc.args, backupPath)
			}

			var result int
			if err := sqlDB.DB.QueryRow(
				fmt.Sprintf(tc.query, strings.Join(tc.files, ", ")), tc.args...,
			).Scan(
				&unused, &unused, &unused, &restored.rows, &restored.idx, &restored.sys, &restored.bytes,
			); err != nil {
				if !testutils.IsError(err, tc.err) {
					t.Fatal(err)
				}
				return
			}

			const jobPrefix = `IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) `
			if err := verifySystemJob(sqlDB, i*2, jobs.TypeImport, jobs.Record{
				Username:    security.RootUser,
				Description: fmt.Sprintf(jobPrefix+tc.jobOpts, strings.Join(tc.files, ", "), `'`+backupPath+`'`),
			}); err != nil {
				t.Fatal(err)
			}

			if strings.Contains(tc.query, "transform_only") {
				if expected, actual := 0, restored.rows; expected != actual {
					t.Fatalf("expected %d rows, got %d", expected, actual)
				}
				if err := sqlDB.DB.QueryRow(`SELECT count(*) FROM t`).Scan(&unused); !testutils.IsError(
					err, "does not exist",
				) {
					t.Fatal(err)
				}
				if err := sqlDB.DB.QueryRow(
					`RESTORE csv.* FROM $1 WITH into_db = $2`, backupPath, fmt.Sprintf(`csv%d`, i),
				).Scan(
					&unused, &unused, &unused, &restored.rows, &restored.idx, &restored.sys, &restored.bytes,
				); err != nil {
					t.Fatal(err)
				}
			}

			if expected, actual := expectedRows, restored.rows; expected != actual {
				t.Fatalf("expected %d rows, got %d", expected, actual)
			}

			// Verify correct number of rows via COUNT.
			sqlDB.QueryRow(`SELECT count(*) FROM t`).Scan(&result)
			if expect := expectedRows; result != expect {
				t.Fatalf("expected %d rows, got %d", expect, result)
			}

			// Verify correct number of NULLs via COUNT.
			sqlDB.QueryRow(`SELECT count(*) FROM t WHERE b IS NULL`).Scan(&result)
			expectedNulls := 0
			if strings.Contains(tc.query, "nullif") {
				expectedNulls = expectedRows / 4
			}
			if result != expectedNulls {
				t.Fatalf("expected %d rows, got %d", expectedNulls, result)
			}
		})
	}
}

func BenchmarkImport(b *testing.B) {
	const (
		nodes    = 3
		numFiles = nodes + 2
	)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(b, nodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(b, tc.Conns[0])

	dir, cleanup := testutils.TempDir(b)
	defer cleanup()
	files, _, _ := makeCSVData(b, dir, numFiles, b.N*100)
	tmp := fmt.Sprintf("nodelocal://%s", filepath.Join(dir, b.Name()))

	b.ResetTimer()

	sqlDB.Exec(
		fmt.Sprintf(
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b))
			CSV DATA (%s) WITH temp = $1, distributed, transform_only`,
			strings.Join(files, ","),
		),
		tmp,
	)
}
