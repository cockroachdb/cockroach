// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestLoadCSV(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE d`)

	tests := []struct {
		name   string
		create string
		with   string
		csv    string
		err    string
		query  map[string][][]string
	}{
		{
			name: "duplicate unique index key",
			create: `
				a int primary key,
				i int,
				unique index idx_f (i)
			`,
			csv: `1,1
2,2
3,3
4,3
5,4`,
			err: "duplicate key",
		},
		{
			name: "duplicate PK",
			create: `
				i int primary key
			`,
			csv: `1
2
3
3
4`,
			err: "duplicate key",
		},
		{
			name: "duplicate collated string key",
			create: `
				s string collate en_u_ks_level1 primary key
			`,
			csv: `a
B
c
D
d
`,
			err: "duplicate key",
		},
		{
			name: "duplicate PK at sst boundary",
			create: `
				i int primary key,
				s string
			`,
			with: `WITH sstsize = '10B'`,
			csv: `1,0000000000
1,0000000000`,
			err: "duplicate key",
		},
		{
			name: "verify no splits mid row",
			create: `
				i int primary key,
				s string,
				b int,
				c int,
				index (s),
				index (i, s),
				family (i, b),
				family (s, c)
			`,
			with: `WITH sstsize = '1B'`,
			csv:  `5,STRING,7,9`,
			query: map[string][][]string{
				`SELECT count(*) from d.t`: {{"1"}},
			},
		},
	}

	var csvString string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, csvString)
		}
	}))
	defer srv.Close()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB.Exec(t, `DROP TABLE IF EXISTS d.t`)
			q := fmt.Sprintf(`IMPORT TABLE d.t (%s) CSV DATA ($1) %s`, tc.create, tc.with)
			t.Log(q)
			csvString = tc.csv
			_, err := db.Exec(q, srv.URL)
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("unexpected: %v", err)
			}
			for query, res := range tc.query {
				sqlDB.CheckQueryResults(t, query, res)
			}
		})
	}
}

// TODO(dt): switch to a helper in sampledataccl.
func makeCSVData(
	t testing.TB, in string, numFiles, rowsPerFile int,
) (files []string, filesWithOpts []string, filesWithDups []string) {
	if err := os.Mkdir(filepath.Join(in, "csv"), 0777); err != nil {
		t.Fatal(err)
	}
	for fn := 0; fn < numFiles; fn++ {
		path := filepath.Join("csv", fmt.Sprintf("data-%d", fn))
		f, err := os.Create(filepath.Join(in, path))
		if err != nil {
			t.Fatal(err)
		}
		pathWithOpts := filepath.Join("csv", fmt.Sprintf("data-%d-opts", fn))
		fWithOpts, err := os.Create(filepath.Join(in, pathWithOpts))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprint(fWithOpts, "This is a header line to be skipped\n"); err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprint(fWithOpts, "So is this\n"); err != nil {
			t.Fatal(err)
		}
		pathDup := filepath.Join("csv", fmt.Sprintf("data-%d-dup", fn))
		fDup, err := os.Create(filepath.Join(in, pathDup))
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
		files = append(files, fmt.Sprintf(`'nodelocal:///%s'`, path))
		filesWithOpts = append(filesWithOpts, fmt.Sprintf(`'nodelocal:///%s'`, pathWithOpts))
		filesWithDups = append(filesWithDups, fmt.Sprintf(`'nodelocal:///%s'`, pathDup))
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
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '10KB'`)

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

	// Get the number of existing jobs.
	baseNumJobs := jobutils.GetSystemJobsCount(t, sqlDB)

	if err := ioutil.WriteFile(filepath.Join(dir, "empty.csv"), nil, 0666); err != nil {
		t.Fatal(err)
	}
	empty := []string{"'nodelocal:///empty.csv'"}
	schema := []interface{}{"nodelocal:///table"}

	files, filesWithOpts, dups := makeCSVData(t, dir, numFiles, rowsPerFile)
	expectedRows := numFiles * rowsPerFile

	// Support subtests by keeping track of the number of jobs that are executed.
	testNum := -1
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
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			files,
			``,
			"",
		},
		{
			"schema-in-file-intodb",
			`IMPORT TABLE csv1.t CREATE USING $1 CSV DATA (%s)`,
			schema,
			files,
			``,
			"",
		},
		{
			"schema-in-query",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s)`,
			nil,
			files,
			``,
			"",
		},
		{
			"schema-in-query-opts",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH delimiter = '|', comment = '#', nullif='', skip = '2'`,
			nil,
			filesWithOpts,
			`WITH comment = '#', delimiter = '|', "nullif" = '', skip = '2'`,
			"",
		},
		{
			// Force some SST splits.
			"schema-in-file-sstsize",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH sstsize = '10K'`,
			schema,
			files,
			`WITH sstsize = '10K'`,
			"",
		},
		{
			"schema-in-query-transform-only",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH delimiter = '|', comment = '#', nullif='', skip = '2', transform = $1`,
			nil,
			filesWithOpts,
			`WITH comment = '#', delimiter = '|', "nullif" = '', skip = '2', transform = 'nodelocal:///5'`,
			"",
		},
		{
			"empty-file",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			empty,
			``,
			"",
		},
		{
			"empty-with-files",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			append(empty, files...),
			``,
			"",
		},
		// NB: successes above, failures below, because we check the i-th job.
		{
			"bad-opt-name",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH foo = 'bar'`,
			nil,
			files,
			``,
			"invalid option \"foo\"",
		},
		{
			"bad-opt-no-arg",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH transform`,
			nil,
			files,
			``,
			"option \"transform\" requires a value",
		},
		{
			"bad-computed-column",
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING AS ('hello') STORED, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH skip = '2', transform = $1`,
			nil,
			filesWithOpts,
			``,
			"computed columns not supported",
		},
		{
			"primary-key-dup",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH transform = $2`,
			schema,
			dups,
			``,
			"primary or unique index has duplicate keys",
		},
		{
			"no-database",
			`IMPORT TABLE nonexistent.t CREATE USING $1 CSV DATA (%s)`,
			schema,
			files,
			``,
			`database does not exist: "nonexistent.t"`,
		},
		{
			"into-db-fails",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH into_db = 'test'`,
			schema,
			files,
			``,
			`invalid option "into_db"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			intodb := fmt.Sprintf(`csv%d`, i)
			sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, intodb))
			sqlDB.Exec(t, fmt.Sprintf(`SET DATABASE = %s`, intodb))

			var unused string
			var restored struct {
				rows, idx, sys, bytes int
			}

			backupPath := fmt.Sprintf("nodelocal:///%d", i)
			hasTransform := strings.Contains(tc.query, "transform = $")
			if hasTransform {
				tc.args = append(tc.args, backupPath)
			}

			var result int
			query := fmt.Sprintf(tc.query, strings.Join(tc.files, ", "))
			testNum++
			if err := sqlDB.DB.QueryRow(query, tc.args...).Scan(
				&unused, &unused, &unused, &restored.rows, &restored.idx, &restored.sys, &restored.bytes,
			); err != nil {
				if !testutils.IsError(err, tc.err) {
					t.Fatalf("%s: %v (%#v)", query, err, tc.args)
				}
				return
			}

			jobPrefix := `IMPORT TABLE `
			if !hasTransform {
				jobPrefix += intodb + ".public."
			} else {
				jobPrefix += `""."".`
			}
			jobPrefix += `t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) `

			if err := jobutils.VerifySystemJob(t, sqlDB, baseNumJobs+testNum, jobs.TypeImport, jobs.Record{
				Username:    security.RootUser,
				Description: fmt.Sprintf(jobPrefix+tc.jobOpts, strings.Join(tc.files, ", ")),
			}); err != nil {
				t.Fatal(err)
			}

			isEmpty := len(tc.files) == 1 && tc.files[0] == empty[0]

			if hasTransform {
				if expected, actual := 0, restored.rows; expected != actual {
					t.Fatalf("expected %d rows, got %d", expected, actual)
				}
				if err := sqlDB.DB.QueryRow(`SELECT count(*) FROM t`).Scan(&unused); !testutils.IsError(
					err, "does not exist",
				) {
					t.Fatal(err)
				}
				testNum++
				if err := sqlDB.DB.QueryRow(
					`RESTORE csv.* FROM $1 WITH into_db = $2`, backupPath, intodb,
				).Scan(
					&unused, &unused, &unused, &restored.rows, &restored.idx, &restored.sys, &restored.bytes,
				); err != nil {
					t.Fatal(err)
				}
				if expected, actual := expectedRows, restored.rows; expected != actual && !isEmpty {
					t.Fatalf("expected %d rows, got %d", expected, actual)
				}
			}

			if isEmpty {
				sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
				if expect := 0; result != expect {
					t.Fatalf("expected %d rows, got %d", expect, result)
				}
				return
			}

			// Verify correct number of rows via COUNT.
			sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
			if expect := expectedRows; result != expect {
				t.Fatalf("expected %d rows, got %d", expect, result)
			}

			// Verify correct number of NULLs via COUNT.
			sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE b IS NULL`).Scan(&result)
			expectedNulls := 0
			if strings.Contains(tc.query, "nullif") {
				expectedNulls = expectedRows / 4
			}
			if result != expectedNulls {
				t.Fatalf("expected %d rows, got %d", expectedNulls, result)
			}

			// Verify sstsize created > 1 SST files.
			if tc.name == "schema-in-file-sstsize-dist" {
				pattern := filepath.Join(dir, fmt.Sprintf("%d", i), "*.sst")
				matches, err := filepath.Glob(pattern)
				if err != nil {
					t.Fatal(err)
				}
				if len(matches) < 2 {
					t.Fatal("expected > 1 SST files")
				}
			}
		})
	}

	// Verify unique_rowid is replaced for tables without primary keys.
	t.Run("unique_rowid", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE pk")
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT TABLE pk.t (a INT, b STRING) CSV DATA (%s)`, strings.Join(files, ", ")))
		// Verify the rowids are being generated as expected.
		sqlDB.CheckQueryResults(t,
			`SELECT count(*), sum(rowid) FROM pk.t`,
			sqlDB.QueryStr(t, `
				SELECT count(*), sum(rowid) FROM
					(SELECT file + (rownum << $3) as rowid FROM
						(SELECT generate_series(0, $1 - 1) file),
						(SELECT generate_series(1, $2) rownum)
					)
			`, numFiles, rowsPerFile, builtins.NodeIDBits),
		)
	})

	// Verify a failed IMPORT won't prevent a second IMPORT.
	t.Run("checkpoint-leftover", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE checkpoint; USE checkpoint")

		// Specify wrong number of columns.
		_, err := conn.Exec(fmt.Sprintf(`IMPORT TABLE t (a INT PRIMARY KEY) CSV DATA (%s)`, files[0]))
		if !testutils.IsError(err, "expected 1 fields, got 2") {
			t.Fatalf("unexpected: %v", err)
		}

		// Specify wrong table name; still shouldn't leave behind a checkpoint file.
		_, err = conn.Exec(fmt.Sprintf(`IMPORT TABLE bad CREATE USING $1 CSV DATA (%s)`, files[0]), schema[0])
		if !testutils.IsError(err, `file specifies a schema for table t`) {
			t.Fatalf("unexpected: %v", err)
		}

		// Expect it to succeed with correct columns.
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT TABLE t (a INT PRIMARY KEY, b STRING) CSV DATA (%s)`, files[0]))

		// A second attempt should fail fast. A "slow fail" is the error message
		// "restoring table desc and namespace entries: table already exists".
		_, err = conn.Exec(fmt.Sprintf(`IMPORT TABLE t (a INT PRIMARY KEY, b STRING) CSV DATA (%s)`, files[0]))
		if !testutils.IsError(err, `relation "t" already exists`) {
			t.Fatalf("unexpected: %v", err)
		}
	})

	// Verify DEFAULT columns and SERIAL are allowed but not evaluated.
	t.Run("allow-default", func(t *testing.T) {
		var data string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()

		sqlDB.Exec(t, `CREATE DATABASE d`)
		sqlDB.Exec(t, `SET DATABASE = d`)

		const (
			query = `IMPORT TABLE t (
			a SERIAL,
			b INT DEFAULT unique_rowid(),
			c STRING DEFAULT 's',
			d SERIAL,
			e INT DEFAULT unique_rowid(),
			f STRING DEFAULT 's',
			PRIMARY KEY (a, b, c)
		) CSV DATA ($1)`
			nullif = ` WITH nullif=''`
		)

		data = ",5,e,,,"
		if _, err := conn.Exec(query, srv.URL); !testutils.IsError(err, `could not parse "" as type int`) {
			t.Fatalf("unexpected: %v", err)
		}
		if _, err := conn.Exec(query+nullif, srv.URL); !testutils.IsError(err, `"a" violates not-null constraint`) {
			t.Fatalf("unexpected: %v", err)
		}
		data = "2,,e,,,"
		if _, err := conn.Exec(query+nullif, srv.URL); !testutils.IsError(err, `"b" violates not-null constraint`) {
			t.Fatalf("unexpected: %v", err)
		}

		data = "2,5,,,,"
		if _, err := conn.Exec(query+nullif, srv.URL); !testutils.IsError(err, `"c" violates not-null constraint`) {
			t.Fatalf("unexpected: %v", err)
		}

		data = "2,5,e,,,"
		sqlDB.Exec(t, query+nullif, srv.URL)
		sqlDB.CheckQueryResults(t,
			`SELECT * FROM t`,
			sqlDB.QueryStr(t, `SELECT 2, 5, 'e', NULL, NULL, NULL`),
		)
	})
}

func BenchmarkImport(b *testing.B) {
	const (
		nodes    = 3
		numFiles = nodes + 2
	)
	dir, cleanup := testutils.TempDir(b)
	defer cleanup()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(b, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	files, _, _ := makeCSVData(b, dir, numFiles, b.N*100)
	tmp := fmt.Sprintf("nodelocal://%s", filepath.Join(dir, b.Name()))

	b.ResetTimer()

	sqlDB.Exec(b,
		fmt.Sprintf(
			`IMPORT TABLE t (a INT PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b))
			CSV DATA (%s) WITH transform = $1`,
			strings.Join(files, ","),
		),
		tmp,
	)
}

func BenchmarkConvertRecord(b *testing.B) {
	ctx := context.TODO()

	tpchLineItemDataRows := [][]string{
		{"1", "155190", "7706", "1", "17", "21168.23", "0.04", "0.02", "N", "O", "1996-03-13", "1996-02-12", "1996-03-22", "DELIVER IN PERSON", "TRUCK", "egular courts above the"},
		{"1", "67310", "7311", "2", "36", "45983.16", "0.09", "0.06", "N", "O", "1996-04-12", "1996-02-28", "1996-04-20", "TAKE BACK RETURN", "MAIL", "ly final dependencies: slyly bold "},
		{"1", "63700", "3701", "3", "8", "13309.60", "0.10", "0.02", "N", "O", "1996-01-29", "1996-03-05", "1996-01-31", "TAKE BACK RETURN", "REG AIR", "riously. regular, express dep"},
		{"1", "2132", "4633", "4", "28", "28955.64", "0.09", "0.06", "N", "O", "1996-04-21", "1996-03-30", "1996-05-16", "NONE", "AIR", "lites. fluffily even de"},
		{"1", "24027", "1534", "5", "24", "22824.48", "0.10", "0.04", "N", "O", "1996-03-30", "1996-03-14", "1996-04-01", "NONE", "FOB", " pending foxes. slyly re"},
		{"1", "15635", "638", "6", "32", "49620.16", "0.07", "0.02", "N", "O", "1996-01-30", "1996-02-07", "1996-02-03", "DELIVER IN PERSON", "MAIL", "arefully slyly ex"},
		{"2", "106170", "1191", "1", "38", "44694.46", "0.00", "0.05", "N", "O", "1997-01-28", "1997-01-14", "1997-02-02", "TAKE BACK RETURN", "RAIL", "ven requests. deposits breach a"},
		{"3", "4297", "1798", "1", "45", "54058.05", "0.06", "0.00", "R", "F", "1994-02-02", "1994-01-04", "1994-02-23", "NONE", "AIR", "ongside of the furiously brave acco"},
		{"3", "19036", "6540", "2", "49", "46796.47", "0.10", "0.00", "R", "F", "1993-11-09", "1993-12-20", "1993-11-24", "TAKE BACK RETURN", "RAIL", " unusual accounts. eve"},
		{"3", "128449", "3474", "3", "27", "39890.88", "0.06", "0.07", "A", "F", "1994-01-16", "1993-11-22", "1994-01-23", "DELIVER IN PERSON", "SHIP", "nal foxes wake."},
	}
	b.SetBytes(120) // Raw input size. With 8 indexes, expect more on output side.

	stmt, err := parser.ParseOne(`CREATE TABLE lineitem (
		l_orderkey      INTEGER NOT NULL,
		l_partkey       INTEGER NOT NULL,
		l_suppkey       INTEGER NOT NULL,
		l_linenumber    INTEGER NOT NULL,
		l_quantity      DECIMAL(15,2) NOT NULL,
		l_extendedprice DECIMAL(15,2) NOT NULL,
		l_discount      DECIMAL(15,2) NOT NULL,
		l_tax           DECIMAL(15,2) NOT NULL,
		l_returnflag    CHAR(1) NOT NULL,
		l_linestatus    CHAR(1) NOT NULL,
		l_shipdate      DATE NOT NULL,
		l_commitdate    DATE NOT NULL,
		l_receiptdate   DATE NOT NULL,
		l_shipinstruct  CHAR(25) NOT NULL,
		l_shipmode      CHAR(10) NOT NULL,
		l_comment       VARCHAR(44) NOT NULL,
		PRIMARY KEY     (l_orderkey, l_linenumber),
		INDEX l_ok      (l_orderkey ASC),
		INDEX l_pk      (l_partkey ASC),
		INDEX l_sk      (l_suppkey ASC),
		INDEX l_sd      (l_shipdate ASC),
		INDEX l_cd      (l_commitdate ASC),
		INDEX l_rd      (l_receiptdate ASC),
		INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
		INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC)
	)`)
	if err != nil {
		b.Fatal(err)
	}
	create := stmt.(*tree.CreateTable)
	st := cluster.MakeTestingClusterSettings()

	tableDesc, err := MakeSimpleTableDescriptor(ctx, st, create, sqlbase.ID(100), sqlbase.ID(100), 1)
	if err != nil {
		b.Fatal(err)
	}
	recordCh := make(chan csvRecord)
	kvCh := make(chan []roachpb.KeyValue)
	group := errgroup.Group{}

	// no-op drain kvs channel.
	go func() {
		for range kvCh {
		}
	}()

	// start up workers.
	for i := 0; i < runtime.NumCPU(); i++ {
		group.Go(func() error {
			return convertRecord(ctx, recordCh, kvCh, nil, tableDesc)
		})
	}
	const batchSize = 500

	batch := csvRecord{
		file:      "some/path/to/some/file/of/csv/data.tbl",
		rowOffset: 1,
		r:         make([][]string, 0, batchSize),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(batch.r) > batchSize {
			recordCh <- batch
			batch.r = make([][]string, 0, batchSize)
			batch.rowOffset = i
		}

		batch.r = append(batch.r, tpchLineItemDataRows[i%len(tpchLineItemDataRows)])
	}
	recordCh <- batch
	close(recordCh)

	if err := group.Wait(); err != nil {
		b.Fatal(err)
	}
	close(kvCh)
}

// TestImportControlJob tests that PAUSE JOB, RESUME JOB, and CANCEL JOB
// work as intended on import jobs.
func TestImportControlJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	var serverArgs base.TestServerArgs
	// Disable external processing of mutations so that the final check of
	// crdb_internal.tables is guaranteed to not be cleaned up. Although this
	// was never observed by a stress test, it is here for safety.
	serverArgs.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: func() error {
			return errors.New("async schema changer disabled")
		},
	}

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE data`)

	t.Run("cancel", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE cancelimport`)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				<-allowResponse
				_, _ = w.Write([]byte(r.URL.Path[1:]))
			}
		}))
		defer srv.Close()

		var urls []string
		for i := 0; i < 10; i++ {
			urls = append(urls, fmt.Sprintf("'%s/%d'", srv.URL, i))
		}
		csvURLs := strings.Join(urls, ", ")

		query := fmt.Sprintf(`IMPORT TABLE cancelimport.t (i INT PRIMARY KEY) CSV DATA (%s)`, csvURLs)

		if _, err := jobutils.RunJob(t, sqlDB, &allowResponse, "cancel", query); !testutils.IsError(err, "job canceled") {
			t.Fatalf("expected 'job canceled' error, but got %+v", err)
		}
		// Check that executing again succeeds. This won't work if the first import
		// was not successfully canceled.
		sqlDB.Exec(t, query)
	})

	t.Run("pause", func(t *testing.T) {
		// Test that IMPORT can be paused and resumed. This test also attempts to
		// only pause the job after it has begun splitting ranges. When the job
		// is resumed, if the sampling phase is re-run, the splits points will
		// differ. When AddSSTable attempts to import the new ranges, they will
		// fail because there is an existing split in the key space that it cannot
		// handle. Use a sstsize that will more-or-less (since it is statistical)
		// always cause this condition.

		sqlDB.Exec(t, `CREATE DATABASE pauseimport`)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(r.URL.Path[1:]))
			}
		}))
		defer srv.Close()

		count := 100
		// This test takes a while with the race detector, so reduce the number of
		// files in an attempt to speed it up.
		if util.RaceEnabled {
			count = 20
		}

		urls := make([]string, count)
		for i := 0; i < count; i++ {
			urls[i] = fmt.Sprintf("'%s/%d'", srv.URL, i)
		}
		csvURLs := strings.Join(urls, ", ")
		query := fmt.Sprintf(`IMPORT TABLE pauseimport.t (i INT PRIMARY KEY) CSV DATA (%s) WITH sstsize = '50B'`, csvURLs)

		jobID, err := jobutils.RunJob(t, sqlDB, &allowResponse, "PAUSE", query)
		if !testutils.IsError(err, "job paused") {
			t.Fatalf("unexpected: %v", err)
		}
		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, jobID))
		if err := jobutils.WaitForJob(sqlDB.DB, jobID); err != nil {
			t.Fatal(err)
		}
		sqlDB.CheckQueryResults(t,
			`SELECT * FROM pauseimport.t ORDER BY i`,
			sqlDB.QueryStr(t, `SELECT * FROM generate_series(0, $1)`, count-1),
		)
	})
}

// TestImportLivenessWithRestart tests that a node liveness transition
// during IMPORT correctly resumes after the node executing the job
// becomes non-live (from the perspective of the jobs registry).
//
// Its actual purpose is to address the second bug listed in #22924 about
// the addsstable arguments not in request range. The theory was that the
// restart in that issue was caused by node liveness and that the work
// already performed (the splits and addsstables) somehow caused the second
// error. However this does not appear to be the case, as running many stress
// iterations with differing constants (rows, sstsize, kv.import.batch_size)
// was not able to fail in the way listed by the second bug.
func TestImportLivenessWithRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond
	jobs.DefaultCancelInterval = 100 * time.Millisecond

	const nodes = 1
	nl := jobs.NewFakeNodeLiveness(nodes)
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			RegistryLiveness: nl,
		},
	}

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	// Prevent hung HTTP connections in leaktest.
	sqlDB.Exec(t, `SET CLUSTER SETTING cloudstorage.timeout = '3s'`)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '300B'`)
	sqlDB.Exec(t, `CREATE DATABASE liveness`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const rows = 5000
		if r.Method == "GET" {
			for i := 0; i < rows; i++ {
				fmt.Fprintln(w, i)
			}
		}
	}))
	defer srv.Close()

	const query = `IMPORT TABLE liveness.t (i INT PRIMARY KEY) CSV DATA ($1) WITH sstsize = '500B'`

	// Start an IMPORT and wait until it's done one addsstable.
	allowResponse = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := sqlDB.DB.Exec(query, srv.URL)
		errCh <- err
	}()
	// Allow many, but not all, addsstables to complete.
	for i := 0; i < 50; i++ {
		select {
		case allowResponse <- struct{}{}:
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	// Fetch the new job ID and lease since we know it's running now.
	var jobID int64
	originalLease := &jobs.Payload{}
	{
		var expectedLeaseBytes []byte
		sqlDB.QueryRow(
			t, `SELECT id, payload FROM system.jobs ORDER BY created DESC LIMIT 1`,
		).Scan(&jobID, &expectedLeaseBytes)
		if err := protoutil.Unmarshal(expectedLeaseBytes, originalLease); err != nil {
			t.Fatal(err)
		}
	}

	// addsstable is done, make the node non-live and wait for cancellation
	nl.FakeSetExpiration(1, hlc.MinTimestamp)
	// Wait for the registry cancel loop to run and cancel the job.
	<-nl.SelfCalledCh
	<-nl.SelfCalledCh
	close(allowResponse)
	err := <-errCh
	if !testutils.IsError(err, "job .*: node liveness error") {
		t.Fatalf("unexpected: %v", err)
	}
	// Make the node live again
	nl.FakeSetExpiration(1, hlc.MaxTimestamp)
	// The registry should now adopt the job and resume it.
	if err := jobutils.WaitForJob(sqlDB.DB, jobID); err != nil {
		t.Fatal(err)
	}
	// Verify that the job lease was updated
	rescheduledLease := &jobs.Payload{}
	{
		var actualLeaseBytes []byte
		sqlDB.QueryRow(
			t, `SELECT payload FROM system.jobs WHERE id = $1`, jobID,
		).Scan(&actualLeaseBytes)
		if err := protoutil.Unmarshal(actualLeaseBytes, rescheduledLease); err != nil {
			t.Fatal(err)
		}
	}
	if rescheduledLease.ModifiedMicros <= originalLease.ModifiedMicros {
		t.Fatalf("expecting rescheduled job to have a later modification time: %d vs %d",
			rescheduledLease.StartedMicros, originalLease.StartedMicros)
	}
}

// TestImportLivenessWithLeniency tests that a temporary node liveness
// transition during IMPORT doesn't cancel the job, but allows the
// owning node to continue processing.
func TestImportLivenessWithLeniency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond
	jobs.DefaultCancelInterval = 100 * time.Millisecond

	const nodes = 1
	nl := jobs.NewFakeNodeLiveness(nodes)
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			RegistryLiveness: nl,
		},
	}

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	// Prevent hung HTTP connections in leaktest.
	sqlDB.Exec(t, `SET CLUSTER SETTING cloudstorage.timeout = '3s'`)
	// We want to know exactly how much leniency is configured.
	sqlDB.Exec(t, `SET CLUSTER SETTING jobs.registry.leniency = '1m'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '300B'`)
	sqlDB.Exec(t, `CREATE DATABASE liveness`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const rows = 5000
		if r.Method == "GET" {
			for i := 0; i < rows; i++ {
				fmt.Fprintln(w, i)
			}
		}
	}))
	defer srv.Close()

	const query = `IMPORT TABLE liveness.t (i INT PRIMARY KEY) CSV DATA ($1) WITH sstsize = '500B'`

	// Start an IMPORT and wait until it's done one addsstable.
	allowResponse = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := sqlDB.DB.Exec(query, srv.URL)
		errCh <- err
	}()
	// Allow many, but not all, addsstables to complete.
	for i := 0; i < 50; i++ {
		select {
		case allowResponse <- struct{}{}:
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	// Fetch the new job ID and lease since we know it's running now.
	var jobID int64
	originalLease := &jobs.Payload{}
	{
		var expectedLeaseBytes []byte
		sqlDB.QueryRow(
			t, `SELECT id, payload FROM system.jobs ORDER BY created DESC LIMIT 1`,
		).Scan(&jobID, &expectedLeaseBytes)
		if err := protoutil.Unmarshal(expectedLeaseBytes, originalLease); err != nil {
			t.Fatal(err)
		}
	}

	// addsstable is done, make the node slightly tardy.
	nl.FakeSetExpiration(1, hlc.Timestamp{
		WallTime: hlc.UnixNano() - (15 * time.Second).Nanoseconds(),
	})

	// Wait for the registry cancel loop to run and not cancel the job.
	<-nl.SelfCalledCh
	<-nl.SelfCalledCh
	close(allowResponse)

	// Set the node to be fully live again.  This prevents the registry
	// from canceling all of the jobs if the test node is saturated
	// and the import runs slowly.
	nl.FakeSetExpiration(1, hlc.MaxTimestamp)

	// Verify that the client didn't see anything amiss.
	if err := <-errCh; err != nil {
		t.Fatalf("import job should have completed: %s", err)
	}

	// The job should have completed normally.
	if err := jobutils.WaitForJob(sqlDB.DB, jobID); err != nil {
		t.Fatal(err)
	}
}

// TestImportMVCCChecksums verifies that MVCC checksums are correctly
// computed by issuing a secondary index change that runs a CPut on the
// index. See #23984.
func TestImportMVCCChecksums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE d`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, "1,1,1")
		}
	}))
	defer srv.Close()

	sqlDB.Exec(t, `IMPORT TABLE d.t (
		a INT PRIMARY KEY,
		b INT,
		c INT,
		INDEX (b) STORING (c)
	) CSV DATA ($1)`, srv.URL)
	sqlDB.Exec(t, `UPDATE d.t SET c = 2 WHERE a = 1`)
}
