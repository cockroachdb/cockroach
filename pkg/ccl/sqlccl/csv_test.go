// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	if err := ioutil.WriteFile(tablePath, []byte(tableCreate), 0666); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(dataPath, []byte(tableCSV), 0666); err != nil {
		t.Fatal(err)
	}

	null := ""
	if _, _, _, err := LoadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, &null, testSSTMaxSize); err != nil {
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

	_, _, _, err := LoadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, testSSTMaxSize)
	if !testutils.IsError(err, "duplicate key") {
		t.Fatalf("unexpected error: %v", err)
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

	_, _, _, err := LoadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, testSSTMaxSize)
	if !testutils.IsError(err, "duplicate key") {
		t.Fatalf("unexpected error: %v", err)
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

	_, _, _, err := LoadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, sstMaxSize)
	if !testutils.IsError(err, "duplicate key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestLoadCSVOptions tests LoadCSV with the comma, comment, and nullif
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
	csv, kv, sst, err := LoadCSV(ctx, tablePath, []string{dataPath}, tmp, '|' /* comma */, '#' /* comment */, &null /* nullif */, 500)
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

func TestSampleRate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		numRows = 10000
		keySize = 100
		valSize = 50
	)

	tests := []struct {
		sampleSize float64
		expected   int
	}{
		{0, numRows},
		{100, numRows},
		{1000, 1448},
		{10000, 126},
		{100000, 13},
		{1000000, 1},
	}
	kv := roachpb.KeyValue{
		Key:   bytes.Repeat([]byte("0"), keySize),
		Value: roachpb.Value{RawBytes: bytes.Repeat([]byte("0"), valSize)},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.sampleSize), func(t *testing.T) {
			sr := sampleRate{
				rnd:        rand.New(rand.NewSource(0)),
				sampleSize: tc.sampleSize,
			}

			var sampled int
			for i := 0; i < numRows; i++ {
				if sr.sample(kv) {
					sampled++
				}
			}
			if sampled != tc.expected {
				t.Fatalf("got %d, expected %d", sampled, tc.expected)
			}
		})
	}
}

func TestImportStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const nodes = 3
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(t, tc.Conns[0])

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
	csvPath := filepath.Join(dir, "csv")
	if err := os.Mkdir(csvPath, 0777); err != nil {
		t.Fatal(err)
	}
	var files []string
	const (
		numFiles    = 5
		rowsPerFile = 100000
	)
	for fn := 0; fn < numFiles; fn++ {
		path := filepath.Join(csvPath, fmt.Sprintf("data-%d", fn))
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < rowsPerFile; i++ {
			x := fn*rowsPerFile + i
			if _, err := fmt.Fprintf(f, "%d,%c\n", x, 'A'+x%26); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		files = append(files, fmt.Sprintf(`'nodelocal://%s'`, path))
	}

	for _, tc := range []struct {
		name  string
		query string        // must have one `%s` for the files list.
		args  []interface{} // will have backupPath appended
		err   string
	}{
		{
			"schema-in-file",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH temp = $2`,
			[]interface{}{fmt.Sprintf("nodelocal://%s", tablePath)},
			"",
		},
		{
			"schema-in-query",
			`IMPORT TABLE t (a int primary key, b string, index (b), index (a, b)) CSV DATA (%s) WITH temp = $1`,
			nil,
			"",
		},
		{
			"schema-in-file-dist",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH temp = $2, distributed`,
			[]interface{}{fmt.Sprintf("nodelocal://%s", tablePath)},
			"",
		},
		{
			"schema-in-query-dist",
			`IMPORT TABLE t (a int primary key, b string, index (b), index (a, b)) CSV DATA (%s) WITH temp = $1, distributed`,
			nil,
			"",
		},
		{
			"missing-temp",
			`IMPORT TABLE t (a int primary key, b string, index (b), index (a, b)) CSV DATA (%s)`,
			nil,
			"must provide a temporary storage location",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.query, "temp = $") {
				tc.args = append(tc.args, fmt.Sprintf("nodelocal://%s", filepath.Join(dir, t.Name())))
			}

			var result int
			err := sqlDB.DB.QueryRow(
				fmt.Sprintf(tc.query, strings.Join(files, ", ")), tc.args...,
			).Scan(&result)

			if err != nil {
				if !testutils.IsError(err, tc.err) {
					t.Fatal(err)
				}
			} else {
				if expected := 1; result < expected {
					t.Errorf("expected >= %d, got %d", expected, result)
				}
			}
		})
	}
}
