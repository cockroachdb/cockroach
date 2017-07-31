// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
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

func TestLoadStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const nodes = 3
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(t, tc.Conns[0])

	var startKey, endKey, sha512 []byte
	var path gosql.NullString
	var result int
	sqlDB.QueryRow(`LOAD`).Scan(&startKey, &endKey, &path, &sha512, &result)

	// TODO(dan): This entire method is a placeholder to get the distsql
	// plumbing worked out. It currently returns a single row, with the sum of
	// the node ids (1+2+3=6) in the final column.
	if expected := 6; result != expected {
		t.Errorf("expected %d got %d", expected, result)
	}
}
