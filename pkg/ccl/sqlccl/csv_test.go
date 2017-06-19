// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

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
	if _, _, _, err := loadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, &null, false /* overwrite */); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("SET CLUSTER SETTING enterprise.enabled = true"); err != nil {
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

	_, _, _, err := loadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, false /* overwrite */)
	if !testutils.IsError(err, "duplicate values") {
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

	_, _, _, err := loadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, false /* overwrite */)
	if !testutils.IsError(err, "duplicate values") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestLoadCSVOverwrite tests canOverwrite.
func TestLoadCSVOverwrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmp, tmpCleanup := testutils.TempDir(t)
	defer tmpCleanup()
	ctx := context.Background()

	const (
		tableName   = "t"
		csvName     = tableName + ".dat"
		tableCreate = `
			CREATE TABLE ` + tableName + ` (
				i int
			)
		`
		tableCSV = `1
2
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
	if _, _, _, err := loadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, false /* overwrite */); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := loadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, false /* overwrite */); !testutils.IsError(err, "file exists") {
		t.Fatalf("unexpected error %v", err)
	}
	if _, _, _, err := loadCSV(ctx, tablePath, []string{dataPath}, tmp, 0 /* comma */, 0 /* comment */, nil /* nullif */, true /* overwrite */); err != nil {
		t.Fatal(err)
	}
}

// TestLoadCSVOptions tests loadCSV with the comma, comment, and nullif
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
	csv, kv, sst, err := loadCSV(ctx, tablePath, []string{dataPath}, tmp, '|' /* comma */, '#' /* comment */, &null /* nullif */, false /* overwrite */)
	if err != nil {
		t.Fatal(err)
	}
	if csv != 11 {
		t.Fatalf("read %d rows, expected %d", csv, 11)
	}
	if kv != 22 {
		t.Fatalf("created %d KVs, expected %d", kv, 22)
	}
	if sst != 1 {
		t.Fatalf("created %d SSTs, expected %d", sst, 1)
	}
}
