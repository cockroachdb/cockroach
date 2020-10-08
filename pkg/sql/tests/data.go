// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// CheckKeyCount checks that the number of keys in the provided span matches
// numKeys.
func CheckKeyCount(t *testing.T, kvDB *kv.DB, span roachpb.Span, numKeys int) {
	t.Helper()
	if kvs, err := kvDB.Scan(context.TODO(), span.Key, span.EndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := numKeys; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}
}

// CreateKVTable creates a basic table named t.<name> that stores key/value
// pairs with numRows of arbitrary data.
func CreateKVTable(sqlDB *gosql.DB, name string, numRows int) error {
	// Fix the column families so the key counts don't change if the family
	// heuristics are updated.
	schema := fmt.Sprintf(`
		CREATE DATABASE IF NOT EXISTS t;
		CREATE TABLE t.%s (k INT PRIMARY KEY, v INT, FAMILY (k), FAMILY (v));
		CREATE INDEX foo on t.%s (v);`, name, name)

	if _, err := sqlDB.Exec(schema); err != nil {
		return err
	}

	// Bulk insert.
	var insert bytes.Buffer
	if _, err := insert.WriteString(
		fmt.Sprintf(`INSERT INTO t.%s VALUES (%d, %d)`, name, 0, numRows-1)); err != nil {
		return err
	}
	for i := 1; i < numRows; i++ {
		if _, err := insert.WriteString(fmt.Sprintf(` ,(%d, %d)`, i, numRows-i)); err != nil {
			return err
		}
	}
	_, err := sqlDB.Exec(insert.String())
	return err
}

// CreateKVInterleavedTable is like CreateKVTable, but it interleaves table
// t.intlv inside of t.kv and adds rows to both.
func CreateKVInterleavedTable(t *testing.T, sqlDB *gosql.DB, numRows int) {
	// Fix the column families so the key counts don't change if the family
	// heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
SET DATABASE=t;
CREATE TABLE kv (k INT PRIMARY KEY, v INT);
CREATE TABLE intlv (k INT, m INT, n INT, PRIMARY KEY (k, m)) INTERLEAVE IN PARENT kv (k);
CREATE INDEX intlv_idx ON intlv (k, n) INTERLEAVE IN PARENT kv (k);
`); err != nil {
		t.Fatal(err)
	}

	var insert bytes.Buffer
	if _, err := insert.WriteString(fmt.Sprintf(`INSERT INTO t.kv VALUES (%d, %d)`, 0, numRows-1)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < numRows; i++ {
		if _, err := insert.WriteString(fmt.Sprintf(` ,(%d, %d)`, i, numRows-i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sqlDB.Exec(insert.String()); err != nil {
		t.Fatal(err)
	}
	insert.Reset()
	if _, err := insert.WriteString(fmt.Sprintf(`INSERT INTO t.intlv VALUES (%d, %d, %d)`, 0, numRows-1, numRows-1)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < numRows; i++ {
		if _, err := insert.WriteString(fmt.Sprintf(` ,(%d, %d, %d)`, i, numRows-i, numRows-i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sqlDB.Exec(insert.String()); err != nil {
		t.Fatal(err)
	}
}
