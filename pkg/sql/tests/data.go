// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/errors"
)

// CheckKeyCount checks that the number of keys in the provided span matches
// numKeys.
func CheckKeyCount(t *testing.T, kvDB *kv.DB, span roachpb.Span, numKeys int) {
	t.Helper()
	if err := CheckKeyCountE(t, kvDB, span, numKeys); err != nil {
		t.Fatal(err)
	}
}

// CheckKeyCountIncludingTombstoned checks that the number of keys (including
// those whose tombstones are marked but not GC'ed yet) in the provided span
// matches the expected number.
func CheckKeyCountIncludingTombstoned(
	t *testing.T, s serverutils.StorageLayerInterface, span roachpb.Span, expectedNum int,
) {
	t.Helper()
	if err := CheckKeyCountIncludingTombstonedE(t, s, span, expectedNum); err != nil {
		t.Fatal(err)
	}
}

// CheckKeyCountE returns an error if the number of keys in the
// provided span does not match numKeys.
func CheckKeyCountE(t *testing.T, kvDB *kv.DB, span roachpb.Span, numKeys int) error {
	t.Helper()
	if kvs, err := kvDB.Scan(context.TODO(), span.Key, span.EndKey, 0); err != nil {
		return err
	} else if l := numKeys; len(kvs) != l {
		return errors.Newf("expected %d key value pairs, but got %d", l, len(kvs))
	}
	return nil
}

func CheckKeyCountIncludingTombstonedE(
	t *testing.T, s serverutils.StorageLayerInterface, tableSpan roachpb.Span, expectedNum int,
) error {
	// Check key count including tombstoned ones.
	engines := s.Engines()
	if len(engines) != 1 {
		return errors.Errorf("expecting 1 engine from the test server, but found %d", len(engines))
	}

	keyCount := 0
	it, err := engines[0].NewMVCCIterator(
		context.Background(),
		storage.MVCCKeyIterKind,
		storage.IterOptions{
			LowerBound: tableSpan.Key,
			UpperBound: tableSpan.EndKey,
		},
	)
	if err != nil {
		return err
	}

	for it.SeekGE(storage.MVCCKey{Key: tableSpan.Key}); ; it.NextKey() {
		ok, err := it.Valid()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		keyCount++
	}
	it.Close()
	if keyCount != expectedNum {
		return errors.Errorf("expecting %d keys, but found %d", expectedNum, keyCount)
	}
	return nil
}

// CreateKVTable creates a basic table named t.<name> that stores key/value
// pairs with numRows of arbitrary data.
func CreateKVTable(sqlDB *gosql.DB, name string, numRows int) error {
	// Fix the column families so the key counts don't change if the family
	// heuristics are updated.
	schemaStmts := []string{
		`CREATE DATABASE IF NOT EXISTS t;`,
		fmt.Sprintf(`CREATE TABLE t.%s (k INT PRIMARY KEY, v INT, FAMILY (k), FAMILY (v));`, name),
		fmt.Sprintf(`CREATE INDEX foo on t.%s (v);`, name),
	}

	for _, stmt := range schemaStmts {
		if _, err := sqlDB.Exec(stmt); err != nil {
			return err
		}
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
