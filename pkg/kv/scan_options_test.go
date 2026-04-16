// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDB_ScanWithTargetBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	b := db.NewTxn(ctx, "setup").NewBatch()
	b.Put("aa", "1")
	b.Put("ab", strings.Repeat("x", 1024))
	b.Put("ac", "3")
	b.Put("ad", "4")
	if err := db.Run(ctx, b); err != nil {
		t.Fatal(err)
	}

	// Without target bytes: return all rows.
	rows, err := db.Scan(ctx, "a", "b", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows without byte limit, got %d", len(rows))
	}

	// With a small target bytes limit: should return fewer rows than the full
	// set. The limit is best-effort; at least one row is always returned.
	rows, err = db.Scan(ctx, "a", "b", 0, kv.WithTargetBytes(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("expected at least one row with target bytes")
	}
	if len(rows) >= 4 {
		t.Fatalf("expected fewer than 4 rows with a 1-byte limit, got %d", len(rows))
	}
}

func TestDB_ReverseScanWithTargetBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	b := db.NewTxn(ctx, "setup").NewBatch()
	b.Put("aa", "1")
	b.Put("ab", strings.Repeat("x", 1024))
	b.Put("ac", "3")
	b.Put("ad", "4")
	if err := db.Run(ctx, b); err != nil {
		t.Fatal(err)
	}

	rows, err := db.ReverseScan(ctx, "a", "b", 0, kv.WithTargetBytes(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("expected at least one row with target bytes")
	}
	if len(rows) >= 4 {
		t.Fatalf("expected fewer than 4 rows with a 1-byte limit, got %d", len(rows))
	}
	// Reverse scan should start from the end.
	if string(rows[0].Key) != "ad" {
		t.Fatalf("expected first reverse-scan row key 'ad', got %q", rows[0].Key)
	}
}

func TestTxn_ScanWithTargetBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	b := db.NewTxn(ctx, "setup").NewBatch()
	b.Put("aa", "1")
	b.Put("ab", strings.Repeat("x", 1024))
	b.Put("ac", "3")
	b.Put("ad", "4")
	if err := db.Run(ctx, b); err != nil {
		t.Fatal(err)
	}

	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		rows, err := txn.Scan(ctx, "a", "b", 0)
		if err != nil {
			return err
		}
		if len(rows) != 4 {
			t.Fatalf("expected 4 rows without byte limit, got %d", len(rows))
		}

		rows, err = txn.Scan(ctx, "a", "b", 0, kv.WithTargetBytes(1))
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			t.Fatal("expected at least one row with target bytes")
		}
		if len(rows) >= 4 {
			t.Fatalf("expected fewer than 4 rows with a 1-byte limit, got %d", len(rows))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestScanWithTargetBytesAndMaxRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	b := db.NewTxn(ctx, "setup").NewBatch()
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("ac", "3")
	b.Put("ad", "4")
	if err := db.Run(ctx, b); err != nil {
		t.Fatal(err)
	}

	// Both maxRows and targetBytes set. maxRows=2 should cap the result
	// regardless of a generous byte limit.
	rows, err := db.Scan(ctx, "a", "b", 2, kv.WithTargetBytes(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (capped by maxRows), got %d", len(rows))
	}
}
