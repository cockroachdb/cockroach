// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setup(t testing.TB) (serverutils.TestServerInterface, *kv.DB) {
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	return s, kvDB
}

func checkIntResult(t *testing.T, expected, result int64) {
	if expected != result {
		t.Errorf("expected %d, got %d", expected, result)
	}
}

func checkResult(t *testing.T, expected, result []byte) {
	if !bytes.Equal(expected, result) {
		t.Errorf("expected \"%s\", got \"%s\"", expected, result)
	}
}

func checkResults(t *testing.T, expected map[string][]byte, results []kv.Result) {
	count := 0
	for _, result := range results {
		checkRows(t, expected, result.Rows)
		count++
	}
	checkLen(t, len(expected), count)
}

func checkRows(t *testing.T, expected map[string][]byte, rows []kv.KeyValue) {
	for i, row := range rows {
		if !bytes.Equal(expected[string(row.Key)], row.ValueBytes()) {
			t.Errorf("expected %d: %s=\"%s\", got %s=\"%s\"",
				i,
				row.Key,
				expected[string(row.Key)],
				row.Key,
				row.ValueBytes())
		}
	}
}

func checkKeys(t *testing.T, expected []string, keys []roachpb.Key) {
	for i, key := range keys {
		if !bytes.Equal([]byte(expected[i]), key) {
			t.Errorf("expected %d: %s, got %s",
				i,
				key,
				expected[i])
		}
	}
}

func checkLen(t *testing.T, expected, count int) {
	if expected != count {
		t.Errorf("expected length to be %d, got %d", expected, count)
	}
}

func TestDB_Get(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())

	result, err := db.Get(context.Background(), "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte(""), result.ValueBytes())
}

func TestDB_GetForUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	testutils.RunTrueAndFalse(t, "durability-guaranteed", func(t *testing.T, durabilityGuaranteed bool) {
		var result kv.KeyValue
		var err error
		if durabilityGuaranteed {
			result, err = db.GetForUpdate(ctx, "aa", kvpb.GuaranteedDurability)
		} else {
			result, err = db.GetForUpdate(ctx, "aa", kvpb.BestEffort)
		}
		if err != nil {
			t.Fatal(err)
		}
		checkResult(t, []byte(""), result.ValueBytes())
	})
}

func TestDB_GetForShare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	testutils.RunTrueAndFalse(t, "durability-guaranteed", func(t *testing.T, durabilityGuaranteed bool) {
		var result kv.KeyValue
		var err error
		if durabilityGuaranteed {
			result, err = db.GetForShare(ctx, "aa", kvpb.GuaranteedDurability)
		} else {
			result, err = db.GetForShare(ctx, "aa", kvpb.BestEffort)
		}
		if err != nil {
			t.Fatal(err)
		}
		checkResult(t, []byte(""), result.ValueBytes())
	})
}

func TestDB_Put(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	if err := db.Put(context.Background(), "aa", "1"); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("1"), result.ValueBytes())
}

func TestDB_CPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	if err := db.Put(ctx, "aa", "1"); err != nil {
		t.Fatal(err)
	}
	if err := db.CPut(ctx, "aa", "2", kvclientutils.StrToCPutExistingValue("1")); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("2"), result.ValueBytes())

	if err = db.CPut(ctx, "aa", "3", kvclientutils.StrToCPutExistingValue("1")); err == nil {
		t.Fatal("expected error from conditional put")
	}
	result, err = db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("2"), result.ValueBytes())

	if err = db.CPut(ctx, "bb", "4", kvclientutils.StrToCPutExistingValue("1")); err == nil {
		t.Fatal("expected error from conditional put")
	}
	result, err = db.Get(ctx, "bb")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte(""), result.ValueBytes())

	if err = db.CPut(ctx, "bb", "4", nil); err != nil {
		t.Fatal(err)
	}
	result, err = db.Get(ctx, "bb")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("4"), result.ValueBytes())
}

func TestDB_CPutInline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db := setup(t)
	defer s.Stopper().Stop(ctx)

	if err := db.PutInline(ctx, "aa", "1"); err != nil {
		t.Fatal(err)
	}
	if err := db.CPutInline(ctx, "aa", "2", kvclientutils.StrToCPutExistingValue("1")); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("2"), result.ValueBytes())

	if err = db.CPutInline(ctx, "aa", "3", kvclientutils.StrToCPutExistingValue("1")); err == nil {
		t.Fatal("expected error from conditional put")
	}
	result, err = db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("2"), result.ValueBytes())

	if err = db.CPutInline(ctx, "bb", "4", kvclientutils.StrToCPutExistingValue("1")); err == nil {
		t.Fatal("expected error from conditional put")
	}
	result, err = db.Get(ctx, "bb")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte(""), result.ValueBytes())

	if err = db.CPutInline(ctx, "bb", "4", nil); err != nil {
		t.Fatal(err)
	}
	result, err = db.Get(ctx, "bb")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("4"), result.ValueBytes())

	if err = db.CPutInline(ctx, "aa", nil, kvclientutils.StrToCPutExistingValue("2")); err != nil {
		t.Fatal(err)
	}
	result, err = db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	if result.Value != nil {
		t.Fatalf("expected deleted value, got %x", result.ValueBytes())
	}
}

func TestDB_Inc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	if _, err := db.Inc(ctx, "aa", 100); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkIntResult(t, 100, result.ValueInt())
}

func TestBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())

	b := &kv.Batch{}
	b.Get("aa")
	b.Put("bb", "2")
	if err := db.Run(context.Background(), b); err != nil {
		t.Fatal(err)
	}

	expected := map[string][]byte{
		"aa": []byte(""),
		"bb": []byte("2"),
	}
	checkResults(t, expected, b.Results)

	b2 := &kv.Batch{}
	b2.Put(42, "the answer")
	if err := db.Run(context.Background(), b2); !testutils.IsError(err, "unable to marshal key") {
		t.Fatal("expected marshaling error from running bad put")
	}
	if err := b2.MustPErr(); !testutils.IsPError(err, "unable to marshal key") {
		t.Fatal("expected marshaling error from MustPErr")
	}
}

func TestDB_Scan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())

	b := &kv.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	if err := db.Run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Scan(context.Background(), "a", "b", 100)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{
		"aa": []byte("1"),
		"ab": []byte("2"),
	}

	checkRows(t, expected, rows)
	checkLen(t, len(expected), len(rows))
}

func TestDB_ScanForUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	testutils.RunTrueAndFalse(t, "durability-guaranteed", func(t *testing.T, durabilityGuaranteed bool) {
		b := &kv.Batch{}
		b.Put("aa", "1")
		b.Put("ab", "2")
		b.Put("bb", "3")
		if err := db.Run(context.Background(), b); err != nil {
			t.Fatal(err)
		}
		var rows []kv.KeyValue
		var err error
		if durabilityGuaranteed {
			rows, err = db.ScanForUpdate(ctx, "a", "b", 100, kvpb.GuaranteedDurability)
		} else {
			rows, err = db.ScanForUpdate(ctx, "a", "b", 100, kvpb.BestEffort)
		}
		if err != nil {
			t.Fatal(err)
		}
		expected := map[string][]byte{
			"aa": []byte("1"),
			"ab": []byte("2"),
		}

		checkRows(t, expected, rows)
		checkLen(t, len(expected), len(rows))
	})
}

func TestDB_ScanForShare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	testutils.RunTrueAndFalse(t, "durability-guaranteed", func(t *testing.T, durabilityGuaranteed bool) {
		b := &kv.Batch{}
		b.Put("aa", "1")
		b.Put("ab", "2")
		b.Put("bb", "3")
		if err := db.Run(ctx, b); err != nil {
			t.Fatal(err)
		}
		var rows []kv.KeyValue
		var err error
		if durabilityGuaranteed {
			rows, err = db.ScanForShare(ctx, "a", "b", 100, kvpb.GuaranteedDurability)
		} else {
			rows, err = db.ScanForShare(ctx, "a", "b", 100, kvpb.BestEffort)
		}
		if err != nil {
			t.Fatal(err)
		}
		expected := map[string][]byte{
			"aa": []byte("1"),
			"ab": []byte("2"),
		}

		checkRows(t, expected, rows)
		checkLen(t, len(expected), len(rows))
	})
}

func TestDB_ReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())

	b := &kv.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	if err := db.Run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	rows, err := db.ReverseScan(context.Background(), "ab", "c", 100)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{
		"bb": []byte("3"),
		"ab": []byte("2"),
	}

	checkRows(t, expected, rows)
	checkLen(t, len(expected), len(rows))
}

func TestDB_ReverseScanForUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	testutils.RunTrueAndFalse(t, "durability-guaranteed", func(t *testing.T, durabilityGuaranteed bool) {
		b := &kv.Batch{}
		b.Put("aa", "1")
		b.Put("ab", "2")
		b.Put("bb", "3")
		if err := db.Run(ctx, b); err != nil {
			t.Fatal(err)
		}
		var rows []kv.KeyValue
		var err error

		if durabilityGuaranteed {
			rows, err = db.ReverseScanForUpdate(ctx, "ab", "c", 100, kvpb.GuaranteedDurability)
		} else {
			rows, err = db.ReverseScanForUpdate(ctx, "ab", "c", 100, kvpb.BestEffort)
		}
		if err != nil {
			t.Fatal(err)
		}
		expected := map[string][]byte{
			"bb": []byte("3"),
			"ab": []byte("2"),
		}

		checkRows(t, expected, rows)
		checkLen(t, len(expected), len(rows))
	})
}

func TestDB_ReverseScanForShare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	testutils.RunTrueAndFalse(t, "durability-guaranteed", func(t *testing.T, durabilityGuaranteed bool) {
		b := &kv.Batch{}
		b.Put("aa", "1")
		b.Put("ab", "2")
		b.Put("bb", "3")
		if err := db.Run(ctx, b); err != nil {
			t.Fatal(err)
		}
		var rows []kv.KeyValue
		var err error

		if durabilityGuaranteed {
			rows, err = db.ReverseScanForShare(ctx, "ab", "c", 100, kvpb.GuaranteedDurability)
		} else {
			rows, err = db.ReverseScanForShare(ctx, "ab", "c", 100, kvpb.BestEffort)
		}
		if err != nil {
			t.Fatal(err)
		}
		expected := map[string][]byte{
			"bb": []byte("3"),
			"ab": []byte("2"),
		}

		checkRows(t, expected, rows)
		checkLen(t, len(expected), len(rows))
	})
}

func TestDB_TxnIterate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())

	b := &kv.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	if err := db.Run(context.Background(), b); err != nil {
		t.Fatal(err)
	}

	tc := []struct{ pageSize, numPages int }{
		{1, 2},
		{2, 1},
	}
	var rows []kv.KeyValue = nil
	var p int
	for _, c := range tc {
		if err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			p = 0
			rows = make([]kv.KeyValue, 0)
			return txn.Iterate(context.Background(), "a", "b", c.pageSize,
				func(rs []kv.KeyValue) error {
					p++
					rows = append(rows, rs...)
					return nil
				})
		}); err != nil {
			t.Fatal(err)
		}
		expected := map[string][]byte{
			"aa": []byte("1"),
			"ab": []byte("2"),
		}

		checkRows(t, expected, rows)
		checkLen(t, len(expected), len(rows))
		if p != c.numPages {
			t.Errorf("expected %d pages, got %d", c.numPages, p)
		}
	}
}

func TestDB_Del(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())

	b := &kv.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("ac", "3")
	if err := db.Run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Del(context.Background(), "ab"); err != nil {
		t.Fatal(err)
	}
	// Also try deleting a non-existent key and verify that no key is
	// returned.
	if deletedKeys, err := db.Del(context.Background(), "ad"); err != nil {
		t.Fatal(err)
	} else if len(deletedKeys) > 0 {
		t.Errorf("expected deleted key to be empty when deleting a non-existent key, got %v", deletedKeys)
	}
	rows, err := db.Scan(context.Background(), "a", "b", 100)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{
		"aa": []byte("1"),
		"ac": []byte("3"),
	}
	checkRows(t, expected, rows)
	checkLen(t, len(expected), len(rows))
}

func TestDB_DelRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())

	b := &kv.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("ac", "3")
	b.Put("ad", "4")
	if err := db.Run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	deletedKeys, err := db.DelRange(context.Background(), "aa", "ac", true)
	if err != nil {
		t.Fatal(err)
	}

	expectedKeys := []string{"aa", "ab"}
	checkKeys(t, expectedKeys, deletedKeys)
	checkLen(t, len(expectedKeys), len(deletedKeys))

	rows, err := db.Scan(context.Background(), "a", "b", 100)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{
		"ac": []byte("3"),
		"ad": []byte("4"),
	}
	checkRows(t, expected, rows)
	checkLen(t, len(expected), len(rows))

	deletedKeys, err = db.DelRange(context.Background(), "aa", "ad", false)
	if err != nil {
		t.Fatal(err)
	}
	if deletedKeys != nil {
		t.Errorf("expected deletedKeys to be nil when returnKeys set to false, got %v", deletedKeys)
	}

	rows, err = db.Scan(context.Background(), "a", "b", 100)
	if err != nil {
		t.Fatal(err)
	}
	expected = map[string][]byte{
		"ad": []byte("4"),
	}
	checkRows(t, expected, rows)
	checkLen(t, len(expected), len(rows))
}

func TestTxn_Commit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db := setup(t)
	defer s.Stopper().Stop(ctx)

	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		b.Put("aa", "1")
		b.Put("ab", "2")
		return txn.CommitInBatch(ctx, b)
	})
	if err != nil {
		t.Fatal(err)
	}

	b := &kv.Batch{}
	b.Get("aa")
	b.Get("ab")
	if err := db.Run(ctx, b); err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{
		"aa": []byte("1"),
		"ab": []byte("2"),
	}
	checkResults(t, expected, b.Results)
}

func TestTxn_Rollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db := setup(t)
	defer s.Stopper().Stop(ctx)

	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.Put(ctx, "a", "1"); err != nil {
			return err
		}
		return txn.Rollback(ctx)
	})
	require.NoError(t, err)

	// Check that the transaction was rolled back.
	r, err := db.Get(ctx, "a")
	require.NoError(t, err)
	require.False(t, r.Exists())
}

func TestDB_Put_insecure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	if err := db.Put(context.Background(), "aa", "1"); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("1"), result.ValueBytes())
}

func TestDB_QueryResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())

	// Perform a write to ensure that the range sets a non-zero closed timestamp.
	err := db.Put(context.Background(), "a", "val")
	require.NoError(t, err)

	// One node cluster, so "nearest" should not make a difference. Test both.
	testutils.RunTrueAndFalse(t, "nearest", func(t *testing.T, nearest bool) {
		resTS, err := db.QueryResolvedTimestamp(context.Background(), "a", "c", nearest)
		require.NoError(t, err)
		require.NotEmpty(t, resTS)
	})
}

// Test that all operations on a decommissioned node will return a
// permission denied error rather than hanging indefinitely due to
// internal retries.
func TestDBDecommissionedOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
	require.Len(t, scratchRange.InternalReplicas, 1)
	require.Equal(t, tc.Server(0).NodeID(), scratchRange.InternalReplicas[0].NodeID)

	// Decommission server 1.
	srv := tc.Server(0)
	decomSrv := tc.Server(1)
	for _, status := range []livenesspb.MembershipStatus{
		livenesspb.MembershipStatus_DECOMMISSIONING, livenesspb.MembershipStatus_DECOMMISSIONED,
	} {
		require.NoError(t, srv.Decommission(ctx, status, []roachpb.NodeID{decomSrv.NodeID()}))
	}

	// Run a few different operations, which should all eventually error with
	// PermissionDenied. We don't need full coverage of methods, since they use
	// the same sender infrastructure, but should use a few variations to hit
	// different sender code paths (e.g. with and without txns).
	db := decomSrv.DB()
	key := roachpb.Key([]byte("a"))
	keyEnd := roachpb.Key([]byte("x"))
	value := []byte{1, 2, 3}

	testcases := []struct {
		name string
		op   func() error
	}{
		{"Del", func() error {
			_, err := db.Del(ctx, key)
			return err
		}},
		{"DelRange", func() error {
			_, err := db.DelRange(ctx, key, keyEnd, false)
			return err
		}},
		{"Get", func() error {
			_, err := db.Get(ctx, key)
			return err
		}},
		{"GetForUpdate", func() error {
			_, err := db.GetForUpdate(ctx, key, kvpb.BestEffort)
			return err
		}},
		{"Put", func() error {
			return db.Put(ctx, key, value)
		}},
		{"Scan", func() error {
			_, err := db.Scan(ctx, key, keyEnd, 0)
			return err
		}},
		{"TxnGet", func() error {
			_, err := db.NewTxn(ctx, "").Get(ctx, key)
			return err
		}},
		{"TxnPut", func() error {
			return db.NewTxn(ctx, "").Put(ctx, key, value)
		}},
		{"AdminTransferLease", func() error {
			return db.AdminTransferLease(ctx, scratchKey, srv.GetFirstStoreID())
		}},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var err error
			require.Eventually(t, func() bool {
				err = tc.op()
				s, ok := status.FromError(errors.UnwrapAll(err))
				if s == nil || !ok {
					return false
				}
				require.Equal(t, codes.PermissionDenied, s.Code())
				return true
			}, 10*time.Second, 100*time.Millisecond, "timed out waiting for gRPC error, got %v", err)
		})
	}
}

// TestGenerateForcedRetryableError verifies that GenerateForcedRetryableErr
// returns an error with a transaction that had the epoch bumped (and not epoch
// 0) for isolation levels that cannot move their read timestamp between
// operations.
func TestGenerateForcedRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	isolation.RunEachLevel(t, func(t *testing.T, isoLevel isolation.Level) {
		ctx := context.Background()
		s, db := setup(t)
		defer s.Stopper().Stop(context.Background())
		txn := db.NewTxn(ctx, "test: TestGenerateForcedRetryableError")
		require.NoError(t, txn.SetIsoLevel(isoLevel))
		require.Equal(t, 0, int(txn.Epoch()))
		err := txn.GenerateForcedRetryableErr(ctx, "testing TestGenerateForcedRetryableError")
		var retryErr *kvpb.TransactionRetryWithProtoRefreshError
		require.True(t, errors.As(err, &retryErr))
		var expEpoch enginepb.TxnEpoch
		if isoLevel == isolation.ReadCommitted {
			expEpoch = 0 // partial retry
		} else {
			expEpoch = 1 // full retry
		}
		require.Equal(t, expEpoch, retryErr.NextTransaction.Epoch)
	})
}

// Get a retryable error within a db.Txn transaction and verify the retry
// succeeds. We are verifying the behavior is the same whether the retryable
// callback returns the retryable error or returns nil. Both implementations are
// legal - returning early (with either nil or the error) after a retryable
// error is optional.
func TestDB_TxnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	isolation.RunEachLevel(t, func(t *testing.T, isoLevel isolation.Level) {
		testutils.RunTrueAndFalse(t, "returnNil", func(t *testing.T, returnNil bool) {
			testDBTxnRetry(t, isoLevel, returnNil)
		})
	})
}

func testDBTxnRetry(t *testing.T, isoLevel isolation.Level, returnNil bool) {
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	keyA := fmt.Sprintf("a_return_nil_%t", returnNil)
	keyB := fmt.Sprintf("b_return_nil_%t", returnNil)
	runNumber := 0
	err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		require.NoError(t, txn.SetIsoLevel(isoLevel))
		require.NoError(t, txn.Put(ctx, keyA, "1"))
		require.NoError(t, txn.Put(ctx, keyB, "1"))

		{
			// High priority txn - will abort the other txn.
			hpTxn := kv.NewTxn(ctx, db, 0)
			require.NoError(t, hpTxn.SetUserPriority(roachpb.MaxUserPriority))
			// Only write if we have not written before, because otherwise we will keep aborting
			// the other txn forever.
			r, err := hpTxn.Get(ctx, keyA)
			require.NoError(t, err)
			if !r.Exists() {
				require.Zero(t, runNumber)
				require.NoError(t, hpTxn.Put(ctx, keyA, "hp txn"))
				require.NoError(t, hpTxn.Commit(ctx))
			} else {
				// We already wrote to keyA, meaning this is a retry, no need to write again.
				require.Equal(t, 1, runNumber)
				require.NoError(t, hpTxn.Rollback(ctx))
			}
		}

		// Read, so that we'll get a retryable error.
		r, err := txn.Get(ctx, keyA)
		if runNumber == 0 {
			// First run, we should get a retryable error.
			require.Zero(t, runNumber)
			require.IsType(t, &kvpb.TransactionRetryWithProtoRefreshError{}, err)
			require.Equal(t, []byte(nil), r.ValueBytes())

			// At this point txn is poisoned, and any op returns the same (poisoning) error.
			r, err = txn.Get(ctx, keyB)
			require.IsType(t, &kvpb.TransactionRetryWithProtoRefreshError{}, err)
			require.Equal(t, []byte(nil), r.ValueBytes())
		} else {
			// The retry should succeed.
			require.Equal(t, 1, runNumber)
			require.NoError(t, err)
			require.Equal(t, []byte("1"), r.ValueBytes())
		}
		runNumber++

		if returnNil {
			return nil
		}
		// Return the retryable error.
		return err
	})
	require.NoError(t, err)
	require.Equal(t, 2, runNumber)

	err1 := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		// The high priority txn was overwritten by the successful retry.
		kv, e1 := txn.Get(ctx, keyA)
		require.NoError(t, e1)
		require.Equal(t, []byte("1"), kv.ValueBytes())
		kv, e2 := txn.Get(ctx, keyB)
		require.NoError(t, e2)
		require.Equal(t, []byte("1"), kv.ValueBytes())
		return nil
	})
	require.NoError(t, err1)
}

// TestDB_TxnRetryLimit tests kv.transaction.internal.max_auto_retries.
func TestDB_TxnRetryLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	isolation.RunEachLevel(t, func(t *testing.T, isoLevel isolation.Level) {
		testDBTxnRetryLimit(t, isoLevel)
	})
}

func testDBTxnRetryLimit(t *testing.T, isoLevel isolation.Level) {
	ctx := context.Background()
	s, db := setup(t)
	defer s.Stopper().Stop(ctx)

	// Configure a low retry limit.
	const maxRetries = 7
	const maxAttempts = maxRetries + 1
	kv.MaxInternalTxnAutoRetries.Override(ctx, &s.ClusterSettings().SV, maxRetries)

	// Run the txn, aborting it on each attempt.
	attempts := 0
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		attempts++
		require.NoError(t, txn.SetIsoLevel(isoLevel))
		require.NoError(t, txn.Put(ctx, "a", "1"))

		{
			// High priority txn - will abort the other txn each attempt.
			hpTxn := kv.NewTxn(ctx, db, 0)
			require.NoError(t, hpTxn.SetUserPriority(roachpb.MaxUserPriority))
			require.NoError(t, hpTxn.Put(ctx, "a", "hp txn"))
			require.NoError(t, hpTxn.Commit(ctx))
		}

		// Read, so that we'll get a retryable error.
		_, err := txn.Get(ctx, "a")
		require.Error(t, err)
		require.IsType(t, &kvpb.TransactionRetryWithProtoRefreshError{}, err)
		return err
	})
	require.Error(t, err)
	require.Regexp(t, "Terminating retry loop and returning error", err)
	require.Equal(t, maxAttempts, attempts)
}

func TestPreservingSteppingOnSenderReplacement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "stepping", func(t *testing.T, stepping bool) {
		// Create a txn which will be aborted by a high priority txn, with stepping
		// enabled and disabled.
		var txn *kv.Txn
		var expectedStepping kv.SteppingMode
		if stepping {
			txn = kv.NewTxnWithSteppingEnabled(ctx, db, 0, sessiondatapb.Normal)
			expectedStepping = kv.SteppingEnabled
		} else {
			txn = kv.NewTxn(ctx, db, 0)
			expectedStepping = kv.SteppingDisabled
		}
		keyA := fmt.Sprintf("a_%t", stepping)
		require.NoError(t, txn.Put(ctx, keyA, "1"))

		{
			// High priority txn - will abort the other txn.
			hpTxn := kv.NewTxn(ctx, db, 0)
			require.NoError(t, hpTxn.SetUserPriority(roachpb.MaxUserPriority))
			require.NoError(t, hpTxn.Put(ctx, keyA, "hp txn"))
			require.NoError(t, hpTxn.Commit(ctx))
		}

		_, err := txn.Get(ctx, keyA)
		require.NotNil(t, err)
		require.IsType(t, &kvpb.TransactionRetryWithProtoRefreshError{}, err)
		pErr := (*kvpb.TransactionRetryWithProtoRefreshError)(nil)
		require.ErrorAs(t, err, &pErr)
		require.Equal(t, txn.ID(), pErr.PrevTxnID)

		// The transaction was aborted, therefore we should have a new transaction ID.
		require.NotEqual(t, pErr.PrevTxnID, pErr.NextTransaction.ID)

		// Reset the handle in order to get a new sender.
		require.NoError(t, txn.PrepareForRetry(ctx))

		// Make sure we have a new txn ID.
		require.NotEqual(t, pErr.PrevTxnID, txn.ID())

		// Using ConfigureStepping() to read the current state.
		require.Equal(t, expectedStepping, txn.ConfigureStepping(ctx, expectedStepping))
	})
}

type byteSliceBulkSource[T kv.GValue] struct {
	keys   []roachpb.Key
	values []T
}

var _ kv.BulkSource[[]byte] = &byteSliceBulkSource[[]byte]{}

func (s *byteSliceBulkSource[T]) Len() int {
	return len(s.keys)
}

func (s *byteSliceBulkSource[T]) Iter() kv.BulkSourceIterator[T] {
	return &byteSliceBulkSourceIterator[T]{s: s, cursor: 0}
}

type byteSliceBulkSourceIterator[T kv.GValue] struct {
	s      *byteSliceBulkSource[T]
	cursor int
}

func (s *byteSliceBulkSourceIterator[T]) Next() (roachpb.Key, T) {
	k, v := s.s.keys[s.cursor], s.s.values[s.cursor]
	s.cursor++
	return k, v
}

func TestBulkBatchAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	kys := []roachpb.Key{[]byte("a"), []byte("b"), []byte("c")}
	vals := [][]byte{[]byte("you"), []byte("know"), []byte("me")}

	type putter func(*kv.Batch)

	clearKeys := func() {
		txn := db.NewTxn(ctx, "bulk-test")
		b := txn.NewBatch()
		b.Del("a", "b", "c")
		err := txn.CommitInBatch(ctx, b)
		require.NoError(t, err)
	}

	verify := func() {
		for i, k := range kys {
			v, err := db.Get(ctx, k)
			require.NoError(t, err)
			raw := vals[i]
			if tv, err := v.Value.GetTuple(); err == nil {
				raw = tv
			}
			err = v.Value.Verify(k)
			require.NoError(t, err)
			require.Equal(t, raw, vals[i])
		}
	}

	testF := func(p putter) {
		txn := db.NewTxn(ctx, "bulk-test")
		b := txn.NewBatch()
		p(b)
		err := txn.CommitInBatch(ctx, b)
		require.NoError(t, err)
		verify()
		require.Greater(t, len(b.Results), 1)
		r := b.Results[0]
		require.Equal(t, len(r.Rows), len(kys))
		require.NoError(t, r.Err)
		clearKeys()
	}

	testF(func(b *kv.Batch) { b.PutBytes(&byteSliceBulkSource[[]byte]{kys, vals}) })
	testF(func(b *kv.Batch) { b.PutTuples(&byteSliceBulkSource[[]byte]{kys, vals}) })
	testF(func(b *kv.Batch) { b.CPutBytesEmpty(&byteSliceBulkSource[[]byte]{kys, vals}) })
	testF(func(b *kv.Batch) { b.CPutTuplesEmpty(&byteSliceBulkSource[[]byte]{kys, vals}) })

	values := make([]roachpb.Value, len(kys))
	for i, v := range vals {
		if kys[i] != nil {
			values[i].InitChecksum(kys[i])
			values[i].SetTuple(v)
		}
	}
	testF(func(b *kv.Batch) { b.CPutValuesEmpty(&byteSliceBulkSource[roachpb.Value]{kys, values}) })
}

func TestGetResult(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	kys1 := []roachpb.Key{[]byte("a"), []byte("b"), []byte("c")}
	kys2 := []roachpb.Key{[]byte("d"), []byte("e"), []byte("f")}
	vals := [][]byte{[]byte("you"), []byte("know"), []byte("me")}
	txn := db.NewTxn(ctx, "bulk-test")
	b := txn.NewBatch()
	b.PutBytes(&byteSliceBulkSource[[]byte]{kys1, vals})
	b.PutBytes(&byteSliceBulkSource[[]byte]{kys2, vals})
	keyToDel := roachpb.Key("b")
	b.Del(keyToDel)
	err := txn.CommitInBatch(ctx, b)
	require.NoError(t, err)
	for i := 0; i < len(kys1)+len(kys2); i++ {
		res, row, err := b.GetResult(i)
		require.Equal(t, res, &b.Results[i/3])
		require.Equal(t, row, b.Results[i/3].Rows[i%3])
		require.NoError(t, err)
	}
	// test Del request (it uses Result.Keys rather than Result.Rows)
	_, kv, err := b.GetResult(len(kys1) + len(kys2))
	require.NoError(t, err)
	require.Equal(t, keyToDel, kv.Key)
	require.Nil(t, kv.Value)
	// test EndTxn result
	_, _, err = b.GetResult(len(kys1) + len(kys2) + 1)
	require.NoError(t, err)

	// test out of bounds
	_, _, err = b.GetResult(len(kys1) + len(kys2) + 2)
	require.Error(t, err)
}

func BenchmarkBulkBatchAPI(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	s, db := setup(b)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()
	txn := db.NewTxn(ctx, "bulk-test")
	kys := make([]roachpb.Key, 1000)
	vals := make([][]byte, len(kys))
	for i := 0; i < len(kys); i++ {
		kys[i] = []byte("asdf" + strconv.Itoa(i))
		vals[i] = []byte("qwerty" + strconv.Itoa(i))
	}
	b.Run("single", func(b *testing.B) {
		ba := txn.NewBatch()
		for i := 0; i < b.N; i++ {
			for j, k := range kys {
				ba.Put(k, vals[j])
			}
		}
	})
	b.Run("bulk", func(b *testing.B) {
		ba := txn.NewBatch()
		for i := 0; i < b.N; i++ {
			ba.PutBytes(&byteSliceBulkSource[[]byte]{kys, vals})
		}
	})
}
