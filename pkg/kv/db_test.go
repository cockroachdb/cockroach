// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

func setup(t *testing.T) (serverutils.TestServerInterface, *kv.DB) {
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
	defer s.Stopper().Stop(context.Background())

	result, err := db.GetForUpdate(context.Background(), "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte(""), result.ValueBytes())
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
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := kv.CtxForCPutInline(context.Background())

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

func TestDB_InitPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	if err := db.InitPut(ctx, "aa", "1", false); err != nil {
		t.Fatal(err)
	}
	if err := db.InitPut(ctx, "aa", "1", false); err != nil {
		t.Fatal(err)
	}
	if err := db.InitPut(ctx, "aa", "2", false); err == nil {
		t.Fatal("expected error from init put")
	}
	if err := db.Del(ctx, "aa"); err != nil {
		t.Fatal(err)
	}
	if err := db.InitPut(ctx, "aa", "2", true); err == nil {
		t.Fatal("expected error from init put")
	}
	if err := db.InitPut(ctx, "aa", "1", false); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("1"), result.ValueBytes())
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
	defer s.Stopper().Stop(context.Background())

	b := &kv.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	if err := db.Run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	rows, err := db.ScanForUpdate(context.Background(), "a", "b", 100)
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
	defer s.Stopper().Stop(context.Background())

	b := &kv.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	if err := db.Run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	rows, err := db.ReverseScanForUpdate(context.Background(), "ab", "c", 100)
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
	if err := db.Del(context.Background(), "ab"); err != nil {
		t.Fatal(err)
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

func TestTxn_Commit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())

	err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
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
	if err := db.Run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{
		"aa": []byte("1"),
		"ab": []byte("2"),
	}
	checkResults(t, expected, b.Results)
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
			return db.Del(ctx, key)
		}},
		{"DelRange", func() error {
			return db.DelRange(ctx, key, keyEnd)
		}},
		{"Get", func() error {
			_, err := db.Get(ctx, key)
			return err
		}},
		{"GetForUpdate", func() error {
			_, err := db.GetForUpdate(ctx, key)
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
