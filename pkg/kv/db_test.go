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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
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
			return db.Del(ctx, key)
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

// TestGenerateForcedRetryableError verifies that GenerateForcedRetryableError
// returns an error with a transaction that had the epoch bumped (and not epoch 0).
func TestGenerateForcedRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())
	txn := db.NewTxn(ctx, "test: TestGenerateForcedRetryableError")
	require.Equal(t, 0, int(txn.Epoch()))
	err := txn.GenerateForcedRetryableError(ctx, "testing TestGenerateForcedRetryableError")
	var retryErr *roachpb.TransactionRetryWithProtoRefreshError
	require.True(t, errors.As(err, &retryErr))
	require.Equal(t, 1, int(retryErr.Transaction.Epoch))
}

// Get a retryable error within a db.Txn transaction and verify the retry
// succeeds. We are verifying the behavior is the same whether the retryable
// callback returns the retryable error or returns nil. Both implementations are
// legal - returning early (with either nil or the error) after a retryable
// error is optional.
func TestDB_TxnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db := setup(t)
	defer s.Stopper().Stop(context.Background())

	testutils.RunTrueAndFalse(t, "returnNil", func(t *testing.T, returnNil bool) {
		keyA := fmt.Sprintf("a_return_nil_%t", returnNil)
		keyB := fmt.Sprintf("b_return_nil_%t", returnNil)
		runNumber := 0
		err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
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
				require.IsType(t, &roachpb.TransactionRetryWithProtoRefreshError{}, err)
				require.Equal(t, []byte(nil), r.ValueBytes())

				// At this point txn is poisoned, and any op returns the same (poisoning) error.
				r, err = txn.Get(ctx, keyB)
				require.IsType(t, &roachpb.TransactionRetryWithProtoRefreshError{}, err)
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
	})
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
		require.IsType(t, &roachpb.TransactionRetryWithProtoRefreshError{}, err)
		pErr := (*roachpb.TransactionRetryWithProtoRefreshError)(nil)
		require.ErrorAs(t, err, &pErr)
		require.Equal(t, txn.ID(), pErr.TxnID)

		// The transaction was aborted, therefore we should have a new transaction ID.
		require.NotEqual(t, pErr.TxnID, pErr.Transaction.ID)

		// Reset the handle in order to get a new sender.
		txn.PrepareForRetry(ctx)

		// Make sure we have a new txn ID.
		require.NotEqual(t, pErr.TxnID, txn.ID())

		// Using ConfigureStepping() to read the current state.
		require.Equal(t, expectedStepping, txn.ConfigureStepping(ctx, expectedStepping))
	})
}
