// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()

	a := MakeApplier(db, db)
	check := func(t *testing.T, s Step, expected string) {
		t.Helper()
		require.NoError(t, a.Apply(ctx, &s))
		actual := s.String()
		// Trim out the txn stuff. It has things like timestamps in it that are not
		// stable from run to run.
		actual = regexp.MustCompile(` // nil txnpb:\(.*\)`).ReplaceAllString(actual, ` // nil txnpb:(...)`)
		assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(actual))
	}
	checkErr := func(t *testing.T, s Step, expected string) {
		t.Helper()
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, a.Apply(cancelledCtx, &s))
		actual := s.String()
		// Trim out context canceled location, which can be non-deterministic.
		// The wrapped string around the context canceled error depends on where
		// the context cancellation was noticed.
		actual = regexp.MustCompile(` aborted .*: context canceled`).ReplaceAllString(actual, ` context canceled`)
		assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(actual))
	}

	// Basic operations
	check(t, step(get(`a`)), `db0.Get(ctx, "a") // (nil, nil)`)
	check(t, step(scan(`a`, `c`)), `db1.Scan(ctx, "a", "c", 0) // ([], nil)`)

	check(t, step(put(`a`, `1`)), `db0.Put(ctx, "a", 1) // nil`)
	check(t, step(get(`a`)), `db1.Get(ctx, "a") // ("1", nil)`)
	check(t, step(scanForUpdate(`a`, `c`)), `db0.ScanForUpdate(ctx, "a", "c", 0) // (["a":"1"], nil)`)

	check(t, step(put(`b`, `2`)), `db1.Put(ctx, "b", 2) // nil`)
	check(t, step(get(`b`)), `db0.Get(ctx, "b") // ("2", nil)`)
	check(t, step(scan(`a`, `c`)), `db1.Scan(ctx, "a", "c", 0) // (["a":"1", "b":"2"], nil)`)

	checkErr(t, step(get(`a`)), `db0.Get(ctx, "a") // (nil, context canceled)`)
	checkErr(t, step(put(`a`, `1`)), `db1.Put(ctx, "a", 1) // context canceled`)
	checkErr(t, step(scanForUpdate(`a`, `c`)), `db0.ScanForUpdate(ctx, "a", "c", 0) // (nil, context canceled)`)

	// Batch
	check(t, step(batch(put(`b`, `2`), get(`a`), scan(`a`, `c`))), `
{
  b := &Batch{}
  b.Put(ctx, "b", 2) // nil
  b.Get(ctx, "a") // ("1", nil)
  b.Scan(ctx, "a", "c") // (["a":"1", "b":"2"], nil)
  db1.Run(ctx, b) // nil
}
`)
	checkErr(t, step(batch(put(`b`, `2`), get(`a`), scanForUpdate(`a`, `c`))), `
{
  b := &Batch{}
  b.Put(ctx, "b", 2) // context canceled
  b.Get(ctx, "a") // (nil, context canceled)
  b.ScanForUpdate(ctx, "a", "c") // (nil, context canceled)
  db0.Run(ctx, b) // context canceled
}
`)

	// Txn commit
	check(t, step(closureTxn(ClosureTxnType_Commit, put(`e`, `5`), batch(put(`f`, `6`)))), `
db1.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
  txn.Put(ctx, "e", 5) // nil
  {
    b := &Batch{}
    b.Put(ctx, "f", 6) // nil
    txn.Run(ctx, b) // nil
  }
  return nil
}) // nil txnpb:(...)
		`)

	// Txn commit in batch
	check(t, step(closureTxnCommitInBatch(opSlice(get(`a`), put(`f`, `6`)), put(`e`, `5`))), `
db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
  txn.Put(ctx, "e", 5) // nil
  b := &Batch{}
  b.Get(ctx, "a") // ("1", nil)
  b.Put(ctx, "f", 6) // nil
  txn.CommitInBatch(ctx, b) // nil
  return nil
}) // nil txnpb:(...)
		`)

	// Txn rollback
	check(t, step(closureTxn(ClosureTxnType_Rollback, put(`e`, `5`))), `
db1.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
  txn.Put(ctx, "e", 5) // nil
  return errors.New("rollback")
}) // rollback
		`)

	// Txn error
	checkErr(t, step(closureTxn(ClosureTxnType_Rollback, put(`e`, `5`))), `
db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
  txn.Put(ctx, "e", 5)
  return errors.New("rollback")
}) // context canceled
		`)

	// Splits and merges
	check(t, step(split(`foo`)), `db1.AdminSplit(ctx, "foo") // nil`)
	check(t, step(merge(`foo`)), `db0.AdminMerge(ctx, "foo") // nil`)
	checkErr(t, step(split(`foo`)),
		`db1.AdminSplit(ctx, "foo") // context canceled`)
	checkErr(t, step(merge(`foo`)),
		`db0.AdminMerge(ctx, "foo") // context canceled`)

	// Lease transfers
	check(t, step(transferLease(`foo`, 1)),
		`db1.TransferLeaseOperation(ctx, "foo", 1) // nil`)
	checkErr(t, step(transferLease(`foo`, 1)),
		`db0.TransferLeaseOperation(ctx, "foo", 1) // context canceled`)
}
