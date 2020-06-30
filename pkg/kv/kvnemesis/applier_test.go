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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplier(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
		assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(s.String()))
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

	checkErr(t, step(get(`a`)), `db0.Get(ctx, "a") // (nil, aborted in distSender: context canceled)`)
	checkErr(t, step(put(`a`, `1`)), `db1.Put(ctx, "a", 1) // aborted in distSender: context canceled`)
	checkErr(t, step(scanForUpdate(`a`, `c`)), `db0.ScanForUpdate(ctx, "a", "c", 0) // (nil, aborted in distSender: context canceled)`)

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
  b.Put(ctx, "b", 2) // aborted in distSender: context canceled
  b.Get(ctx, "a") // (nil, aborted in distSender: context canceled)
  b.ScanForUpdate(ctx, "a", "c") // (nil, aborted in distSender: context canceled)
  db0.Run(ctx, b) // aborted in distSender: context canceled
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
		`db1.AdminSplit(ctx, "foo") // aborted in distSender: context canceled`)
	checkErr(t, step(merge(`foo`)),
		`db0.AdminMerge(ctx, "foo") // aborted in distSender: context canceled`)
}
