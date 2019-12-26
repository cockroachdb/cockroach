// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemeses

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()

	a := MakeApplier(db)
	check := func(t *testing.T, s Step, expected string) {
		t.Helper()
		require.NoError(t, a.Apply(ctx, &s))
		assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(s.String()))
	}
	checkErr := func(t *testing.T, s Step, expected string) {
		t.Helper()
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		require.EqualError(t, a.Apply(cancelledCtx, &s), `context canceled`)
		assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(s.String()))
	}

	// Basic operations
	check(t, step(get(`a`)), `db.Get(ctx, "a") -> (nil, nil)`)

	check(t, step(put(`a`, `1`)), `db.Put(ctx, "a", 1) -> nil`)
	check(t, step(get(`a`)), `db.Get(ctx, "a") -> ("1", nil)`)

	check(t, step(put(`b`, `2`), put(`c`, `3`), get(`a`)), `
Concurrent
  db.Put(ctx, "b", 2) -> nil
  db.Put(ctx, "c", 3) -> nil
  db.Get(ctx, "a") -> ("1", nil)
	`)

	checkErr(t, step(get(`a`)), `db.Get(ctx, "a") -> (nil, aborted in distSender: context canceled)`)
	checkErr(t, step(put(`a`, `1`)), `db.Put(ctx, "a", 1) -> aborted in distSender: context canceled`)

	// Batch
	check(t, step(batch(put(`d`, `4`), get(`c`))), `
{
  b := &Batch{}
  b.Put(ctx, "d", 4) -> nil
  b.Get(ctx, "c") -> ("3", nil)
  db.Run(ctx, b) -> nil
}
`)
	checkErr(t, step(batch(put(`d`, `4`), get(`c`))), `
{
  b := &Batch{}
  b.Put(ctx, "d", 4) -> aborted in distSender: context canceled
  b.Get(ctx, "c") -> (nil, aborted in distSender: context canceled)
  db.Run(ctx, b) -> aborted in distSender: context canceled
}
`)

	// Basic txn stuff
	check(t, step(beginTxn(`1`)), `txn1 := db.NewTxn(ctx)`)
	check(t, step(useTxn(`1`, put(`e`, `5`))), `
{
  txn1.Put(ctx, "e", 5) -> nil
}
	`)
	check(t, step(commitTxn(`1`)), `txn1.Commit(ctx) -> nil`)

	// Txn rollback
	check(t, step(beginTxn(`2`)), `txn2 := db.NewTxn(ctx)`)
	check(t, step(rollbackTxn(`2`)), `txn2.Rollback(ctx) -> nil`)

	// Txn errors
	check(t, step(beginTxn(`3`)), `txn3 := db.NewTxn(ctx)`)
	checkErr(t, step(useTxn(`3`, put(`f`, `6`))), `
{
  txn3.Put(ctx, "f", 6) -> aborted in distSender: context canceled
}
	`)
	checkErr(t, step(commitTxn(`3`)), `txn3.Commit(ctx) -> aborted in distSender: context canceled`)

	check(t, step(beginTxn(`4`)), `txn4 := db.NewTxn(ctx)`)
	// txn.Rollback doesn't error on context canceled, so here are some nice hacks
	// for ya.
	{
		proto := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{
			ID:             uuid.MakeV4(),
			WriteTimestamp: db.Clock().Now(),
		}}
		a.mu.txns[`4`] = client.NewTxnFromProto(ctx, db, 0, db.Clock().Now(), client.LeafTxn, proto)
	}
	checkErr(t, step(rollbackTxn(`4`)), `txn4.Rollback(ctx) -> Rollback() called on leaf txn`)

	// Splits and merges
	check(t, step(split(`foo`)), `db.Split(ctx, "foo") -> nil`)
	check(t, step(merge(`foo`)), `db.Merge(ctx, "foo") -> nil`)
	checkErr(t, step(split(`foo`)), `db.Split(ctx, "foo") -> aborted in distSender: context canceled`)
	checkErr(t, step(merge(`foo`)), `db.Merge(ctx, "foo") -> aborted in distSender: context canceled`)
}
