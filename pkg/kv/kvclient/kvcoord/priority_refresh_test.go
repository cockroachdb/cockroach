// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRefreshWithHighPriority reproduces the issue described in #104728:
//   - a normal-priority txn writes an intent on a key,
//   - a high-priority txn reads the key and pushes the timestamp of the
//     normal-priority txn,
//   - something pushes the write timestamp of the high-priority txn (in this
//     test, that's a read-write conflict on another key),
//   - the high-priority txn attempts to refresh before committing but encounters
//     the normal-priority txn's intent again, causing the refresh to fail.
//
// This can potentially repeat indefinitely as the high-priority txn retries,
// causing a livelock. This livelock is not possible after #108190 because
// refresh requests now declare isolated keys and go through the lock table
// where they can push a lower-priority request. In this particular example,
// when the refresh encounters the intent, it returns a WriteIntentError, which
// is handled and a lock is added to the lock table; then, when the refresh
// request retries, it pushes the lower-priority lock-holder's timestamp and
// succeeds.
func TestRefreshWithHighPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(context.Background())

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	txn1 := kvDB.NewTxn(ctx, "txn1")
	err := txn1.Put(ctx, keyA, "a")
	require.NoError(t, err)
	txn2 := kvDB.NewTxn(ctx, "txn2")
	err = txn2.SetUserPriority(roachpb.MaxUserPriority)
	require.NoError(t, err)
	// This read request pushes the timestamp of txn1 due to priority.
	_, err = txn2.Get(ctx, keyA)
	require.NoError(t, err)
	// This read request pushes the timestamp of txn2 due to txn2's subsequent
	// conflicting write to keyB.
	_, err = kvDB.Get(ctx, keyB)
	require.NoError(t, err)
	// Txn2's write timestamp is pushed.
	err = txn2.Put(ctx, keyB, "b")
	require.NoError(t, err)
	// Txn2 tries to refresh before committing because its read and write
	// timestamps differ. The refresh encounters txn1's intent but pushes its
	// timestamp and succeeds.
	err = txn2.Commit(ctx)
	// before #108190 this would be:
	// require.Regexp(t, ".*RETRY_SERIALIZABLE - failed preemptive refresh due to
	// a conflict: intent on key.*", err)
	require.NoError(t, err)
}
