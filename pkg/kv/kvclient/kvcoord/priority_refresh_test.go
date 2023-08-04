// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRefreshWithHighPriority reproduces the issue described in #104728:
//   - a normal-priority txn writes an intent on a key,
//   - a high-priority txn reads the key and pushes the timestamp of the
//     normal-priority txn,
//   - something pushes the timestamp of the high-priority txn (in this test, the
//     closed timestamp target),
//   - the high-priority txn attempts to refresh before committing but encounters
//     the normal-priority txn's intent again, causing the refresh to fail.
//
// This can potentially repeat indefinitely as the high-priority txn retries,
// causing a livelock.
func TestRefreshWithHighPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	st := cluster.MakeTestingClusterSettings()
	closedts.TargetDuration.Override(context.Background(), &st.SV, 0)
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
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
	// The closed timestamp target duration is 0. Writing to a timestamp below that
	// will be treated as a read-write conflict, so Txn2's timestamp gets pushed.
	err = txn2.Put(ctx, keyB, "b")
	require.NoError(t, err)
	// Txn2 tries to refresh before committing. The refresh encounters txn1's
	// intent but pushes its timestamp and succeeds.
	err = txn2.Commit(ctx)
	// before #104728 this would be:
	// require.Regexp(t, ".*RETRY_SERIALIZABLE - failed preemptive refresh due to
	// a conflict: intent on key.*", err)
	require.NoError(t, err)
}
