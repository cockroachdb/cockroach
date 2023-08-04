// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvclient

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

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
	// 3 seconds is the closed timestamp target duration. Writing to a timestamp
	// below that will be treated as a read-write conflict.
	time.Sleep(3 * time.Second)
	// Txn2's timestamp gets pushed.
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
