// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestUndetectedAmbiguousCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var args base.TestClusterArgs
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	// Txn record GC populates txn tscache which prevents second commit
	// attempt from hitting TransactionStatusError(alreadyCommitted).
	defer batcheval.TestingSetTxnAutoGC(false)

	k := tc.ScratchRange(t)
	err := tc.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.DisablePipelining() // keep it simple
		if err := txn.Put(ctx, k, "hello"); err != nil {
			t.Log(err)
			return err
		}
		err := txn.Commit(ctx) // hits fatal error
		t.Log(err)
		return err
	})
	require.NoError(t, err) // unreached
}
