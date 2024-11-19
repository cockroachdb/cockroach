// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCommitSanityCheckAssertionFiresOnUndetectedAmbiguousCommit sets up a situation
// in which DistSender retries an (unbeknownst to it) successful EndTxn(commit=true)
// RPC. It documents that this triggers an assertion failure in TxnCoordSender.
//
// See: https://github.com/cockroachdb/cockroach/issues/103817
func TestCommitSanityCheckAssertionFiresOnUndetectedAmbiguousCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var args base.TestClusterArgs
	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
			// Disable async intent resolution, as it could possibly GC the txn record
			// out from under us, leading to the retried commit taking a path
			// different from the one we want to exercise in this test.
			DisableAsyncIntentResolution: true,
		},
	}
	args.ServerArgs.Knobs.KVClient = &kvcoord.ClientTestingKnobs{
		TransportFactory: func(factory kvcoord.TransportFactory) kvcoord.TransportFactory {
			return func(options kvcoord.SendOptions, slice kvcoord.ReplicaSlice) kvcoord.Transport {
				tf := factory(options, slice)
				return &interceptingTransport{
					Transport: tf,
					afterSend: func(ctx context.Context, req *interceptedReq, resp *interceptedResp) (overrideResp *interceptedResp) {
						if resp.err != nil || req.ba.Txn == nil || resp.br.Txn == nil ||
							req.ba.Txn.Status != roachpb.PENDING || resp.br.Txn.Status != roachpb.COMMITTED ||
							!keys.ScratchRangeMin.Equal(resp.br.Txn.Key) {
							// Only want to inject error on successful commit for "our" txn.
							return nil
						}

						err := &netutil.InitialHeartbeatFailedError{}
						assert.True(t, grpcutil.RequestDidNotStart(err)) // avoid Fatal on goroutine
						return &interceptedResp{err: err}
					},
				}

			}

		},
	}

	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	kNext := k.Next()
	require.Equal(t, keys.ScratchRangeMin, k) // interceptor above relies on this
	tc.SplitRangeOrFatal(t, kNext)

	err := tc.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_ = txn.DisablePipelining() // keep it simple
		if err := txn.Put(ctx, k, "hello"); err != nil {
			t.Log(err)
			return err
		}
		// We need to give the txn an external lock (i.e. one on a different range),
		// or we'll auto-GC the txn record on the first commit attempt, preventing
		// the second one from getting the "desired"
		// TransactionStatusError(alreadyCommitted).
		if err := txn.Put(ctx, kNext, "hullo"); err != nil {
			t.Log(err)
			return err
		}
		err := txn.Commit(ctx) // hits fatal error
		t.Log(err)
		return err
	})
	require.True(t, errors.IsAssertionFailure(err), "%+v", err)
}
