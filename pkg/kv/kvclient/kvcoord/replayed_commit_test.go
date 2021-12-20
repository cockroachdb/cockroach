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

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type interceptingTransport struct {
	kvcoord.Transport
	intercept func(context.Context, roachpb.BatchRequest, *roachpb.BatchResponse, error) (*roachpb.BatchResponse, error)
}

func (f *interceptingTransport) SendNext(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	br, err := f.Transport.SendNext(ctx, ba)
	return f.intercept(ctx, ba, br, err)
}

// TestCommitSanityCheckAssertionFiresOnUndetectedAmbiguousCommit sets up a situation
// in which DistSender retries an (unbeknownst to it) successful EndTxn(commit=true)
// RPC. It documents that this triggers an assertion failure in TxnCoordSender.
//
// See: https://github.com/cockroachdb/cockroach/issues/67765
func TestCommitSanityCheckAssertionFiresOnUndetectedAmbiguousCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var args base.TestClusterArgs
	args.ServerArgs.Knobs.KVClient = &kvcoord.ClientTestingKnobs{TransportFactory: func(
		options kvcoord.SendOptions,
		dialer kvcoord.NodeDialer,
		slice kvcoord.ReplicaSlice,
	) (kvcoord.Transport, error) {
		tf, err := kvcoord.GRPCTransportFactory(options, dialer, slice)
		if err != nil {
			return nil, err
		}
		return &interceptingTransport{
			Transport: tf,
			intercept: func(ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse, err error) (*roachpb.BatchResponse, error) {
				if err != nil || ba.Txn == nil || br.Txn == nil ||
					ba.Txn.Status != roachpb.PENDING || br.Txn.Status != roachpb.COMMITTED ||
					!keys.ScratchRangeMin.Equal(br.Txn.Key) {
					// Only want to inject error on successful commit for "our" txn.
					return br, err
				}
				err = circuit.ErrBreakerOpen
				assert.True(t, grpcutil.RequestDidNotStart(err)) // avoid Fatal on goroutine
				return nil, err
			},
		}, nil
	},
	}

	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	// Txn record GC populates txn tscache which prevents second commit
	// attempt from hitting TransactionStatusError(alreadyCommitted).
	defer batcheval.TestingSetTxnAutoGC(false)()
	{
		// Turn the assertion into an error.
		prev := kvcoord.DisableCommitSanityCheck
		kvcoord.DisableCommitSanityCheck = true
		defer func() {
			kvcoord.DisableCommitSanityCheck = prev
		}()
	}

	k := tc.ScratchRange(t)
	err := tc.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_ = txn.DisablePipelining() // keep it simple
		if err := txn.Put(ctx, k, "hello"); err != nil {
			t.Log(err)
			return err
		}
		err := txn.Commit(ctx) // hits fatal error
		t.Log(err)
		return err
	})
	require.True(t, errors.IsAssertionFailure(err), "%+v", err)
}
