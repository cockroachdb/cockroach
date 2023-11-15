// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestLeaseInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	lease := roachpb.Lease{Sequence: 1}
	nextLease := roachpb.Lease{Sequence: 2}
	storeID := roachpb.StoreID(7)
	lai := kvpb.LeaseAppliedIndex(314)

	// Test with and without pending lease transfer.
	testutils.RunTrueAndFalse(t, "transfer", func(t *testing.T, transfer bool) {
		evalCtx := batcheval.MockEvalCtx{
			Lease:             lease,
			StoreID:           storeID,
			LeaseAppliedIndex: lai,
		}
		if transfer {
			evalCtx.NextLease = nextLease
		}

		var resp kvpb.LeaseInfoResponse
		_, err := batcheval.LeaseInfo(ctx, nil /* reader */, batcheval.CommandArgs{
			EvalCtx: evalCtx.EvalContext(),
			Args:    &kvpb.LeaseInfoRequest{},
		}, &resp)
		require.NoError(t, err)

		if transfer {
			require.Equal(t, kvpb.LeaseInfoResponse{
				Lease:             nextLease,
				CurrentLease:      &lease,
				LeaseAppliedIndex: lai,
				EvaluatedBy:       storeID,
			}, resp)
		} else {
			require.Equal(t, kvpb.LeaseInfoResponse{
				Lease:             lease,
				LeaseAppliedIndex: lai,
				EvaluatedBy:       storeID,
			}, resp)
		}
	})
}
