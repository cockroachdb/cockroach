// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestLeaseInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	now := clock.Now()

	const id = 1
	lease := roachpb.Lease{
		Start:    now.UnsafeToClockTimestamp(),
		Sequence: 1,
		Epoch:    1,
		Replica:  roachpb.ReplicaDescriptor{NodeID: id, StoreID: id, ReplicaID: id},
	}
	lai := kvpb.LeaseAppliedIndex(7)
	closedTS := hlc.MinTimestamp

	// Test both with and without a tentative lease request.
	testutils.RunTrueAndFalse(t, "leaseReq", func(t *testing.T, leaseReq bool) {
		var nextLease roachpb.Lease
		if leaseReq {
			nextLease = lease
			nextLease.Sequence += 1
			nextLease.Start.Logical += 1
		}

		evalCtx := (&batcheval.MockEvalCtx{
			Clock:             clock,
			Lease:             lease,
			NextLease:         nextLease,
			LeaseAppliedIndex: lai,
			ClosedTimestamp:   closedTS,
			StoreID:           id,
		}).EvalContext()

		resp := kvpb.LeaseInfoResponse{}
		res, err := batcheval.LeaseInfo(ctx, engine, batcheval.CommandArgs{
			EvalCtx: evalCtx,
			Header: kvpb.Header{
				Timestamp: clock.Now(),
			},
			Args: &kvpb.LeaseInfoRequest{},
		}, &resp)
		require.NoError(t, err)

		expect := kvpb.LeaseInfoResponse{
			Lease:             lease,
			LeaseAppliedIndex: lai,
			ClosedTimestamp:   closedTS,
			EvaluatedBy:       id,
		}
		if leaseReq {
			expect.Lease = nextLease
			expect.CurrentLease = &lease
		}

		require.Equal(t, result.Result{}, res)
		require.Equal(t, expect, resp)
	})
}
