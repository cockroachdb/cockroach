// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestLeaseTransferWithPipelinedWrite verifies that pipelined writes
// do not cause retry errors to be leaked to clients when the error
// can be handled internally. Pipelining dissociates a write from its
// caller, so the retries of internally-generated errors (specifically
// out-of-order lease indexes) must be retried below that level.
//
// This issue was observed in practice to affect the first insert
// after table creation with high probability.
func TestLeaseTransferWithPipelinedWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)

	// More than 30 iterations is flaky under stressrace on teamcity.
	for iter := 0; iter < 30; iter++ {
		log.Infof(ctx, "iter %d", iter)
		if _, err := db.ExecContext(ctx, "drop table if exists test"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "create table test (a int, b int, primary key (a, b))"); err != nil {
			t.Fatal(err)
		}

		workerErrCh := make(chan error, 1)
		go func() {
			workerErrCh <- func() error {
				for i := 0; i < 1; i++ {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						return err
					}
					defer func() {
						if tx != nil {
							if err := tx.Rollback(); err != nil {
								log.Warningf(ctx, "error rolling back: %+v", err)
							}
						}
					}()
					// Run two inserts in a transaction to ensure that we have
					// pipelined writes that cannot be retried at the SQL layer
					// due to the first-statement rule.
					if _, err := tx.ExecContext(ctx, "INSERT INTO test (a, b) VALUES ($1, $2)", i, 1); err != nil {
						return err
					}
					if _, err := tx.ExecContext(ctx, "INSERT INTO test (a, b) VALUES ($1, $2)", i, 2); err != nil {
						return err
					}
					if err := tx.Commit(); err != nil {
						return err
					}
					tx = nil
				}
				return nil
			}()
		}()

		// TODO(bdarnell): This test reliably reproduced the issue when
		// introduced, because table creation causes splits and repeated
		// table creation leads to lease transfers due to rebalancing.
		// This is a subtle thing to rely on and the test might become
		// more reliable if we ran more iterations in the worker goroutine
		// and added a second goroutine to explicitly transfer leases back
		// and forth.

		select {
		case <-time.After(15 * time.Second):
			// TODO(bdarnell): The test seems flaky under stress with a 5s
			// timeout. Why? I'm giving it a high timeout since hanging
			// isn't a failure mode we're particularly concerned about here,
			// but it shouldn't be taking this long even with stress.
			t.Fatal("timed out")
		case err := <-workerErrCh:
			if err != nil {
				t.Fatalf("worker failed: %+v", err)
			}
		}
	}
}

func TestLeaseCommandLearnerReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const voterStoreID, learnerStoreID roachpb.StoreID = 1, 2
	replicas := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: voterStoreID, Type: roachpb.ReplicaTypeVoterFull(), ReplicaID: 1},
		{NodeID: 2, StoreID: learnerStoreID, Type: roachpb.ReplicaTypeLearner(), ReplicaID: 2},
	}
	desc := roachpb.RangeDescriptor{}
	desc.SetReplicas(roachpb.MakeReplicaSet(replicas))
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	cArgs := CommandArgs{
		EvalCtx: (&MockEvalCtx{
			StoreID: voterStoreID,
			Desc:    &desc,
			Clock:   clock,
		}).EvalContext(),
		Args: &roachpb.TransferLeaseRequest{
			Lease: roachpb.Lease{
				Replica: replicas[1],
			},
		},
	}

	// Learners are not allowed to become leaseholders for now, see the comments
	// in TransferLease and RequestLease.
	_, err := TransferLease(ctx, nil, cArgs, nil)
	require.EqualError(t, err, `replica cannot hold lease`)

	cArgs.Args = &roachpb.RequestLeaseRequest{}
	_, err = RequestLease(ctx, nil, cArgs, nil)

	const expForUnknown = `cannot replace lease <empty> with <empty>: ` +
		`replica not found in RangeDescriptor`
	require.EqualError(t, err, expForUnknown)

	cArgs.Args = &roachpb.RequestLeaseRequest{
		Lease: roachpb.Lease{
			Replica: replicas[1],
		},
	}
	_, err = RequestLease(ctx, nil, cArgs, nil)

	const expForLearner = `cannot replace lease <empty> ` +
		`with repl=(n2,s2):2LEARNER seq=0 start=0,0 exp=<nil>: ` +
		`replica cannot hold lease`
	require.EqualError(t, err, expForLearner)
}

// TestLeaseTransferForwardsStartTime tests that during a lease transfer, the
// start time of the new lease is determined during evaluation, after latches
// have granted the lease transfer full mutual exclusion over the leaseholder.
func TestLeaseTransferForwardsStartTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "epoch", func(t *testing.T, epoch bool) {
		testutils.RunTrueAndFalse(t, "served-future-reads", func(t *testing.T, servedFutureReads bool) {
			ctx := context.Background()
			db := storage.NewDefaultInMemForTesting()
			defer db.Close()
			batch := db.NewBatch()
			defer batch.Close()

			replicas := []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, Type: roachpb.ReplicaTypeVoterFull(), ReplicaID: 1},
				{NodeID: 2, StoreID: 2, Type: roachpb.ReplicaTypeVoterFull(), ReplicaID: 2},
			}
			desc := roachpb.RangeDescriptor{}
			desc.SetReplicas(roachpb.MakeReplicaSet(replicas))
			manual := hlc.NewManualClock(123)
			clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

			prevLease := roachpb.Lease{
				Replica:  replicas[0],
				Sequence: 1,
			}
			nextLease := roachpb.Lease{
				Replica: replicas[1],
				Start:   clock.NowAsClockTimestamp(),
			}
			if epoch {
				nextLease.Epoch = 1
			} else {
				exp := nextLease.Start.ToTimestamp().Add(9*time.Second.Nanoseconds(), 0)
				nextLease.Expiration = &exp
			}

			var maxPriorReadTS hlc.Timestamp
			if servedFutureReads {
				maxPriorReadTS = nextLease.Start.ToTimestamp().Add(1*time.Second.Nanoseconds(), 0)
			} else {
				maxPriorReadTS = nextLease.Start.ToTimestamp().Add(-2*time.Second.Nanoseconds(), 0)
			}
			currentReadSummary := rspb.FromTimestamp(maxPriorReadTS)

			evalCtx := &MockEvalCtx{
				ClusterSettings:    cluster.MakeTestingClusterSettings(),
				StoreID:            1,
				Desc:               &desc,
				Clock:              clock,
				Lease:              prevLease,
				CurrentReadSummary: currentReadSummary,
			}
			cArgs := CommandArgs{
				EvalCtx: evalCtx.EvalContext(),
				Args: &roachpb.TransferLeaseRequest{
					Lease:     nextLease,
					PrevLease: prevLease,
				},
			}

			manual.Increment(1000)
			beforeEval := clock.NowAsClockTimestamp()

			res, err := TransferLease(ctx, batch, cArgs, nil)
			require.NoError(t, err)

			// The proposed lease start time should be assigned at eval time.
			propLease := res.Replicated.State.Lease
			require.NotNil(t, propLease)
			require.True(t, nextLease.Start.Less(propLease.Start))
			require.True(t, beforeEval.Less(propLease.Start))
			require.Equal(t, prevLease.Sequence+1, propLease.Sequence)

			// The previous lease should have been revoked.
			require.Equal(t, prevLease.Sequence, evalCtx.RevokedLeaseSeq)

			// The prior read summary should reflect the maximum read times
			// served under the current leaseholder.
			propReadSum, err := readsummary.Load(ctx, batch, desc.RangeID)
			require.NoError(t, err)
			require.NotNil(t, propReadSum, "should write prior read summary")
			if servedFutureReads {
				require.Equal(t, maxPriorReadTS, propReadSum.Local.LowWater)
				require.Equal(t, maxPriorReadTS, propReadSum.Global.LowWater)
			} else {
				require.Equal(t, propLease.Start.ToTimestamp(), propReadSum.Local.LowWater)
				require.Equal(t, propLease.Start.ToTimestamp(), propReadSum.Global.LowWater)
			}
		})
	})
}

func TestCheckCanReceiveLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		leaseholderType roachpb.ReplicaType
		eligible        bool
	}{
		{leaseholderType: roachpb.VOTER_FULL, eligible: true},
		{leaseholderType: roachpb.VOTER_INCOMING, eligible: false},
		{leaseholderType: roachpb.VOTER_OUTGOING, eligible: false},
		{leaseholderType: roachpb.VOTER_DEMOTING_LEARNER, eligible: false},
		{leaseholderType: roachpb.VOTER_DEMOTING_NON_VOTER, eligible: false},
		{leaseholderType: roachpb.LEARNER, eligible: false},
		{leaseholderType: roachpb.NON_VOTER, eligible: false},
	} {
		t.Run(tc.leaseholderType.String(), func(t *testing.T) {
			repDesc := roachpb.ReplicaDescriptor{
				ReplicaID: 1,
				Type:      &tc.leaseholderType,
			}
			rngDesc := roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{repDesc},
			}
			err := roachpb.CheckCanReceiveLease(rngDesc.InternalReplicas[0], &rngDesc)
			require.Equal(t, tc.eligible, err == nil, "err: %v", err)
		})
	}

	t.Run("replica not in range desc", func(t *testing.T) {
		repDesc := roachpb.ReplicaDescriptor{ReplicaID: 1}
		rngDesc := roachpb.RangeDescriptor{}
		require.Regexp(t, "replica.*not found", roachpb.CheckCanReceiveLease(repDesc, &rngDesc))
	})
}
