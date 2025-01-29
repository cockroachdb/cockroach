// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestLeaseCommandLearnerReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const voterStoreID, learnerStoreID roachpb.StoreID = 1, 2
	replicas := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: voterStoreID, Type: roachpb.VOTER_FULL, ReplicaID: 1},
		{NodeID: 2, StoreID: learnerStoreID, Type: roachpb.LEARNER, ReplicaID: 2},
	}
	desc := roachpb.RangeDescriptor{}
	desc.SetReplicas(roachpb.MakeReplicaSet(replicas))
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))
	st := cluster.MakeTestingClusterSettings()
	cArgs := CommandArgs{
		EvalCtx: (&MockEvalCtx{
			ClusterSettings: st,
			StoreID:         voterStoreID,
			Desc:            &desc,
			Clock:           clock,
			ConcurrencyManager: concurrency.NewManager(concurrency.Config{
				NodeDesc:       &roachpb.NodeDescriptor{NodeID: 1},
				RangeDesc:      &desc,
				Settings:       st,
				Clock:          clock,
				IntentResolver: &noopIntentResolver{},
				TxnWaitMetrics: txnwait.NewMetrics(time.Minute),
			}),
		}).EvalContext(),
		Args: &kvpb.TransferLeaseRequest{
			Lease: roachpb.Lease{
				Replica: replicas[1],
			},
		},
	}

	// Learners are not allowed to become leaseholders for now, see the comments
	// in TransferLease and RequestLease.
	_, err := TransferLease(ctx, nil, cArgs, nil)
	require.EqualError(t, err, `lease target replica cannot hold lease`)

	cArgs.Args = &kvpb.RequestLeaseRequest{}
	_, err = RequestLease(ctx, nil, cArgs, nil)

	const expForUnknown = `cannot replace lease <empty> with <empty>: ` +
		`lease target replica not found in RangeDescriptor`
	require.EqualError(t, err, expForUnknown)

	cArgs.Args = &kvpb.RequestLeaseRequest{
		Lease: roachpb.Lease{
			Replica:         replicas[1],
			AcquisitionType: roachpb.LeaseAcquisitionType_Request,
		},
	}
	_, err = RequestLease(ctx, nil, cArgs, nil)

	const expForLearner = `cannot replace lease <empty> ` +
		`with repl=(n2,s2):2LEARNER seq=0 start=0,0 exp=<nil> pro=0,0 acq=Request: ` +
		`lease target replica cannot hold lease`
	require.EqualError(t, err, expForLearner)
}

// TestLeaseTransferForwardsStartTime tests that during a lease transfer, the
// start time of the new lease is determined during evaluation, after latches
// have granted the lease transfer full mutual exclusion over the leaseholder.
func TestLeaseTransferForwardsStartTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(), func(t *testing.T, leaseType roachpb.LeaseType) {
		testutils.RunTrueAndFalse(t, "served-future-reads", func(t *testing.T, servedFutureReads bool) {
			ctx := context.Background()
			db := storage.NewDefaultInMemForTesting()
			defer db.Close()
			batch := db.NewBatch()
			defer batch.Close()

			replicas := []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, Type: roachpb.VOTER_FULL, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, Type: roachpb.VOTER_FULL, ReplicaID: 2},
			}
			desc := roachpb.RangeDescriptor{}
			desc.SetReplicas(roachpb.MakeReplicaSet(replicas))
			manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
			clock := hlc.NewClockForTesting(manual)

			prevLease := roachpb.Lease{
				Replica:  replicas[0],
				Sequence: 1,
			}
			now := clock.NowAsClockTimestamp()
			nextLease := roachpb.Lease{
				Replica:    replicas[1],
				ProposedTS: now,
				Start:      now,
				Sequence:   prevLease.Sequence + 1,
			}
			switch leaseType {
			case roachpb.LeaseExpiration:
				exp := nextLease.Start.ToTimestamp().Add(9*time.Second.Nanoseconds(), 0)
				nextLease.Expiration = &exp
			case roachpb.LeaseEpoch:
				nextLease.Epoch = 1
			case roachpb.LeaseLeader:
				nextLease.Term = 1
			default:
				t.Fatalf("unexpected lease type: %s", leaseType)
			}

			var maxPriorReadTS hlc.Timestamp
			if servedFutureReads {
				maxPriorReadTS = nextLease.Start.ToTimestamp().Add(1*time.Second.Nanoseconds(), 0)
			} else {
				maxPriorReadTS = nextLease.Start.ToTimestamp().Add(-2*time.Second.Nanoseconds(), 0)
			}
			currentReadSummary := rspb.FromTimestamp(maxPriorReadTS.Add(-100, 0))
			currentReadSummary.Local.AddReadSpan(rspb.ReadSpan{
				Key:       keys.MakeRangeKeyPrefix([]byte("a")),
				Timestamp: maxPriorReadTS,
			})
			currentReadSummary.Global.AddReadSpan(rspb.ReadSpan{
				Key:       roachpb.Key("a"),
				Timestamp: maxPriorReadTS,
			})
			st := cluster.MakeTestingClusterSettings()
			evalCtx := &MockEvalCtx{
				ClusterSettings:    st,
				StoreID:            1,
				Desc:               &desc,
				Clock:              clock,
				CurrentReadSummary: currentReadSummary,
				ConcurrencyManager: concurrency.NewManager(concurrency.Config{
					NodeDesc:       &roachpb.NodeDescriptor{NodeID: 1},
					RangeDesc:      &desc,
					Settings:       st,
					Clock:          clock,
					IntentResolver: &noopIntentResolver{},
					TxnWaitMetrics: txnwait.NewMetrics(time.Minute),
				}),
			}
			cArgs := CommandArgs{
				EvalCtx: evalCtx.EvalContext(),
				Args: &kvpb.TransferLeaseRequest{
					Lease:     nextLease,
					PrevLease: prevLease,
				},
			}

			manual.Advance(1000)
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

			// The prior read summary should reflect the maximum read times served
			// under the current leaseholder, even if this time is below the proposed
			// lease start time.
			propReadSum := res.Replicated.PriorReadSummary
			require.NoError(t, err)
			require.NotNil(t, propReadSum, "should include prior read summary")
			require.Equal(t, currentReadSummary, *propReadSum)

			// The prior read summary should also be persisted, but compressed.
			persistReadSum, err := readsummary.Load(ctx, batch, desc.RangeID)
			require.NoError(t, err)
			require.NotNil(t, persistReadSum, "should persist prior read summary")
			require.NotEqual(t, currentReadSummary, *persistReadSum)
			require.Equal(t, rspb.FromTimestamp(maxPriorReadTS), *persistReadSum)
		})
	})
}

// TestLeaseTransferForwardsStartTime tests that during a lease transfer, the
// start time of the new lease is determined during evaluation, after latches
// have granted the lease transfer full mutual exclusion over the leaseholder.
func TestLeaseRequestTypeSwitchForwardsExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(), func(t *testing.T, leaseType roachpb.LeaseType) {
		testutils.RunTrueAndFalse(t, "revoke", func(t *testing.T, revoke bool) {
			ctx := context.Background()
			db := storage.NewDefaultInMemForTesting()
			defer db.Close()
			batch := db.NewBatch()
			defer batch.Close()

			replicas := []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, Type: roachpb.VOTER_FULL, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, Type: roachpb.VOTER_FULL, ReplicaID: 2},
			}
			desc := roachpb.RangeDescriptor{}
			desc.SetReplicas(roachpb.MakeReplicaSet(replicas))
			manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
			clock := hlc.NewClockForTesting(manual)
			const rangeLeaseDuration = 6 * time.Second

			prevLease := roachpb.Lease{
				Replica:  replicas[0],
				Sequence: 1,
			}
			now := clock.NowAsClockTimestamp()
			nowPlusExp := now.ToTimestamp().Add(rangeLeaseDuration.Nanoseconds(), 0)
			nextLease := roachpb.Lease{
				Replica:    replicas[0],
				ProposedTS: now,
				Start:      now,
				Sequence:   prevLease.Sequence + 1,
			}
			switch leaseType {
			case roachpb.LeaseExpiration:
				exp := nowPlusExp
				nextLease.Expiration = &exp
			case roachpb.LeaseEpoch:
				nextLease.Epoch = 1
			case roachpb.LeaseLeader:
				nextLease.Term = 1
			default:
				t.Fatalf("unexpected lease type: %s", leaseType)
			}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := &MockEvalCtx{
				ClusterSettings:    st,
				StoreID:            1,
				Desc:               &desc,
				Clock:              clock,
				RangeLeaseDuration: rangeLeaseDuration,
				ConcurrencyManager: concurrency.NewManager(concurrency.Config{
					NodeDesc:       &roachpb.NodeDescriptor{NodeID: 1},
					RangeDesc:      &desc,
					Settings:       st,
					Clock:          clock,
					IntentResolver: &noopIntentResolver{},
					TxnWaitMetrics: txnwait.NewMetrics(time.Minute),
				}),
			}
			cArgs := CommandArgs{
				EvalCtx: evalCtx.EvalContext(),
				Args: &kvpb.RequestLeaseRequest{
					Lease:                          nextLease,
					PrevLease:                      prevLease,
					RevokePrevAndForwardExpiration: revoke,
				},
			}

			manual.Advance(1000)
			beforeEval := clock.Now()

			res, err := RequestLease(ctx, batch, cArgs, nil)
			require.NoError(t, err)

			propLease := res.Replicated.State.Lease
			if revoke {
				// The previous lease should have been revoked.
				require.Equal(t, prevLease.Sequence, evalCtx.RevokedLeaseSeq)

				// The expiration of the new lease should have been forwarded.
				expExp := beforeEval.Add(int64(evalCtx.RangeLeaseDuration), 1)
				if leaseType == roachpb.LeaseExpiration {
					require.True(t, nowPlusExp.Less(propLease.GetExpiration()))
					require.Equal(t, expExp, propLease.GetExpiration())
				} else {
					require.Equal(t, expExp, propLease.MinExpiration)
				}
			} else {
				// The previous lease should NOT have been revoked.
				require.Zero(t, evalCtx.RevokedLeaseSeq)

				// The expiration of the new lease should NOT have been forwarded.
				if leaseType == roachpb.LeaseExpiration {
					require.Equal(t, nowPlusExp, propLease.GetExpiration())
				} else {
					require.Zero(t, propLease.MinExpiration)
				}
			}
		})
	})
}

func TestCheckCanReceiveLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const none = roachpb.ReplicaType(-1)

	for _, tc := range []struct {
		leaseholderType              roachpb.ReplicaType
		anotherReplicaType           roachpb.ReplicaType
		expIfWasLastLeaseholderTrue  bool
		expIfWasLastLeaseholderFalse bool
	}{
		{leaseholderType: roachpb.VOTER_FULL, anotherReplicaType: none, expIfWasLastLeaseholderTrue: true, expIfWasLastLeaseholderFalse: true},
		{leaseholderType: roachpb.VOTER_INCOMING, anotherReplicaType: none, expIfWasLastLeaseholderTrue: true, expIfWasLastLeaseholderFalse: true},

		// A VOTER_OUTGOING should only be able to get the lease if there's a VOTER_INCOMING and wasLastLeaseholderTrue.
		{leaseholderType: roachpb.VOTER_OUTGOING, anotherReplicaType: none, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},
		{leaseholderType: roachpb.VOTER_OUTGOING, anotherReplicaType: roachpb.VOTER_INCOMING, expIfWasLastLeaseholderTrue: true, expIfWasLastLeaseholderFalse: false},
		{leaseholderType: roachpb.VOTER_OUTGOING, anotherReplicaType: roachpb.VOTER_OUTGOING, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},
		{leaseholderType: roachpb.VOTER_OUTGOING, anotherReplicaType: roachpb.VOTER_FULL, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},

		// A VOTER_DEMOTING_LEARNER should only be able to get the lease if there's a VOTER_INCOMING and wasLastLeaseholderTrue.
		{leaseholderType: roachpb.VOTER_DEMOTING_LEARNER, anotherReplicaType: none, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},
		{leaseholderType: roachpb.VOTER_DEMOTING_LEARNER, anotherReplicaType: roachpb.VOTER_INCOMING, expIfWasLastLeaseholderTrue: true, expIfWasLastLeaseholderFalse: false},
		{leaseholderType: roachpb.VOTER_DEMOTING_LEARNER, anotherReplicaType: roachpb.VOTER_FULL, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},
		{leaseholderType: roachpb.VOTER_DEMOTING_LEARNER, anotherReplicaType: roachpb.VOTER_OUTGOING, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},

		// A VOTER_DEMOTING_NON_VOTER should only be able to get the lease if there's a VOTER_INCOMING.
		{leaseholderType: roachpb.VOTER_DEMOTING_NON_VOTER, anotherReplicaType: none, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},
		{leaseholderType: roachpb.VOTER_DEMOTING_NON_VOTER, anotherReplicaType: roachpb.VOTER_INCOMING, expIfWasLastLeaseholderTrue: true, expIfWasLastLeaseholderFalse: false},
		{leaseholderType: roachpb.VOTER_DEMOTING_NON_VOTER, anotherReplicaType: roachpb.VOTER_FULL, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},
		{leaseholderType: roachpb.VOTER_DEMOTING_NON_VOTER, anotherReplicaType: roachpb.VOTER_OUTGOING, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},

		{leaseholderType: roachpb.LEARNER, anotherReplicaType: none, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},
		{leaseholderType: roachpb.NON_VOTER, anotherReplicaType: none, expIfWasLastLeaseholderTrue: false, expIfWasLastLeaseholderFalse: false},
	} {
		t.Run(tc.leaseholderType.String(), func(t *testing.T) {
			repDesc := roachpb.ReplicaDescriptor{
				ReplicaID: 1,
				Type:      tc.leaseholderType,
			}
			rngDesc := roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{repDesc},
			}
			if tc.anotherReplicaType != none {
				anotherDesc := roachpb.ReplicaDescriptor{
					ReplicaID: 2,
					Type:      tc.anotherReplicaType,
				}
				rngDesc.InternalReplicas = append(rngDesc.InternalReplicas, anotherDesc)
			}
			err := roachpb.CheckCanReceiveLease(rngDesc.InternalReplicas[0], rngDesc.Replicas(), true)
			require.Equal(t, tc.expIfWasLastLeaseholderTrue, err == nil, "err: %v", err)

			err = roachpb.CheckCanReceiveLease(rngDesc.InternalReplicas[0], rngDesc.Replicas(), false)
			require.Equal(t, tc.expIfWasLastLeaseholderFalse, err == nil, "err: %v", err)
		})
	}

	t.Run("replica not in range desc", func(t *testing.T) {
		repDesc := roachpb.ReplicaDescriptor{ReplicaID: 1}
		rngDesc := roachpb.RangeDescriptor{}
		require.Regexp(t, "replica.*not found", roachpb.CheckCanReceiveLease(repDesc,
			rngDesc.Replicas(), true))
	})
}
