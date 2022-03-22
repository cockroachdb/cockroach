// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestBumpSideTransportClosed tests the various states that a replica can find
// itself in when its BumpSideTransportClosed is called. It verifies that the
// method only returns successfully if it can bump its closed timestamp to the
// target.
func TestBumpSideTransportClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)

	ctx := context.Background()

	type setupArgs struct {
		tc                  *testcluster.TestCluster
		leftDesc, rightDesc roachpb.RangeDescriptor
		repl                *kvserver.Replica
		now                 hlc.ClockTimestamp
		target              hlc.Timestamp
		filterC             chan chan struct{}
	}
	testCases := []struct {
		name string
		// exp controls whether the BumpSideTransportClosed is expected to succeed.
		exp bool
		// computeTarget controls what timestamp the test will try to close. If not
		// set, the test will try to close the current time.
		computeTarget func(r *kvserver.Replica) (target hlc.Timestamp, exp bool)

		// Optional, to configure testing filters.
		knobs func() (_ *kvserver.StoreTestingKnobs, filterC chan chan struct{})
		// Configures the replica to test different situations.
		setup func(_ setupArgs) (unblockFilterC chan struct{}, asyncErrC chan error, _ error)
	}{
		{
			name: "basic",
			exp:  true,
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Nothing going on.
				return nil, nil, nil
			},
		},
		{
			name: "replica destroyed",
			exp:  false,
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Merge the range away to destroy it.
				_, err := a.tc.Server(0).MergeRanges(a.leftDesc.StartKey.AsRawKey())
				return nil, nil, err
			},
		},
		{
			name: "lease invalid",
			exp:  false,
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Revoke the range's lease to prevent it from being valid.
				l, _ := a.repl.GetLease()
				a.repl.RevokeLease(ctx, l.Sequence)
				return nil, nil, nil
			},
		},
		{
			name: "lease owned elsewhere",
			exp:  false,
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Transfer the range's lease.
				return nil, nil, a.tc.TransferRangeLease(a.rightDesc, a.tc.Target(1))
			},
		},
		{
			name: "merge in progress",
			exp:  false,
			knobs: func() (*kvserver.StoreTestingKnobs, chan chan struct{}) {
				mergeC := make(chan chan struct{})
				testingResponseFilter := func(ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
					if ba.IsSingleSubsumeRequest() {
						unblockC := make(chan struct{})
						mergeC <- unblockC
						<-unblockC
					}
					return nil
				}
				return &kvserver.StoreTestingKnobs{TestingResponseFilter: testingResponseFilter}, mergeC
			},
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Initiate a range merge and pause it after subsumption.
				errC := make(chan error, 1)
				_ = a.tc.Stopper().RunAsyncTask(ctx, "merge", func(context.Context) {
					_, err := a.tc.Server(0).MergeRanges(a.leftDesc.StartKey.AsRawKey())
					errC <- err
				})
				unblockFilterC := <-a.filterC
				return unblockFilterC, errC, nil
			},
		},
		{
			name: "raft application in progress",
			exp:  false,
			knobs: func() (*kvserver.StoreTestingKnobs, chan chan struct{}) {
				applyC := make(chan chan struct{})
				var once sync.Once // ignore reproposals
				testingApplyFilter := func(filterArgs kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
					if filterArgs.Req != nil && filterArgs.Req.IsSingleRequest() {
						put := filterArgs.Req.Requests[0].GetPut()
						if put != nil && put.Key.Equal(roachpb.Key("key_filter")) {
							once.Do(func() {
								unblockC := make(chan struct{})
								applyC <- unblockC
								<-unblockC
							})
						}
					}
					return 0, nil
				}
				return &kvserver.StoreTestingKnobs{TestingApplyFilter: testingApplyFilter}, applyC
			},
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Initiate a Raft proposal and pause it during application.
				errC := make(chan error, 1)
				_ = a.tc.Stopper().RunAsyncTask(ctx, "write", func(context.Context) {
					errC <- a.tc.Server(0).DB().Put(ctx, "key_filter", "val")
				})
				unblockFilterC := <-a.filterC
				return unblockFilterC, errC, nil
			},
		},
		{
			name: "evaluating request below closed timestamp target",
			exp:  false,
			knobs: func() (*kvserver.StoreTestingKnobs, chan chan struct{}) {
				proposeC := make(chan chan struct{})
				testingProposalFilter := func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
					if args.Req.IsSingleRequest() {
						put := args.Req.Requests[0].GetPut()
						if put != nil && put.Key.Equal(roachpb.Key("key_filter")) {
							unblockC := make(chan struct{})
							proposeC <- unblockC
							<-unblockC
						}
					}
					return nil
				}
				return &kvserver.StoreTestingKnobs{TestingProposalFilter: testingProposalFilter}, proposeC
			},
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Initiate a write and pause it during evaluation.
				errC := make(chan error, 1)
				_ = a.tc.Stopper().RunAsyncTask(ctx, "write", func(context.Context) {
					ts := a.target.Add(-1, 0)
					putArgs := putArgs(roachpb.Key("key_filter"), []byte("val"))
					sender := a.tc.Server(0).DB().NonTransactionalSender()
					_, pErr := kv.SendWrappedWith(ctx, sender, roachpb.Header{Timestamp: ts}, putArgs)
					errC <- pErr.GoError()
				})
				unblockFilterC := <-a.filterC
				return unblockFilterC, errC, nil
			},
		},
		{
			name: "evaluating request at closed timestamp target",
			exp:  false,
			knobs: func() (*kvserver.StoreTestingKnobs, chan chan struct{}) {
				proposeC := make(chan chan struct{})
				testingProposalFilter := func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
					if args.Req.IsSingleRequest() {
						put := args.Req.Requests[0].GetPut()
						if put != nil && put.Key.Equal(roachpb.Key("key_filter")) {
							unblockC := make(chan struct{})
							proposeC <- unblockC
							<-unblockC
						}
					}
					return nil
				}
				return &kvserver.StoreTestingKnobs{TestingProposalFilter: testingProposalFilter}, proposeC
			},
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Initiate a write and pause it during evaluation.
				errC := make(chan error, 1)
				_ = a.tc.Stopper().RunAsyncTask(ctx, "write", func(context.Context) {
					ts := a.target
					putArgs := putArgs(roachpb.Key("key_filter"), []byte("val"))
					sender := a.tc.Server(0).DB().NonTransactionalSender()
					_, pErr := kv.SendWrappedWith(ctx, sender, roachpb.Header{Timestamp: ts}, putArgs)
					errC <- pErr.GoError()
				})
				unblockFilterC := <-a.filterC
				return unblockFilterC, errC, nil
			},
		},
		{
			name: "evaluating request above closed timestamp target",
			exp:  true,
			knobs: func() (*kvserver.StoreTestingKnobs, chan chan struct{}) {
				proposeC := make(chan chan struct{})
				testingProposalFilter := func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
					if args.Req.IsSingleRequest() {
						put := args.Req.Requests[0].GetPut()
						if put != nil && put.Key.Equal(roachpb.Key("key_filter")) {
							unblockC := make(chan struct{})
							proposeC <- unblockC
							<-unblockC
						}
					}
					return nil
				}
				return &kvserver.StoreTestingKnobs{TestingProposalFilter: testingProposalFilter}, proposeC
			},
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Initiate a write and pause it during evaluation.
				errC := make(chan error, 1)
				_ = a.tc.Stopper().RunAsyncTask(ctx, "write", func(context.Context) {
					ts := a.target.Add(1, 0)
					putArgs := putArgs(roachpb.Key("key_filter"), []byte("val"))
					sender := a.tc.Server(0).DB().NonTransactionalSender()
					_, pErr := kv.SendWrappedWith(ctx, sender, roachpb.Header{Timestamp: ts}, putArgs)
					errC <- pErr.GoError()
				})
				unblockFilterC := <-a.filterC
				return unblockFilterC, errC, nil
			},
		},
		{
			name: "existing closed timestamp before",
			exp:  true,
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Manually bump the assigned closed timestamp to a time below
				// where the test will attempt to bump it to.
				var targets [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp
				targets[roachpb.LAG_BY_CLUSTER_SETTING] = a.target.Add(-1, 0)
				return nil, nil, testutils.SucceedsSoonError(func() error {
					res := a.repl.BumpSideTransportClosed(ctx, a.now, targets)
					if !res.OK {
						return errors.New("bumping side-transport unexpectedly failed")
					}
					return nil
				})
			},
		},
		{
			name: "existing closed timestamp equal",
			exp:  false,
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Manually bump the assigned closed timestamp to a time equal
				// to where the test will attempt to bump it to.
				var targets [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp
				targets[roachpb.LAG_BY_CLUSTER_SETTING] = a.target
				return nil, nil, testutils.SucceedsSoonError(func() error {
					res := a.repl.BumpSideTransportClosed(ctx, a.now, targets)
					if !res.OK {
						return errors.New("bumping side-transport unexpectedly failed")
					}
					return nil
				})
			},
		},
		{
			name: "existing closed timestamp above",
			exp:  false,
			setup: func(a setupArgs) (chan struct{}, chan error, error) {
				// Manually bump the assigned closed timestamp to a time above
				// where the test will attempt to bump it to.
				var targets [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp
				targets[roachpb.LAG_BY_CLUSTER_SETTING] = a.target.Add(1, 0)
				return nil, nil, testutils.SucceedsSoonError(func() error {
					res := a.repl.BumpSideTransportClosed(ctx, a.now, targets)
					if !res.OK {
						return errors.New("bumping side-transport unexpectedly failed")
					}
					return nil
				})
			},
		},
		{
			// We can't close all the way up to the lease expiration. See
			// propBuf.assignClosedTimestampAndLAIToProposalLocked.
			name: "close lease expiration",
			computeTarget: func(r *kvserver.Replica) (target hlc.Timestamp, exp bool) {
				ls := r.LeaseStatusAt(context.Background(), r.Clock().NowAsClockTimestamp())
				return ls.Expiration(), false
			},
		},
		{
			// Like above, but we can't even close in the same nanosecond as the lease
			// expiration (or previous nanosecond with Logical: MaxInt32).
			name: "close lease expiration prev",
			computeTarget: func(r *kvserver.Replica) (target hlc.Timestamp, exp bool) {
				ls := r.LeaseStatusAt(context.Background(), r.Clock().NowAsClockTimestamp())
				return ls.Expiration().Prev(), false
			},
		},
		{
			// Differently from above, we can close up to leaseExpiration.WallPrev.
			// Notably, we can close timestamps that fall inside the lease's stasis
			// period.
			name: "close lease expiration WallPrev",
			computeTarget: func(r *kvserver.Replica) (target hlc.Timestamp, exp bool) {
				ls := r.LeaseStatusAt(context.Background(), r.Clock().NowAsClockTimestamp())
				return ls.Expiration().WallPrev(), true
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			var knobs base.ModuleTestingKnobs
			var filterC chan chan struct{}
			if test.knobs != nil {
				knobs, filterC = test.knobs()
			}

			tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Store: knobs,
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			leftDesc, rightDesc, err := tc.SplitRange(roachpb.Key("key"))
			require.NoError(t, err)
			tc.AddVotersOrFatal(t, leftDesc.StartKey.AsRawKey(), tc.Target(1))
			tc.AddVotersOrFatal(t, rightDesc.StartKey.AsRawKey(), tc.Target(1))
			store := tc.GetFirstStoreFromServer(t, 0)
			require.NoError(t, err)
			repl := store.LookupReplica(rightDesc.StartKey)
			require.NotNil(t, repl)

			now := tc.Server(0).Clock().NowAsClockTimestamp()
			var target hlc.Timestamp
			var exp bool
			if test.computeTarget == nil {
				// We'll attempt to close `now`.
				target = now.ToTimestamp()
				exp = test.exp
			} else {
				target, exp = test.computeTarget(repl)
			}
			var targets [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp
			targets[roachpb.LAG_BY_CLUSTER_SETTING] = target

			// Run the setup function to get the replica in the desired state.
			var unblockFilterC chan struct{}
			var asyncErrC chan error
			if test.setup != nil {
				var err error
				unblockFilterC, asyncErrC, err = test.setup(setupArgs{
					tc:        tc,
					leftDesc:  leftDesc,
					rightDesc: rightDesc,
					repl:      repl,
					now:       now,
					target:    target,
					filterC:   filterC,
				})
				require.NoError(t, err)
			}

			// Try to bump the closed timestamp. Use succeeds soon if we are
			// expecting the call to succeed, to avoid any flakiness. Don't do
			// so if we expect the call to fail, in which case any flakiness
			// would be a serious bug.
			if exp {
				testutils.SucceedsSoon(t, func() error {
					res := repl.BumpSideTransportClosed(ctx, now, targets)
					if !res.OK {
						return errors.New("bumping side-transport unexpectedly failed")
					}
					return nil
				})
			} else {
				res := repl.BumpSideTransportClosed(ctx, now, targets)
				require.False(t, res.OK)
			}

			// Clean up, if necessary.
			if unblockFilterC != nil {
				close(unblockFilterC)
				require.NoError(t, <-asyncErrC)
			}
		})
	}
}

// Test that a lease proposal that gets rejected doesn't erroneously dictate the
// closed timestamp of further requests. If it would, then writes could violate
// that closed timestamp.
//
// NOTE: This test was written back when lease requests were closing the lease
// start time. This is no longer true; currently lease requests don't carry a
// closed timestamp. Still, we leave the test as a regression test.
//
// The tricky scenario tested is the following:
//
// 1. A lease held by rep1 is getting close to its expiration.
// 2. Rep1 begins the process of transferring its lease to rep2 with a start
//    time of 100.
// 3. The transfer goes slowly. From the perspective of rep2, the original lease
//    expires, so it begins acquiring a new lease with a start time of 200. The
//    lease acquisition is slow to propose.
// 4. The lease transfer finally applies. Rep2 is the new leaseholder and bumps
//    its tscache to 100.
// 5. Two writes start evaluating on rep2 under the new lease. They bump their
//    write timestamp to 100,1.
// 6. Rep2's lease acquisition from step 3 is proposed. Here's where the
//    regression that this test is protecting against comes in: if rep2 was to
//    mechanically bump its assignedClosedTimestamp to 200, that'd be incorrect
//    because there are in-flight writes at 100. If those writes get proposed
//    after the lease acquisition request, the second of them to get proposed
//    would violate the closed time carried by the first (see below).
// 7. The lease acquisition gets rejected below Raft because the previous lease
//    it asserts doesn't correspond to the lease that it applies under.
// 8. The two writes from step 5 are proposed. The closed timestamp that they
//    each carry has a lower bound of rep2.assignedClosedTimestmap. If this was
//    200, then the second one would violate the closed timestamp carried by the
//    first one - the first one says that 200 is closed, but then the second
//    tries to write at 100. Note that the first write is OK writing at 100 even
//    though it carries a closed timestamp of 200 - the closed timestamp carried
//    by a command only binds future commands.
//
// The test simulates the scenario and verifies that we don't crash with a
// closed timestamp violation assertion. We avoid the violation because, in step
// 6, the lease proposal doesn't bump the assignedClosedTimestamp.
func TestRejectedLeaseDoesntDictateClosedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// We're going to orchestrate the scenario by controlling the timing of the
	// lease transfer, the lease acquisition and the writes. Note that we'll block
	// the lease acquisition and the writes after they evaluate but before they
	// get proposed, but we'll block the lease transfer when it's just about to be
	// proposed, after it gets assigned the closed timestamp that it will carry.
	// We want it to carry a relatively low closed timestamp, so we want its
	// closed timestamp to be assigned before we bump the clock to expire the
	// original lease.

	// leaseTransferCh is used to block the lease transfer.
	leaseTransferCh := make(chan struct{})
	// leaseAcqCh is used to block the lease acquisition.
	leaseAcqCh := make(chan struct{})
	// writeCh is used to wait for the two writes to block.
	writeCh := make(chan struct{})
	// unblockWritesCh is used to unblock the two writes.
	unblockWritesCh := make(chan struct{})
	var writeKey1, writeKey2 atomic.Value
	// Initialize the atomics so they get bound to a specific type.
	writeKey1.Store(roachpb.Key{})
	writeKey2.Store(roachpb.Key{})
	var blockedRangeID int64
	var trappedLeaseAcquisition int64

	blockLeaseAcquisition := func(args kvserverbase.FilterArgs) {
		blockedRID := roachpb.RangeID(atomic.LoadInt64(&blockedRangeID))
		leaseReq, ok := args.Req.(*roachpb.RequestLeaseRequest)
		if !ok || args.Hdr.RangeID != blockedRID || leaseReq.Lease.Replica.NodeID != 2 {
			return
		}
		if atomic.CompareAndSwapInt64(&trappedLeaseAcquisition, 0, 1) {
			leaseAcqCh <- struct{}{}
			<-leaseAcqCh
		}
	}

	blockWrites := func(args kvserverbase.FilterArgs) {
		wk1 := writeKey1.Load().(roachpb.Key)
		wk2 := writeKey2.Load().(roachpb.Key)
		if put, ok := args.Req.(*roachpb.PutRequest); ok && (put.Key.Equal(wk1) || put.Key.Equal(wk2)) {
			writeCh <- struct{}{}
			<-unblockWritesCh
		}
	}

	blockTransfer := func(p *kvserver.ProposalData) {
		blockedRID := roachpb.RangeID(atomic.LoadInt64(&blockedRangeID))
		ba := p.Request
		if ba.RangeID != blockedRID {
			return
		}
		_, ok := p.Request.GetArg(roachpb.TransferLease)
		if !ok {
			return
		}
		leaseTransferCh <- struct{}{}
		<-leaseTransferCh
	}

	manual := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			RaftConfig: base.RaftConfig{
				// Disable preemptive lease extensions because, if the server startup
				// takes too long before we pause the clock, such an extension can
				// happen on the range of interest, and messes up the test that expects
				// the lease to expire.
				RangeLeaseRenewalFraction: -1,
				// Also make expiration-based leases last for a long time, as the test
				// wants a valid lease after cluster start.
				RaftElectionTimeoutTicks: 1000,
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ClockSource: manual.UnixNano,
				},
				Store: &kvserver.StoreTestingKnobs{
					DisableConsistencyQueue: true,
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						TestingPostEvalFilter: func(args kvserverbase.FilterArgs) *roachpb.Error {
							blockWrites(args)
							blockLeaseAcquisition(args)
							return nil
						},
					},
					TestingProposalSubmitFilter: func(p *kvserver.ProposalData) (drop bool, _ error) {
						blockTransfer(p)
						return false, nil
					},
				},
			},
		}})
	defer tc.Stopper().Stop(ctx)

	manual.Pause()
	// Upreplicate a range.
	n1, n2 := tc.Servers[0], tc.Servers[1]
	// One of the filters hardcodes a node id.
	require.Equal(t, roachpb.NodeID(2), n2.NodeID())
	key := tc.ScratchRangeWithExpirationLease(t)
	s1 := tc.GetFirstStoreFromServer(t, 0)
	t1, t2 := tc.Target(0), tc.Target(1)
	repl0 := s1.LookupReplica(keys.MustAddr(key))
	desc := *repl0.Desc()
	require.NotNil(t, repl0)
	tc.AddVotersOrFatal(t, key, t2)
	require.NoError(t, tc.WaitForVoters(key, t2))
	// Make sure the lease starts off on n1.
	lease, _ /* now */, err := tc.FindRangeLease(desc, &t1 /* hint */)
	require.NoError(t, err)
	require.Equal(t, n1.NodeID(), lease.Replica.NodeID)

	// Advance the time a bit. We'll then initiate a transfer, and we want the
	// transferred lease to be valid for a while after the original lease expires.
	remainingNanos := lease.GetExpiration().WallTime - manual.UnixNano()
	// NOTE: We don't advance the clock past the mid-point of the lease, otherwise
	// it gets extended.
	pause1 := remainingNanos / 3
	manual.Increment(pause1)

	// Start a lease transfer from n1 to n2. We'll block the proposal of the transfer for a while.
	atomic.StoreInt64(&blockedRangeID, int64(desc.RangeID))
	transferErrCh := make(chan error)
	go func() {
		transferErrCh <- tc.TransferRangeLease(desc, t2)
	}()
	defer func() {
		require.NoError(t, <-transferErrCh)
	}()
	// Wait for the lease transfer to evaluate and then block.
	<-leaseTransferCh
	// With the lease transfer still blocked, we now advance the clock beyond the
	// original lease's expiration and we make n2 try to acquire a lease. This
	// lease acquisition request will also be blocked.
	manual.Increment(remainingNanos - pause1 + 1)
	leaseAcqErrCh := make(chan error)
	go func() {
		r, _, err := n2.Stores().GetReplicaForRangeID(ctx, desc.RangeID)
		if err != nil {
			leaseAcqErrCh <- err
			return
		}
		_, err = r.TestingAcquireLease(ctx)
		leaseAcqErrCh <- err
	}()
	// Wait for the lease acquisition to be blocked.
	select {
	case <-leaseAcqCh:
	case err := <-leaseAcqErrCh:
		close(leaseTransferCh)
		t.Fatalf("lease request unexpectedly finished. err: %v", err)
	}
	// Let the previously blocked transfer succeed. n2's lease acquisition remains
	// blocked.
	close(leaseTransferCh)
	// Wait until n2 has applied the lease transfer.
	desc = *repl0.Desc()
	testutils.SucceedsSoon(t, func() error {
		li, _ /* now */, err := tc.FindRangeLeaseEx(ctx, desc, &t2 /* hint */)
		if err != nil {
			return err
		}
		lease = li.Current()
		if !lease.OwnedBy(n2.GetFirstStoreID()) {
			return errors.Errorf("n2 still unaware of its lease: %s", li.Current())
		}
		return nil
	})

	// Now we send two writes. We'll block them after evaluation. Then we'll
	// unblock the lease acquisition, let the respective command fail to apply,
	// and then we'll unblock the writes.
	err1 := make(chan error)
	err2 := make(chan error)
	go func() {
		writeKey1.Store(key)
		sender := n2.DB().NonTransactionalSender()
		pArgs := putArgs(key, []byte("test val"))
		_, pErr := kv.SendWrappedWith(ctx, sender, roachpb.Header{Timestamp: lease.Start.ToTimestamp()}, pArgs)
		err1 <- pErr.GoError()
	}()
	go func() {
		k := key.Next()
		writeKey2.Store(k)
		sender := n2.DB().NonTransactionalSender()
		pArgs := putArgs(k, []byte("test val2"))
		_, pErr := kv.SendWrappedWith(ctx, sender, roachpb.Header{Timestamp: lease.Start.ToTimestamp()}, pArgs)
		err2 <- pErr.GoError()
	}()
	// Wait for the writes to evaluate and block before proposal.
	<-writeCh
	<-writeCh

	// Unblock the lease acquisition.
	close(leaseAcqCh)
	if err := <-leaseAcqErrCh; err != nil {
		close(unblockWritesCh)
		t.Fatal(err)
	}

	// Now unblock the writes.
	close(unblockWritesCh)
	require.NoError(t, <-err1)
	require.NoError(t, <-err2)
	// Not crashing with a closed timestamp violation assertion marks the success
	// of this test.
}

// BenchmarkBumpSideTransportClosed measures the latency of a single call to
// (*Replica).BumpSideTransportClosed. The closed timestamp side-transport was
// designed with a performance expectation of this check taking no more than
// 100ns, so that calling the method on 10,000 leaseholders on a node could be
// done in less than 1ms.
//
// TODO(nvanbenschoten,andrei): Currently, the benchmark indicates that a call
// takes about 130ns. This exceeds the latency budget we've allocated to the
// call. However, it looks like there is some low-hanging fruit. 70% of the time
// is spent in leaseStatusForRequestRLocked, within which 24% of the total time
// is spent zeroing and copying memory and 30% of the total time is spent in
// (*NodeLiveness).GetLiveness, grabbing the current node's liveness record. If
// we eliminate some memory copying and pass the node liveness record in to the
// function so that we only have to grab it once instead of on each call to
// BumpSideTransportClosed, we should be able to reach our target latency.
func BenchmarkBumpSideTransportClosed(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	manual := hlc.NewHybridManualClock()
	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClockSource: manual.UnixNano,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	key, err := s.ScratchRange()
	require.NoError(b, err)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(b, err)
	rkey := keys.MustAddr(key)
	r := store.LookupReplica(rkey)
	require.NotNil(b, r)

	manual.Pause()
	now := s.Clock().NowAsClockTimestamp()
	var targets [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Advance time and the closed timestamp target.
		now = now.ToTimestamp().Add(1, 0).UnsafeToClockTimestamp()
		targets[roachpb.LAG_BY_CLUSTER_SETTING] = now.ToTimestamp()

		// Perform the call.
		res := r.BumpSideTransportClosed(ctx, now, targets)
		if !res.OK {
			b.Fatal("BumpSideTransportClosed unexpectedly failed")
		}
	}
}

// TestNonBlockingReadsAtResolvedTimestamp tests that reads served at or below a
// key span's resolved timestamp never block or redirect to the leaseholder. The
// test also verifies that the resolved timestamp on each replica increases
// monotonically.
func TestNonBlockingReadsAtResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testNonBlockingReadsWithReaderFn(t, func(
		store *kvserver.Store, rangeID roachpb.RangeID, keySpan roachpb.Span,
	) func(context.Context) error {
		// The reader queries the resolved timestamp and then reads at that time.
		var lastResTS hlc.Timestamp
		return func(ctx context.Context) error {
			// Query the key span's resolved timestamp.
			queryResTS := roachpb.QueryResolvedTimestampRequest{
				RequestHeader: roachpb.RequestHeaderFromSpan(keySpan),
			}
			queryResTSHeader := roachpb.Header{
				RangeID:         rangeID,
				ReadConsistency: roachpb.INCONSISTENT,
			}
			resp, pErr := kv.SendWrappedWith(ctx, store, queryResTSHeader, &queryResTS)
			if pErr != nil {
				return pErr.GoError()
			}

			// Validate that the resolved timestamp increases monotonically.
			resTS := resp.(*roachpb.QueryResolvedTimestampResponse).ResolvedTS
			if resTS.IsEmpty() {
				return errors.Errorf("empty resolved timestamp")
			}
			if resTS.Less(lastResTS) {
				return errors.Errorf("resolved timestamp regression: %s -> %s", lastResTS, resTS)
			}
			lastResTS = resTS

			// Issue a transactional scan over the keys at the resolved timestamp on
			// the same store. Use an error wait policy so that we'll hear an error
			// (WriteIntentError) under conditions that would otherwise cause us to
			// block on an intent. Send to a specific store instead of through a
			// DistSender so that we'll hear an error (NotLeaseholderError) if the
			// request would otherwise be redirected to the leaseholder.
			scan := roachpb.ScanRequest{
				RequestHeader: roachpb.RequestHeaderFromSpan(keySpan),
			}
			txn := roachpb.MakeTransaction("test", keySpan.Key, 0, resTS, 0, 0)
			scanHeader := roachpb.Header{
				RangeID:         rangeID,
				ReadConsistency: roachpb.CONSISTENT,
				Txn:             &txn,
				WaitPolicy:      lock.WaitPolicy_Error,
			}
			_, pErr = kv.SendWrappedWith(ctx, store, scanHeader, &scan)
			return pErr.GoError()
		}
	})
}

// TestNonBlockingReadsWithServerSideBoundedStalenessNegotiation tests that
// bounded staleness reads that hit the server-side negotiation fast-path (i.e.
// those that negotiate and execute in a single RPC) never block on conflicting
// intents or redirect to the leaseholder if run on follower replicas. For
// details, see (*Store).executeServerSideBoundedStalenessNegotiation.
//
// The test is a higher-level version of TestNonBlockingReadsAtResolvedTimestamp
// that exercises the server-side negotiation fast-path.
func TestNonBlockingReadsWithServerSideBoundedStalenessNegotiation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testNonBlockingReadsWithReaderFn(t, func(
		store *kvserver.Store, rangeID roachpb.RangeID, keySpan roachpb.Span,
	) func(context.Context) error {
		// The reader performs bounded-staleness reads that hit the server-side
		// negotiation fast-path.
		minTSBound := hlc.MinTimestamp
		var lastTS hlc.Timestamp
		return func(ctx context.Context) error {
			// Issue a bounded-staleness read (a read with a MinTimestampBound)
			// over the keys. Use an error wait policy so that we'll hear an error
			// (WriteIntentError) under conditions that would otherwise cause us
			// to block on an intent. Send to a specific store instead of through
			// a DistSender so that we'll hear an error (NotLeaseholderError) if
			// the request would otherwise be redirected to the leaseholder.
			var ba roachpb.BatchRequest
			ba.RangeID = rangeID
			ba.BoundedStaleness = &roachpb.BoundedStalenessHeader{
				MinTimestampBound:       minTSBound,
				MinTimestampBoundStrict: true,
			}
			ba.WaitPolicy = lock.WaitPolicy_Error
			ba.Add(&roachpb.ScanRequest{
				RequestHeader: roachpb.RequestHeaderFromSpan(keySpan),
			})
			br, pErr := store.Send(ctx, ba)
			if pErr != nil {
				return pErr.GoError()
			}

			// Validate that the bounded staleness timestamp increases monotonically
			// across attempts, since we're issuing bounded staleness reads to the
			// same replica.
			reqTS := br.Timestamp
			if reqTS.IsEmpty() {
				return errors.Errorf("empty bounded staleness timestamp")
			}
			if reqTS.Less(lastTS) {
				return errors.Errorf("bounded staleness timestamp regression: %s -> %s", lastTS, reqTS)
			}
			lastTS = reqTS

			// Forward the minimum timestamp bound for the next request. Once a
			// bounded staleness read has been served at a timestamp, future reads
			// should be able to be served at the same timestamp (or later) as long
			// as they are routed to the same replica.
			minTSBound = lastTS
			return nil
		}
	})
}

// testNonBlockingReadsWithReaderFn is shared between the preceding two tests.
// It sets up a three node cluster, launches a collection of writers writing to
// a set of keys, and uses the provided readerFnFactory to create a reader per
// node. The reader is invoked repeatedly.
func testNonBlockingReadsWithReaderFn(
	t *testing.T,
	readerFnFactory func(*kvserver.Store, roachpb.RangeID, roachpb.Span) func(context.Context) error,
) {
	const testTime = 1 * time.Second
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	// Create a scratch range. Then add one voting follower and one non-voting
	// follower to the range.
	scratchKey := tc.ScratchRange(t)
	scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
	tc.AddVotersOrFatal(t, scratchKey, tc.Target(1))
	tc.AddNonVotersOrFatal(t, scratchKey, tc.Target(2))

	// Drop the closed timestamp interval far enough so that we can create
	// situations where an active intent is at a lower timestamp than the
	// range's closed timestamp - thereby being the limiting factor for the
	// range's resolved timestamp.
	sqlRunner := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlRunner.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1ms'")

	keySet := make([]roachpb.Key, 5)
	for i := range keySet {
		n := len(scratchKey)
		keySet[i] = append(scratchKey[:n:n], byte(i))
	}
	keySpan := roachpb.Span{Key: scratchKey, EndKey: scratchKey.PrefixEnd()}

	var g errgroup.Group
	var done int32
	sleep := func() {
		time.Sleep(time.Duration(rand.Intn(2000)) * time.Microsecond)
	}

	// Writer goroutines: write intents and commit them.
	for _, key := range keySet {
		key := key // copy for goroutine
		g.Go(func() error {
			for ; atomic.LoadInt32(&done) == 0; sleep() {
				if err := tc.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					if _, err := txn.Inc(ctx, key, 1); err != nil {
						return err
					}
					// Let the intent stick around for a bit.
					sleep()
					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// Reader goroutines: run one reader per store.
	for _, s := range tc.Servers {
		store, err := s.Stores().GetStore(s.GetFirstStoreID())
		require.NoError(t, err)
		g.Go(func() error {
			readerFn := readerFnFactory(store, scratchRange.RangeID, keySpan)
			for ; atomic.LoadInt32(&done) == 0; sleep() {
				if err := readerFn(ctx); err != nil {
					return err
				}
			}
			return nil
		})
	}

	time.Sleep(testTime)
	atomic.StoreInt32(&done, 1)
	require.NoError(t, g.Wait())
}
