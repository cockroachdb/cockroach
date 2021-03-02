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
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestBumpSideTransportClosed tests the various states that a replica can find
// itself in when its TestBumpSideTransportClosed is called. It verifies that
// the method only returns successfully if it can bump its closed timestamp to
// the target.
func TestBumpSideTransportClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
		exp  bool
		// Optional, to configure testing filters.
		knobs func() (_ *kvserver.StoreTestingKnobs, filterC chan chan struct{})
		// Configures the replica to test different situtations.
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
					ok, _, _ := a.repl.BumpSideTransportClosed(ctx, a.now, targets)
					if !ok {
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
					ok, _, _ := a.repl.BumpSideTransportClosed(ctx, a.now, targets)
					if !ok {
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
					ok, _, _ := a.repl.BumpSideTransportClosed(ctx, a.now, targets)
					if !ok {
						return errors.New("bumping side-transport unexpectedly failed")
					}
					return nil
				})
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
			var targets [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp
			target := now.ToTimestamp()
			targets[roachpb.LAG_BY_CLUSTER_SETTING] = target

			// Run the setup function to get the replica in the desired state.
			unblockFilterC, asyncErrC, err := test.setup(setupArgs{
				tc:        tc,
				leftDesc:  leftDesc,
				rightDesc: rightDesc,
				repl:      repl,
				now:       now,
				target:    target,
				filterC:   filterC,
			})
			require.NoError(t, err)

			// Try to bump the closed timestamp. Use succeeds soon if we are
			// expecting the call to succeed, to avoid any flakiness. Don't do
			// so if we expect the call to fail, in which case any flakiness
			// would be a serious bug.
			if test.exp {
				testutils.SucceedsSoon(t, func() error {
					ok, _, _ := repl.BumpSideTransportClosed(ctx, now, targets)
					if !ok {
						return errors.New("bumping side-transport unexpectedly failed")
					}
					return nil
				})
			} else {
				ok, _, _ := repl.BumpSideTransportClosed(ctx, now, targets)
				require.False(t, ok)
			}

			// Clean up, if necessary.
			if unblockFilterC != nil {
				close(unblockFilterC)
				require.NoError(t, <-asyncErrC)
			}
		})
	}
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
		ok, _, _ := r.BumpSideTransportClosed(ctx, now, targets)
		if !ok {
			b.Fatal("BumpSideTransportClosed unexpectedly failed")
		}
	}
}
