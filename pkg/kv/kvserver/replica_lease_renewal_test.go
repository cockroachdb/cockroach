// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestLeaseRenewer tests that the store lease renewer correctly tracks and
// extends expiration-based leases.
func TestLeaseRenewer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// stressrace and deadlock make the test too slow, resulting in an inability
	// to maintain leases and Raft leadership.
	skip.UnderStressRace(t)
	skip.UnderDeadlock(t)

	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			RaftConfig: base.RaftConfig{
				// Speed up lease extensions to speed up the test, but adjust tick-based
				// timeouts to retain their default wall-time values.
				RangeLeaseRenewalFraction: 0.95,
				RaftTickInterval:          100 * time.Millisecond,
				RaftElectionTimeoutTicks:  30,
			},
			Knobs: base.TestingKnobs{
				Store: &StoreTestingKnobs{
					LeaseRenewalDurationOverride: 100 * time.Millisecond,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	require.NoError(t, tc.WaitForFullReplication())

	lookupNode := func(nodeID roachpb.NodeID) int {
		for i := 0; i < tc.NumServers(); i++ {
			if tc.Server(i).NodeID() == nodeID {
				return i
			}
		}
		t.Fatalf("couldn't look up node %d", nodeID)
		return 0
	}

	getNodeStore := func(nodeID roachpb.NodeID) *Store {
		srv := tc.Server(lookupNode(nodeID))
		s, err := srv.GetStores().(*Stores).GetStore(srv.GetFirstStoreID())
		require.NoError(t, err)
		return s
	}

	getNodeReplica := func(nodeID roachpb.NodeID, rangeID roachpb.RangeID) *Replica {
		repl, err := getNodeStore(nodeID).GetReplica(rangeID)
		require.NoError(t, err)
		return repl
	}

	getLeaseRenewers := func(rangeID roachpb.RangeID) []roachpb.NodeID {
		var renewers []roachpb.NodeID
		for i := 0; i < tc.NumServers(); i++ {
			nodeID := tc.Server(i).NodeID()
			if _, ok := getNodeStore(nodeID).renewableLeases.Load(int64(rangeID)); ok {
				renewers = append(renewers, nodeID)
			}
		}
		return renewers
	}

	// assertLeaseRenewal asserts that the given range has an expiration-based
	// lease, that it's eagerly extended, and only actively renewed by the
	// leaseholder.
	assertLeaseRenewal := func(rangeID roachpb.RangeID) {
		repl := getNodeReplica(1, rangeID)
		lease, _ := repl.GetLease()
		require.Equal(t, roachpb.LeaseExpiration, lease.Type())

		var extensions int
		require.Eventually(t, func() bool {
			newLease, _ := repl.GetLease()
			require.Equal(t, roachpb.LeaseExpiration, newLease.Type())
			if *newLease.Expiration != *lease.Expiration {
				extensions++
				lease = newLease
				t.Logf("r%d lease extended: %v", rangeID, lease)
			}

			renewers := getLeaseRenewers(repl.RangeID)
			renewedByLeaseholder := len(renewers) == 1 && renewers[0] == lease.Replica.NodeID
			if !renewedByLeaseholder {
				t.Logf("r%d renewers: %v", rangeID, renewers)
			}

			return extensions >= 3 && renewedByLeaseholder
		}, 20*time.Second, 100*time.Millisecond)
	}

	// The meta range should always be eagerly renewed.
	assertLeaseRenewal(tc.LookupRangeOrFatal(t, keys.MinKey).RangeID)

	// Split off an expiration-based range, and assert that the lease is extended.
	desc := tc.LookupRangeOrFatal(t, tc.ScratchRangeWithExpirationLease(t))
	assertLeaseRenewal(desc.RangeID)

	// Transfer the lease to a different leaseholder, and assert that the lease is
	// still extended. Wait for the split to apply on all nodes first.
	require.NoError(t, tc.WaitForFullReplication())
	lease, _ := getNodeReplica(1, desc.RangeID).GetLease()
	target := tc.Target(lookupNode(lease.Replica.NodeID%3 + 1))
	tc.TransferRangeLeaseOrFatal(t, desc, target)
	assertLeaseRenewal(desc.RangeID)

	// Merge the range back. This should unregister it from the lease renewer.
	require.NoError(t, tc.Server(0).DB().AdminMerge(ctx, desc.StartKey.AsRawKey().Prevish(16)))
	require.Eventually(t, func() bool {
		if renewers := getLeaseRenewers(desc.RangeID); len(renewers) > 0 {
			t.Logf("r%d renewers: %v", desc.RangeID, renewers)
			return false
		}
		return true
	}, 20*time.Second, 100*time.Millisecond)
}

func setupLeaseRenewerTest(
	ctx context.Context, t *testing.T, init func(*base.TestClusterArgs),
) (
	cycles *int32, /* atomic */
	_ serverutils.TestClusterInterface,
) {
	cycles = new(int32)
	var args base.TestClusterArgs
	args.ServerArgs.Knobs.Store = &StoreTestingKnobs{
		LeaseRenewalOnPostCycle: func() {
			atomic.AddInt32(cycles, 1)
		},
	}
	init(&args)
	tc := serverutils.StartNewTestCluster(t, 1, args)
	t.Cleanup(func() { tc.Stopper().Stop(ctx) })

	desc := tc.LookupRangeOrFatal(t, tc.ScratchRangeWithExpirationLease(t))
	srv := tc.Server(0)
	s, err := srv.GetStores().(*Stores).GetStore(srv.GetFirstStoreID())
	require.NoError(t, err)

	_, err = s.DB().Get(ctx, desc.StartKey)
	require.NoError(t, err)

	repl, err := s.GetReplica(desc.RangeID)
	require.NoError(t, err)

	// There's a lease since we just split off the range.
	lease, _ := repl.GetLease()
	require.Equal(t, s.NodeID(), lease.Replica.NodeID)

	return cycles, tc
}

func TestLeaseRenewerExtendsExpirationBasedLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("triggered", func(t *testing.T) {
		renewCh := make(chan struct{})
		cycles, tc := setupLeaseRenewerTest(ctx, t, func(args *base.TestClusterArgs) {
			args.ServerArgs.Knobs.Store.(*StoreTestingKnobs).LeaseRenewalSignalChan = renewCh
		})
		defer tc.Stopper().Stop(ctx)

		trigger := func() {
			// Need to signal the chan twice to make sure we see the effect
			// of at least the first iteration.
			for i := 0; i < 2; i++ {
				select {
				case renewCh <- struct{}{}:
				case <-time.After(10 * time.Second):
					t.Fatal("unable to send on renewal chan for 10s")
				}
			}
		}

		for i := 0; i < 5; i++ {
			trigger()
			n := atomic.LoadInt32(cycles)
			require.NotZero(t, n, "during cycle #%v", i+1)
			atomic.AddInt32(cycles, -n)
		}
	})

	t.Run("periodic", func(t *testing.T) {
		cycles, tc := setupLeaseRenewerTest(ctx, t, func(args *base.TestClusterArgs) {
			args.ServerArgs.Knobs.Store.(*StoreTestingKnobs).LeaseRenewalDurationOverride = 10 * time.Millisecond
		})
		defer tc.Stopper().Stop(ctx)

		testutils.SucceedsSoon(t, func() error {
			if n := atomic.LoadInt32(cycles); n < 5 {
				return errors.Errorf("saw only %d renewal cycles", n)
			}
			return nil
		})
	})
}
