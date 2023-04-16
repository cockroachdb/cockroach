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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestLeaseRenewer tests that the store lease renewer or the Raft scheduler
// correctly tracks and extends expiration-based leases. The responsibility
// is given by kv.expiration_leases_only.enabled.
func TestLeaseRenewer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// stressrace and deadlock make the test too slow, resulting in an inability
	// to maintain leases and Raft leadership.
	skip.UnderStressRace(t)
	skip.UnderDeadlock(t)

	// When kv.expiration_leases_only.enabled is true, the Raft scheduler is
	// responsible for extensions, but we still track expiration leases for system
	// ranges in the store lease renewer in case the setting changes.
	testutils.RunTrueAndFalse(t, "kv.expiration_leases_only.enabled", func(t *testing.T, expOnly bool) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		ExpirationLeasesOnly.Override(ctx, &st.SV, expOnly)
		tc := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: st,
				// Speed up lease extensions to speed up the test, but adjust tick-based
				// timeouts to retain their default wall-time values.
				RaftConfig: base.RaftConfig{
					RangeLeaseRenewalFraction:  0.95,
					RaftTickInterval:           100 * time.Millisecond,
					RaftElectionTimeoutTicks:   20,
					RaftReproposalTimeoutTicks: 30,
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
			for idx, id := range tc.NodeIDs() {
				if id == nodeID {
					return idx
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
			for _, nodeID := range tc.NodeIDs() {
				if _, ok := getNodeStore(nodeID).renewableLeases.Load(int64(rangeID)); ok {
					renewers = append(renewers, nodeID)
				}
			}
			return renewers
		}

		// assertLeaseExtension asserts that the given range has an expiration-based
		// lease that is eagerly extended.
		assertLeaseExtension := func(rangeID roachpb.RangeID) {
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
				return extensions >= 3
			}, 20*time.Second, 100*time.Millisecond)
		}

		// assertLeaseUpgrade asserts that the range is eventually upgraded
		// to an epoch lease.
		assertLeaseUpgrade := func(rangeID roachpb.RangeID) {
			repl := getNodeReplica(1, rangeID)
			require.Eventually(t, func() bool {
				lease, _ := repl.GetLease()
				return lease.Type() == roachpb.LeaseEpoch
			}, 20*time.Second, 100*time.Millisecond)
		}

		// assertStoreLeaseRenewer asserts that the range is tracked by the store lease
		// renewer on the leaseholder.
		assertStoreLeaseRenewer := func(rangeID roachpb.RangeID) {
			repl := getNodeReplica(1, rangeID)
			require.Eventually(t, func() bool {
				lease, _ := repl.GetLease()
				renewers := getLeaseRenewers(rangeID)
				renewedByLeaseholder := len(renewers) == 1 && renewers[0] == lease.Replica.NodeID
				// If kv.expiration_leases_only.enabled is true, then the store lease
				// renewer is disabled -- it will still track the ranges in case the
				// setting changes, but won't detect the new lease and untrack them as
				// long as it has a replica. We therefore allow multiple nodes to track
				// it, as long as the leaseholder is one of them.
				if expOnly {
					for _, renewer := range renewers {
						if renewer == lease.Replica.NodeID {
							renewedByLeaseholder = true
							break
						}
					}
				}
				if !renewedByLeaseholder {
					t.Logf("r%d renewers: %v", rangeID, renewers)
				}
				return renewedByLeaseholder
			}, 20*time.Second, 100*time.Millisecond)
		}

		// assertNoStoreLeaseRenewer asserts that the range is not tracked by any
		// lease renewer.
		assertNoStoreLeaseRenewer := func(rangeID roachpb.RangeID) {
			require.Eventually(t, func() bool {
				renewers := getLeaseRenewers(rangeID)
				if len(renewers) > 0 {
					t.Logf("r%d renewers: %v", rangeID, renewers)
					return false
				}
				return true
			}, 20*time.Second, 100*time.Millisecond)
		}

		// The meta range should always be eagerly renewed.
		firstRangeID := tc.LookupRangeOrFatal(t, keys.MinKey).RangeID
		assertLeaseExtension(firstRangeID)
		assertStoreLeaseRenewer(firstRangeID)

		// Split off an expiration-based range, and assert that the lease is extended.
		desc := tc.LookupRangeOrFatal(t, tc.ScratchRangeWithExpirationLease(t))
		assertLeaseExtension(desc.RangeID)
		assertStoreLeaseRenewer(desc.RangeID)

		// Transfer the lease to a different leaseholder, and assert that the lease is
		// still extended. Wait for the split to apply on all nodes first.
		require.NoError(t, tc.WaitForFullReplication())
		lease, _ := getNodeReplica(1, desc.RangeID).GetLease()
		target := tc.Target(lookupNode(lease.Replica.NodeID%3 + 1))
		tc.TransferRangeLeaseOrFatal(t, desc, target)
		assertLeaseExtension(desc.RangeID)
		assertStoreLeaseRenewer(desc.RangeID)

		// Merge the range back. This should unregister it from the lease renewer.
		require.NoError(t, tc.Server(0).DB().AdminMerge(ctx, desc.StartKey.AsRawKey().Prevish(16)))
		assertNoStoreLeaseRenewer(desc.RangeID)

		// Split off a regular non-system range. This should only be eagerly
		// extended if kv.expiration_leases_only.enabled is true, and should never
		// be tracked by the store lease renewer (which only handles system ranges).
		desc = tc.LookupRangeOrFatal(t, tc.ScratchRange(t))
		if expOnly {
			assertLeaseExtension(desc.RangeID)
		} else {
			assertLeaseUpgrade(desc.RangeID)
		}
		assertNoStoreLeaseRenewer(desc.RangeID)
	})
}

func setupLeaseRenewerTest(
	ctx context.Context, t *testing.T, init func(*base.TestClusterArgs),
) (
	cycles *int32, /* atomic */
	_ serverutils.TestClusterInterface,
) {
	st := cluster.MakeTestingClusterSettings()
	ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism

	cycles = new(int32)
	var args base.TestClusterArgs
	args.ServerArgs.Settings = st
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
