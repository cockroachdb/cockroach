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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func setupLeaseRenewerTest(
	ctx context.Context, t *testing.T, init func(*base.TestClusterArgs),
) (
	cycles *int32, /* atomic */
	_ *testcluster.TestCluster,
) {
	cycles = new(int32)
	var args base.TestClusterArgs
	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		LeaseRenewalOnPostCycle: func() {
			atomic.AddInt32(cycles, 1)
		},
	}
	init(&args)
	tc := testcluster.StartTestCluster(t, 1, args)
	t.Cleanup(func() { tc.Stopper().Stop(ctx) })

	desc := tc.LookupRangeOrFatal(t, tc.ScratchRangeWithExpirationLease(t))
	s := tc.GetFirstStoreFromServer(t, 0)

	_, err := s.DB().Get(ctx, desc.StartKey)
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
			args.ServerArgs.Knobs.Store.(*kvserver.StoreTestingKnobs).LeaseRenewalSignalChan = renewCh
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
			args.ServerArgs.Knobs.Store.(*kvserver.StoreTestingKnobs).LeaseRenewalDurationOverride = 10 * time.Millisecond
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
