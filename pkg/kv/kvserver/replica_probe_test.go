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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReplicaProbeRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}
	var seen struct {
		syncutil.Mutex
		m           map[roachpb.StoreID]int
		injectedErr *roachpb.Error
	}
	seen.m = map[roachpb.StoreID]int{}
	filter := func(args kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
		if !args.IsProbe {
			return 0, args.ForcedError
		}
		if args.ForcedError == nil {
			t.Error("probe has no forcedError") // avoid Fatal on goroutine
			return 0, args.ForcedError
		}
		seen.Lock()
		defer seen.Unlock()
		seen.m[args.StoreID]++
		if seen.injectedErr != nil {
			return 0, seen.injectedErr
		}
		return 0, args.ForcedError
	}
	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		// We set an ApplyFilter even though the probe should never
		// show up there (since it always catches a forced error),
		// precisely to ensure that it doesn't.
		TestingApplyCalledTwiceFilter: filter,
		// This is the main workhorse that counts probes and injects
		// errors.
		TestingApplyForcedErrFilter: filter,
	}
	tc := testcluster.StartTestCluster(t, 3 /* nodes */, args)
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	desc := tc.LookupRangeOrFatal(t, k)
	// Establish configuration [LEASEHOLDER FOLLOWER NONVOTER].
	tc.AddVotersOrFatal(t, k, tc.Target(1))
	tc.AddNonVotersOrFatal(t, k, tc.Target(2))

	probeReq := &roachpb.ProbeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: k,
		},
	}
	// Sanity check that ProbeRequest is fit for sending through the entire KV
	// stack, with both routing policies.
	for _, srv := range tc.Servers {
		db := srv.DB()
		{
			var b kv.Batch
			b.AddRawRequest(probeReq)
			b.Header.RoutingPolicy = roachpb.RoutingPolicy_LEASEHOLDER
			require.NoError(t, db.Run(ctx, &b))
		}
		{
			var b kv.Batch
			b.AddRawRequest(probeReq)
			b.Header.RoutingPolicy = roachpb.RoutingPolicy_NEAREST
			require.NoError(t, db.Run(ctx, &b))
		}
	}
	// Check expected number of probes seen on each Replica in the apply loop.
	// Each Replica must see every probe, since the probe is replicated and we
	// don't expect truncations here.
	// There could be more due to reproposals. If there are fewer, some probes
	// returned success but didn't actually go through raft (for example because
	// they short-circuited due to being no-op, which of course we're careful
	// to avoid).
	testutils.SucceedsSoon(t, func() error {
		seen.Lock()
		defer seen.Unlock()
		if exp, act := len(seen.m), len(tc.Servers); exp != act {
			return errors.Errorf("waiting for stores to apply command: %d/%d", act, exp)
		}
		n := 2 * len(tc.Servers) // sent two probes per server
		for storeID, count := range seen.m {
			if count < n {
				return errors.Errorf("saw only %d probes on s%d", count, storeID)
			}
		}
		return nil
	})

	// We can also probe directly at each Replica. This is the intended use case
	// for Replica-level circuit breakers (#33007).
	for _, srv := range tc.Servers {
		repl, _, err := srv.Stores().GetReplicaForRangeID(ctx, desc.RangeID)
		require.NoError(t, err)
		ba := &roachpb.BatchRequest{}
		ba.Add(probeReq)
		ba.Timestamp = srv.Clock().Now()
		_, pErr := repl.Send(ctx, ba)
		require.NoError(t, pErr.GoError())
	}

	// If the probe applies with any nonstandard forced error, we get the error
	// back. Not sure what other error might occur in practice, but checking this
	// anyway gives us extra confidence in the implementation mechanics of this
	// request.
	injErr := roachpb.NewErrorf("bang")
	seen.Lock()
	seen.injectedErr = injErr
	seen.Unlock()
	for _, srv := range tc.Servers {
		repl, _, err := srv.Stores().GetReplicaForRangeID(ctx, desc.RangeID)
		require.NoError(t, err)
		ba := &roachpb.BatchRequest{}
		ba.Timestamp = srv.Clock().Now()
		ba.Add(probeReq)
		_, pErr := repl.Send(ctx, ba)
		require.True(t, errors.Is(pErr.GoError(), injErr.GoError()), "%+v", pErr.GoError())
	}
}
