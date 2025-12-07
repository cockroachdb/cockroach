// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package allocatorimpl

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/mmaintegration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// CreateTestAllocator creates a stopper, gossip, store pool and allocator for
// use in tests. Stopper must be stopped by the caller.
func CreateTestAllocator(
	ctx context.Context, numNodes int, deterministic bool,
) (*stop.Stopper, *gossip.Gossip, *storepool.StorePool, Allocator, *timeutil.ManualTime) {
	return CreateTestAllocatorWithKnobs(ctx, numNodes, deterministic,
		nil /* allocator.TestingKnobs */, nil /* mmaintegration.TestingKnobs */)
}

// CreateTestAllocatorWithKnobs is like `CreateTestAllocator`, but allows the
// caller to pass in custom TestingKnobs. Stopper must be stopped by
// the caller.
func CreateTestAllocatorWithKnobs(
	ctx context.Context,
	numNodes int,
	deterministic bool,
	allocatorKnobs *allocator.TestingKnobs,
	allocSyncKnobs *mmaintegration.TestingKnobs,
) (*stop.Stopper, *gossip.Gossip, *storepool.StorePool, Allocator, *timeutil.ManualTime) {
	st := cluster.MakeTestingClusterSettings()
	stopper, g, manual, storePool, _ := storepool.CreateTestStorePool(ctx, st,
		liveness.TestTimeUntilNodeDeadOff, deterministic,
		func() int { return numNodes },
		livenesspb.NodeLivenessStatus_LIVE)
	mmAllocator := mmaprototype.NewAllocatorState(timeutil.DefaultTimeSource{}, rand.New(rand.NewSource(timeutil.Now().UnixNano())))
	as := mmaintegration.NewAllocatorSync(storePool, mmAllocator, st, allocSyncKnobs)
	a := MakeAllocator(st, as, deterministic, func(id roachpb.NodeID) (time.Duration, bool) {
		return 0, true
	}, allocatorKnobs)
	return stopper, g, storePool, a, manual
}
