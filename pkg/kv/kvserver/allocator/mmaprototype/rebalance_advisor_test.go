// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestBuildMMARebalanceAdvisorUnknownStores documents the crash described in
// #170703: BuildMMARebalanceAdvisor panics with a nil pointer dereference if
// any storeID it is given (either `existing` or anything in `cands`) is not
// yet known to MMA's clusterState. This can happen during startup, when the
// caller (the legacy allocator) builds candidate lists from StorePool's
// gossip-driven view before MMA has been notified of the corresponding stores
// via SetStore.
//
// The assertions below pin the *current, broken* behavior so that the fix in
// the following commit can be reviewed as a clean diff: the require.Panics
// calls flip to require.NotPanics + a behavior check.
func TestBuildMMARebalanceAdvisorUnknownStores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const (
		knownStore   roachpb.StoreID = 1
		unknownStore roachpb.StoreID = 99
	)

	makeAllocator := func() *allocatorState {
		a := NewAllocatorState(timeutil.DefaultTimeSource{}, rand.New(rand.NewSource(0)))
		a.SetStore(StoreAttributesAndLocality{
			StoreID: knownStore,
			NodeID:  roachpb.NodeID(knownStore),
		})
		return a
	}

	t.Run("unknown cand", func(t *testing.T) {
		a := makeAllocator()
		require.Panics(t, func() {
			_ = a.BuildMMARebalanceAdvisor(ctx, knownStore, []roachpb.StoreID{unknownStore})
		})
	})

	t.Run("unknown existing", func(t *testing.T) {
		a := makeAllocator()
		require.Panics(t, func() {
			_ = a.BuildMMARebalanceAdvisor(ctx, unknownStore, nil)
		})
	})
}
