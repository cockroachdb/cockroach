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

// TestBuildMMARebalanceAdvisorUnknownStores covers the crash described in
// #170703: before the fix in this commit, BuildMMARebalanceAdvisor would
// nil-pointer-deref if any storeID it was given (either `existing` or
// anything in `cands`) was not yet known to MMA's clusterState. This
// happens during startup, when the caller (the legacy allocator) builds
// candidate lists from StorePool's gossip-driven view before MMA has been
// notified of the corresponding stores via SetStore.
//
// After the fix, unknown cands are silently dropped, and an unknown
// existing falls back to a no-op advisor (which always reports no
// conflict).
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
		// Unknown cand is silently dropped; the advisor is built over just
		// the known existing store.
		advisor := a.BuildMMARebalanceAdvisor(ctx, knownStore, []roachpb.StoreID{unknownStore})
		require.NotNil(t, advisor)
		require.False(t, advisor.disabled)
		require.Equal(t, knownStore, advisor.existingStoreID)
	})

	t.Run("unknown existing", func(t *testing.T) {
		a := makeAllocator()
		// MMA cannot judge candidates against a source it does not know;
		// the no-op advisor short-circuits IsInConflictWithMMA to false.
		advisor := a.BuildMMARebalanceAdvisor(ctx, unknownStore, nil)
		require.NotNil(t, advisor)
		require.True(t, advisor.disabled)
		require.False(t, a.IsInConflictWithMMA(ctx, knownStore, advisor, false /* cpuOnly */))
	})
}
