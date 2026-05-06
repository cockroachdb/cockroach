// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/stretchr/testify/require"
)

// TestResourceGroupConfigHolder pins the constructor seed and the
// unknown-ID fallback against goldens. The behavioral cases for Set,
// GetOrDefault, and Snapshot live in adjacent TestResourceGroupConfigHolder*
// functions.
func TestResourceGroupConfigHolder(t *testing.T) {
	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))

	t.Run("constructor_seed", w.Run(t, "constructor_seed", func(t *testing.T) string {
		h := newResourceGroupConfigHolder()
		// ResourceGroupConfigSet.String formats the snapshot
		// deterministically (sorted by id, one entry per line).
		return h.Snapshot().String()
	}))

	t.Run("get_or_default_unknown", w.Run(t, "get_or_default_unknown", func(t *testing.T) string {
		h := newResourceGroupConfigHolder()
		cfg := h.GetOrDefault(rgGroupKey(9999))
		return fmt.Sprintf("weight=%d maxCPU=%t\n", cfg.Weight, cfg.MaxCPU)
	}))
}

// TestResourceGroupConfigHolderSet covers Set's wholesale-replace and
// input-aliasing contracts.
func TestResourceGroupConfigHolderSet(t *testing.T) {
	t.Run("replaces_wholesale_dropping_seed", func(t *testing.T) {
		h := newResourceGroupConfigHolder()
		h.Set(ResourceGroupConfigSet{
			rgGroupKey(42): {Weight: 100, MaxCPU: false},
		})
		// Set is wholesale: the seed (high/low) must be gone, and only the
		// freshly-Set key must remain.
		require.Equal(t, ResourceGroupConfigSet{
			rgGroupKey(42): {Weight: 100, MaxCPU: false},
		}, h.Snapshot())
	})

	t.Run("input_aliasing_safe", func(t *testing.T) {
		h := newResourceGroupConfigHolder()
		input := ResourceGroupConfigSet{
			rgGroupKey(7): {Weight: 100, MaxCPU: false},
		}
		h.Set(input)

		// Mutate the input post-Set; the holder must not observe it.
		input[rgGroupKey(7)] = ResourceGroupConfig{Weight: 1, MaxCPU: true}
		input[rgGroupKey(8)] = ResourceGroupConfig{Weight: 1, MaxCPU: false}

		require.Equal(t, ResourceGroupConfigSet{
			rgGroupKey(7): {Weight: 100, MaxCPU: false},
		}, h.Snapshot())

		// And the holder's internal map is not aliased to the input.
		// Same-package access lets us check the underlying pointer.
		h.mu.RLock()
		internalPtr := reflect.ValueOf(h.mu.config).Pointer()
		h.mu.RUnlock()
		require.NotEqual(t, reflect.ValueOf(input).Pointer(), internalPtr,
			"holder must not retain a reference to the caller's input map")
	})
}

// TestResourceGroupConfigHolderGet covers GetOrDefault for a configured key.
func TestResourceGroupConfigHolderGet(t *testing.T) {
	h := newResourceGroupConfigHolder()
	h.Set(ResourceGroupConfigSet{
		rgGroupKey(highResourceGroupID): {Weight: 75, MaxCPU: true},
	})
	require.Equal(t,
		ResourceGroupConfig{Weight: 75, MaxCPU: true},
		h.GetOrDefault(rgGroupKey(highResourceGroupID)))
}

// TestResourceGroupConfigHolderSnapshot verifies Snapshot's contract:
// the returned map is the holder's installed map (no defensive copy),
// and a subsequent Set installs a fresh map without mutating the
// previously-returned snapshot.
func TestResourceGroupConfigHolderSnapshot(t *testing.T) {
	h := newResourceGroupConfigHolder()
	snap1 := h.Snapshot()

	// Snapshot aliases the internal map (no copy on read). This is
	// the contract: callers must treat the returned map as read-only.
	h.mu.RLock()
	internalPtr := reflect.ValueOf(h.mu.config).Pointer()
	h.mu.RUnlock()
	require.Equal(t, internalPtr, reflect.ValueOf(snap1).Pointer(),
		"Snapshot should return the installed map directly")

	// A subsequent Set installs a brand-new map. snap1 must remain
	// stable (it points at the previously-installed map, which is
	// immutable post-install) while h.Snapshot() returns the new one.
	h.Set(ResourceGroupConfigSet{
		rgGroupKey(42): {Weight: 100, MaxCPU: true},
	})
	require.Equal(t, defaultRMResourceGroupConfig, snap1,
		"prior snapshot must be unaffected by a subsequent Set")
	require.Equal(t, ResourceGroupConfigSet{
		rgGroupKey(42): {Weight: 100, MaxCPU: true},
	}, h.Snapshot())
}
