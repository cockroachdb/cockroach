// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestResourceGroupConfigHolder covers the constructor seed and the
// unknown-key fallback. The behavioral cases for Set and Snapshot live in
// adjacent TestResourceGroupConfigHolder* functions.
func TestResourceGroupConfigHolder(t *testing.T) {
	t.Run("constructor_seed", func(t *testing.T) {
		h := newResourceGroupConfigHolder()
		snap := h.Snapshot()
		// Built-in system tenant config is always present.
		require.Equal(t, systemTenantGroupConfig, snap[tenantGroupKey(1)])
		// RM defaults are also present (from the seed).
		require.Equal(t,
			defaultRMResourceGroupConfig[rgGroupKey(highResourceGroupID)],
			snap[rgGroupKey(highResourceGroupID)])
		require.Equal(t,
			defaultRMResourceGroupConfig[rgGroupKey(lowResourceGroupID)],
			snap[rgGroupKey(lowResourceGroupID)])
		require.Len(t, snap, 3) // 1 builtin + 2 RM defaults
	})

	t.Run("get_or_default_unknown_rg", func(t *testing.T) {
		h := newResourceGroupConfigHolder()
		require.Equal(t, defaultRGGroupConfig,
			h.Snapshot().GetOrDefault(rgGroupKey(9999)))
	})

	t.Run("get_or_default_unknown_tenant", func(t *testing.T) {
		h := newResourceGroupConfigHolder()
		require.Equal(t, defaultTenantGroupConfig,
			h.Snapshot().GetOrDefault(tenantGroupKey(9999)))
	})

	t.Run("default_configs_have_burst_frac", func(t *testing.T) {
		h := newResourceGroupConfigHolder()
		snap := h.Snapshot()
		highCfg := snap.GetOrDefault(rgGroupKey(highResourceGroupID))
		require.Equal(t, float64(0.8), highCfg.BurstFrac)
		lowCfg := snap.GetOrDefault(rgGroupKey(lowResourceGroupID))
		require.Equal(t, float64(0.2), lowCfg.BurstFrac)
		tenantCfg := snap.GetOrDefault(tenantGroupKey(9999))
		require.Equal(t, float64(0.25), tenantCfg.BurstFrac)
	})
}

// TestResourceGroupConfigHolderSet covers Set's wholesale-replace and
// input-aliasing contracts.
func TestResourceGroupConfigHolderSet(t *testing.T) {
	t.Run("replaces_wholesale_dropping_seed", func(t *testing.T) {
		h := newResourceGroupConfigHolder()
		h.Set(ResourceGroupConfigSet{
			rgGroupKey(42): {Weight: 100, MaxCPU: false},
		})
		snap := h.Snapshot()
		// Set is wholesale: the RM seed (high/low) must be gone, the
		// freshly-Set key must be present, and built-ins stay.
		require.Equal(t, ResourceGroupConfig{Weight: 100, MaxCPU: false},
			snap[rgGroupKey(42)])
		require.Equal(t, systemTenantGroupConfig, snap[tenantGroupKey(1)])
		require.Len(t, snap, 2) // 1 builtin + 1 caller key
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

		snap := h.Snapshot()
		require.Equal(t, ResourceGroupConfig{Weight: 100, MaxCPU: false},
			snap[rgGroupKey(7)])
		require.Len(t, snap, 2) // 1 builtin + 1 caller key

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
		h.Snapshot().GetOrDefault(rgGroupKey(highResourceGroupID)))
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
	// snap1 must still have the RM defaults + builtins.
	require.Equal(t,
		defaultRMResourceGroupConfig[rgGroupKey(highResourceGroupID)],
		snap1[rgGroupKey(highResourceGroupID)],
		"prior snapshot must be unaffected by a subsequent Set")
	// New snapshot has builtins + the freshly-set key.
	snap2 := h.Snapshot()
	require.Equal(t, ResourceGroupConfig{Weight: 100, MaxCPU: true},
		snap2[rgGroupKey(42)])
	require.Equal(t, systemTenantGroupConfig, snap2[tenantGroupKey(1)])
	require.Len(t, snap2, 2)
}

// TestSystemTenantGroupConfig verifies the field values of the built-in
// system tenant config.
func TestSystemTenantGroupConfig(t *testing.T) {
	require.Equal(t, uint32(math.MaxUint32), systemTenantGroupConfig.Weight)
	require.Equal(t, 1.0, systemTenantGroupConfig.BurstFrac)
	require.True(t, systemTenantGroupConfig.MaxCPU)
}

// TestBuiltinGroupConfigInSnapshot verifies that the system tenant
// config appears in the holder's snapshot immediately after
// construction, without any explicit Set call beyond the seed.
func TestBuiltinGroupConfigInSnapshot(t *testing.T) {
	h := newResourceGroupConfigHolder()
	snap := h.Snapshot()
	cfg, ok := snap[tenantGroupKey(1)]
	require.True(t, ok, "system tenant (ID 1) must be present in snapshot")
	require.Equal(t, systemTenantGroupConfig, cfg)
}

// TestSetPanicsOnBuiltinOverwrite verifies that Set panics if the
// caller tries to overwrite a built-in key.
func TestSetPanicsOnBuiltinOverwrite(t *testing.T) {
	h := newResourceGroupConfigHolder()
	require.Panics(t, func() {
		h.Set(ResourceGroupConfigSet{
			tenantGroupKey(1): {Weight: 1, BurstFrac: 0.5, MaxCPU: false},
		})
	})
}
