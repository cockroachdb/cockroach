// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/stretchr/testify/require"
)

// testHolder constructs a holder backed by a fresh test cluster
// settings.Values. The holder requires non-nil settings for the
// snapshot's mode and utilization-target reads.
func testHolder() *ResourceGroupConfigHolder {
	st := cluster.MakeTestingClusterSettings()
	return newResourceGroupConfigHolder(&st.SV)
}

// TestResourceGroupConfigHolder covers the constructor seed and the
// unknown-key fallback. The behavioral cases for Set and Snapshot live in
// adjacent TestResourceGroupConfigHolder* functions.
func TestResourceGroupConfigHolder(t *testing.T) {
	t.Run("constructor_seed", func(t *testing.T) {
		h := testHolder()
		snap := h.Snapshot()
		// All built-in configs are present.
		require.Equal(t, systemTenantGroupConfig, snap.Groups()[tenantGroupKey(1)])
		require.Equal(t,
			builtinGroupConfigs[highResourceGroupKey],
			snap.Groups()[highResourceGroupKey])
		require.Equal(t,
			builtinGroupConfigs[lowResourceGroupKey],
			snap.Groups()[lowResourceGroupKey])
		require.Len(t, snap.Groups(), 3) // 3 built-ins
	})

	t.Run("get_or_default_unknown_rg", func(t *testing.T) {
		h := testHolder()
		require.Equal(t, defaultRGGroupConfig,
			h.Snapshot().Groups().GetOrDefault(rgGroupKey(0, 9999)))
	})

	t.Run("get_or_default_unknown_tenant", func(t *testing.T) {
		h := testHolder()
		require.Equal(t, defaultTenantGroupConfig,
			h.Snapshot().Groups().GetOrDefault(tenantGroupKey(9999)))
	})

	t.Run("default_configs_have_burst_frac", func(t *testing.T) {
		h := testHolder()
		snap := h.Snapshot()
		highCfg := snap.Groups().GetOrDefault(highResourceGroupKey)
		require.Equal(t, float64(0.8), highCfg.BurstFrac)
		lowCfg := snap.Groups().GetOrDefault(lowResourceGroupKey)
		require.Equal(t, float64(0.2), lowCfg.BurstFrac)
		tenantCfg := snap.Groups().GetOrDefault(tenantGroupKey(9999))
		require.Equal(t, float64(0.20), tenantCfg.BurstFrac)
	})
}

// TestResourceGroupConfigHolderSet covers Set's wholesale-replace and
// input-aliasing contracts.
func TestResourceGroupConfigHolderSet(t *testing.T) {
	t.Run("replaces_wholesale", func(t *testing.T) {
		h := testHolder()
		h.Set(ResourceGroupConfigSet{
			rgGroupKey(0, 42): {Weight: 100, MaxCPU: false},
		})
		groups := h.Snapshot().Groups()
		// Set is wholesale: the freshly-Set key is present alongside
		// the built-ins (system tenant + high/low RM groups).
		require.Equal(t, ResourceGroupConfig{Weight: 100, MaxCPU: false},
			groups[rgGroupKey(0, 42)])
		require.Equal(t, systemTenantGroupConfig, groups[tenantGroupKey(1)])
		require.Len(t, groups, 4) // 3 built-ins + 1 caller key
	})

	t.Run("input_aliasing_safe", func(t *testing.T) {
		h := testHolder()
		input := ResourceGroupConfigSet{
			rgGroupKey(0, 7): {Weight: 100, MaxCPU: false},
		}
		h.Set(input)

		// Mutate the input post-Set; the holder must not observe it.
		input[rgGroupKey(0, 7)] = ResourceGroupConfig{Weight: 1, MaxCPU: true}
		input[rgGroupKey(0, 8)] = ResourceGroupConfig{Weight: 1, MaxCPU: false}

		groups := h.Snapshot().Groups()
		require.Equal(t, ResourceGroupConfig{Weight: 100, MaxCPU: false},
			groups[rgGroupKey(0, 7)])
		require.Len(t, groups, 4) // 3 built-ins + 1 caller key

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
	h := testHolder()
	h.Set(ResourceGroupConfigSet{
		rgGroupKey(0, 42): {Weight: 75, MaxCPU: true},
	})
	require.Equal(t,
		ResourceGroupConfig{Weight: 75, MaxCPU: true},
		h.Snapshot().Groups().GetOrDefault(rgGroupKey(0, 42)))
}

// TestResourceGroupConfigHolderSnapshot verifies Snapshot's contract:
// the Groups map is the holder's installed map (no defensive copy),
// and a subsequent Set installs a fresh map without mutating the
// previously-returned snapshot.
func TestResourceGroupConfigHolderSnapshot(t *testing.T) {
	h := testHolder()
	snap1 := h.Snapshot()

	// Snapshot aliases the internal map (no copy on read). This is
	// the contract: callers must treat the returned map as read-only.
	h.mu.RLock()
	internalPtr := reflect.ValueOf(h.mu.config).Pointer()
	h.mu.RUnlock()
	require.Equal(t, internalPtr, reflect.ValueOf(snap1.Groups()).Pointer(),
		"Snapshot should return the installed map directly")

	// A subsequent Set installs a brand-new map. snap1 must remain
	// stable (it points at the previously-installed map, which is
	// immutable post-install) while h.Snapshot() returns the new one.
	h.Set(ResourceGroupConfigSet{
		rgGroupKey(0, 42): {Weight: 100, MaxCPU: true},
	})
	// snap1 must still have the built-in configs.
	require.Equal(t,
		builtinGroupConfigs[highResourceGroupKey],
		snap1.Groups()[highResourceGroupKey],
		"prior snapshot must be unaffected by a subsequent Set")
	// New snapshot has builtins + the freshly-set key.
	snap2 := h.Snapshot()
	require.Equal(t, ResourceGroupConfig{Weight: 100, MaxCPU: true},
		snap2.Groups()[rgGroupKey(0, 42)])
	require.Equal(t, systemTenantGroupConfig, snap2.Groups()[tenantGroupKey(1)])
	require.Len(t, snap2.Groups(), 4) // 3 built-ins + 1 caller key
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
	h := testHolder()
	groups := h.Snapshot().Groups()
	cfg, ok := groups[tenantGroupKey(1)]
	require.True(t, ok, "system tenant (ID 1) must be present in snapshot")
	require.Equal(t, systemTenantGroupConfig, cfg)
}

// TestSetPanicsOnBuiltinOverwrite verifies that Set panics if the
// caller tries to overwrite any built-in key.
func TestSetPanicsOnBuiltinOverwrite(t *testing.T) {
	h := testHolder()
	for k := range builtinGroupConfigs {
		require.Panics(t, func() {
			h.Set(ResourceGroupConfigSet{
				k: {Weight: 1, BurstFrac: 0.5, MaxCPU: false},
			})
		})
	}
}

// TestConfigSnapshotDefaults verifies the default utilization targets
// surfaced through Snapshot when the underlying cluster settings are
// at their registered defaults.
func TestConfigSnapshotDefaults(t *testing.T) {
	h := testHolder()
	snap := h.Snapshot()
	require.Equal(t, 0.8, snap.MaxNonBurstableFraction())
	require.InEpsilon(t, 0.85, snap.MaxFraction(), 1e-9)
}

// TestConfigSnapshotMaxNonBurstableFraction verifies
// MaxNonBurstableFraction returns the non-burstable target.
func TestConfigSnapshotMaxNonBurstableFraction(t *testing.T) {
	snap := ConfigSnapshot{
		noBurstFrac: 0.75,
		burstDelta:  0.10,
	}
	require.Equal(t, 0.75, snap.MaxNonBurstableFraction())
}

// TestConfigSnapshotMaxFraction verifies MaxFraction returns the
// burstable target (non-burstable target + burst delta).
func TestConfigSnapshotMaxFraction(t *testing.T) {
	snap := ConfigSnapshot{
		noBurstFrac: 0.75,
		burstDelta:  0.10,
	}
	require.Equal(t, 0.85, snap.MaxFraction())
}
