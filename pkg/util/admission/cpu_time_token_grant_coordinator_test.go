// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

// TestObsoleteCode contains nudges for cleanups that may be possible in the
// future. When this test fails (which is necessarily a result of bumping the
// MinSupportedVersion), please carry out the cleanups that are now possible or
// file issues asking for them to be done.
func TestObsoleteCode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	msv := clusterversion.RemoveDevOffset(clusterversion.MinSupported.Version())
	t.Logf("MinSupported: %v", msv)

	// When MinSupported is bumped above V26_3, the legacy
	// cpuTimeTokenACEnabled bool (admission.cpu_time_tokens.enabled) can
	// be removed along with the fallback logic in cpuTimeTokenACIsEnabled.
	// All clusters will have the mode setting by then.
	v26dot3 := clusterversion.RemoveDevOffset(clusterversion.V26_3.Version())
	if !msv.LessEq(v26dot3) {
		_ = cpuTimeTokenACEnabled
		t.Fatalf("cpuTimeTokenACEnabled (admission.cpu_time_tokens.enabled) and " +
			"its fallback in cpuTimeTokenACIsEnabled can be removed")
	}
}

// TestCPUTimeTokenACEnableAndDisable verifies that GetKVWorkQueue
// routes work to the correct queue based on cpuTimeTokenACMode,
// the legacy bool, and the kill switch.
func TestCPUTimeTokenACEnableAndDisable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()
	var opts Options
	knobs := &TestingKnobs{DisableCPUTimeTokenFillerGoroutine: true}
	coords := NewGrantCoordinators(ambientCtx, settings, opts, registry, &noopOnLogEntryAdmitted{}, knobs)
	defer coords.Close()
	cpuCoords := coords.RegularCPU

	ctx := context.Background()
	defer func(prevMode cpuTimeTokenMode, prevEnabled bool) {
		cpuTimeTokenACMode.Override(ctx, &settings.SV, prevMode)
		cpuTimeTokenACEnabled.Override(ctx, &settings.SV, prevEnabled)
	}(cpuTimeTokenACMode.Get(&settings.SV), cpuTimeTokenACEnabled.Get(&settings.SV))

	// Both settings off: slot-based AC.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, offMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue().mode)

	// Mode set to serverless: CPU time token AC.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, serverlessMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue().mode)

	// Mode set to resource_manager: CPU time token AC.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, resourceManagerMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue().mode)

	// Legacy bool fallback: mode is off but enabled=true enables CTT AC.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, offMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, true)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue().mode)

	// Kill switch overrides all modes.
	defer func(prev bool) {
		cpuTimeTokenACKillSwitch = prev
	}(cpuTimeTokenACKillSwitch)

	// Kill switch overrides serverlessMode.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, serverlessMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	cpuTimeTokenACKillSwitch = true
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue().mode)

	// Kill switch overrides resourceManagerMode.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, resourceManagerMode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue().mode)

	// Kill switch overrides legacy bool fallback.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, offMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, true)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue().mode)

	// Disabling kill switch restores CPU time token AC.
	cpuTimeTokenACKillSwitch = false
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue().mode)
}

// TestSetResourceGroupConfigViaCoord exercises the public coord
// entry point end-to-end: it must update the shared holder AND
// signal the RM-mode WorkQueue to push the new config onto its
// cached per-group state. A bug that wires the refresh to the wrong
// queue, drops the holder write, or skips the refresh would not be
// caught by tests that go directly through holder/refresh.
func TestSetResourceGroupConfigViaCoord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()
	var opts Options
	knobs := &TestingKnobs{DisableCPUTimeTokenFillerGoroutine: true}
	coords := NewGrantCoordinators(ambientCtx, settings, opts, registry, &noopOnLogEntryAdmitted{}, knobs)
	defer coords.Close()
	cpuCoords := coords.RegularCPU
	rmQueue := cpuCoords.cpuTimeCoord.queue

	// Force RM mode so the apply path runs synchronously when refresh
	// is invoked. Without this, refresh is a no-op and the test would
	// only assert the holder write.
	ctx := context.Background()
	cpuTimeTokenACMode.Override(ctx, &settings.SV, resourceManagerMode)

	// Public API call with user-defined keys. Built-in keys (high/low)
	// cannot be overwritten, so we test with non-builtin IDs.
	cpuCoords.SetResourceGroupConfig(ResourceGroupConfigSet{
		rgGroupKey(0, 42): {Weight: 70, BurstFrac: 0.7, MaxCPU: true},
		rgGroupKey(0, 43): {Weight: 30, BurstFrac: 0.3, MaxCPU: false},
	})

	// Holder reflects the new config.
	groups := cpuCoords.cpuTimeCoord.configHolder.Snapshot().Groups()
	require.Equal(t, uint32(70), groups[rgGroupKey(0, 42)].Weight)
	require.Equal(t, float64(0.7), groups[rgGroupKey(0, 42)].BurstFrac)
	require.True(t, groups[rgGroupKey(0, 42)].MaxCPU)
	require.Equal(t, uint32(30), groups[rgGroupKey(0, 43)].Weight)
	require.Equal(t, float64(0.3), groups[rgGroupKey(0, 43)].BurstFrac)
	require.False(t, groups[rgGroupKey(0, 43)].MaxCPU)

	// RM-mode WorkQueue's cached per-group state reflects the new
	// config (refresh propagated through to applyConfigLocked).
	rg42 := getGroupLocked(rmQueue, rgGroupKey(0, 42))
	require.NotNil(t, rg42, "rg 42 container should be pre-created by apply")
	require.Equal(t, uint32(70), rg42.weight)
	require.Equal(t, float64(0.7), rg42.burstFrac)
	require.True(t, rg42.cpuTimeBurstBucket.maxCPU)
	rg43 := getGroupLocked(rmQueue, rgGroupKey(0, 43))
	require.NotNil(t, rg43, "rg 43 container should be pre-created by apply")
	require.Equal(t, uint32(30), rg43.weight)
	require.Equal(t, float64(0.3), rg43.burstFrac)
	require.False(t, rg43.cpuTimeBurstBucket.maxCPU)
}
