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
// returns the correct queue based on cpuTimeTokenACMode, the legacy
// bool, and the kill switch.
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
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false), cpuCoords.GetKVWorkQueue(true))

	// Mode set to serverless: single CTT queue for all work.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, serverlessMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false), cpuCoords.GetKVWorkQueue(true))

	// Mode set to resource_manager: same single queue.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, resourceManagerMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false), cpuCoords.GetKVWorkQueue(true))

	// Legacy bool fallback: mode is off but enabled=true enables CTT.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, offMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, true)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false), cpuCoords.GetKVWorkQueue(true))

	// Kill switch overrides all modes.
	defer func(prev bool) {
		cpuTimeTokenACKillSwitch = prev
	}(cpuTimeTokenACKillSwitch)

	cpuTimeTokenACMode.Override(ctx, &settings.SV, serverlessMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	cpuTimeTokenACKillSwitch = true
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true).mode)

	// Kill switch overrides legacy bool fallback.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, offMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, true)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true).mode)

	// Disabling kill switch restores CPU time token AC.
	cpuTimeTokenACKillSwitch = false
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false), cpuCoords.GetKVWorkQueue(true))
}

// TestSetResourceGroupConfigViaCoord exercises the public coord
// entry point end-to-end: it must update the shared holder AND
// signal the WorkQueue to push the new config onto its cached
// per-group state.
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
	rmQueue := cpuCoords.cpuTimeCoord.queue.(*WorkQueue)

	// Public API call. This is the only thing under test.
	cpuCoords.SetResourceGroupConfig(ResourceGroupConfigSet{
		rgGroupKey(highResourceGroupID): {Weight: 70, BurstFrac: 0.7, MaxCPU: true},
		rgGroupKey(lowResourceGroupID):  {Weight: 30, BurstFrac: 0.3, MaxCPU: false},
	})

	// Holder reflects the new config.
	snap := cpuCoords.cpuTimeCoord.configHolder.Snapshot()
	require.Equal(t, uint32(70), snap[rgGroupKey(highResourceGroupID)].Weight)
	require.Equal(t, float64(0.7), snap[rgGroupKey(highResourceGroupID)].BurstFrac)
	require.True(t, snap[rgGroupKey(highResourceGroupID)].MaxCPU)
	require.Equal(t, uint32(30), snap[rgGroupKey(lowResourceGroupID)].Weight)
	require.Equal(t, float64(0.3), snap[rgGroupKey(lowResourceGroupID)].BurstFrac)
	require.False(t, snap[rgGroupKey(lowResourceGroupID)].MaxCPU)

	// WorkQueue's cached per-group state reflects the new
	// config (refresh propagated through to applyConfigLocked).
	high := getGroupLocked(rmQueue, rgGroupKey(highResourceGroupID))
	require.NotNil(t, high, "rg high container should be pre-created by apply")
	require.Equal(t, uint32(70), high.weight)
	require.Equal(t, float64(0.7), high.burstFrac)
	require.True(t, high.cpuTimeBurstBucket.maxCPU)
	low := getGroupLocked(rmQueue, rgGroupKey(lowResourceGroupID))
	require.NotNil(t, low, "rg low container should be pre-created by apply")
	require.Equal(t, uint32(30), low.weight)
	require.Equal(t, float64(0.3), low.burstFrac)
	require.False(t, low.cpuTimeBurstBucket.maxCPU)
}
