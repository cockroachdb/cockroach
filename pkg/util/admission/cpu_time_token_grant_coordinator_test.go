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
// activeMode, the legacy bool, and the kill switch.
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
	// The filler goroutine is disabled, so activeMode must be set
	// manually to simulate what the filler would do on each
	// resetInterval.
	setActiveMode := func(mode cpuTimeTokenMode) {
		cpuCoords.cpuTimeCoord.filler.activeMode.Store(int64(mode))
	}

	ctx := context.Background()
	defer func(prevMode cpuTimeTokenMode, prevEnabled bool) {
		cpuTimeTokenACMode.Override(ctx, &settings.SV, prevMode)
		cpuTimeTokenACEnabled.Override(ctx, &settings.SV, prevEnabled)
	}(cpuTimeTokenACMode.Get(&settings.SV), cpuTimeTokenACEnabled.Get(&settings.SV))

	// Both settings off: slot-based AC.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, offMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	setActiveMode(offMode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Mode set to serverless: CPU time token AC with separate queues
	// per tenant.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, serverlessMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	setActiveMode(serverlessMode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.NotEqual(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Mode set to resource_manager: CPU time token AC with a single
	// queue for all work.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, resourceManagerMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	setActiveMode(resourceManagerMode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Legacy bool fallback: mode is off but enabled=true enables CTT
	// AC. activeMode stays serverless (the default when mode is off).
	cpuTimeTokenACMode.Override(ctx, &settings.SV, offMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, true)
	setActiveMode(serverlessMode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.NotEqual(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Defensive case: setting says serverless but activeMode is offMode.
	// This shouldn't occur in production (the constructor and filler
	// never store offMode), but GetKVWorkQueue handles it by falling
	// back to slots.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, serverlessMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	setActiveMode(offMode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)

	// Kill switch overrides all modes.
	defer func(prev bool) {
		cpuTimeTokenACKillSwitch = prev
	}(cpuTimeTokenACKillSwitch)

	// Kill switch overrides serverlessMode.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, serverlessMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	setActiveMode(serverlessMode)
	cpuTimeTokenACKillSwitch = true
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Kill switch overrides resourceManagerMode.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, resourceManagerMode)
	setActiveMode(resourceManagerMode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)

	// Kill switch overrides legacy bool fallback.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, offMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, true)
	setActiveMode(serverlessMode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)

	// Disabling kill switch restores CPU time token AC.
	cpuTimeTokenACKillSwitch = false
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.NotEqual(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))
}
