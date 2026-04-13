// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Mode set to serverless: CPU time token AC.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, serverlessMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.NotEqual(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Mode set to resource_manager: CPU time token AC.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, resourceManagerMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.NotEqual(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Legacy bool fallback: mode is off but enabled=true enables CTT AC.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, offMode)
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, true)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.NotEqual(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Kill switch overrides both settings.
	cpuTimeTokenACMode.Override(ctx, &settings.SV, serverlessMode)
	defer func(prev bool) {
		cpuTimeTokenACKillSwitch = prev
	}(cpuTimeTokenACKillSwitch)
	cpuTimeTokenACKillSwitch = true
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Disabling kill switch restores CPU time token AC.
	cpuTimeTokenACKillSwitch = false
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.NotEqual(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))
}
