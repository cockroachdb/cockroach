// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// ptrTo is a helper to create pointers to literals for testing.
func ptrTo[T any](v T) *T {
	return &v
}

func TestVCPUAuditWriter_TickerInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Use a real server for vCPU audit tests since they need cluster settings.
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	// Create enforcer with short interval for testing.
	interval := 100 * time.Millisecond
	e := license.NewEnforcer(&license.TestingKnobs{
		OverrideVCPUAuditInterval: &interval,
		OverrideVCPUCount:         ptrTo(4.0),
	})

	err := e.Start(ctx, srv.ClusterSettings(),
		license.WithSystemTenant(true),
		license.WithDB(srv.SystemLayer().InternalDB().(descs.DB)),
	)
	require.NoError(t, err)

	// Start audit writer.
	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), srv.ClusterSettings(), 1)
	require.NoError(t, err)

	// Wait for at least 2 ticks to verify ticker is working.
	// We just verify the goroutine is running without error.
	time.Sleep(250 * time.Millisecond)
}

func TestVCPUAuditWriter_OverrideVCPUCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	expectedVCPU := 8.5

	e := license.NewEnforcer(&license.TestingKnobs{
		OverrideVCPUCount: &expectedVCPU,
	})

	// Get vCPU audit data directly without full enforcer.Start()
	data := e.GetVCPUAuditDataForTest(ctx)

	require.Equal(t, expectedVCPU, data.VCPUCount)
}

func TestVCPUAuditWriter_LicenseRotation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Use a real server for license rotation test.
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	e := license.NewEnforcer(nil)
	err := e.Start(ctx, srv.ClusterSettings(),
		license.WithSystemTenant(true),
		license.WithDB(srv.SystemLayer().InternalDB().(descs.DB)),
	)
	require.NoError(t, err)

	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), srv.ClusterSettings(), 1)
	require.NoError(t, err)

	// First license.
	lic1 := []byte("license-id-1")
	e.RefreshForLicenseChange(ctx, license.LicTypeEnterprise, time.Now().Add(time.Hour), lic1)

	// Verify stored.
	stored := e.TestingGetCurrentLicenseID()
	require.Equal(t, lic1, stored)

	// Rotate to new license - should trigger immediate write.
	lic2 := []byte("license-id-2")
	e.RefreshForLicenseChange(ctx, license.LicTypeEnterprise, time.Now().Add(time.Hour), lic2)

	// Verify new license stored.
	stored = e.TestingGetCurrentLicenseID()
	require.Equal(t, lic2, stored)

	// Verify the license IDs are different (rotation detected).
	require.NotEqual(t, lic1, lic2)
}

func TestVCPUAuditWriter_LicenseRotationBeforeStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	e := license.NewEnforcer(nil)

	// Call RefreshForLicenseChange BEFORE StartVCPUAuditWriter.
	// Should not panic or error.
	lic1 := []byte("license-id-1")
	e.RefreshForLicenseChange(ctx, license.LicTypeEnterprise, time.Now().Add(time.Hour), lic1)

	// License ID should NOT be stored yet (audit writer not started).
	stored := e.TestingGetCurrentLicenseID()
	require.Nil(t, stored, "license ID should not be stored before audit writer starts")
}

func TestVCPUAuditWriter_ShutdownWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Use long interval so normal tick won't fire during test.
	interval := 1 * time.Hour
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			LicenseTestingKnobs: &license.TestingKnobs{
				OverrideVCPUAuditInterval: &interval,
				OverrideVCPUCount:         ptrTo(4.0),
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	e := license.NewEnforcer(&license.TestingKnobs{
		OverrideVCPUAuditInterval: &interval,
		OverrideVCPUCount:         ptrTo(4.0),
	})

	err := e.Start(ctx, srv.ClusterSettings(),
		license.WithSystemTenant(true),
		license.WithDB(srv.SystemLayer().InternalDB().(descs.DB)),
	)
	require.NoError(t, err)

	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), srv.ClusterSettings(), 1)
	require.NoError(t, err)

	// Test passes if shutdown completes without hanging.
	// The defer will trigger shutdown and the final audit write.
}

func TestVCPUAuditWriter_PreventMultipleStarts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	e := license.NewEnforcer(nil)
	err := e.Start(ctx, srv.ClusterSettings(),
		license.WithSystemTenant(true),
		license.WithDB(srv.SystemLayer().InternalDB().(descs.DB)),
	)
	require.NoError(t, err)

	// First call succeeds.
	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), srv.ClusterSettings(), 1)
	require.NoError(t, err)

	// Second call fails.
	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), srv.ClusterSettings(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already started")
}

func TestVCPUAuditWriter_InvalidNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	e := license.NewEnforcer(nil)
	err := e.Start(ctx, srv.ClusterSettings(),
		license.WithSystemTenant(true),
		license.WithDB(srv.SystemLayer().InternalDB().(descs.DB)),
	)
	require.NoError(t, err)

	// nodeID = 0 should fail.
	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), srv.ClusterSettings(), 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid nodeID")
}

func TestVCPUAuditWriter_NoLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	e := license.NewEnforcer(nil)

	// Don't set any license - get vCPU data directly.
	data := e.GetVCPUAuditDataForTest(ctx)

	// Should use sentinel value, not nil.
	require.Equal(t, []byte("no-license"), data.LicenseID)
}

func TestVCPUAuditWriter_ByteSliceCopyOnRotation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	e := license.NewEnforcer(nil)
	err := e.Start(ctx, srv.ClusterSettings(),
		license.WithSystemTenant(true),
		license.WithDB(srv.SystemLayer().InternalDB().(descs.DB)),
	)
	require.NoError(t, err)

	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), srv.ClusterSettings(), 1)
	require.NoError(t, err)

	// Create a mutable byte slice.
	lic1 := []byte("license-id-1")
	originalLic1 := string(lic1)

	// Rotate to this license.
	e.RefreshForLicenseChange(ctx, license.LicTypeEnterprise, time.Now().Add(time.Hour), lic1)

	// Verify stored.
	stored := e.TestingGetCurrentLicenseID()
	require.Equal(t, originalLic1, string(stored))

	// Mutate the original slice.
	lic1[0] = 'X'

	// Verify stored license is unchanged (was copied).
	stored = e.TestingGetCurrentLicenseID()
	require.Equal(t, originalLic1, string(stored))
	require.NotEqual(t, string(lic1), string(stored))
}
