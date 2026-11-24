// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license_test

import (
	"context"
	"sync/atomic"
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

	var writeCount atomic.Int32
	interval := 50 * time.Millisecond
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	e := license.NewEnforcer(&license.TestingKnobs{
		OverrideVCPUAuditInterval: &interval,
		OverrideVCPUCount:         ptrTo(4.0),
		OnAuditRecordWritten: func() {
			writeCount.Add(1)
		},
	})

	err := e.Start(ctx, srv.ClusterSettings(),
		license.WithSystemTenant(true),
		license.WithDB(srv.SystemLayer().InternalDB().(descs.DB)),
	)
	require.NoError(t, err)

	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), 1)
	require.NoError(t, err)

	// Wait for the startup write plus at least 2 ticker fires to verify
	// the ticker loop keeps running.
	require.Eventually(t, func() bool {
		return writeCount.Load() >= 3
	}, 2*time.Second, 10*time.Millisecond, "expected startup write and at least 2 ticker writes")
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

	var writeCount atomic.Int32
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	e := license.NewEnforcer(&license.TestingKnobs{
		// Long interval so the ticker doesn't fire during this test.
		OverrideVCPUAuditInterval: ptrTo(1 * time.Hour),
		OnAuditRecordWritten: func() {
			writeCount.Add(1)
		},
	})
	err := e.Start(ctx, srv.ClusterSettings(),
		license.WithSystemTenant(true),
		license.WithDB(srv.SystemLayer().InternalDB().(descs.DB)),
	)
	require.NoError(t, err)

	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), 1)
	require.NoError(t, err)

	// Wait for the startup write before testing rotation-triggered writes.
	require.Eventually(t, func() bool {
		return writeCount.Load() >= 1
	}, time.Second, 5*time.Millisecond, "expected startup write")
	countAfterStartup := writeCount.Load()

	// First license — no previous value, so this is a rotation from "no-license".
	lic1 := []byte("license-id-1")
	e.RefreshForLicenseChange(ctx, license.LicTypeEnterprise, time.Now().Add(time.Hour), lic1)

	require.Eventually(t, func() bool {
		return writeCount.Load() >= countAfterStartup+1
	}, time.Second, 5*time.Millisecond, "expected immediate write on first license rotation")

	stored := e.TestingGetCurrentLicenseID()
	require.Equal(t, lic1, stored)

	countAfterFirst := writeCount.Load()

	// Rotate to new license — should trigger a second immediate write.
	lic2 := []byte("license-id-2")
	e.RefreshForLicenseChange(ctx, license.LicTypeEnterprise, time.Now().Add(time.Hour), lic2)

	require.Eventually(t, func() bool {
		return writeCount.Load() >= countAfterFirst+1
	}, time.Second, 5*time.Millisecond, "expected immediate write on license rotation")

	stored = e.TestingGetCurrentLicenseID()
	require.Equal(t, lic2, stored)
}

// TestVCPUAuditWriter_LicenseRotationBeforeStart verifies that license ID changes
// arriving before StartVCPUAuditWriter are cached (so the first write uses the
// correct state), but that no audit write is triggered until the writer has started.
func TestVCPUAuditWriter_LicenseRotationBeforeStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var writeCount atomic.Int32
	e := license.NewEnforcer(&license.TestingKnobs{
		OnAuditRecordWritten: func() {
			writeCount.Add(1)
		},
	})

	// Call RefreshForLicenseChange BEFORE StartVCPUAuditWriter.
	lic1 := []byte("license-id-1")
	e.RefreshForLicenseChange(ctx, license.LicTypeEnterprise, time.Now().Add(time.Hour), lic1)

	// The license ID should be cached so that the first write after startup
	// uses the correct value.
	stored := e.TestingGetCurrentLicenseID()
	require.Equal(t, lic1, stored, "license ID should be cached even before audit writer starts")

	// No audit write should have been triggered yet.
	require.Zero(t, writeCount.Load(), "no write should fire before audit writer starts")
}

func TestVCPUAuditWriter_ShutdownWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var writeCount atomic.Int32
	// Long interval so the normal tick won't fire during the test.
	interval := 1 * time.Hour
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer func() {
		srv.Stopper().Stop(ctx)
		// Verify both the startup write and the shutdown write fired.
		require.GreaterOrEqual(t, writeCount.Load(), int32(2), "expected startup write and shutdown write")
	}()

	e := license.NewEnforcer(&license.TestingKnobs{
		OverrideVCPUAuditInterval: &interval,
		OverrideVCPUCount:         ptrTo(4.0),
		OnAuditRecordWritten: func() {
			writeCount.Add(1)
		},
	})

	err := e.Start(ctx, srv.ClusterSettings(),
		license.WithSystemTenant(true),
		license.WithDB(srv.SystemLayer().InternalDB().(descs.DB)),
	)
	require.NoError(t, err)

	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), 1)
	require.NoError(t, err)
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
	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), 1)
	require.NoError(t, err)

	// Second call fails.
	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), 1)
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
	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), 0)
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

	err = e.StartVCPUAuditWriter(ctx, srv.Stopper(), 1)
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
