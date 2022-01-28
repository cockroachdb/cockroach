// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systemspanconfig

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestEncodeDecodeTarget ensures that encoding/decoding a
// systemspanconfig.Target is roundtripable.
func TestEncodeDecodeTarget(t *testing.T) {
	makeTargetOrFatal := func(targeterID roachpb.TenantID, targeteeID roachpb.TenantID) Target {
		target, err := MakeTarget(targeterID, targeteeID)
		require.NoError(t, err)
		return target
	}
	for _, testTarget := range []Target{
		// Tenant targeting its logical cluster.
		makeTargetOrFatal(roachpb.MakeTenantID(10), roachpb.MakeTenantID(10)),
		// System tenant targeting its logical cluster.
		makeTargetOrFatal(roachpb.SystemTenantID, roachpb.SystemTenantID),
		// System tenant targeting a secondary tenant.
		makeTargetOrFatal(roachpb.SystemTenantID, roachpb.MakeTenantID(10)),
	} {
		target, err := DecodeTarget(EncodeTarget(testTarget))
		require.NoError(t, err)
		require.Equal(t, testTarget, target)
	}
}

// TestTargetValidation ensures target.validate() works as expected.
func TestTargetValidation(t *testing.T) {
	for _, tc := range []struct {
		targeterID roachpb.TenantID
		targeteeID roachpb.TenantID
		expErr     string
	}{
		{
			// Secondary tenants cannot target the system tenant.
			targeterID: roachpb.MakeTenantID(10),
			targeteeID: roachpb.SystemTenantID,
			expErr:     "secondary tenant 10 cannot target another tenant with ID",
		},
		{
			// Secondary tenants cannot target other secondary tenants.
			targeterID: roachpb.MakeTenantID(10),
			targeteeID: roachpb.MakeTenantID(20),
			expErr:     "secondary tenant 10 cannot target another tenant with ID",
		},
		// Test some valid targets.
		{
			// System tenant targeting secondary tenant is allowed.
			targeterID: roachpb.SystemTenantID,
			targeteeID: roachpb.MakeTenantID(20),
		},
		{
			// System tenant targeting itself is allowed.
			targeterID: roachpb.SystemTenantID,
			targeteeID: roachpb.SystemTenantID,
		},
		{
			// Secondary tenant targeting itself is allowed.
			targeterID: roachpb.MakeTenantID(10),
			targeteeID: roachpb.MakeTenantID(10),
		},
	} {
		target := Target{
			TargeterTenantID: tc.targeterID,
			TargeteeTenantID: tc.targeteeID,
		}
		require.True(t, testutils.IsError(target.validate(), tc.expErr))
	}
}

// TestMakeTargetUsingSourceContext ensures that the targeting tenant ID is
// correctly inferred from a context when constructing a systemspanconfig.Target
// from a roachpb.SystemSpanConfigTarget.
func TestMakeTargetUsingSourceContext(t *testing.T) {
	makeSystemSpanConfigTarget := func(tenantID roachpb.TenantID) roachpb.SystemSpanConfigTarget {
		return roachpb.SystemSpanConfigTarget{
			TenantID: &tenantID,
		}
	}
	clusterTarget := roachpb.SystemSpanConfigTarget{}
	for _, tc := range []struct {
		tenantID               roachpb.TenantID
		systemSpanConfigTarget roachpb.SystemSpanConfigTarget
		expErr                 string
	}{
		{
			tenantID:               roachpb.SystemTenantID,
			systemSpanConfigTarget: makeSystemSpanConfigTarget(roachpb.MakeTenantID(10)),
		},
		{
			tenantID:               roachpb.SystemTenantID,
			systemSpanConfigTarget: clusterTarget,
		},
		{
			tenantID:               roachpb.MakeTenantID(10),
			systemSpanConfigTarget: clusterTarget,
		},
		// Invalid scenarios.
		{
			tenantID:               roachpb.MakeTenantID(10),
			systemSpanConfigTarget: makeSystemSpanConfigTarget(roachpb.SystemTenantID),
			expErr:                 "secondary tenant 10 cannot target another tenant with ID",
		},
		{
			tenantID:               roachpb.MakeTenantID(10),
			systemSpanConfigTarget: makeSystemSpanConfigTarget(roachpb.MakeTenantID(20)),
			expErr:                 "secondary tenant 10 cannot target another tenant with ID",
		},
	} {
		ctx := roachpb.NewContextForTenant(context.Background(), tc.tenantID)
		target, err := MakeTargetUsingSourceContext(ctx, tc.systemSpanConfigTarget)
		require.True(t, testutils.IsError(err, tc.expErr))
		if tc.expErr != "" {
			require.Equal(t, tc.tenantID, target.TargeterTenantID)
			expectedTargeteeTenantID := tc.tenantID
			if tc.systemSpanConfigTarget.TenantID != nil {
				expectedTargeteeTenantID = *tc.systemSpanConfigTarget.TenantID
			}
			require.Equal(t, expectedTargeteeTenantID, target.TargeteeTenantID)
		}
		require.True(t, testutils.IsError(target.validate(), tc.expErr))
	}
}
