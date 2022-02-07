// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigtarget

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestEncodeDecodeTarget ensures that encoding/decoding a
// systemspanconfig.Target is roundtripable.
func TestEncodeDecodeTarget(t *testing.T) {
	makeTargetOrFatal := func(targeterID roachpb.TenantID, targeteeID roachpb.TenantID) *SystemTarget {
		target, err := NewSystemTarget(targeterID, targeteeID)
		require.NoError(t, err)
		return target
	}
	for _, testTarget := range []*SystemTarget{
		// Tenant targeting its logical cluster.
		makeTargetOrFatal(roachpb.MakeTenantID(10), roachpb.MakeTenantID(10)),
		// System tenant targeting its logical cluster.
		makeTargetOrFatal(roachpb.SystemTenantID, roachpb.SystemTenantID),
		// System tenant targeting a secondary tenant.
		makeTargetOrFatal(roachpb.SystemTenantID, roachpb.MakeTenantID(10)),
	} {
		target := Decode(testTarget.Encode())
		require.Equal(t, testTarget, target)
	}
}

// TestTargetValidation ensures target.validate() works as expected.
func TestTargetValidation(t *testing.T) {
	for _, tc := range []struct {
		sourceTenantID roachpb.TenantID
		targetTenantID roachpb.TenantID
		expErr         string
	}{
		{
			// Secondary tenants cannot target the system tenant.
			sourceTenantID: roachpb.MakeTenantID(10),
			targetTenantID: roachpb.SystemTenantID,
			expErr:         "secondary tenant 10 cannot target another tenant with ID",
		},
		{
			// Secondary tenants cannot target other secondary tenants.
			sourceTenantID: roachpb.MakeTenantID(10),
			targetTenantID: roachpb.MakeTenantID(20),
			expErr:         "secondary tenant 10 cannot target another tenant with ID",
		},
		// Test some valid targets.
		{
			// System tenant targeting secondary tenant is allowed.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: roachpb.MakeTenantID(20),
		},
		{
			// System tenant targeting itself is allowed.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: roachpb.SystemTenantID,
		},
		{
			// Secondary tenant targeting itself is allowed.
			sourceTenantID: roachpb.MakeTenantID(10),
			targetTenantID: roachpb.MakeTenantID(10),
		},
	} {
		target := SystemTarget{
			SystemSpanConfigTarget: roachpb.SystemSpanConfigTarget{
				SourceTenantID: tc.sourceTenantID,
				TargetTenantID: tc.targetTenantID,
			},
		}
		require.True(t, testutils.IsError(target.validate(), tc.expErr))
	}
}
