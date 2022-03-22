// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestEncodeDecodeSystemTarget ensures that encoding/decoding a SystemTarget
// is roundtripable.
func TestEncodeDecodeSystemTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, testTarget := range []SystemTarget{
		// Tenant targeting its logical cluster.
		TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.MakeTenantID(10), roachpb.MakeTenantID(10)),
		// System tenant targeting its logical cluster.
		TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, roachpb.SystemTenantID),
		// System tenant targeting a secondary tenant.
		TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, roachpb.MakeTenantID(10)),
		// System tenant targeting the entire keyspace.
		MakeEntireKeyspaceTarget(),
	} {
		systemTarget, err := decodeSystemTarget(testTarget.encode())
		require.NoError(t, err)
		require.Equal(t, testTarget, systemTarget)

		// Next, we encode/decode a spanconfig.Target that wraps a SystemTarget.
		target := MakeTargetFromSystemTarget(systemTarget)
		decodedTarget := DecodeTarget(target.Encode())
		require.Equal(t, target, decodedTarget)
	}
}

// TestTargetToFromProto ensures that converting system targets to protos and
// from protos is roundtripable.
func TestTargetToFromProto(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, testSystemTarget := range []SystemTarget{
		// Tenant targeting its logical cluster.
		TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.MakeTenantID(10), roachpb.MakeTenantID(10)),
		// System tenant targeting its logical cluster.
		TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, roachpb.SystemTenantID),
		// System tenant targeting a secondary tenant.
		TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, roachpb.MakeTenantID(10)),
		// System tenant targeting the entire keyspace.
		MakeEntireKeyspaceTarget(),
		// System tenant's read-only target to fetch all system span configurations
		// it has set over secondary tenant keyspaces.
		MakeAllTenantKeyspaceTargetsSet(roachpb.SystemTenantID),
		// A secondary tenant's read-only target to fetch all system span
		// configurations it has set over secondary tenant keyspaces.
		MakeAllTenantKeyspaceTargetsSet(roachpb.MakeTenantID(10)),
	} {
		systemTarget, err := makeSystemTargetFromProto(testSystemTarget.toProto())
		require.NoError(t, err)
		require.Equal(t, testSystemTarget, systemTarget)

		// For good measure, let's also test at the level of a spanconfig.Target.
		testTarget := MakeTargetFromSystemTarget(testSystemTarget)
		target, err := MakeTarget(testTarget.ToProto())
		require.NoError(t, err)
		require.Equal(t, target, testTarget)
	}
}

// TestKeyspaceTargeted ensures targets correctly return the keyspace they
// target.
func TestKeyspaceTargeted(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ten10 := roachpb.MakeTenantID(10)

	for _, tc := range []struct {
		target  Target
		expSpan roachpb.Span
	}{
		{
			target:  MakeTargetFromSystemTarget(MakeEntireKeyspaceTarget()),
			expSpan: keys.EverythingSpan,
		},
		{
			target: MakeTargetFromSystemTarget(TestingMakeTenantKeyspaceTargetOrFatal(t, ten10, ten10)),
			expSpan: roachpb.Span{
				Key:    keys.MakeTenantPrefix(ten10),
				EndKey: keys.MakeTenantPrefix(ten10).PrefixEnd(),
			},
		},
		{
			target: MakeTargetFromSystemTarget(
				TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, roachpb.SystemTenantID),
			),
			expSpan: roachpb.Span{
				Key:    keys.MinKey,
				EndKey: keys.TenantTableDataMin,
			},
		},
		{
			target: MakeTargetFromSystemTarget(
				TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, ten10),
			),
			expSpan: roachpb.Span{
				Key:    keys.MakeTenantPrefix(ten10),
				EndKey: keys.MakeTenantPrefix(ten10).PrefixEnd(),
			},
		},
		{
			target:  MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}),
			expSpan: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
		},
	} {
		require.Equal(t, tc.target.KeyspaceTargeted(), tc.expSpan)
	}
}

// TestDecodeInvalidSpanAsSystemTarget ensures that decoding an invalid span
// as a system target fails.
func TestDecodeInvalidSpanAsSystemTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		span        roachpb.Span
		expectedErr string
	}{
		{
			span:        roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			expectedErr: "span .* did not conform to SystemTarget encoding",
		},
		{
			// No end key.
			span:        roachpb.Span{Key: keys.SystemSpanConfigEntireKeyspace},
			expectedErr: "invalid end key in span",
		},
		{
			// Invalid end key.
			span: roachpb.Span{
				Key:    keys.SystemSpanConfigEntireKeyspace,
				EndKey: append(keys.SystemSpanConfigEntireKeyspace, byte('a')).PrefixEnd(),
			},
			expectedErr: "invalid end key in span",
		},
		{
			// Sentinel key for SystemSpanConfigEntireKeyspace should not have a
			// suffix.
			span: roachpb.Span{
				Key:    append(keys.SystemSpanConfigEntireKeyspace, byte('a')),
				EndKey: append(keys.SystemSpanConfigEntireKeyspace, byte('a')).PrefixEnd(),
			},
			expectedErr: "span .* did not conform to SystemTarget encoding",
		},
	} {
		_, err := decodeSystemTarget(tc.span)
		require.Error(t, err)
		require.True(t, testutils.IsError(err, tc.expectedErr))
	}
}

// TestSystemTargetValidation ensures target.validate() works as expected.
func TestSystemTargetValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tenant10 := roachpb.MakeTenantID(10)
	tenant20 := roachpb.MakeTenantID(20)
	for _, tc := range []struct {
		sourceTenantID roachpb.TenantID
		targetTenantID roachpb.TenantID
		targetType     systemTargetType
		expErr         string
	}{
		{
			// Secondary tenants cannot target the system tenant.
			sourceTenantID: tenant10,
			targetTenantID: roachpb.SystemTenantID,
			targetType:     SystemTargetTypeSpecificTenantKeyspace,
			expErr:         "secondary tenant 10 cannot target another tenant with ID system",
		},
		{
			// Secondary tenants cannot target other secondary tenants.
			sourceTenantID: tenant10,
			targetTenantID: tenant20,
			targetType:     SystemTargetTypeSpecificTenantKeyspace,
			expErr:         "secondary tenant 10 cannot target another tenant with ID 20",
		},
		{
			// Secondary tenants cannot target the entire keyspace.
			sourceTenantID: tenant10,
			targetTenantID: roachpb.TenantID{},
			targetType:     SystemTargetTypeEntireKeyspace,
			expErr:         "only the host tenant is allowed to target the entire keyspace",
		},
		{
			// Ensure secondary tenants can't target the entire keyspace even if they
			// set targetTenantID to themselves.
			sourceTenantID: tenant10,
			targetTenantID: tenant10,
			targetType:     SystemTargetTypeEntireKeyspace,
			expErr:         "only the host tenant is allowed to target the entire keyspace",
		},
		{
			// System tenant can't set both targetTenantID and target everything
			// installed on tenants.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: tenant10,
			targetType:     SystemTargetTypeAllTenantKeyspaceTargetsSet,
			expErr:         "targetTenantID must be unset when targeting everything installed",
		},
		{
			// System tenant must fill in a targetTenantID when targeting a specific
			// tenant.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: roachpb.TenantID{},
			targetType:     SystemTargetTypeSpecificTenantKeyspace,
			expErr:         "malformed system target for specific tenant keyspace; targetTenantID unset",
		},
		{
			// System tenant can't set both targetTenantID and target the entire
			// keyspace.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: tenant10,
			targetType:     SystemTargetTypeEntireKeyspace,
			expErr:         "malformed system target for entire keyspace; targetTenantID set",
		},
		{
			// secondary tenant can't set both targetTenantID and target everything
			// installed on tenants.
			sourceTenantID: tenant10,
			targetTenantID: tenant10,
			targetType:     SystemTargetTypeAllTenantKeyspaceTargetsSet,
			expErr:         "targetTenantID must be unset when targeting everything installed",
		},
		// Test some valid targets.
		{
			// System tenant targeting secondary tenant is allowed.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: tenant20,
			targetType:     SystemTargetTypeSpecificTenantKeyspace,
		},
		{
			// System tenant targeting the entire keyspace is allowed.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: roachpb.TenantID{},
			targetType:     SystemTargetTypeEntireKeyspace,
		},
		{
			// System tenant targeting itself is allowed.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: roachpb.SystemTenantID,
			targetType:     SystemTargetTypeSpecificTenantKeyspace,
		},
		{
			// Secondary tenant targeting itself is allowed.
			sourceTenantID: tenant10,
			targetTenantID: tenant10,
			targetType:     SystemTargetTypeSpecificTenantKeyspace,
		},
		{
			// Secondary tenant targeting everything installed on tenants by it is
			// allowed.
			sourceTenantID: tenant10,
			targetTenantID: roachpb.TenantID{},
			targetType:     SystemTargetTypeAllTenantKeyspaceTargetsSet,
		},
		{
			// System tenant targeting everything installed on tenants by it is
			// allowed.
			sourceTenantID: roachpb.SystemTenantID,
			targetTenantID: roachpb.TenantID{},
			targetType:     SystemTargetTypeAllTenantKeyspaceTargetsSet,
		},
	} {
		target := SystemTarget{
			sourceTenantID:   tc.sourceTenantID,
			targetTenantID:   tc.targetTenantID,
			systemTargetType: tc.targetType,
		}
		err := target.validate()
		require.True(
			t,
			testutils.IsError(err, tc.expErr),
			"expected: %s got: %s ",
			tc.expErr,
			err,
		)
	}
}

// TestTargetSortingRandomized ensures we sort targets correctly.
func TestTargetSortingRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Construct a set of sorted targets.
	sortedTargets := Targets{
		MakeTargetFromSystemTarget(MakeAllTenantKeyspaceTargetsSet(roachpb.SystemTenantID)),
		MakeTargetFromSystemTarget(MakeAllTenantKeyspaceTargetsSet(roachpb.MakeTenantID(10))),
		MakeTargetFromSystemTarget(MakeEntireKeyspaceTarget()),
		MakeTargetFromSystemTarget(
			TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, roachpb.SystemTenantID),
		),
		MakeTargetFromSystemTarget(
			TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, roachpb.MakeTenantID(10)),
		),
		MakeTargetFromSystemTarget(
			TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, roachpb.MakeTenantID(20)),
		),
		MakeTargetFromSystemTarget(
			TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.MakeTenantID(5), roachpb.MakeTenantID(5)),
		),
		MakeTargetFromSystemTarget(
			TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.MakeTenantID(10), roachpb.MakeTenantID(10)),
		),
		MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}),
		MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}),
		MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("y"), EndKey: roachpb.Key("z")}),
	}

	const numOps = 20
	for i := 0; i < numOps; i++ {
		tc := make(Targets, len(sortedTargets))
		copy(tc, sortedTargets)

		rand.Shuffle(len(tc), func(i, j int) {
			tc[i], tc[j] = tc[j], tc[i]
		})

		sort.Sort(tc)
		require.Equal(t, sortedTargets, tc)
	}
}

// TestSpanTargetsConstructedInSystemSpanConfigKeyspace ensures that
// constructing span targets
func TestSpanTargetsConstructedInSystemSpanConfigKeyspace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []roachpb.Span{
		MakeEntireKeyspaceTarget().encode(),
		TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.MakeTenantID(10), roachpb.MakeTenantID(10)).encode(),
		TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, roachpb.SystemTenantID).encode(),
		TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.SystemTenantID, roachpb.MakeTenantID(10)).encode(),
		{
			// Extends into from the left
			Key:    keys.TimeseriesKeyMax,
			EndKey: keys.SystemSpanConfigPrefix.Next(), // End Key isn't inclusive.
		},
		{
			// Entirely contained.
			Key:    keys.SystemSpanConfigPrefix.Next(),
			EndKey: keys.SystemSpanConfigPrefix.Next().PrefixEnd(),
		},
		{
			// Extends beyond on the right.
			Key:    keys.SystemSpanConfigPrefix.Next().PrefixEnd(),
			EndKey: keys.SystemSpanConfigKeyMax.Next().Next(),
		},
	} {
		require.Panics(t, func() { MakeTargetFromSpan(tc) })
	}
}
