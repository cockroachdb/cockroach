// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type enumVersionSkewCompareContext struct {
	noopUnwrapCompareContext
	allow bool
}

func (c enumVersionSkewCompareContext) AllowEnumVersionSkew() bool { return c.allow }

// TestEnumVersionSkewComparison verifies which comparisons of enum values
// produced with versions of the type more than one version apart are allowed by
// the sql.enum.allow_version_skew_in_comparisons.enabled escape hatch.
func TestEnumVersionSkewComparison(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test exercises the relaxation of the assertion, which test builds
	// disable by default.
	defer TestingRelaxEnumVersionSkewAssertion()()

	ctx := context.Background()

	type member struct {
		logical  string
		physical byte
	}
	makeEnum := func(version uint32, members []member) *types.T {
		typ := types.MakeEnum(catid.TypeIDToOID(500), catid.TypeIDToOID(100500))
		typ.TypeMeta = types.UserDefinedTypeMetadata{
			Name:    &types.UserDefinedTypeName{Schema: "public", Name: "e"},
			Version: version,
		}
		if len(members) == 0 {
			// Leave the type unhydrated.
			return typ
		}
		meta := &types.EnumMetadata{IsMemberReadOnly: make([]bool, len(members))}
		for _, m := range members {
			meta.LogicalRepresentations = append(meta.LogicalRepresentations, m.logical)
			meta.PhysicalRepresentations = append(meta.PhysicalRepresentations, []byte{m.physical})
		}
		typ.TypeMeta.EnumData = meta
		return typ
	}
	// value describes the value with the given logical representation of an
	// enum with the given members at the given version.
	type value struct {
		version uint32
		members []member
		logical string
	}
	makeDEnum := func(v value) *DEnum {
		d, err := MakeDEnumFromLogicalRepresentation(makeEnum(v.version, v.members), v.logical)
		require.NoError(t, err)
		return &d
	}

	// abc is the initial set of members. Each of the others is derived from abc
	// by the changes described above it, so that each test case below compares
	// versions that differ only by the change the case is named after. The
	// descriptor versions in the test cases are far apart so that the escape
	// hatch applies.
	abc := []member{{"a", 0x40}, {"b", 0x80}, {"c", 0xc0}}
	// ADD VALUE 'b2' BEFORE 'c'.
	added := []member{{"a", 0x40}, {"b", 0x80}, {"b2", 0xa0}, {"c", 0xc0}}
	// DROP VALUE 'b'.
	dropped := []member{{"a", 0x40}, {"c", 0xc0}}
	// ADD VALUE 'b2' BEFORE 'c' and DROP VALUE 'b'.
	addedAndDropped := []member{{"a", 0x40}, {"b2", 0xa0}, {"c", 0xc0}}
	// DROP VALUE 'b' and ADD VALUE 'x' AFTER 'a', reusing the physical
	// representation of 'b'.
	reused := []member{{"a", 0x40}, {"x", 0x80}, {"c", 0xc0}}
	// RENAME VALUE 'a' TO 'aa'.
	renamed := []member{{"aa", 0x40}, {"b", 0x80}, {"c", 0xc0}}
	// DROP VALUE 'b' and ADD VALUE 'b' AFTER 'c', moving 'b' to a new physical
	// representation.
	moved := []member{{"a", 0x40}, {"c", 0xc0}, {"b", 0xe0}}
	// DROP VALUE 'b' and ADD VALUE 'b' AFTER 'a', which gives 'b' the same
	// physical representation as before, so the members are the same as abc.
	readded := abc

	testCases := []struct {
		name        string
		left, right value
		// expected is the result of the comparison when the escape hatch is
		// enabled.
		expected int
		// expectedErr, if set, indicates that the comparison fails even when
		// the escape hatch is enabled.
		expectedErr bool
	}{
		{name: "older value vs. added value", left: value{10, abc, "a"}, right: value{20, added, "b2"}, expected: -1},
		{name: "added value vs. older value", left: value{20, added, "b2"}, right: value{10, abc, "c"}, expected: -1},
		{name: "same value across an addition", left: value{10, abc, "c"}, right: value{20, added, "c"}, expected: 0},
		{name: "dropped value vs. surviving value", left: value{10, abc, "b"}, right: value{20, dropped, "c"}, expected: -1},
		{name: "surviving value vs. dropped value", left: value{20, dropped, "a"}, right: value{10, abc, "b"}, expected: -1},
		{name: "dropped value vs. added value", left: value{10, abc, "b"}, right: value{30, addedAndDropped, "b2"}, expected: -1},
		{name: "dropped value vs. reused bytes", left: value{10, abc, "b"}, right: value{30, reused, "x"}, expectedErr: true},
		{name: "reused bytes vs. unrelated value", left: value{30, reused, "x"}, right: value{10, abc, "c"}, expectedErr: true},
		{name: "older name vs. renamed value", left: value{10, abc, "a"}, right: value{20, renamed, "aa"}, expectedErr: true},
		{name: "renamed value vs. older name", left: value{20, renamed, "aa"}, right: value{10, abc, "a"}, expectedErr: true},
		{name: "unrelated value across a rename", left: value{10, abc, "c"}, right: value{20, renamed, "c"}, expectedErr: true},
		{name: "older value vs. moved value", left: value{10, abc, "b"}, right: value{30, moved, "b"}, expectedErr: true},
		{name: "unrelated value across a move", left: value{10, abc, "c"}, right: value{30, moved, "c"}, expectedErr: true},
		{name: "older value vs. re-added value", left: value{10, abc, "b"}, right: value{30, readded, "b"}, expected: 0},
	}

	for _, enabled := range []bool{false, true} {
		cmpCtx := enumVersionSkewCompareContext{allow: enabled}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("enabled=%t/%s", enabled, tc.name), func(t *testing.T) {
				res, err := makeDEnum(tc.left).Compare(ctx, cmpCtx, makeDEnum(tc.right))
				if !enabled || tc.expectedErr {
					require.ErrorContains(t, err, "comparison of two different versions of enum")
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.expected, res)
			})
		}
	}

	enabled := enumVersionSkewCompareContext{allow: true}
	// Comparing values of the same version is unaffected by the setting.
	res, err := makeDEnum(value{10, abc, "a"}).Compare(ctx, enabled, makeDEnum(value{10, abc, "b"}))
	require.NoError(t, err)
	require.Equal(t, -1, res)

	// A value is identified by its own logical representation since the version
	// of its type might not be the one that produced it (e.g. if the type was
	// hydrated in place with another version). Here 'x' was produced by the
	// version with reused bytes, but its type is at a version in which its
	// physical representation belongs to 'b'.
	stale := &DEnum{EnumTyp: makeEnum(20, added), PhysicalRep: []byte{0x80}, LogicalRep: "x"}
	_, err = stale.Compare(ctx, enabled, makeDEnum(value{10, abc, "b"}))
	require.ErrorContains(t, err, "comparison of two different versions of enum")

	// Values of an unhydrated enum can't be checked, so the comparison fails.
	unhydrated := &DEnum{EnumTyp: makeEnum(0, nil), PhysicalRep: []byte{0x40}, LogicalRep: "a"}
	_, err = makeDEnum(value{10, abc, "a"}).Compare(ctx, enabled, unhydrated)
	require.ErrorContains(t, err, "comparison of two different versions of enum")
}
