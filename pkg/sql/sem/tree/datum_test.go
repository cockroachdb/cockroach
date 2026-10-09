// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pacificTimeZone := int32(7 * 60 * 60)
	sydneyTimeZone := int32(-10 * 60 * 60)

	sydneyFixedZone := time.FixedZone("otan@sydney", -int(sydneyTimeZone))
	// kiwiFixedZone is 2 hours ahead of Sydney.
	kiwiFixedZone := time.FixedZone("otan@auckland", -int(sydneyTimeZone)+2*60*60)

	// No daylight savings in Hawaii!
	ddate, err := NewDDateFromTime(time.Date(2019, time.November, 22, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	testCases := []struct {
		desc     string
		left     Datum
		right    Datum
		location *time.Location
		expected int
	}{
		{
			desc:     "same DTime are equal",
			left:     MakeDTime(timeofday.New(12, 0, 0, 0)),
			right:    MakeDTime(timeofday.New(12, 0, 0, 0)),
			expected: 0,
		},
		{
			desc:     "same DTimeTZ are equal",
			left:     NewDTimeTZFromOffset(timeofday.New(22, 0, 0, 0), sydneyTimeZone),
			right:    NewDTimeTZFromOffset(timeofday.New(22, 0, 0, 0), sydneyTimeZone),
			expected: 0,
		},
		{
			desc:     "DTime and DTimeTZ both UTC, and so are equal",
			left:     MakeDTime(timeofday.New(12, 0, 0, 0)),
			right:    NewDTimeTZFromOffset(timeofday.New(12, 0, 0, 0), 0),
			expected: 0,
		},
		{
			desc:     "DTime and DTimeTZ both Sydney time, and so are equal",
			left:     MakeDTime(timeofday.New(12, 0, 0, 0)),
			right:    NewDTimeTZFromOffset(timeofday.New(12, 0, 0, 0), sydneyTimeZone),
			location: sydneyFixedZone,
			expected: 0,
		},
		{
			desc:     "DTimestamp and DTimestampTZ (Sydney) equal in Sydney zone",
			left:     MustMakeDTimestamp(time.Date(2019, time.November, 22, 10, 0, 0, 0, time.UTC), time.Microsecond),
			right:    MustMakeDTimestampTZ(time.Date(2019, time.November, 22, 10, 0, 0, 0, sydneyFixedZone), time.Microsecond),
			location: sydneyFixedZone,
			expected: 0,
		},
		{
			desc:     "DTimestamp and DTimestampTZ (Sydney) equal in Sydney+2 zone",
			left:     MustMakeDTimestamp(time.Date(2019, time.November, 22, 12, 0, 0, 0, time.UTC), time.Microsecond),
			right:    MustMakeDTimestampTZ(time.Date(2019, time.November, 22, 10, 0, 0, 0, sydneyFixedZone), time.Microsecond),
			location: kiwiFixedZone,
			expected: 0,
		},
		{
			desc:     "Date and DTimestampTZ (Sydney) equal in Sydney zone",
			left:     ddate,
			right:    MustMakeDTimestampTZ(time.Date(2019, time.November, 22, 0, 0, 0, 0, sydneyFixedZone), time.Microsecond),
			location: sydneyFixedZone,
			expected: 0,
		},
		{
			desc:     "Date and DTimestampTZ (Sydney) equal in Sydney+2 zone",
			left:     ddate,
			right:    MustMakeDTimestampTZ(time.Date(2019, time.November, 21, 22, 0, 0, 0, sydneyFixedZone), time.Microsecond),
			location: kiwiFixedZone,
			expected: 0,
		},
		{
			desc:     "equal wall clock time for DTime and DTimeTZ, with TimeTZ ahead",
			left:     MakeDTime(timeofday.New(12, 0, 0, 0)),
			right:    NewDTimeTZFromOffset(timeofday.New(22, 0, 0, 0), sydneyTimeZone),
			expected: 1,
		},
		{
			desc:     "equal wall clock time for DTime and DTimeTZ, with TimeTZ behind",
			left:     MakeDTime(timeofday.New(12, 0, 0, 0)),
			right:    NewDTimeTZFromOffset(timeofday.New(5, 0, 0, 0), pacificTimeZone),
			expected: -1,
		},
		{
			desc:     "equal wall clock time for DTime and DTimeTZ, with TimeTZ ahead",
			left:     NewDTimeTZFromOffset(timeofday.New(22, 0, 0, 0), sydneyTimeZone),
			right:    NewDTimeTZFromOffset(timeofday.New(5, 0, 0, 0), pacificTimeZone),
			expected: -1,
		},
		{
			desc:     "wall clock time different for DTimeTZ and DTimeTZ",
			left:     NewDTimeTZFromOffset(timeofday.New(23, 0, 0, 0), sydneyTimeZone),
			right:    NewDTimeTZFromOffset(timeofday.New(5, 0, 0, 0), pacificTimeZone),
			expected: 1,
		},
		{
			desc:     "positive infinite date",
			left:     dMaxDate,
			right:    MakeDTime(timeofday.New(12, 0, 0, 0)),
			expected: 1,
		},
		{
			desc:     "negative infinite date",
			left:     dMinDate,
			right:    MakeDTime(timeofday.New(12, 0, 0, 0)),
			expected: -1,
		},
	}

	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(
			tc.desc,
			func(t *testing.T) {
				cmpCtx := &testTimestampCompareContext{loc: tc.location}
				res, err := compareTimestamps(ctx, cmpCtx, tc.left, tc.right)
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, res)
				res, err = compareTimestamps(ctx, cmpCtx, tc.right, tc.left)
				assert.NoError(t, err)
				assert.Equal(t, -tc.expected, res)
			},
		)
	}
	_, err = compareTimestamps(ctx, nil /* ctx */, dMaxDate, dMinDate)
	assert.Error(t, err, "should not be able to compare infinite timestamps")
}

type testTimestampCompareContext struct {
	loc *time.Location
}

func (fcc *testTimestampCompareContext) MustGetPlaceholderValue(
	ctx context.Context, p *Placeholder,
) Datum {
	panic("not implemented")
}

var _ CompareContext = (*testTimestampCompareContext)(nil)

func (fcc *testTimestampCompareContext) GetRelativeParseTime() time.Time {
	if fcc.loc != nil {
		return timeutil.Now().In(fcc.loc)
	}
	return timeutil.Now()
}

func (fcc *testTimestampCompareContext) GetLocation() *time.Location {
	if fcc.loc != nil {
		return fcc.loc
	}
	return time.UTC
}

func (fcc *testTimestampCompareContext) UnwrapDatum(ctx context.Context, d Datum) Datum {
	return d
}

func BenchmarkDatumCompare(b *testing.B) {
	ctx := context.Background()
	compareCtx := &testTimestampCompareContext{}
	for _, tc := range []struct {
		name     string
		d, other Datum
	}{
		{name: "DIntToDOid", d: DZero, other: NewDOid(oid.Oid(0))},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = tc.d.Compare(ctx, compareCtx, tc.other)
			}
		})
	}
}

// TestDEnumCompareAcrossVersions verifies that values of an enum produced with
// adjacent descriptor versions of the type can be compared as long as the
// versions are consistent, while values of versions further apart cannot.
func TestDEnumCompareAcrossVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test exercises the relaxation of the assertion for adjacent enum
	// versions, which test builds disable by default.
	defer TestingRelaxEnumVersionSkewAssertion()()

	ctx := context.Background()
	type member struct {
		logical  string
		physical byte
		readOnly bool
	}
	// value describes the value with the given logical representation of an
	// enum with the given members at the given version.
	type value struct {
		version uint32
		members []member
		logical string
	}
	makeDEnum := func(v value) *DEnum {
		meta := &types.EnumMetadata{}
		for _, m := range v.members {
			meta.LogicalRepresentations = append(meta.LogicalRepresentations, m.logical)
			meta.PhysicalRepresentations = append(meta.PhysicalRepresentations, []byte{m.physical})
			meta.IsMemberReadOnly = append(meta.IsMemberReadOnly, m.readOnly)
		}
		typ := types.MakeEnum(catid.TypeIDToOID(500), catid.TypeIDToOID(100500))
		typ.TypeMeta = types.UserDefinedTypeMetadata{
			Name:    &types.UserDefinedTypeName{Schema: "public", Name: "e"},
			Version: v.version,
		}
		typ.TypeMeta.EnumData = meta
		// Construct the value directly since read-only members can't be
		// constructed from their logical representation.
		idx := slices.Index(meta.LogicalRepresentations, v.logical)
		require.NotEqual(t, -1, idx)
		return &DEnum{EnumTyp: typ, PhysicalRep: meta.PhysicalRepresentations[idx], LogicalRep: v.logical}
	}

	ab := []member{{logical: "a", physical: 0x40}, {logical: "b", physical: 0x80}}
	// The declarative schema changer renames a value by first adding the new
	// name as a read-only member with the same physical representation while
	// demoting the old one, and it removes the old one in the next version.
	declarativeRename := []member{
		{logical: "aa", physical: 0x40, readOnly: true},
		{logical: "a", physical: 0x40, readOnly: true},
		{logical: "b", physical: 0x80},
	}
	testCases := []struct {
		name        string
		left, right value
		expected    int
		expectedErr bool
	}{
		{name: "same version", left: value{2, ab, "a"}, right: value{2, ab, "b"}, expected: -1},
		{name: "newer version on the right", left: value{2, ab, "a"}, right: value{3, ab, "b"}, expected: -1},
		{name: "newer version on the left", left: value{3, ab, "a"}, right: value{2, ab, "b"}, expected: -1},
		{name: "two versions apart", left: value{2, ab, "a"}, right: value{4, ab, "b"}, expectedErr: true},
		{name: "two versions apart reversed", left: value{4, ab, "a"}, right: value{2, ab, "b"}, expectedErr: true},
		{
			name:     "added value",
			left:     value{2, ab, "a"},
			right:    value{3, append(slices.Clone(ab), member{logical: "c", physical: 0xc0, readOnly: true}), "c"},
			expected: -1,
		},
		{
			name:     "dropped value",
			left:     value{2, []member{{logical: "a", physical: 0x40}, {logical: "b", physical: 0x80, readOnly: true}}, "b"},
			right:    value{3, []member{{logical: "a", physical: 0x40}}, "a"},
			expected: 1,
		},
		{
			// The legacy schema changer renames a value in place.
			name:        "renamed value/legacy schema changer",
			left:        value{2, ab, "a"},
			right:       value{3, []member{{logical: "aa", physical: 0x40}, {logical: "b", physical: 0x80}}, "aa"},
			expectedErr: true,
		},
		{
			name:        "renamed value/declarative schema changer/new name",
			left:        value{2, ab, "a"},
			right:       value{3, declarativeRename, "aa"},
			expectedErr: true,
		},
		{
			name:        "renamed value/declarative schema changer/old name",
			left:        value{3, declarativeRename, "a"},
			right:       value{4, []member{{logical: "aa", physical: 0x40}, {logical: "b", physical: 0x80}}, "aa"},
			expectedErr: true,
		},
		{
			// The legacy schema changer can swap two names in a single version,
			// so the same logical representation denotes different values.
			name:        "swapped names",
			left:        value{2, ab, "a"},
			right:       value{3, []member{{logical: "b", physical: 0x40}, {logical: "a", physical: 0x80}}, "a"},
			expectedErr: true,
		},
		// The versions must agree on all values, not only the compared ones,
		// since a plan built against one version might contain other values
		// that mean something else in the other version.
		{
			name:        "unrelated value/renamed value/legacy schema changer",
			left:        value{2, ab, "b"},
			right:       value{3, []member{{logical: "aa", physical: 0x40}, {logical: "b", physical: 0x80}}, "b"},
			expectedErr: true,
		},
		{
			name:        "unrelated value/renamed value/declarative schema changer",
			left:        value{2, ab, "b"},
			right:       value{3, declarativeRename, "b"},
			expectedErr: true,
		},
		{
			name: "unrelated value/swapped names",
			left: value{2, append(slices.Clone(ab), member{logical: "c", physical: 0xc0}), "c"},
			right: value{3, []member{
				{logical: "b", physical: 0x40}, {logical: "a", physical: 0x80}, {logical: "c", physical: 0xc0},
			}, "c"},
			expectedErr: true,
		},
		{
			// A value can't move to another physical representation between
			// adjacent versions in practice (that takes dropping it and adding it
			// back), but the check must still detect that the same logical
			// representation denotes a different value.
			name:        "unrelated value/moved value",
			left:        value{2, ab, "a"},
			right:       value{3, []member{{logical: "a", physical: 0x40}, {logical: "b", physical: 0xe0}}, "a"},
			expectedErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := makeDEnum(tc.left).Compare(ctx, noopUnwrapCompareContext{}, makeDEnum(tc.right))
			if tc.expectedErr {
				require.ErrorContains(t, err, "comparison of two different versions of enum")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, res)
		})
	}

	// A value that is a member of neither version was produced with yet another
	// version of the type.
	t.Run("value of neither version", func(t *testing.T) {
		left := makeDEnum(value{2, ab, "a"})
		left.PhysicalRep, left.LogicalRep = []byte{0x60}, "x"
		_, err := left.Compare(ctx, noopUnwrapCompareContext{}, makeDEnum(value{3, ab, "a"}))
		require.ErrorContains(t, err, "comparison of two different versions of enum")
	})
}
