// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

	for _, tc := range testCases {
		t.Run(
			tc.desc,
			func(t *testing.T) {
				ctx := &testTimestampCompareContext{loc: tc.location}
				res, err := compareTimestamps(ctx, tc.left, tc.right)
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, res)
				res, err = compareTimestamps(ctx, tc.right, tc.left)
				assert.NoError(t, err)
				assert.Equal(t, -tc.expected, res)
			},
		)
	}
	_, err = compareTimestamps(nil /* ctx */, dMaxDate, dMinDate)
	assert.Error(t, err, "should not be able to compare infinite timestamps")
}

type testTimestampCompareContext struct {
	loc *time.Location
}

func (fcc *testTimestampCompareContext) MustGetPlaceholderValue(p *Placeholder) Datum {
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

func (fcc *testTimestampCompareContext) UnwrapDatum(d Datum) Datum {
	return d
}
