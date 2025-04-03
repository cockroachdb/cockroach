// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"strings"
	"testing"

	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestParseVersion(t *testing.T) {
	testData := []struct {
		s         string
		v         Version
		roundtrip bool
	}{
		{s: "23.1", v: Version{Major: 23, Minor: 1}, roundtrip: true},
		{s: "23.1-upgrading-to-23.2-step-004", v: Version{Major: 23, Minor: 1, Internal: 4}, roundtrip: true},
		{s: "1000023.1-upgrading-to-1000023.2-step-004", v: Version{Major: 1000023, Minor: 1, Internal: 4}, roundtrip: true},
		{s: "23.1-4", v: Version{Major: 23, Minor: 1, Internal: 4}},
		{s: "23.1-upgrading-step-004", v: Version{Major: 23, Minor: 1, Internal: 4}},
		// NB: The fence version for a final version will have Internal=-1.
		{s: "23.2-upgrading-final-step", v: Version{Major: 23, Minor: 2, Internal: -1}, roundtrip: true},
		// We used to have unintuitive formatting logic for the -1 internal version.
		// See https://github.com/cockroachdb/cockroach/issues/129460.
		{s: "23.2-upgrading-step--01", v: Version{Major: 23, Minor: 2, Internal: -1}},
	}
	for _, tc := range testData {
		t.Run("", func(t *testing.T) {
			v, err := ParseVersion(tc.s)
			require.NoError(t, err)
			require.Equal(t, tc.v, v)
			if tc.roundtrip {
				require.Equal(t, tc.s, v.String())
			}
		})
	}
}

func TestVersionCmp(t *testing.T) {
	v := func(major, minor, patch, internal int32) Version {
		return Version{
			Major:    major,
			Minor:    minor,
			Patch:    patch,
			Internal: internal,
		}
	}
	testData := []struct {
		v1, v2 Version
		less   bool
	}{
		{v1: Version{}, v2: Version{}, less: false},
		{v1: v(0, 0, 0, 0), v2: v(0, 0, 0, 1), less: true},
		{v1: v(0, 0, 0, 2), v2: v(0, 0, 0, 1), less: false},
		{v1: v(0, 0, 1, 0), v2: v(0, 0, 0, 1), less: false},
		{v1: v(0, 0, 1, 0), v2: v(0, 0, 0, 2), less: false},
		{v1: v(0, 0, 1, 1), v2: v(0, 0, 1, 1), less: false},
		{v1: v(0, 0, 1, 0), v2: v(0, 0, 1, 1), less: true},
		{v1: v(0, 1, 1, 0), v2: v(0, 1, 0, 1), less: false},
		{v1: v(0, 1, 0, 1), v2: v(0, 1, 1, 0), less: true},
		{v1: v(1, 0, 0, 0), v2: v(1, 1, 0, 0), less: true},
		{v1: v(1, 1, 0, 1), v2: v(1, 1, 0, 0), less: false},
		{v1: v(1, 1, 0, 1), v2: v(1, 2, 0, 0), less: true},
		{v1: v(2, 1, 0, 0), v2: v(19, 1, 0, 0), less: true},
		{v1: v(19, 1, 0, 0), v2: v(19, 2, 0, 0), less: true},
		{v1: v(19, 2, 0, 0), v2: v(20, 1, 0, 0), less: true},
	}

	for _, test := range testData {
		t.Run("", func(t *testing.T) {
			if a, e := test.v1.Less(test.v2), test.less; a != e {
				t.Errorf("expected %s < %s? %t; got %t", pretty.Sprint(test.v1), pretty.Sprint(test.v2), e, a)
			}
			if a, e := test.v1.Equal(test.v2), test.v1 == test.v2; a != e {
				t.Errorf("expected %s = %s? %t; got %t", pretty.Sprint(test.v1), pretty.Sprint(test.v2), e, a)
			}
			if a, e := test.v1.AtLeast(test.v2), test.v1 == test.v2 || !test.less; a != e {
				t.Errorf("expected %s >= %s? %t; got %t", pretty.Sprint(test.v1), pretty.Sprint(test.v2), e, a)
			}
		})
	}
}

func TestReleaseSeriesSuccessor(t *testing.T) {
	r := ReleaseSeries{20, 1}
	var seq []string
	for ok := true; ok; r, ok = r.Successor() {
		seq = append(seq, r.String())
	}
	expected := "20.1, 20.2, 21.1, 21.2, 22.1, 22.2, 23.1, 23.2, 24.1, 24.2, 24.3, 25.1, 25.2"
	require.Equal(t, expected, strings.Join(seq, ", "))
}

func TestReleaseSeries(t *testing.T) {
	testCases := []struct {
		v Version
		s ReleaseSeries
	}{
		{
			v: Version{Major: 22, Minor: 2, Internal: 0},
			s: ReleaseSeries{Major: 22, Minor: 2},
		},
		{
			v: Version{Major: 22, Minor: 2, Internal: 8},
			s: ReleaseSeries{Major: 23, Minor: 1},
		},
		{
			v: Version{Major: 23, Minor: 1, Internal: 0},
			s: ReleaseSeries{Major: 23, Minor: 1},
		},
		{
			v: Version{Major: 23, Minor: 1, Internal: 2},
			s: ReleaseSeries{Major: 23, Minor: 2},
		},
		{
			v: Version{Major: 23, Minor: 2, Internal: 2},
			s: ReleaseSeries{Major: 24, Minor: 1},
		},
		{
			v: Version{Major: 24, Minor: 1, Internal: 2},
			s: ReleaseSeries{Major: 24, Minor: 2},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			res, ok := tc.v.ReleaseSeries()
			require.True(t, ok)
			require.Equal(t, tc.s, res)
		})
	}
}
