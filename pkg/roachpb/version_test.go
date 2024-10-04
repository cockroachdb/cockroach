// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"testing"

	"github.com/kr/pretty"
)

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
