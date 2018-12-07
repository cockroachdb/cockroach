// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package roachpb

import (
	"testing"

	"github.com/kr/pretty"
)

func TestVersionLess(t *testing.T) {
	v := func(major, minor, patch, unstable int32) Version {
		return Version{
			Major:    major,
			Minor:    minor,
			Patch:    patch,
			Unstable: unstable,
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
		{v1: v(2, 0, 0, 0), v2: v(3, 0, 0, 0), less: true},
	}

	for _, test := range testData {
		t.Run("", func(t *testing.T) {
			if a, e := test.v1.Less(test.v2), test.less; a != e {
				t.Errorf("expected %s < %s? %t; got %t", pretty.Sprint(test.v1), pretty.Sprint(test.v2), e, a)
			}
		})
	}
}
