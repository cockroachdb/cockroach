// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package version

import (
	"fmt"
	"testing"
)

func TestGetters(t *testing.T) {
	v, err := Parse("v1.2.3-beta+md")
	if err != nil {
		t.Fatal(err)
	}
	str := fmt.Sprintf(
		"%d %d %d %s %s", v.Major(), v.Minor(), v.Patch(), v.PreRelease(), v.Metadata(),
	)
	exp := "1 2 3 beta md"
	if str != exp {
		t.Errorf("got '%s', expected '%s'", str, exp)
	}
}

func TestValid(t *testing.T) {
	testData := []string{
		"v0.0.0",
		"v0.0.1",
		"v0.1.0",
		"v1.0.0",
		"v1.0.0-alpha",
		"v1.0.0-beta.20190101",
		"v1.0.0-rc1-with-hyphen",
		"v1.0.0-rc2.dot.dot",
		"v1.2.3+metadata",
		"v1.2.3+metadata-with-hyphen",
		"v1.2.3+metadata.with.dots",
		"v1.1.2-beta.20190101+metadata",
		"v1.2.3-rc1-with-hyphen+metadata-with-hyphen",
	}
	for _, str := range testData {
		v, err := Parse(str)
		if err != nil {
			t.Errorf("%s: %s", str, err)
		}
		if v.String() != str {
			t.Errorf("%s roundtripped to %s", str, v.String())
		}
	}
}

func TestInvalid(t *testing.T) {
	testData := []string{
		"v1",
		"v1.2",
		"v1.2-beta",
		"v1x2.3",
		"v1.2x3",
		"1.0.0",
		" v1.0.0",
		"v1.0.0  ",
		"v1.2.beta",
		"v1.2-beta",
		"v1.2.3.beta",
		"v1.2.3-beta$",
		"v1.2.3-bet;a",
		"v1.2.3+metadata%",
		"v01.2.3",
		"v1.02.3",
		"v1.2.03",
	}
	for _, str := range testData {
		if _, err := Parse(str); err == nil {
			t.Errorf("expected error for %s", str)
		}
	}
}

func TestCompare(t *testing.T) {
	testData := []struct {
		a, b string
		cmp  int
	}{
		{"v1.0.0", "v1.0.0", 0},
		{"v1.0.0", "v1.0.1", -1},
		{"v1.2.3", "v1.3.0", -1},
		{"v1.2.3", "v2.0.0", -1},
		{"v1.0.0+metadata", "v1.0.0", 0},
		{"v1.0.0+metadata", "v1.0.0+other.metadata", 0},
		{"v1.0.1+metadata", "v1.0.0+other.metadata", +1},
		{"v1.0.0", "v1.0.0-alpha", +1},
		{"v1.0.0", "v1.0.0-rc2", +1},
		{"v1.0.0-alpha", "v1.0.0-beta", -1},
		{"v1.0.0-beta", "v1.0.0-rc.2", -1},
		{"v1.0.0-rc.2", "v1.0.0-rc.10", -1},
		{"v1.0.1", "v1.0.0-alpha", +1},

		// Tests below taken from coreos/go-semver, which carries the following
		// copyright:
		//
		// Copyright 2013-2015 CoreOS, Inc.
		// Copyright 2018 The Cockroach Authors.
		//
		// Licensed under the Apache License, Version 2.0 (the "License");
		// you may not use this file except in compliance with the License.
		// You may obtain a copy of the License at
		//
		//     http://www.apache.org/licenses/LICENSE-2.0
		//
		// Unless required by applicable law or agreed to in writing, software
		// distributed under the License is distributed on an "AS IS" BASIS,
		// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
		// See the License for the specific language governing permissions and
		// limitations under the License.
		{"v0.0.0", "v0.0.0-foo", +1},
		{"v0.0.1", "v0.0.0", +1},
		{"v1.0.0", "v0.9.9", +1},
		{"v0.10.0", "v0.9.0", +1},
		{"v0.99.0", "v0.10.0", +1},
		{"v2.0.0", "v1.2.3", +1},
		{"v0.0.0", "v0.0.0-foo", +1},
		{"v0.0.1", "v0.0.0", +1},
		{"v1.0.0", "v0.9.9", +1},
		{"v0.10.0", "v0.9.0", +1},
		{"v0.99.0", "v0.10.0", +1},
		{"v2.0.0", "v1.2.3", +1},
		{"v0.0.0", "v0.0.0-foo", +1},
		{"v0.0.1", "v0.0.0", +1},
		{"v1.0.0", "v0.9.9", +1},
		{"v0.10.0", "v0.9.0", +1},
		{"v0.99.0", "v0.10.0", +1},
		{"v2.0.0", "v1.2.3", +1},
		{"v1.2.3", "v1.2.3-asdf", +1},
		{"v1.2.3", "v1.2.3-4", +1},
		{"v1.2.3", "v1.2.3-4-foo", +1},
		{"v1.2.3-5-foo", "v1.2.3-5", +1},
		{"v1.2.3-5", "v1.2.3-4", +1},
		{"v1.2.3-5-foo", "v1.2.3-5-Foo", +1},
		{"v3.0.0", "v2.7.2+asdf", +1},
		{"v3.0.0+foobar", "v2.7.2", +1},
		{"v1.2.3-a.10", "v1.2.3-a.5", +1},
		{"v1.2.3-a.b", "v1.2.3-a.5", +1},
		{"v1.2.3-a.b", "v1.2.3-a", +1},
		{"v1.2.3-a.b.c.10.d.5", "v1.2.3-a.b.c.5.d.100", +1},
		{"v1.0.0", "v1.0.0-rc.1", +1},
		{"v1.0.0-rc.2", "v1.0.0-rc.1", +1},
		{"v1.0.0-rc.1", "v1.0.0-beta.11", +1},
		{"v1.0.0-beta.11", "v1.0.0-beta.2", +1},
		{"v1.0.0-beta.2", "v1.0.0-beta", +1},
		{"v1.0.0-beta", "v1.0.0-alpha.beta", +1},
		{"v1.0.0-alpha.beta", "v1.0.0-alpha.1", +1},
		{"v1.0.0-alpha.1", "v1.0.0-alpha", +1},
	}
	for _, tc := range testData {
		a, err := Parse(tc.a)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Parse(tc.b)
		if err != nil {
			t.Fatal(err)
		}
		if cmp := a.Compare(b); cmp != tc.cmp {
			t.Errorf("'%s' vs '%s': expected %d, got %d", tc.a, tc.b, tc.cmp, cmp)
		}
		if cmp := b.Compare(a); cmp != -tc.cmp {
			t.Errorf("'%s' vs '%s': expected %d, got %d", tc.b, tc.a, -tc.cmp, cmp)
		}
	}
}
