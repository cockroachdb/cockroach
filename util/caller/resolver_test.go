// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package caller

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/testutils/fakeflags"
)

func init() {
	// Required since `util/log`->`util/caller`.
	fakeflags.InitLogFlags()
}

func TestCallResolver(t *testing.T) {
	cr := NewCallResolver(0, `resolver_test\.go.*$`)
	// Also test that caching doesn't have obvious hiccups.
	for i := 0; i < 2; i++ {
		if l := len(cr.cache); l != i {
			t.Fatalf("cache has %d entries, expected %d", l, i)
		}
		file, line, fun := func() (string, int, string) {
			return cr.Lookup(1)
		}()
		fileline := fmt.Sprintf("%s:%d", file, line)
		if matched, err := regexp.MatchString(`^resolver_test\.go`, fileline); !matched || err != nil {
			t.Fatalf("wrong file:line '%s'", fileline)
		}
		if fun != "TestCallResolver" {
			t.Fatalf("unexpected caller reported: %s", fun)
		}
	}
}

func TestDefaultCallResolver(t *testing.T) {
	for i := 0; i < 2; i++ {
		if l := len(defaultCallResolver.cache); l != i {
			t.Fatalf("cache has %d entries, expected %d", l, i)
		}
		file, line, fun := Lookup(0)
		if fun != "TestDefaultCallResolver" {
			t.Fatalf("unexpected caller reported: %s", fun)
		}

		fileline := fmt.Sprintf("%s:%d", file, line)
		if matched, err := regexp.MatchString(`^util/caller/resolver_test\.go`, fileline); !matched || err != nil {
			t.Fatalf("wrong file:line '%s'", fileline)
		}
	}
}
