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
	"path"
	"testing"
)

func TestCallResolver(t *testing.T) {
	cr := NewCallResolver(0, `resolver_test\.go.*$`)
	// Also test that caching doesn't have obvious hiccups.
	for i := 0; i < 2; i++ {
		if l := len(cr.cache); l != i {
			t.Fatalf("cache has %d entries, expected %d", l, i)
		}
		file, _, fun := func() (string, int, string) {
			return cr.Lookup(1)
		}()
		if file != "resolver_test.go" {
			t.Fatalf("wrong file '%s'", file)
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
		file, _, fun := Lookup(0)
		if fun != "TestDefaultCallResolver" {
			t.Fatalf("unexpected caller reported: %s", fun)
		}

		if file != path.Join("util", "caller", "resolver_test.go") {
			t.Fatalf("wrong file '%s'", file)
		}
	}
}
