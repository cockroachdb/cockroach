// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package caller

import (
	"fmt"
	"path"
	"regexp"
	"testing"
)

func TestCallResolver(t *testing.T) {
	cr := NewCallResolver(regexp.MustCompile(`resolver_test\.go.*$`))
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
	defer func() { defaultCallResolver.cache = map[uintptr]*cachedLookup{} }()

	for i := 0; i < 2; i++ {
		if l := len(defaultCallResolver.cache); l != i {
			t.Fatalf("cache has %d entries, expected %d", l, i)
		}
		file, _, fun := Lookup(0)
		if fun != "TestDefaultCallResolver" {
			t.Fatalf("unexpected caller reported: %s", fun)
		}

		// NB: runtime.Caller always returns unix paths.
		if expected := path.Join("util", "caller", "resolver_test.go"); file != expected {
			t.Fatalf("expected '%s' got '%s'", expected, file)
		}
	}
}

func BenchmarkFormattedCaller(b *testing.B) {
	for i := 0; i < b.N; i++ {
		file, line, _ := Lookup(1)
		s := fmt.Sprintf("%s:%d", file, line)
		if testing.Verbose() {
			b.Log(s)
		}
	}
}

func BenchmarkSimpleCaller(b *testing.B) {
	for i := 0; i < b.N; i++ {
		file, line, _ := Lookup(1)
		if testing.Verbose() {
			s := fmt.Sprintf("%s:%d", file, line)
			b.Log(s)
		}
	}
}
