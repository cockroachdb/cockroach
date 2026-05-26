// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package caller

import (
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultCallResolver(t *testing.T) {
	defer func() { defaultCallResolver.cache = map[uintptr]*cachedLookup{} }()

	for i := 0; i < 2; i++ {
		if l := len(defaultCallResolver.cache); l != i {
			t.Fatalf("cache has %d entries, expected %d", l, i)
		}
		file, _, fun := Lookup(0)
		_, runFile, _, _ := runtime.Caller(0)
		t.Logf("%s %s (from %s)", file, fun, runFile)
		if fun != "TestDefaultCallResolver" {
			t.Fatalf("unexpected caller reported: %s", fun)
		}

		// NB: runtime.Caller always returns unix paths.
		if expected := path.Join("util", "caller", "resolver_test.go"); file != expected {
			t.Fatalf("expected '%s' got '%s'", expected, file)
		}
	}
}

func BenchmarkSimpleCaller(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = Lookup(1)
	}
}

func Test_parseFQFun(t *testing.T) {
	tests := []struct {
		fqFun  string
		expPkg string
		expFun string
	}{
		{
			fqFun:  "github.com/foo/bar/baz.(*Something).Else",
			expPkg: "github.com/foo/bar/baz",
			expFun: "(*Something).Else",
		},
		{
			fqFun:  "github.com/foo/bar/baz.Else",
			expPkg: "github.com/foo/bar/baz",
			expFun: "Else",
		},
		{
			fqFun:  "github.com/foo/bar/baz..func1",
			expPkg: "github.com/foo/bar/baz",
			expFun: ".func1",
		},
		{
			fqFun:  "main.(*Bar).Baz",
			expPkg: "main",
			expFun: "(*Bar).Baz",
		},
		{
			fqFun:  "main.init",
			expPkg: "main",
			expFun: "init",
		},
		{
			fqFun:  "github.com/cockroachdb/cockroach/pkg/kv/kvserver.(*Replica).Foo",
			expPkg: "kv/kvserver", // special-cased prefix was stripped
			expFun: "(*Replica).Foo",
		},
		{
			fqFun:  "github.com/cockroachdb/pebble/foo.Bar",
			expPkg: "pebble/foo", // special-cased prefix was stripped
			expFun: "Bar",
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			pkg, fun := parseFQFun(tt.fqFun)
			require.Equal(t, tt.expPkg, pkg)
			require.Equal(t, tt.expFun, fun)
		})
	}
}
