// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
)

// WriteProfile serialized the pprof profile with the given name to a file at
// the given path.
func WriteProfile(t testing.TB, name string, path string) {
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pprof.Lookup(name).WriteTo(f, 0); err != nil {
		t.Fatal(err)
	}
}

// AllocProfileDiff writes two alloc profiles, one before running closure and
// one after. This is similar in spirit to passing the -memprofile flag to a
// test or benchmark, but make it possible to subtract out setup code, which
// -memprofile does not.
//
// Example usage:
//     setupCode()
//     AllocProfileDiff(t, "mem.before", "mem.after", func() {
//       interestingCode()
//     })
//
// The resulting profiles are then diffed via:
//     go tool pprof -base mem.before mem.after
func AllocProfileDiff(t testing.TB, beforePath, afterPath string, fn func()) {
	// Use "allocs" instead of "heap" to match what -memprofile does. Also run
	// runtime.GC immediately before grabbing the profile because the allocs
	// profile is materialized on gc, so this makes sure we have the latest data.
	//
	// https://github.com/golang/go/blob/go1.12.4/src/testing/testing.go#L1264-L1269
	runtime.GC()
	WriteProfile(t, "allocs", beforePath)
	fn()
	runtime.GC()
	WriteProfile(t, "allocs", afterPath)
	t.Logf("to use your alloc profiles: go tool pprof -base %s %s", beforePath, afterPath)
}

// Make the unused linter happy.
var _ = WriteProfile
var _ = AllocProfileDiff
