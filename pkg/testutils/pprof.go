// Copyright 2019 The Cockroach Authors.
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
