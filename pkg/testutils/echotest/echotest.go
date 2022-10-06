// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package echotest

import (
	"fmt"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// Require checks that the string matches what is found in the file located at
// the provided path. The file must follow the datadriven format:
//
// echo
// ----
// <output of exp>
//
// The contents of the file can be updated automatically using datadriven's
// -rewrite flag.
//
// For table-driven tests, consider using Walk() instead.
func Require(t *testing.T, act, path string) {
	t.Helper()
	var ran bool
	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
		t.Helper()
		if d.Cmd != "echo" {
			return "only 'echo' is supported"
		}
		ran = true
		return act
	})
	if !ran {
		// Guard against a possible error in which the file is created, then datadriven
		// is invoked with -rewrite to seed it (which it does not do, since there is
		// no directive in the file), and then also the tests pass despite not checking
		// anything.
		t.Errorf("no tests run for %s, is the file empty?", path)
	}
}

// Walker helps automate table-driven tests. See Walk.
type Walker struct {
	dir   string
	files map[string]struct{} // base file names

	missingFiles []string
}

// Walk helps automate table-driven tests in which each test case produces
// output that should be verified. The provided directory is the base in which
// all files (i.e. the expected outputs, one per test case) are kept.
//
// Model usage:
//
//	for _, test := range []struct{ name string }{
//		{name: "foo"},
//		{name: "bar"},
//	} {
//		t.Run(test.name, w.Do(t, test.name, func(t *testing.T, path string) {
//			Require(t, fmt.Sprintf("hello, %s", test.name), path)
//		}))
//	}
func Walk(t T, dir string) Walker {
	maybeHelper(t)()
	files := map[string]struct{}{}
	sl, err := filepath.Glob(filepath.Join(dir, "*"))
	require.NoError(t, err)
	for _, f := range sl {
		base := filepath.Base(f)
		files[base] = struct{}{}
	}
	return Walker{
		dir:   dir,
		files: files,
	}
}

// T is *testing.T stripped down to the bare essentials.
type T interface {
	require.TestingT
}

type tHelper interface {
	Helper()
}

func maybeHelper(t T) func() {
	h, ok := t.(tHelper)
	if !ok {
		return func() {}
	}
	return h.Helper
}

// Do should be passed to `t.Run` to run an individual test case by the given
// name, like so:
//
//	t.Run(test.name, w.Do(t, test.name, func(t *testing.T, path string) {
//	  Require(t, fmt.Sprintf("hello, %s", test.name), path)
//	}))
func (w *Walker) Do(t T, name string, invoke func(t *testing.T, path string)) func(t *testing.T) {
	maybeHelper(t)()
	nname := regexp.MustCompile(`[^0-9a-zA-Z-]+`).ReplaceAllLiteralString(name, "_")
	path := filepath.Join(w.dir, nname)

	_, found := w.files[nname]
	if !found {
		w.missingFiles = append(w.missingFiles, nname)
		return func(t *testing.T) {
			skip.IgnoreLint(t, "testdata file missing")
		}
	}
	// Note that this is outside t.Run (in intended usage), so it's always
	// invoked regardless of -test.run flag.
	delete(w.files, nname)
	return func(t *testing.T) {
		invoke(t, path)
	}
}

// Check should be called before teardown of the test suite. It checks
// that the table driven tests that are present match exactly the list
// of testdata files. If this is not the case, Check outputs a helpful
// error via the provided T.
func (w *Walker) Check(t T) {
	if len(w.files) > 0 {
		var rm = "rm "
		for f := range w.files {
			rm += filepath.Join(w.dir, f) + " "
		}
		t.Errorf("some test files are not referenced by a test case, to remove them use:\n%s", rm)
	}
	if len(w.missingFiles) > 0 {
		mk := fmt.Sprintf(`mkdir -p %[1]q && cd %[1]q && echo 'echo'`, w.dir)
		for _, f := range w.missingFiles {
			mk += fmt.Sprintf(" \\\n  | tee %q", f)
		}
		t.Errorf("some test cases have no test file, to initialize them use the below snippet, then re-run with -rewrite:\n%s", mk)
	}
}
