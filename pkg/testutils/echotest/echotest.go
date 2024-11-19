// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package echotest

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
// For table-driven tests, consider using NewWalker() instead.
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

// Walker helps automate table-driven tests. See NewWalker.
type Walker struct {
	dir   string
	files map[string]struct{} // base file names

	// populate toggles creation of missing files and removal of now unreferenced
	// ones. This is determined based on datadriven's --rewrite flag.
	populate     bool
	missingFiles []string
}

// NewWalker helps automate table-driven tests in which each test case produces
// output that should be verified. The provided directory is the base in which
// all files (i.e. the expected outputs, one per test case) are kept.
//
// Model usage:
//
//	   w := NewWalker(t, datapathutils.TestDataPath(t))
//		 for _, test := range []struct{ name string }{
//		    {name: "foo"},
//		    {name: "bar"},
//		 } {
//		    t.Run(test.name, w.Run(t, test.name, func(t *testing.T, path string) {
//		       Require(t, fmt.Sprintf("hello, %s", test.name), path)
//		    }))
//		 }
//
// w := NewWalker(t, datapathutils.TestDataPath(t))
//
//	for _, test := range []struct{ name string }{
//		{name: "foo"},
//		{name: "bar"},
//	} {
//		t.Run(test.name, w.Run(t, test.name, func(t *testing.T, path string) {
//			Require(t, fmt.Sprintf("hello, %s", test.name), path)
//		}))
//	}
func NewWalker(t T, dir string) *Walker {
	maybeHelper(t)()
	files := map[string]struct{}{}
	sl, err := filepath.Glob(filepath.Join(dir, "*"))
	require.NoError(t, err)
	for _, f := range sl {
		base := filepath.Base(f)
		files[base] = struct{}{}
	}
	var createMissing bool
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-rewrite") || strings.HasPrefix(arg, "--rewrite") {
			createMissing = true
		}
	}
	w := &Walker{
		dir:      dir,
		files:    files,
		populate: createMissing,
	}
	if c := t.(interface {
		T
		Cleanup(func())
		Failed() bool
	}); c != nil {
		c.Cleanup(func() {
			maybeHelper(c)()
			if c.Failed() {
				return
			}
			w.Check(c)
		})
	}
	return w
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
//	t.Run(test.name, w.Do(t, test.name, func(t *testing.T) string {
//	  return fmt.Sprintf("hello, %s", test.name)
//	}))
func (w *Walker) Run(t T, name string, invoke func(t *testing.T) string) func(t *testing.T) {
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
		t.Helper()
		Require(t, invoke(t), path)
	}
}

// Check should be called before teardown of the test suite. It checks
// that the table driven tests that are present match exactly the list
// of testdata files. If this is not the case, Check outputs a helpful
// error via the provided T.
func (w *Walker) Check(t T) {
	if len(w.files) > 0 {
		var removedFiles []string
		var rm = "rm "
		for f := range w.files {
			if w.populate {
				if err := os.Remove(filepath.Join(w.dir, f)); err != nil {
					t.Errorf("error removing unused testdata file %s: %s", f, err)
					t.FailNow()
				}
				removedFiles = append(removedFiles, f)
			}
			rm += filepath.Join(w.dir, f) + " "
		}
		if w.populate {
			t.Errorf("deleted the following unused test files: %s", removedFiles)
		} else {
			t.Errorf("some test files are not referenced by a test case, to remove them re-run with --rewrite or use:\n%s", rm)
		}
	}
	if len(w.missingFiles) > 0 {
		mk := fmt.Sprintf(`mkdir -p %[1]q && cd %[1]q && echo 'echo'`, w.dir)
		_ = os.MkdirAll(w.dir, 0755)
		for _, f := range w.missingFiles {
			if w.populate {
				if err := os.WriteFile(filepath.Join(w.dir, f), []byte("echo\n"), 0644); err != nil {
					t.Errorf("unable to create missing file %s: %s", f, err)
					t.FailNow()
				}
			}
			mk += fmt.Sprintf(" \\\n  | tee %q", f)
		}
		if w.populate {
			t.Errorf("created the following missing files, please re-run with --rewrite to populate them: %s", w.missingFiles)
		} else {
			t.Errorf("some test cases have no test file, to initialize them either re-run with `--rewrite` twice, OR use the below snippet, then re-run with -rewrite:\n%s", mk)
		}
	}
}
