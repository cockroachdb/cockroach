// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testfixtures

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors/oserror"
)

const baseDirEnv = "COCKROACH_TEST_FIXTURES_DIR"
const subdir = "crdb-test-fixtures"

// ReuseOrGenerate is used for fixtures that can be reused across invocations of tests or
// benchmarks (or across different tests / benchmarks).
//
// A fixture corresponds to a directory. If a fixture with the given name was
// already generated, ReuseOrGenerate just returns the directory.
//
// Otherwise, generate() is called with the directory; upon completion
// ReuseOrGenerate returns the same directory.
//
// The directory is normally $HOME/.cache/crdb-text-fixtures/<name>
//
// The base directory can be customized via the `COCKROACH_TEST_FIXTURES_DIR`
// env var (which is set to $HOME/.cache/crdb-test-fixtures when using `dev`).
// If this variable is not specified and $HOME is not set, a temporary directory
// is used.
//
// ReuseOrGenerate gracefully handles interruptions / generation errors: we only
// declare a fixture as reusable once the generate() function returns.
// ReuseOrGenerate allows multiple calls in parallel with the same fixture name.
//
// ReuseOrGenerate does not support parallel invocations (with the same fixture
// name) in different processes.
//
// Note: currently this function is only used for benchmarks and `dev` only sets
// COCKROACH_TEST_FIXTURES_DIR for benchmarks. If we start using it for regular tests,
// `dev` should be updated to set it for tests as well.
func ReuseOrGenerate(tb testing.TB, name string, generate func(dir string)) (dir string) {
	// If the same fixture name is in the in-progress map, wait.
	for first := true; ; first = false {
		mu.Lock()
		ch, ok := mu.inProgress[name]
		if !ok {
			mu.inProgress[name] = make(chan struct{})
			mu.Unlock()
			break
		}
		mu.Unlock()
		if first {
			tb.Logf("waiting for fixture %q", name)
		}
		// Wait for channel close.
		<-ch
	}

	defer func() {
		mu.Lock()
		defer mu.Unlock()
		close(mu.inProgress[name])
		delete(mu.inProgress, name)
	}()

	baseDir := envutil.EnvOrDefaultString(baseDirEnv, "")
	if baseDir == "" {
		if cacheDir, err := os.UserCacheDir(); err == nil {
			baseDir = filepath.Join(cacheDir, subdir)
		} else {
			baseDir = filepath.Join(os.TempDir(), subdir)
		}
	}
	dir = filepath.Join(baseDir, name)
	const completedFile = ".crdb-test-fixture-completed"
	completedPath := filepath.Join(dir, completedFile)
	if exists(tb, dir, true /* expectDir */) {
		if exists(tb, completedPath, false /* expectDir */) {
			tb.Logf("using existing fixture %q in %q", name, dir)
			return dir
		}
		// Directory exists but fixture was not completed; clean up.
		if err := os.RemoveAll(dir); err != nil {
			tb.Fatal(err)
		}
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		tb.Fatal(err)
	}
	tb.Logf("generating fixture %q in %q", name, dir)
	generate(dir)
	if err := os.WriteFile(completedPath, []byte("foo"), 0755); err != nil {
		tb.Fatal(err)
	}
	tb.Logf("successfully generated fixture %q in %q", name, dir)
	return dir
}

type inProgressMap struct {
	syncutil.Mutex
	inProgress map[string]chan struct{}
}

var mu = inProgressMap{
	inProgress: make(map[string]chan struct{}),
}

// exists returns true if the file or directory exists. If expectDir is true, it
// also asserts that the path is a directory.
func exists(tb testing.TB, path string, expectDir bool) bool {
	tb.Helper()
	stat, err := os.Stat(path)
	if err != nil {
		if oserror.IsNotExist(err) {
			return false
		}
		tb.Fatal(err)
	}
	if expectDir && !stat.IsDir() {
		tb.Fatalf("%q exists but is not directory", path)
	}
	return true
}
