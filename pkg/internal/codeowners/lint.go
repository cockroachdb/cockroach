// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package codeowners

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/stretchr/testify/require"
)

// LintEverythingIsOwned verifies that all files in ./pkg/... have
// at least one owning team. It is called from lints.
func LintEverythingIsOwned(
	t interface {
		Logf(string, ...interface{})
		Errorf(string, ...interface{})
		FailNow()
		Helper()
	},
	verbose bool,
	co *CodeOwners,
	repoRoot string,
	walkDir string, // filepath.Join(repoRoot, walkDir) will need owners
) {
	debug := func(format string, args ...interface{}) {
		if !verbose {
			return
		}
		t.Helper()
		t.Logf(format, args...)
	}

	// Files to be skipped. For each directory or file we're walking over, we
	// check both the path relative to ./pkg (=walkDir in this example) and the
	// base name.
	//
	// Example: with entries "ccl/foo.go", "generated", and "README.md", we will skip:
	// - ./pkg/ccl/foo.go
	// - ./pkg/asd/xyz/README.md
	// - ./pkg/README.md
	// - ./pkg/foo/generated (and anything within)
	// - ./pkg/generated (and anything within)
	// - ./pkg/bar/generated (as a file)
	// but not
	// - ./pkg/asd/foo.go
	// - ./pkg/ccl/bar/foo.go
	// - ./pkg/asd/generated.go
	skip := map[string]struct{}{
		filepath.Join("ccl", "ccl_init.go"): {},
		filepath.Join("ui", "node_modules"): {},
		filepath.Join("ui", "yarn-vendor"):  {},
		"Makefile":                          {},
		"BUILD.bazel":                       {},
		".gitignore":                        {},
		"README.md":                         {},
	}

	// Map of (unowned dir relative to walkRoot) -> (triggering file relative to walkRoot).
	// For example, kv/kvserver -> kv/kvserver/foo.go.
	unowned := map[string]string{}

	walkRoot := filepath.Join(repoRoot, walkDir)

	unownedWalkFn := func(path string, info os.FileInfo) error {
		teams := co.Match(path)
		if len(teams) > 0 {
			// The file has an owner, so nothing to report.
			debug("%s <- has team(s) %v", path, teams)
			return nil
		}
		if !info.IsDir() {
			// We're looking at a file that has no owner.
			//
			// Let's say `path = ./pkg/foo/bar/baz.go`.
			// If ./pkg, ./pkg/foo, or ./pkg/foo/bar are already
			// marked as "unowned", avoid emitting an additional failure.
			// If neither are, we mark ./pkg/foo/bar as unowned as a
			// result of containing an unowned file. We could also mark
			// the file itself as unowned, but most of the time we have
			// one owner for the directory and also the failures get less
			// noisy by tracking per-directory.
			parts := strings.Split(path, string(filepath.Separator))
			var ok bool
			for i := range parts {
				prefix := filepath.Join(parts[:i+1]...)
				_, ok = unowned[prefix]
				if ok {
					debug("pruning %s; %s is already unowned", path, prefix)
					break
				}
			}
			if !ok {
				debug("unowned: %s", path)
				unowned[filepath.Dir(path)] = path
			}
		}
		return nil
	}

	dirsToWalk := []string{walkRoot}
	for len(dirsToWalk) != 0 {
		// We first visit each directory's files, and then the subdirectories.
		// See TestLintEverythingIsOwned for details.
		require.NoError(t, filepath.Walk(dirsToWalk[0], func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Path relative to walkRoot, i.e. `acceptance` instead of
			// `some/stuff/pkg/acceptance`.
			relPath, err := filepath.Rel(walkRoot, path)
			if err != nil {
				return err
			}

			if _, ok := skip[relPath]; ok {
				debug("skipping %s", relPath)
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			fname := filepath.Base(relPath)
			if _, ok := skip[fname]; ok {
				debug("skipping %s", relPath)
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}

			if info.IsDir() {
				if path == dirsToWalk[0] {
					return nil
				}
				dirsToWalk = append(dirsToWalk, path)
				return filepath.SkipDir
			}
			return unownedWalkFn(filepath.Join(walkDir, relPath), info)
		}))
		dirsToWalk = dirsToWalk[1:]
	}
	var sl []string
	for path := range unowned {
		sl = append(sl, path)
	}
	sort.Strings(sl)

	var buf strings.Builder
	for _, s := range sl {
		fmt.Fprintf(&buf, "%-28s @cockroachdb/<TODO>-noreview\n", string(filepath.Separator)+s+string(filepath.Separator))
	}
	if buf.Len() > 0 {
		t.Errorf(`unowned packages found, please fill out the below and augment .github/CODEOWNERS:
Remove the '-noreview' suffix if the team should be requested for Github reviews.

%s`, buf.String())
	}
}
