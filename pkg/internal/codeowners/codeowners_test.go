// Copyright 2020 The Cockroach Authors.
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
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/stretchr/testify/require"
)

func init() {
	if bazel.BuiltWithBazel() {
		codeOwnersFile, err := bazel.Runfile(".github/CODEOWNERS")
		if err != nil {
			log.Fatal(err)
		}
		DefaultCodeOwnersLocation = codeOwnersFile

		teamFile, err := bazel.Runfile("TEAMS.yaml")
		if err != nil {
			log.Fatal(err)
		}
		team.DefaultTeamsYAMLLocation = teamFile
	}
}

func TestMatch(t *testing.T) {
	owners := `
/a/ @cockroachdb/team-a
/b/ @cockroachdb/team-b
/a/b* @cockroachdb/team-b @cockroachdb/team-a
**/c/ @cockroachdb/team-c
`
	teams := map[team.Alias]team.Team{
		"cockroachdb/team-a": {Alias: "cockroachdb/team-a"},
		"cockroachdb/team-b": {Alias: "cockroachdb/team-c"},
		"cockroachdb/team-c": {Alias: "cockroachdb/team-c"},
	}

	codeOwners, err := LoadCodeOwners(strings.NewReader(owners), teams)
	require.NoError(t, err)

	testCases := []struct {
		path     string
		expected []team.Team
	}{
		{"/a", []team.Team{teams["cockroachdb/team-a"]}},
		{"/a/file.txt", []team.Team{teams["cockroachdb/team-a"]}},
		{"/a/b", []team.Team{teams["cockroachdb/team-b"], teams["cockroachdb/team-a"]}},
		{"/a/bob", []team.Team{teams["cockroachdb/team-b"], teams["cockroachdb/team-a"]}},
		{"/no/owner/", nil},
		{"/hmm/what/about/c/file", []team.Team{teams["cockroachdb/team-c"]}},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			ret, err := codeOwners.Match(tc.path)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestEverythingHasOwners(t *testing.T) {
	co, err := DefaultLoadCodeOwners()
	require.NoError(t, err)
	root, err := filepath.Abs("../../") // pkg
	require.NoError(t, err)

	skip := map[string]struct{}{
		filepath.Join("ccl", "ccl_init.go"): {},
		filepath.Join("ui", "node_modules"): {},
		filepath.Join("ui", "yarn-vendor"):  {},
		"Makefile":                          {},
		"BUILD.bazel":                       {},
		".gitignore":                        {},
		"README.md":                         {},
	}

	unowned := map[string]string{} // unowned dir relative to root -> triggering file

	require.NoError(t, filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		path, err = filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if _, ok := skip[path]; ok {
			t.Logf("skipping %s", path)
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		fname := filepath.Base(path)
		if _, ok := skip[fname]; ok {
			t.Logf("skipping %s", path)
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		teams, err := co.Match(filepath.Join(root, path))
		if err != nil {
			return err
		}
		if len(teams) > 0 {
			t.Logf("%s <- has team(s) %v", path, teams)
			return nil
		}
		if !info.IsDir() {
			// Let's say `path = ./pkg/foo/bar/baz.go`.
			// If ./pkg, ./pkg/foo, or ./pkg/foo/bar are already
			// marked as "unowned", avoid emitting a spurious failure.
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
					t.Logf("pruning %s; %s is already unowned", path, prefix)
					break
				}
			}
			if !ok {
				unowned[filepath.Dir(path)] = path
			}
		}
		return nil
	}))
	var sl []string
	for path := range unowned {
		sl = append(sl, path)
	}
	sort.Strings(sl)

	isPrefix := func(s, prefix string) bool {
		sl, pl := strings.Split(s, string(filepath.Separator)), strings.Split(prefix, string(filepath.Separator))
		if len(sl) <= len(pl) {
			// [foo, bar] can't have [x, y] or [x, y, z] as prefix.
			return false
		}
		for i := range pl {
			if sl[i] != pl[i] {
				return false
			}
		}
		return true
	}

	var i int
	for {
		if i >= len(sl) {
			break
		}
		if i > 0 && isPrefix(sl[i], sl[i-1]) {
			t.Logf("dropping %s; already have %s", sl[i], sl[i-1])
			copy(sl[i:], sl[i+1:])
			sl = sl[:len(sl)-1]
			continue // don't increment i
		}
		i++
	}
	var buf strings.Builder
	pkg := string(filepath.Separator) + filepath.Base(root)
	for _, s := range sl {
		fmt.Fprintf(&buf, "%-28s @cockroachdb/<TODO>\n", filepath.Join(pkg, s))
	}
	if buf.Len() > 0 {
		t.Errorf("unowned packages found, please fill out the below and augment .github/CODEOWNERS:\n\n%s", buf.String())
	}
}
