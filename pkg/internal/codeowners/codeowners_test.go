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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/reporoot"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/require"
)

func TestMatch(t *testing.T) {
	owners := `
/a/ @cockroachdb/team-a
/b/ @cockroachdb/team-b-noreview
/a/b* @cockroachdb/team-b @cockroachdb/team-a
**/c/ @cockroachdb/team-c
`
	teams := map[team.Alias]team.Team{
		"cockroachdb/team-a": {},
		"cockroachdb/team-b": {},
		"cockroachdb/team-c": {},
	}

	codeOwners, err := LoadCodeOwners(strings.NewReader(owners), teams)
	require.NoError(t, err)

	testCases := []struct {
		path     string
		expected []team.Team
	}{
		{"a", []team.Team{teams["cockroachdb/team-a"]}},
		{"a/file.txt", []team.Team{teams["cockroachdb/team-a"]}},
		{"a/b", []team.Team{teams["cockroachdb/team-b"], teams["cockroachdb/team-a"]}},
		{"a/bob", []team.Team{teams["cockroachdb/team-b"], teams["cockroachdb/team-a"]}},
		{"no/owner/", nil},
		{"hmm/what/about/c/file", []team.Team{teams["cockroachdb/team-c"]}},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			ret := codeOwners.Match(tc.path)
			require.Equal(t, tc.expected, ret)
		})
	}
}

type bufT struct {
	strings.Builder
	*testing.T
}

func (b *bufT) Errorf(format string, args ...interface{}) {
	format += "\n"
	fmt.Fprintf(&b.Builder, format, args...)
}

func TestLintEverythingIsOwned(t *testing.T) {
	d := t.TempDir()
	// Note that naive `filepath.Walk` would visit in
	// the lexicographical order within each dir:
	// - a
	// - a/b
	// - a/b/c.file
	// - a/c.file
	//
	// It would thus mark `a/b` as unowned (due to `a/b/c.file`)
	// before realizing that `a` itself is unowned (due to `a/c.file`).
	// We avoid this by visiting all files in the current directory
	// first before visiting the subdirectories.
	for _, path := range []string{
		filepath.Join("a", "b", "c.file"),
		filepath.Join("a", "c.file"),
		filepath.Join("b", "a.file"),
		filepath.Join("b", "b.file"),
		filepath.Join("b", "c.file"),
		"c",
	} {
		var mkf string
		mkd := path
		if strings.HasSuffix(path, ".file") {
			mkf = filepath.Base(path)
			mkd = filepath.Dir(path)
		}
		require.NoError(t, os.MkdirAll(filepath.Join(d, "pkg", mkd), 0755))
		if mkf != "" {
			require.NoError(t, ioutil.WriteFile(filepath.Join(d, "pkg", mkd, mkf), []byte("foo"), 0644))
		}
	}
	co, err := LoadCodeOwners(strings.NewReader("# nothing!"), nil /* no teams! */)
	require.NoError(t, err)
	b := &bufT{T: t}
	LintEverythingIsOwned(b, true /* verbose */, co, d, "pkg")
	require.Equal(t,
		`unowned packages found, please fill out the below and augment .github/CODEOWNERS:
Remove the '-noreview' suffix if the team should be requested for Github reviews.

/pkg/a/                      @cockroachdb/<TODO>-noreview
/pkg/b/                      @cockroachdb/<TODO>-noreview

`, b.String())
}

func TestLintEverythingIsOwnedDefaultCodeOwners(t *testing.T) {
	skip.IgnoreLint(t, "only for manual testing")
	co, err := DefaultLoadCodeOwners()
	require.NoError(t, err)
	const verbose = true
	repoRoot := reporoot.Get()
	LintEverythingIsOwned(t, verbose, co, repoRoot, "pkg")
}
