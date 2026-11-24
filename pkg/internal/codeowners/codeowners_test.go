// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package codeowners

import (
	"fmt"
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
#!/q/ @cockroachdb/team-q
/qq/ @cockroachdb/team-q #! @cockroachdb/team-b-noreview
`
	teams := map[team.Alias]team.Team{
		"cockroachdb/team-a": {},
		"cockroachdb/team-b": {},
		"cockroachdb/team-c": {},
		"cockroachdb/team-q": {},
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
		{"q/foo.txt", []team.Team{teams["cockroachdb/team-q"]}},
		{"qq/foo.txt", []team.Team{teams["cockroachdb/team-q"], teams["cockroachdb/team-b"]}},
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
			require.NoError(t, os.WriteFile(filepath.Join(d, "pkg", mkd, mkf), []byte("foo"), 0644))
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

func TestLintNoStalePaths(t *testing.T) {
	d := t.TempDir()
	// Create a minimal repo structure.
	for _, path := range []string{
		filepath.Join("pkg", "kv", "kv.go"),
		filepath.Join("pkg", "sql", "sql.go"),
		filepath.Join("pkg", "sql", "parser", "parse.go"),
		filepath.Join("Makefile"),
		filepath.Join("docs", "README.md"),
	} {
		dir := filepath.Dir(filepath.Join(d, path))
		require.NoError(t, os.MkdirAll(dir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(d, path), []byte("x"), 0644))
	}

	teams := map[team.Alias]team.Team{
		"cockroachdb/kv": {},
	}

	for _, tc := range []struct {
		name    string
		owners  string
		stale   []string
		noStale []string
	}{
		{
			name:   "existing dir",
			owners: "/pkg/kv/ @cockroachdb/kv",
		},
		{
			name:   "existing file",
			owners: "/Makefile @cockroachdb/kv",
		},
		{
			name:    "missing dir",
			owners:  "/pkg/missing/ @cockroachdb/kv",
			stale:   []string{"/pkg/missing/"},
			noStale: []string{"/pkg/kv/"},
		},
		{
			name:    "missing file",
			owners:  "/pkg/kv/gone.go @cockroachdb/kv",
			stale:   []string{"/pkg/kv/gone.go"},
			noStale: []string{"/pkg/kv/"},
		},
		{
			name:   "single-star glob matches",
			owners: "/pkg/sql/*.go @cockroachdb/kv",
		},
		{
			name:   "single-star glob no match",
			owners: "/pkg/nope/*.go @cockroachdb/kv",
			stale:  []string{"/pkg/nope/*.go"},
		},
		{
			name:   "double-star glob matches",
			owners: "**.go @cockroachdb/kv",
		},
		{
			name:   "double-star glob no match",
			owners: "**.xyz @cockroachdb/kv",
			stale:  []string{"**.xyz"},
		},
		{
			name:    "mix of valid and stale",
			owners:  "/pkg/kv/ @cockroachdb/kv\n/pkg/gone/ @cockroachdb/kv",
			stale:   []string{"/pkg/gone/"},
			noStale: []string{"/pkg/kv/"},
		},
		{
			name:   "commented-out line with #! prefix",
			owners: "#!/pkg/kv/ @cockroachdb/kv",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			co, err := LoadCodeOwners(strings.NewReader(tc.owners), teams)
			require.NoError(t, err)
			stale := LintNoStalePaths(co, d)
			for _, s := range tc.stale {
				found := false
				for _, got := range stale {
					if got == s {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected stale path %q not found in %v", s, stale)
				}
			}
			for _, s := range tc.noStale {
				for _, got := range stale {
					if got == s {
						t.Errorf("path %q should not be stale but was reported as stale", s)
					}
				}
			}
			if len(tc.stale) == 0 && len(stale) > 0 {
				t.Errorf("expected no stale paths, got %v", stale)
			}
		})
	}
}

func TestLintNoStalePathsDefaultCodeOwners(t *testing.T) {
	skip.IgnoreLint(t, "only for manual testing")
	co, err := DefaultLoadCodeOwners()
	require.NoError(t, err)
	repoRoot := reporoot.Get()
	stale := LintNoStalePaths(co, repoRoot)
	for _, s := range stale {
		t.Errorf("stale CODEOWNERS path: %s", s)
	}
}

func TestLintEverythingIsOwnedDefaultCodeOwners(t *testing.T) {
	skip.IgnoreLint(t, "only for manual testing")
	co, err := DefaultLoadCodeOwners()
	require.NoError(t, err)
	const verbose = true
	repoRoot := reporoot.Get()
	LintEverythingIsOwned(t, verbose, co, repoRoot, "pkg")
}
