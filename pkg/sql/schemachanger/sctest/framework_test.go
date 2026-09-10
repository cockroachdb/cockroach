// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sctest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// The fixture bodies below are trimmed versions of real *.side_effects files:
// enough structure for declaresNoPostCommitStages to classify them, with the
// op listings elided.

const fixtureWithPostCommitStages = `setup
CREATE TABLE t (i INT PRIMARY KEY);
----
...

/* test */
ALTER TABLE t ADD COLUMN j INT NOT NULL DEFAULT 1;
----
begin transaction #1
## StatementPhase stage 1 of 1 with 5 MutationType ops
## PreCommitPhase stage 2 of 2 with 4 MutationType ops
commit transaction #1
begin transaction #2
## PostCommitPhase stage 1 of 7 with 5 MutationType ops
commit transaction #2
`

// A change whose only post-commit work is non-revertible still has a stage to
// interrupt. This is not a hypothetical: 22 of the committed end_to_end
// fixtures match only this header, so a pattern that recognized
// PostCommitPhase alone would silently skip every one of them.
const fixtureWithNonRevertibleStagesOnly = `setup
CREATE TABLE t (i INT PRIMARY KEY, j INT);
----
...

/* test */
ALTER TABLE t DROP COLUMN j;
----
begin transaction #1
## StatementPhase stage 1 of 1 with 2 MutationType ops
## PreCommitPhase stage 2 of 2 with 3 MutationType ops
commit transaction #1
begin transaction #2
## PostCommitNonRevertiblePhase stage 1 of 2 with 3 MutationType ops
commit transaction #2
`

const fixtureWithoutPostCommitStages = `setup
CREATE TABLE t (i INT PRIMARY KEY);
----
...

/* test */
COMMENT ON TABLE t IS 'hello';
----
begin transaction #1
## StatementPhase stage 1 of 1 with 1 MutationType op
## PreCommitPhase stage 2 of 2 with 1 MutationType op
commit transaction #1
`

// Some no-op DDLs record no phase header at all.
const fixtureWithoutAnyPhase = `setup
CREATE TABLE t (i INT PRIMARY KEY);
----
...

/* test */
CREATE TABLE IF NOT EXISTS t (i INT PRIMARY KEY);
----
`

func TestDeclaresNoPostCommitStages(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// writeFixture lays out the directory shape declaresNoPostCommitStages
	// expects — <dir>/<basename>.side_effects — and returns the directory to
	// use as CumulativeTestSpec.Path.
	writeFixture := func(t *testing.T, name, contents string) string {
		dir := filepath.Join(t.TempDir(), name)
		require.NoError(t, os.MkdirAll(dir, 0755))
		path := filepath.Join(dir, name+".side_effects")
		require.NoError(t, os.WriteFile(path, []byte(contents), 0644))
		return dir
	}

	factory := SingleNodeTestClusterFactory{}

	t.Run("fixture contents", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			contents string
			expected bool
		}{
			{"post commit stages", fixtureWithPostCommitStages, false},
			{"non-revertible stages only", fixtureWithNonRevertibleStagesOnly, false},
			{"no post commit stages", fixtureWithoutPostCommitStages, true},
			{"no phase at all", fixtureWithoutAnyPhase, true},
			// The pattern is deliberately unanchored so that a change in how
			// headers are indented cannot turn into a suite-wide skip.
			{"indented header", "/* test */\n\t## PostCommitPhase stage 1 of 3 with 2 MutationType ops\n", false},
			// Without the marker the file is not a fixture in the format the
			// pattern was written against, so a missing header means nothing.
			{"no test marker", "## PreCommitPhase stage 1 of 1 with 1 MutationType op\n", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				spec := CumulativeTestSpec{Path: writeFixture(t, "fixture", tc.contents)}
				require.Equal(t, tc.expected, declaresNoPostCommitStages(spec, factory))
			})
		}
	})

	t.Run("missing fixture", func(t *testing.T) {
		spec := CumulativeTestSpec{Path: t.TempDir()}
		require.False(t, declaresNoPostCommitStages(spec, factory))
	})

	t.Run("unreadable fixture", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "fixture")
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "fixture.side_effects"), 0755))
		spec := CumulativeTestSpec{Path: dir}
		require.False(t, declaresNoPostCommitStages(spec, factory))
	})

	// The remaining cases all describe a fixture that would otherwise qualify
	// for the shortcut, and check that it is suppressed anyway.
	t.Run("rewrite", func(t *testing.T) {
		spec := CumulativeTestSpec{
			Path:    writeFixture(t, "fixture", fixtureWithoutPostCommitStages),
			Rewrite: true,
		}
		require.False(t, declaresNoPostCommitStages(spec, factory))
	})

	t.Run("run all cumulative", func(t *testing.T) {
		defer func(old bool) { *runAllCumulative = old }(*runAllCumulative)
		*runAllCumulative = true
		spec := CumulativeTestSpec{Path: writeFixture(t, "fixture", fixtureWithoutPostCommitStages)}
		require.False(t, declaresNoPostCommitStages(spec, factory))
	})

	t.Run("mixed version", func(t *testing.T) {
		spec := CumulativeTestSpec{Path: writeFixture(t, "fixture", fixtureWithoutPostCommitStages)}
		require.False(t, declaresNoPostCommitStages(spec, factory.WithMixedVersion()))
	})
}
