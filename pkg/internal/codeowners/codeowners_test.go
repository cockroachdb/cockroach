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
	"log"
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

func TestCodeOwnersValid(t *testing.T) {
	_, err := DefaultLoadCodeOwners()
	require.NoError(t, err)
}
