// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package owner

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/stretchr/testify/require"
)

func TestFindOwners(t *testing.T) {
	teams, err := team.LoadTeams(bytes.NewReader([]byte(`
team_a:
  board: team_a
team_b:
  board: team_b
team_c:
  board: team_c
team_d:
  board: team_d
`)))
	require.NoError(t, err)

	testCases := []struct {
		fileOrDir string
		expected  []team.Team
	}{
		{"./testdata/dir1", []team.Team{teams["team_a"], teams["team_b"]}},
		{"./testdata/dir1/subdir/file_a", []team.Team{teams["team_d"]}},
		{"./testdata/dir1/subdir/file_b", []team.Team{teams["team_d"]}},
		{"./testdata/dir1/subdir/subsubdir", []team.Team{teams["team_d"]}},
		{"./testdata/dir1/subdir/subsubdir/file_a", []team.Team{teams["team_d"]}},
		{"./testdata/dir1/subdir_2", []team.Team{teams["team_a"], teams["team_b"]}},
		{"./testdata/dir1/subdir_2/file_a", []team.Team{teams["team_a"], teams["team_b"]}},
		{"./testdata/dir1/file_a", []team.Team{teams["team_a"], teams["team_b"]}},
		{"./testdata/dir1/file_b", []team.Team{teams["team_b"]}},
		{"./testdata/dir1/file_c", []team.Team{teams["team_a"], teams["team_b"]}},
		{"./testdata/dir1/afile_a", []team.Team{teams["team_c"]}},
		{"./testdata/dir1/afile_b", []team.Team{teams["team_c"]}},
	}

	for _, tc := range testCases {
		t.Run(tc.fileOrDir, func(t *testing.T) {
			ret, err := FindOwners(tc.fileOrDir, teams)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}
