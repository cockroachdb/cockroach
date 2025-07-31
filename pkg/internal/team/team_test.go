// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package team

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadTeams(t *testing.T) {
	yamlFile := []byte(`
sql:
  aliases:
    sql-alias: other
    sql-roachtest: roachtest
  triage_column_id: 1
  silence_mentions: true
test-infra-team:
  triage_column_id: 2
`)
	ret, err := LoadTeams(bytes.NewReader(yamlFile))
	require.NoError(t, err)
	sqlTeam := Team{
		TeamName: "sql",
		Aliases: map[Alias]Purpose{
			"sql-alias":     PurposeOther,
			"sql-roachtest": PurposeRoachtest,
		},
		TriageColumnID:  1,
		SilenceMentions: true,
	}
	require.Equal(t, sqlTeam.TeamName, sqlTeam.Name())

	{
		_, ok := ret.GetAliasesForPurpose("not-a-team", PurposeRoachtest)
		require.False(t, ok)
	}
	{
		sl, ok := ret.GetAliasesForPurpose("sql-alias", PurposeRoachtest)
		require.True(t, ok)
		require.Equal(t, []Alias{"sql-roachtest"}, sl)
	}
	{
		sl, ok := ret.GetAliasesForPurpose("sql-alias", PurposeOther)
		require.True(t, ok)
		require.Equal(t, []Alias{"sql-alias"}, sl)
	}
	{
		sl, ok := ret.GetAliasesForPurpose("test-infra-team", PurposeRoachtest)
		require.True(t, ok)
		require.Equal(t, []Alias{"test-infra-team"}, sl)
	}

	require.Equal(
		t,
		Map{
			"sql":           sqlTeam,
			"sql-alias":     sqlTeam,
			"sql-roachtest": sqlTeam,
			"test-infra-team": {
				TeamName:       "test-infra-team",
				TriageColumnID: 2,
			},
		},
		ret,
	)
}

func TestTeamsYAMLValid(t *testing.T) {
	_, err := DefaultLoadTeams()
	require.NoError(t, err)

	// TODO(otan): test other volatile validity conditions, e.g. triage_column_id exists.
	// Gate this by a flag so this is only tested with certain flags, as these are
	// not reproducible results in tests.
}
