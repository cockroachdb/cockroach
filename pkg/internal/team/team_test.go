// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package team

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadTeams(t *testing.T) {
	yamlFile := []byte(`
sql:
  aliases: [sql-alias]
  email: otan@cockroachlabs.com
  slack: otan
  triage_column_id: 1
test-infra-team:
  email: jlinder@cockroachlabs.com
  slack: jlinder
  triage_column_id: 2
`)
	ret, err := LoadTeams(bytes.NewReader(yamlFile))
	require.NoError(t, err)
	require.Equal(
		t,
		map[Alias]Team{
			"sql": {
				Aliases:        []Alias{"sql", "sql-alias"},
				Email:          "otan@cockroachlabs.com",
				Slack:          "otan",
				TriageColumnID: 1,
			},
			"sql-alias": {
				Aliases:        []Alias{"sql", "sql-alias"},
				Email:          "otan@cockroachlabs.com",
				Slack:          "otan",
				TriageColumnID: 1,
			},
			"test-infra-team": {
				Aliases:        []Alias{"test-infra-team"},
				Email:          "jlinder@cockroachlabs.com",
				Slack:          "jlinder",
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
