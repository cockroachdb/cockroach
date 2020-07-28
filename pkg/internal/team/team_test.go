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
  board: otan
test-infra-team:
  board: notan
`)
	ret, err := LoadTeams(bytes.NewReader(yamlFile))
	require.NoError(t, err)
	require.Equal(
		t,
		map[Name]Team{
			"sql": {
				Name:  "sql",
				Board: "otan",
			},
			"test-infra-team": {
				Name:  "test-infra-team",
				Board: "notan",
			},
		},
		ret,
	)
}
