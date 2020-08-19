// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package team involves processing team information based on a yaml
// file containing team metadata.
package team

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/internal/gopath"
	"gopkg.in/yaml.v2"
)

// DefaultTeamsYAMLLocation is the default location for the TEAMS.yaml file.
var DefaultTeamsYAMLLocation string

func init() {
	DefaultTeamsYAMLLocation = filepath.Join(
		gopath.Get(),
		"src",
		"github.com",
		"cockroachdb",
		"cockroach",
		"TEAMS.yaml",
	)
}

// Alias is the name of a team.
type Alias string

// Team is a team in the CockroachDB repo.
type Team struct {
	// Alias is the name of the team.
	// It is populated from using the key of the yaml file.
	Alias Alias `yaml:"-"`
	// Email is the email address for this team.
	Email string `yaml:"email"`
	// Slack is the slack channel for this team.
	Slack string `yaml:"slack"`
	// TriageColumnID is the Github Column ID to assign issues to.
	TriageColumnID int `yaml:"triage_column_id"`
}

// DefaultLoadTeams loads teams from the DefaultTeamsYAMLLocation.
func DefaultLoadTeams() (map[Alias]Team, error) {
	f, err := os.Open(DefaultTeamsYAMLLocation)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return LoadTeams(f)
}

// LoadTeams loads the teams from an io input.
// It is expected the input is in YAML format.
func LoadTeams(f io.Reader) (map[Alias]Team, error) {
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var ret map[Alias]Team
	if err := yaml.UnmarshalStrict(b, &ret); err != nil {
		return nil, err
	}
	// Populate the Alias value of each team.
	for k, v := range ret {
		v.Alias = k
		ret[k] = v
	}
	return ret, nil
}
