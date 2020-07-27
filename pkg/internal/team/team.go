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

	"gopkg.in/yaml.v2"
)

// Name is the name of a team.
type Name string

// Team is a team in the CockroachDB repo.
type Team struct {
	// Name is the name of the team.
	// It is populated from using the key of the yaml file.
	Name Name `yaml:"-"`
	// Board is the ID of the GitHub board belonging to the team.
	Board string `yaml:"board"`
}

// LoadTeams loads the teams from an io input.
// It is expected the input is in YAML format.
func LoadTeams(f io.Reader) (map[Name]Team, error) {
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var ret map[Name]Team
	if err := yaml.UnmarshalStrict(b, &ret); err != nil {
		return nil, err
	}
	// Populate the Name value of each team.
	for k, v := range ret {
		v.Name = k
		ret[k] = v
	}
	return ret, nil
}
