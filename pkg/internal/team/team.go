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

	"github.com/cockroachdb/cockroach/pkg/internal/reporoot"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

// Alias is the name of a team.
type Alias string

// Team is a team in the CockroachDB repo.
type Team struct {
	// TeamName is the distinguished Alias of the team.
	TeamName Alias
	// Aliases is a map from additional team name to purpose for which to use
	// them. The purpose "other" indicates a team that exists but which has no
	// particular purpose as far as `teams` is concerned (for example, teams like
	// the @cockroachdb/bulk-prs team which exists primarily to route, via
	// CODEOWNERS, code reviews for the @cockroachdb/bulk-io team). This map
	// does not contain TeamName.
	Aliases map[Alias]Purpose `yaml:"aliases"`
	// TriageColumnID is the Github Column ID to assign issues to.
	TriageColumnID int `yaml:"triage_column_id"`
	// Email is the email address for this team.
	//
	// Currently unused.
	Email string `yaml:"email"`
	// Slack is the slack channel for this team.
	//
	// Currently unused.
	Slack string `yaml:"slack"`
}

// Name returns the main Alias of the team.
func (t Team) Name() Alias {
	return t.TeamName
}

// Map contains the in-memory representation of TEAMS.yaml.
type Map map[Alias]Team

// GetAliasesForPurpose collects and returns, for the team indicated by any of
// its aliases, the aliases for the supplied purpose. If the team exists but no
// alias for the purpose is defined, falls back to the team's main alias. In
// particular, when `true` is returned, the slice is nonempty.
//
// Returns `nil, false` if the supplied alias does not belong to any team.
func (m Map) GetAliasesForPurpose(alias Alias, purpose Purpose) ([]Alias, bool) {
	tm, ok := m[alias]
	if !ok {
		return nil, false
	}
	var sl []Alias
	for hdl, purp := range tm.Aliases {
		if purpose != purp {
			continue
		}
		sl = append(sl, hdl)
	}
	if len(sl) == 0 {
		sl = append(sl, tm.Name())
	}
	return sl, true
}

// DefaultLoadTeams loads teams from the repo root's TEAMS.yaml.
func DefaultLoadTeams() (Map, error) {
	path := reporoot.GetFor(".", "TEAMS.yaml")
	if path == "" {
		return nil, errors.New("TEAMS.yaml not found")
	}
	path = filepath.Join(path, "TEAMS.yaml")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return LoadTeams(f)
}

// Purpose determines which alias to return for a given team via
type Purpose string

const (
	// PurposeOther is the generic catch-all.
	PurposeOther = Purpose("other")
	// PurposeRoachtest indicates that the team handles that should be mentioned
	// in roachtest issues should be returned.
	PurposeRoachtest = Purpose("roachtest")
)

var validPurposes = map[Purpose]struct{}{
	PurposeOther:     {},
	PurposeRoachtest: {}, // mention in roachtest issues
}

// LoadTeams loads the teams from an io input.
// It is expected the input is in YAML format.
func LoadTeams(f io.Reader) (Map, error) {
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var src map[Alias]Team
	if err := yaml.UnmarshalStrict(b, &src); err != nil {
		return nil, err
	}
	// Populate the Alias value of each team.
	dst := map[Alias]Team{}

	for k, v := range src {
		v.TeamName = k
		aliases := []Alias{v.TeamName}
		for alias, purpose := range v.Aliases {
			if _, ok := validPurposes[purpose]; !ok {
				return nil, errors.Errorf("team %s has alias %s with invalid purpose %s", k, alias, purpose)
			}
			aliases = append(aliases, alias)
		}
		for _, alias := range aliases {
			if conflicting, ok := dst[alias]; ok {
				return nil, errors.Errorf(
					"team %s has alias %s which conflicts with team %s",
					k, alias, conflicting.Name(),
				)
			}
			dst[alias] = v
		}
		dst[k] = v
	}
	return dst, nil
}
