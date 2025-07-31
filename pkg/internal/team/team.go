// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package team involves processing team information based on a yaml
// file containing team metadata.
package team

import (
	_ "embed"
	"io"
	"strings"

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
	// the @cockroachdb/kv-prs team which exists primarily to route, via
	// CODEOWNERS, code reviews for the @cockroachdb/kv team). This map
	// does not contain TeamName.
	Aliases map[Alias]Purpose `yaml:"aliases"`
	// GitHub label will be added to issues posted for this team.
	Label string `yaml:"label"`
	// TriageColumnID is the GitHub Column ID to assign issues to.
	TriageColumnID int `yaml:"triage_column_id"`
	// SilenceMentions is true if @-mentions should be supressed for this team.
	SilenceMentions bool `yaml:"silence_mentions"`
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

//go:generate cp ../../../TEAMS.yaml TEAMS.yaml

//go:embed TEAMS.yaml
var teamsYaml string

// DefaultLoadTeams loads teams from the repo root's TEAMS.yaml.
func DefaultLoadTeams() (Map, error) {
	return LoadTeams(strings.NewReader(teamsYaml))
}

// Purpose determines which alias to return for a given team via
type Purpose string

const (
	// PurposeOther is the generic catch-all.
	PurposeOther = Purpose("other")
	// PurposeRoachtest indicates that the team handles that should be mentioned
	// in roachtest issues should be returned.
	PurposeRoachtest = Purpose("roachtest")
	PurposeUnittest  = Purpose("unittest")
)

var validPurposes = map[Purpose]struct{}{
	PurposeOther:     {},
	PurposeRoachtest: {}, // mention in roachtest issues
	PurposeUnittest:  {}, // mention in unit test issues
}

// LoadTeams loads the teams from an io input.
// It is expected the input is in YAML format.
func LoadTeams(f io.Reader) (Map, error) {
	b, err := io.ReadAll(f)
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
