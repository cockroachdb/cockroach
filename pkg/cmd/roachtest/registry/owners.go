// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package registry

import (
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/internal/team"
)

// Owner is a valid entry for the Owners field of a roachtest. They should be
// teams, not individuals.
type Owner string

// The allowable values of Owner.
const (
	OwnerCDC                Owner = `cdc`
	OwnerDisasterRecovery   Owner = `disaster-recovery`
	OwnerKV                 Owner = `kv`
	OwnerAdmissionControl   Owner = `admission-control`
	OwnerObservability      Owner = `obs-prs`
	OwnerObservabilityIndia Owner = `obs-india-prs`
	OwnerServer             Owner = `server`
	OwnerSQLFoundations     Owner = `sql-foundations`
	OwnerMigrations         Owner = `migrations`
	OwnerProductSecurity    Owner = `product-security`
	OwnerReleaseEng         Owner = `release-eng`
	OwnerSQLQueries         Owner = `sql-queries`
	OwnerStorage            Owner = `storage`
	OwnerTestEng            Owner = `test-eng`
	OwnerDevInf             Owner = `dev-inf`
	OwnerFieldEng           Owner = `field-engineering`
)

// IsValid returns true if the owner is valid, i.e. it has a corresponding team
// in TEAMS.yaml.
func (o Owner) IsValid() bool {
	_, ok := getTeams()[o.ToTeamAlias()]
	return ok
}

// ToTeamAlias returns the team alias corresponding to the owner.
func (o Owner) ToTeamAlias() team.Alias {
	return team.Alias(fmt.Sprintf("cockroachdb/%s", o))
}

var teams struct {
	once  sync.Once
	value team.Map
}

// OverrideTeams is used for tests. It must be called before any code that needs
// the teams run.
func OverrideTeams(value team.Map) {
	ran := false
	teams.once.Do(func() {
		ran = true
		teams.value = value
	})
	if !ran {
		panic("OverrideTeams called too late")
	}
}

// getTeams returns the teams map (defined in TEAMS.yaml in the repo root).
func getTeams() team.Map {
	teams.once.Do(func() {
		val, err := team.DefaultLoadTeams()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error loading teams: %s", err)
			os.Exit(1)
		}
		teams.value = val
	})
	return teams.value
}
