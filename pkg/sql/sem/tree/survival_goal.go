// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/errors"

// SurvivalGoal represents the desired survivability level
// for a given database.
type SurvivalGoal uint32

const (
	// SurvivalGoalDefault indicates default survive behavior.
	// This will get translated to the appropriate value when persisted.
	SurvivalGoalDefault SurvivalGoal = iota
	// SurvivalGoalRegionFailure indicates a database being able to withstand
	// an entire region failure.
	SurvivalGoalRegionFailure
	// SurvivalGoalZoneFailure indicates a database being able to
	// withstand a failure of an availibility zone.
	SurvivalGoalZoneFailure
)

// Format implements the NodeFormatter interface.
func (node *SurvivalGoal) Format(ctx *FmtCtx) {
	switch *node {
	case SurvivalGoalRegionFailure:
		ctx.WriteString("SURVIVE REGION FAILURE")
	case SurvivalGoalZoneFailure:
		ctx.WriteString("SURVIVE ZONE FAILURE")
	default:
		panic(errors.AssertionFailedf("unknown survival goal: %d", *node))
	}
}

// TelemetryName returns a representation of SurvivalGoal suitable for telemetry
func (node *SurvivalGoal) TelemetryName() string {
	switch *node {
	case SurvivalGoalDefault:
		return "survive_default"
	case SurvivalGoalRegionFailure:
		return "survive_region_failure"
	case SurvivalGoalZoneFailure:
		return "survive_zone_failure"
	default:
		panic(errors.AssertionFailedf("unknown survival goal: %d", *node))
	}
}
