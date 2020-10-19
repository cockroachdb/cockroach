// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// Survive represents the desired survivability level
// for a given database.
type Survive uint32

const (
	// SurviveDefault indicates default survive behavior.
	SurviveDefault Survive = iota
	// SurviveRegionFailure indicates a database being able to withstand
	// an entire region failure.
	SurviveRegionFailure
	// SurviveAvailabilityZoneFailure indicates a database being able to
	// withstand a failure of an availibility zone.
	SurviveAvailabilityZoneFailure
)

// Format implements the NodeFormatter interface.
func (node *Survive) Format(ctx *FmtCtx) {
	switch *node {
	case SurviveRegionFailure:
		ctx.WriteString("SURVIVE REGION FAILURE")
	case SurviveAvailabilityZoneFailure:
		ctx.WriteString("SURVIVE AVAILABILITY ZONE FAILURE")
	case SurviveDefault:
		ctx.WriteString("SURVIVE DEFAULT")
	}
}
