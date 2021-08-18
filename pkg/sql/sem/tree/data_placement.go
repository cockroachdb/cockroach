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

import "github.com/cockroachdb/errors"

// DataPlacement represents the desired data placement strategy for a given
// database.
type DataPlacement uint32

const (
	// DataPlacementUnspecified indicates an unspecified placement policy.
	// This will get translated to the default value when persisted.
	DataPlacementUnspecified DataPlacement = iota
	// DataPlacementDefault indicates specified default data placement policy,
	DataPlacementDefault
	// DataPlacementRestricted indicates the database will not use non-voters for
	// REGIONAL BY [TABLE | ROW] tables.
	DataPlacementRestricted
)

// Format implements the NodeFormatter interface.
func (node *DataPlacement) Format(ctx *FmtCtx) {
	switch *node {
	case DataPlacementRestricted:
		ctx.WriteString("PLACEMENT RESTRICTED")
	case DataPlacementDefault:
		ctx.WriteString("PLACEMENT DEFAULT")
	case DataPlacementUnspecified:
	default:
		panic(errors.AssertionFailedf("unknown data placement strategy: %d", *node))
	}
}

// TelemetryName returns a representation of DataPlacement suitable for
// telemetry.
func (node *DataPlacement) TelemetryName() string {
	switch *node {
	case DataPlacementRestricted:
		return "restricted"
	case DataPlacementDefault:
		return "default"
	case DataPlacementUnspecified:
		return "unspecified"
	default:
		panic(errors.AssertionFailedf("unknown data placement: %d", *node))
	}
}
