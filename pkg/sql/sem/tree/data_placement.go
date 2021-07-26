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
	default:
		panic(errors.AssertionFailedf("unknown data placement strategy: %d", *node))
	}
}

// TelemetryName returns a representation of SurvivalGoal suitable for telemetry
func (node *DataPlacement) TelemetryName() string {
	switch *node {
	case DataPlacementUnspecified:
		return "data_placement_unspecified"
	case DataPlacementDefault:
		return "data_placement_default"
	case DataPlacementRestricted:
		return "data_placement_restricted"
	default:
		panic(errors.AssertionFailedf("unknown data placement strategy: %d", *node))
	}
}
