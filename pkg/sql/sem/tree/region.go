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

import "fmt"

// LocalityLevel is a defined locality.
type LocalityLevel int

const (
	// LocalityLevelGlobal distributes a table across
	// a global cluster.
	LocalityLevelGlobal LocalityLevel = iota
	// LocalityLevelTable implies a table is homed
	// in a fixed region.
	LocalityLevelTable
	// LocalityLevelRow implies a table's rows are
	// homed depending on values within the row.
	LocalityLevelRow
)

const (
	// RegionEnum is the name of the per-database region enum required for
	// multi-region.
	RegionEnum string = "crdb_internal_region"
	// RegionalByRowRegionDefaultCol is the default name of the REGIONAL BY ROW
	// column name if the AS field is not populated.
	RegionalByRowRegionDefaultCol string = "crdb_region"
	// RegionalByRowRegionDefaultColName is the same, typed as Name.
	RegionalByRowRegionDefaultColName Name = Name(RegionalByRowRegionDefaultCol)
	// PrimaryRegionLocalityName is the string denoting the primary region in the
	// locality config.
	// TODO(#multiregion): clean this up to use something nicer,
	// see https://github.com/cockroachdb/cockroach/issues/59455.
	PrimaryRegionLocalityName Name = ""
)

// Locality defines the locality for a given table.
type Locality struct {
	LocalityLevel LocalityLevel
	// TableRegion is set if is LocalityLevelTable and a non-primary region is set.
	TableRegion Name
	// RegionalByRowColumn is set if col_name on REGIONAL BY ROW ON <col_name> is
	// set.
	RegionalByRowColumn Name
}

// InPrimaryRegion returns true if the table is REGIONAL BY TABLE and the table
// region is unset (representing the primary region).
func (node *Locality) InPrimaryRegion() bool {
	return node.LocalityLevel == LocalityLevelTable && node.TableRegion == ""
}

// Format implements the NodeFormatter interface.
func (node *Locality) Format(ctx *FmtCtx) {
	ctx.WriteString("LOCALITY ")
	switch node.LocalityLevel {
	case LocalityLevelGlobal:
		ctx.WriteString("GLOBAL")
	case LocalityLevelTable:
		ctx.WriteString("REGIONAL BY TABLE IN ")
		if node.TableRegion != "" {
			node.TableRegion.Format(ctx)
		} else {
			ctx.WriteString("PRIMARY REGION")
		}
	case LocalityLevelRow:
		ctx.WriteString("REGIONAL BY ROW")
		if node.RegionalByRowColumn != "" {
			ctx.WriteString(" AS ")
			node.RegionalByRowColumn.Format(ctx)
		}
	default:
		panic(fmt.Sprintf("unknown regional affinity: %#v", node.LocalityLevel))
	}
}
