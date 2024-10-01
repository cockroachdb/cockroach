// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	// RegionalByRowRegionNotSpecifiedName is the string denoting the REGIONAL BY ROW
	// has no AS <col> specified.
	// TODO(#59455): clean this up to use something nicer.
	RegionalByRowRegionNotSpecifiedName = ""
	// PrimaryRegionNotSpecifiedName is the string denoting the lack of primary region in the
	// locality config.
	// TODO(#59455): clean this up to use something nicer.
	PrimaryRegionNotSpecifiedName Name = ""
	// SecondaryRegionNotSpecifiedName is the string denoting the lack of secondary region
	//in the locality config.
	// TODO(#59455): clean this up to use something nicer.
	SecondaryRegionNotSpecifiedName Name = ""
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

// Format implements the NodeFormatter interface.
func (node *Locality) Format(ctx *FmtCtx) {
	ctx.WriteString("LOCALITY ")
	switch node.LocalityLevel {
	case LocalityLevelGlobal:
		ctx.WriteString("GLOBAL")
	case LocalityLevelTable:
		ctx.WriteString("REGIONAL BY TABLE IN ")
		if node.TableRegion != "" {
			ctx.FormatNode(&node.TableRegion)
		} else {
			ctx.WriteString("PRIMARY REGION")
		}
	case LocalityLevelRow:
		ctx.WriteString("REGIONAL BY ROW")
		if node.RegionalByRowColumn != "" {
			ctx.WriteString(" AS ")
			ctx.FormatNode(&node.RegionalByRowColumn)
		}
	default:
		panic(fmt.Sprintf("unknown locality: %#v", node.LocalityLevel))
	}
}
