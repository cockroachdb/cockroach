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

// RegionalAffinityLevel is a defined regional affinity.
type RegionalAffinityLevel int

const (
	// RegionalAffinityLevelGlobal distributes a table across
	// a global cluster.
	RegionalAffinityLevelGlobal RegionalAffinityLevel = iota
	// RegionalAffinityLevelTable implies a table is homed
	// in a fixed region.
	RegionalAffinityLevelTable
	// RegionalAffinityLevelRowLevel implies a table's rows are
	// homed depending on values within the row.
	RegionalAffinityLevelRowLevel
)

// RegionalAffinity defines the locality for a given table.
type RegionalAffinity struct {
	RegionalAffinityLevel RegionalAffinityLevel
	// TableRegion is set if is RegionalAffinityTable.
	TableRegion Name
}

// Format implements the NodeFormatter interface.
func (node *RegionalAffinity) Format(ctx *FmtCtx) {
	switch node.RegionalAffinityLevel {
	case RegionalAffinityLevelGlobal:
		ctx.WriteString("REGIONAL AFFINITY NONE")
	case RegionalAffinityLevelTable:
		ctx.WriteString("REGIONAL AFFINITY ")
		node.TableRegion.Format(ctx)
	case RegionalAffinityLevelRowLevel:
		ctx.WriteString("REGIONAL AFFINITY ROW LEVEL")
	default:
		ctx.WriteString("REGIONAL AFFINITY ???")
	}
}
