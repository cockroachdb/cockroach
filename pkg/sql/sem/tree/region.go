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

// Locality defines the locality for a given table.
type Locality struct {
	LocalityLevel LocalityLevel
	// TableRegion is set if is LocalityLevelTable and a non-primary region is set.
	TableRegion Name
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
	default:
		panic(fmt.Sprintf("unknown regional affinity: %#v", node.LocalityLevel))
	}
}
