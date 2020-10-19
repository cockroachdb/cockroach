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

// AlterDatabaseOwner represents a ALTER DATABASE OWNER TO statement.
type AlterDatabaseOwner struct {
	Name  Name
	Owner Name
}

// Format implements the NodeFormatter interface.
func (node *AlterDatabaseOwner) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" OWNER TO ")
	ctx.FormatNode(&node.Owner)
}

// AlterDatabaseAddRegion represents a ALTER DATABASE ADD REGION(S) statement.
type AlterDatabaseAddRegion struct {
	Name    Name
	Regions NameList
}

var _ Statement = &AlterDatabaseAddRegion{}

// Format implements the NodeFormatter interface.
func (node *AlterDatabaseAddRegion) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" ADD REGION ")
	node.Regions.Format(ctx)
}

// AlterDatabaseDropRegion represents a ALTER DATABASE DROP REGION(S) statement.
type AlterDatabaseDropRegion struct {
	Name    Name
	Regions NameList
}

var _ Statement = &AlterDatabaseDropRegion{}

// Format implements the NodeFormatter interface.
func (node *AlterDatabaseDropRegion) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" DROP REGION ")
	node.Regions.Format(ctx)
}

// AlterDatabaseSurvive represents a ALTER DATABASE SURVIVE ... statement.
type AlterDatabaseSurvive struct {
	Name    Name
	Survive Survive
}

var _ Statement = &AlterDatabaseSurvive{}

// Format implements the NodeFormatter interface.
func (node *AlterDatabaseSurvive) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" ")
	node.Survive.Format(ctx)
}
