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

import "github.com/cockroachdb/cockroach/pkg/security"

// AlterDatabaseOwner represents a ALTER DATABASE OWNER TO statement.
type AlterDatabaseOwner struct {
	Name Name
	// TODO(solon): Adjust this, see
	// https://github.com/cockroachdb/cockroach/issues/54696
	Owner security.SQLUsername
}

// Format implements the NodeFormatter interface.
func (node *AlterDatabaseOwner) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" OWNER TO ")
	ctx.FormatUsername(node.Owner)
}

// AlterDatabaseAddRegion represents a ALTER DATABASE ADD REGION statement.
type AlterDatabaseAddRegion struct {
	Name        Name
	Region      Name
	IfNotExists bool
}

var _ Statement = &AlterDatabaseAddRegion{}

// Format implements the NodeFormatter interface.
func (node *AlterDatabaseAddRegion) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" ADD REGION ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Region)
}

// AlterDatabaseDropRegion represents a ALTER DATABASE DROP REGION statement.
type AlterDatabaseDropRegion struct {
	Name     Name
	Region   Name
	IfExists bool
}

var _ Statement = &AlterDatabaseDropRegion{}

// Format implements the NodeFormatter interface.
func (node *AlterDatabaseDropRegion) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" DROP REGION ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Region)
}

// AlterDatabasePrimaryRegion represents a ALTER DATABASE PRIMARY REGION ... statement.
type AlterDatabasePrimaryRegion struct {
	Name          Name
	PrimaryRegion Name
}

var _ Statement = &AlterDatabasePrimaryRegion{}

// Format implements the NodeFormatter interface.
func (node *AlterDatabasePrimaryRegion) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" PRIMARY REGION ")
	node.PrimaryRegion.Format(ctx)
}

// AlterDatabaseSurvivalGoal represents a ALTER DATABASE SURVIVE ... statement.
type AlterDatabaseSurvivalGoal struct {
	Name         Name
	SurvivalGoal SurvivalGoal
}

var _ Statement = &AlterDatabaseSurvivalGoal{}

// Format implements the NodeFormatter interface.
func (node *AlterDatabaseSurvivalGoal) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" ")
	node.SurvivalGoal.Format(ctx)
}
