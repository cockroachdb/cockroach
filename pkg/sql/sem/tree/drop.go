// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code was derived from https://github.com/youtube/vitess.

package tree

// DropBehavior represents options for dropping schema elements.
type DropBehavior int

// DropBehavior values.
const (
	DropDefault DropBehavior = iota
	DropRestrict
	DropCascade
)

var dropBehaviorName = [...]string{
	DropDefault:  "",
	DropRestrict: "RESTRICT",
	DropCascade:  "CASCADE",
}

func (d DropBehavior) String() string {
	return dropBehaviorName[d]
}

// DropDatabase represents a DROP DATABASE statement.
type DropDatabase struct {
	Name         Name
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP DATABASE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropIndex represents a DROP INDEX statement.
type DropIndex struct {
	IndexList    TableIndexNames
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP INDEX ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.IndexList)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropTable represents a DROP TABLE statement.
type DropTable struct {
	Names        TableNames
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropTable) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP TABLE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropView represents a DROP VIEW statement.
type DropView struct {
	Names        TableNames
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropView) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP VIEW ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropSequence represents a DROP SEQUENCE statement.
type DropSequence struct {
	Names        TableNames
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP SEQUENCE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropUser represents a DROP USER statement
type DropUser struct {
	Names    Exprs
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *DropUser) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP USER ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
}

// DropRole represents a DROP ROLE statement
type DropRole struct {
	Names    Exprs
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *DropRole) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP ROLE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
}
