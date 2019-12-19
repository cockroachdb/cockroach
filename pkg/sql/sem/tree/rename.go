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

// RenameDatabase represents a RENAME DATABASE statement.
type RenameDatabase struct {
	Name    Name
	NewName Name
}

// Format implements the NodeFormatter interface.
func (node *RenameDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

// RenameTable represents a RENAME TABLE or RENAME VIEW or RENAME SEQUENCE
// statement. Whether the user has asked to rename a view or a sequence
// is indicated by the IsView and IsSequence fields.
type RenameTable struct {
	Name       *UnresolvedObjectName
	NewName    *UnresolvedObjectName
	IfExists   bool
	IsView     bool
	IsSequence bool
}

// Format implements the NodeFormatter interface.
func (node *RenameTable) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	if node.IsView {
		ctx.WriteString("VIEW ")
	} else if node.IsSequence {
		ctx.WriteString("SEQUENCE ")
	} else {
		ctx.WriteString("TABLE ")
	}
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Name)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(node.NewName)
}

// RenameIndex represents a RENAME INDEX statement.
type RenameIndex struct {
	Index    *TableIndexName
	NewName  UnrestrictedName
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *RenameIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER INDEX ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Index)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

// RenameColumn represents a RENAME COLUMN statement.
type RenameColumn struct {
	Table   TableName
	Name    Name
	NewName Name
	// IfExists refers to the table, not the column.
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *RenameColumn) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TABLE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Table)
	ctx.WriteString(" RENAME COLUMN ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewName)
}
