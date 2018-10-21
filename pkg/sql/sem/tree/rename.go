// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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

// RenameTable represents a RENAME TABLE or RENAME VIEW statement.
// Whether the user has asked to rename a table or view is indicated
// by the IsView field.
type RenameTable struct {
	Name       TableName
	NewName    TableName
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
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

// RenameIndex represents a RENAME INDEX statement.
type RenameIndex struct {
	Index    *TableNameWithIndex
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
