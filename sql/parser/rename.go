// Copyright 2014 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import "bytes"

// RenameDatabase represents a RENAME DATABASE statement.
type RenameDatabase struct {
	Name    Name
	NewName Name
}

// Format implements the NodeFormatter interface.
func (node *RenameDatabase) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER DATABASE ")
	FormatNode(buf, f, node.Name)
	buf.WriteString(" RENAME TO ")
	FormatNode(buf, f, node.NewName)
}

// RenameTable represents a RENAME TABLE or RENAME VIEW statement.
// Whether the user has asked to rename a table or view is indicated
// by the IsView field.
type RenameTable struct {
	Name     NormalizableTableName
	NewName  NormalizableTableName
	IfExists bool
	IsView   bool
}

// Format implements the NodeFormatter interface.
func (node *RenameTable) Format(buf *bytes.Buffer, f FmtFlags) {
	if node.IsView {
		buf.WriteString("ALTER VIEW ")
	} else {
		buf.WriteString("ALTER TABLE ")
	}
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Name)
	buf.WriteString(" RENAME TO ")
	FormatNode(buf, f, node.NewName)
}

// RenameIndex represents a RENAME INDEX statement.
type RenameIndex struct {
	Index    *TableNameWithIndex
	NewName  Name
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *RenameIndex) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER INDEX ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Index)
	buf.WriteString(" RENAME TO ")
	FormatNode(buf, f, node.NewName)
}

// RenameColumn represents a RENAME COLUMN statement.
type RenameColumn struct {
	Table   NormalizableTableName
	Name    Name
	NewName Name
	// IfExists refers to the table, not the column.
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *RenameColumn) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER TABLE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")

	}
	FormatNode(buf, f, node.Table)
	buf.WriteString(" RENAME COLUMN ")
	FormatNode(buf, f, node.Name)
	buf.WriteString(" TO ")
	FormatNode(buf, f, node.NewName)
}
