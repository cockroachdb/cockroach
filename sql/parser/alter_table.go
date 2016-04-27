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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package parser

import (
	"bytes"
	"fmt"
)

// AlterTable represents an ALTER TABLE statement.
type AlterTable struct {
	IfExists bool
	Table    *QualifiedName
	Cmds     AlterTableCmds
}

// Format implements the NodeFormatter interface.
func (node *AlterTable) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER TABLE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Table)
	buf.WriteByte(' ')
	FormatNode(buf, f, node.Cmds)
}

// AlterTableCmds represents a list of table alterations.
type AlterTableCmds []AlterTableCmd

// Format implements the NodeFormatter interface.
func (node AlterTableCmds) Format(buf *bytes.Buffer, f FmtFlags) {
	var prefix string
	for _, n := range node {
		buf.WriteString(prefix)
		FormatNode(buf, f, n)
		prefix = ", "
	}
}

// AlterTableCmd represents a table modification operation.
type AlterTableCmd interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types
	// (AlterTable*) conform to the AlterTableCmd interface.
	alterTableCmd()
}

func (*AlterTableAddColumn) alterTableCmd()      {}
func (*AlterTableAddConstraint) alterTableCmd()  {}
func (*AlterTableDropColumn) alterTableCmd()     {}
func (*AlterTableDropConstraint) alterTableCmd() {}
func (*AlterTableSetDefault) alterTableCmd()     {}
func (*AlterTableDropNotNull) alterTableCmd()    {}

// ColumnMutationCmd is the subset of AlterTableCmds that modify an
// existing column.
type ColumnMutationCmd interface {
	AlterTableCmd
	GetColumn() string
}

// AlterTableAddColumn represents an ADD COLUMN command.
type AlterTableAddColumn struct {
	columnKeyword bool
	IfNotExists   bool
	ColumnDef     *ColumnTableDef
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAddColumn) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ADD ")
	if node.columnKeyword {
		buf.WriteString("COLUMN ")
	}
	if node.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}
	FormatNode(buf, f, node.ColumnDef)
}

// AlterTableAddConstraint represents an ADD CONSTRAINT command.
type AlterTableAddConstraint struct {
	ConstraintDef ConstraintTableDef
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAddConstraint) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ADD ")
	FormatNode(buf, f, node.ConstraintDef)
}

// AlterTableDropColumn represents a DROP COLUMN command.
type AlterTableDropColumn struct {
	columnKeyword bool
	IfExists      bool
	Column        string
	DropBehavior  DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropColumn) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP")
	if node.columnKeyword {
		buf.WriteString(" COLUMN")
	}
	if node.IfExists {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(buf, " %s", node.Column)
	if node.DropBehavior != DropDefault {
		fmt.Fprintf(buf, " %s", node.DropBehavior)
	}
}

// AlterTableDropConstraint represents a DROP CONSTRAINT command.
type AlterTableDropConstraint struct {
	IfExists     bool
	Constraint   string
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropConstraint) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP CONSTRAINT ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	buf.WriteString(node.Constraint)
	if node.DropBehavior != DropDefault {
		fmt.Fprintf(buf, " %s", node.DropBehavior)
	}
}

// AlterTableSetDefault represents an ALTER COLUMN SET DEFAULT
// or DROP DEFAULT command.
type AlterTableSetDefault struct {
	columnKeyword bool
	Column        string
	Default       Expr
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetDefault) GetColumn() string {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetDefault) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER ")
	if node.columnKeyword {
		buf.WriteString("COLUMN ")
	}
	buf.WriteString(node.Column)
	if node.Default == nil {
		buf.WriteString(" DROP DEFAULT")
	} else {
		fmt.Fprintf(buf, " SET DEFAULT ")
		FormatNode(buf, f, node.Default)
	}
}

// AlterTableDropNotNull represents an ALTER COLUMN DROP NOT NULL
// command.
type AlterTableDropNotNull struct {
	columnKeyword bool
	Column        string
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableDropNotNull) GetColumn() string {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropNotNull) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER ")
	if node.columnKeyword {
		buf.WriteString("COLUMN ")
	}
	buf.WriteString(node.Column)
	buf.WriteString(" DROP NOT NULL")
}
