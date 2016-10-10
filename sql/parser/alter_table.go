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
	Table    NormalizableTableName
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
	for i, n := range node {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, n)
	}
}

// AlterTableCmd represents a table modification operation.
type AlterTableCmd interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types
	// (AlterTable*) conform to the AlterTableCmd interface.
	alterTableCmd()
}

func (*AlterTableAddColumn) alterTableCmd()          {}
func (*AlterTableAddConstraint) alterTableCmd()      {}
func (*AlterTableDropColumn) alterTableCmd()         {}
func (*AlterTableDropConstraint) alterTableCmd()     {}
func (*AlterTableDropNotNull) alterTableCmd()        {}
func (*AlterTableSetDefault) alterTableCmd()         {}
func (*AlterTableValidateConstraint) alterTableCmd() {}

var _ AlterTableCmd = &AlterTableAddColumn{}
var _ AlterTableCmd = &AlterTableAddConstraint{}
var _ AlterTableCmd = &AlterTableDropColumn{}
var _ AlterTableCmd = &AlterTableDropConstraint{}
var _ AlterTableCmd = &AlterTableDropNotNull{}
var _ AlterTableCmd = &AlterTableSetDefault{}
var _ AlterTableCmd = &AlterTableValidateConstraint{}

// ColumnMutationCmd is the subset of AlterTableCmds that modify an
// existing column.
type ColumnMutationCmd interface {
	AlterTableCmd
	GetColumn() Name
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

// ValidationBehavior specifies whether or not a constraint is validated.
type ValidationBehavior int

const (
	// ValidationDefault is the default validation behavior (immediate).
	ValidationDefault ValidationBehavior = iota
	// ValidationSkip skips validation of any existing data.
	ValidationSkip
)

// AlterTableAddConstraint represents an ADD CONSTRAINT command.
type AlterTableAddConstraint struct {
	ConstraintDef      ConstraintTableDef
	ValidationBehavior ValidationBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAddConstraint) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ADD ")
	FormatNode(buf, f, node.ConstraintDef)
	if node.ValidationBehavior == ValidationSkip {
		buf.WriteString(" NOT VALID")
	}
}

// AlterTableDropColumn represents a DROP COLUMN command.
type AlterTableDropColumn struct {
	columnKeyword bool
	IfExists      bool
	Column        Name
	DropBehavior  DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropColumn) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP ")
	if node.columnKeyword {
		buf.WriteString("COLUMN ")
	}
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Column)
	if node.DropBehavior != DropDefault {
		fmt.Fprintf(buf, " %s", node.DropBehavior)
	}
}

// AlterTableDropConstraint represents a DROP CONSTRAINT command.
type AlterTableDropConstraint struct {
	IfExists     bool
	Constraint   Name
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropConstraint) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP CONSTRAINT ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Constraint)
	if node.DropBehavior != DropDefault {
		fmt.Fprintf(buf, " %s", node.DropBehavior)
	}
}

// AlterTableValidateConstraint represents a VALIDATE CONSTRAINT command.
type AlterTableValidateConstraint struct {
	Constraint Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableValidateConstraint) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("VALIDATE CONSTRAINT ")
	FormatNode(buf, f, node.Constraint)
}

// AlterTableSetDefault represents an ALTER COLUMN SET DEFAULT
// or DROP DEFAULT command.
type AlterTableSetDefault struct {
	columnKeyword bool
	Column        Name
	Default       Expr
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetDefault) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetDefault) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER ")
	if node.columnKeyword {
		buf.WriteString("COLUMN ")
	}
	FormatNode(buf, f, node.Column)
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
	Column        Name
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableDropNotNull) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropNotNull) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER ")
	if node.columnKeyword {
		buf.WriteString("COLUMN ")
	}
	FormatNode(buf, f, node.Column)
	buf.WriteString(" DROP NOT NULL")
}
