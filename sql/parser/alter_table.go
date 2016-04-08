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

func (node *AlterTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE")
	if node.IfExists {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s %s", node.Table, node.Cmds)
	return buf.String()
}

// AlterTableCmds represents a list of table alterations.
type AlterTableCmds []AlterTableCmd

func (node AlterTableCmds) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// AlterTableCmd represents a table modification operation.
type AlterTableCmd interface {
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

func (node *AlterTableAddColumn) String() string {
	var buf bytes.Buffer
	buf.WriteString("ADD")
	if node.columnKeyword {
		buf.WriteString(" COLUMN")
	}
	if node.IfNotExists {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s", node.ColumnDef)
	return buf.String()
}

// AlterTableAddConstraint represents an ADD CONSTRAINT command.
type AlterTableAddConstraint struct {
	ConstraintDef ConstraintTableDef
}

func (node *AlterTableAddConstraint) String() string {
	return fmt.Sprintf("ADD %s", node.ConstraintDef)
}

// AlterTableDropColumn represents a DROP COLUMN command.
type AlterTableDropColumn struct {
	columnKeyword bool
	IfExists      bool
	Column        string
	DropBehavior  DropBehavior
}

func (node *AlterTableDropColumn) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP")
	if node.columnKeyword {
		_, _ = buf.WriteString(" COLUMN")
	}
	if node.IfExists {
		_, _ = buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", node.Column)
	if node.DropBehavior != DropDefault {
		fmt.Fprintf(&buf, " %s", node.DropBehavior)
	}
	return buf.String()
}

// AlterTableDropConstraint represents a DROP CONSTRAINT command.
type AlterTableDropConstraint struct {
	IfExists     bool
	Constraint   string
	DropBehavior DropBehavior
}

func (node *AlterTableDropConstraint) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP CONSTRAINT ")
	if node.IfExists {
		_, _ = buf.WriteString("IF EXISTS ")
	}
	_, _ = buf.WriteString(node.Constraint)
	if node.DropBehavior != DropDefault {
		fmt.Fprintf(&buf, " %s", node.DropBehavior)
	}
	return buf.String()
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

func (node *AlterTableSetDefault) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("ALTER")
	if node.columnKeyword {
		_, _ = buf.WriteString(" COLUMN")
	}
	fmt.Fprintf(&buf, " %s", node.Column)
	if node.Default == nil {
		_, _ = buf.WriteString(" DROP DEFAULT")
	} else {
		fmt.Fprintf(&buf, " SET DEFAULT %s", node.Default)
	}
	return buf.String()
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

func (node *AlterTableDropNotNull) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("ALTER ")
	if node.columnKeyword {
		_, _ = buf.WriteString("COLUMN ")
	}
	_, _ = buf.WriteString(node.Column)
	_, _ = buf.WriteString(" DROP NOT NULL")
	return buf.String()
}
