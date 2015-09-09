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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
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
	_, _ = buf.WriteString("ALTER TABLE")
	if node.IfExists {
		_, _ = buf.WriteString(" IF EXISTS")
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
	// (AlterTable*Cmd) conform to the AlterTableCmd interface.
	alterTableCmd()
}

func (*AlterTableAddColumn) alterTableCmd()     {}
func (*AlterTableAddConstraint) alterTableCmd() {}

// AlterTableAddColumn represents an ADD COLUMN command.
type AlterTableAddColumn struct {
	columnKeyword bool
	IfNotExists   bool
	ColumnDef     *ColumnTableDef
}

func (node *AlterTableAddColumn) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("ADD")
	if node.columnKeyword {
		_, _ = buf.WriteString(" COLUMN")
	}
	if node.IfNotExists {
		_, _ = buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s", node.ColumnDef)
	return buf.String()
}

// AlterTableAddConstraint represents an ADD CONSTRAINT command.
type AlterTableAddConstraint struct {
	ConstraintDef *IndexTableDef
}

func (node *AlterTableAddConstraint) String() string {
	return fmt.Sprintf("ADD %s", node.ConstraintDef)
}
