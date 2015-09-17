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
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import (
	"bytes"
	"fmt"
)

// CreateDatabase represents a CREATE DATABASE statement.
type CreateDatabase struct {
	IfNotExists bool
	Name        Name
}

func (node *CreateDatabase) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE DATABASE ")
	if node.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}
	buf.WriteString(node.Name.String())
	return buf.String()
}

// CreateIndex represents a CREATE INDEX statement.
type CreateIndex struct {
	Name        Name
	Table       *QualifiedName
	Unique      bool
	IfNotExists bool
	Columns     NameList
	Storing     NameList
}

func (node *CreateIndex) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE ")
	if node.Unique {
		buf.WriteString("UNIQUE ")
	}
	buf.WriteString("INDEX ")
	if node.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}
	if node.Name != "" {
		fmt.Fprintf(&buf, "%s ", node.Name)
	}
	fmt.Fprintf(&buf, "ON %s (%s)", node.Table, node.Columns)
	if node.Storing != nil {
		fmt.Fprintf(&buf, " STORING (%s)", node.Storing)
	}
	return buf.String()
}

// TableDef represents a column or index definition within a CREATE TABLE
// statement.
type TableDef interface {
	// Placeholder function to ensure that only desired types (*TableDef) conform
	// to the TableDef interface.
	tableDef()
	setName(name Name)
}

func (*ColumnTableDef) tableDef() {}
func (*IndexTableDef) tableDef()  {}

// TableDefs represents a list of table definitions.
type TableDefs []TableDef

func (node TableDefs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// Nullability represents either NULL, NOT NULL or an unspecified value (silent
// NULL).
type Nullability int

// The values for NullType.
const (
	NotNull Nullability = iota
	Null
	SilentNull
)

// ColumnTableDef represents a column definition within a CREATE TABLE
// statement.
type ColumnTableDef struct {
	Name       Name
	Type       ColumnType
	Nullable   Nullability
	PrimaryKey bool
	Unique     bool
}

func newColumnTableDef(name Name, typ ColumnType,
	constraints []ColumnConstraint) *ColumnTableDef {
	d := &ColumnTableDef{
		Name:     name,
		Type:     typ,
		Nullable: SilentNull,
	}
	for _, c := range constraints {
		switch c.(type) {
		case NotNullConstraint:
			d.Nullable = NotNull
		case NullConstraint:
			d.Nullable = Null
		case PrimaryKeyConstraint:
			d.PrimaryKey = true
		case UniqueConstraint:
			d.Unique = true
		}
	}
	return d
}

func (node *ColumnTableDef) setName(name Name) {
	node.Name = name
}

func (node *ColumnTableDef) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %s", node.Name, node.Type)
	switch node.Nullable {
	case Null:
		buf.WriteString(" NULL")
	case NotNull:
		buf.WriteString(" NOT NULL")
	}
	if node.PrimaryKey {
		buf.WriteString(" PRIMARY KEY")
	} else if node.Unique {
		buf.WriteString(" UNIQUE")
	}
	return buf.String()
}

// ColumnConstraint represents a constraint on a column.
type ColumnConstraint interface {
	columnConstraint()
}

func (NotNullConstraint) columnConstraint()    {}
func (NullConstraint) columnConstraint()       {}
func (PrimaryKeyConstraint) columnConstraint() {}
func (UniqueConstraint) columnConstraint()     {}

// NotNullConstraint represents NOT NULL on a column.
type NotNullConstraint struct{}

// NullConstraint represents NULL on a column.
type NullConstraint struct{}

// PrimaryKeyConstraint represents NULL on a column.
type PrimaryKeyConstraint struct{}

// UniqueConstraint represents UNIQUE on a column.
type UniqueConstraint struct{}

// IndexTableDef represents an index definition within a CREATE TABLE
// statement.
type IndexTableDef struct {
	Name    Name
	Columns NameList
	Storing NameList
}

func (node *IndexTableDef) setName(name Name) {
	node.Name = name
}

func (node *IndexTableDef) String() string {
	var buf bytes.Buffer
	buf.WriteString("INDEX ")
	if node.Name != "" {
		fmt.Fprintf(&buf, "%s ", node.Name)
	}
	fmt.Fprintf(&buf, "(%s)", node.Columns)
	if node.Storing != nil {
		fmt.Fprintf(&buf, " STORING (%s)", node.Storing)
	}
	return buf.String()
}

// ConstraintTableDef represents a constraint definition within a CREATE TABLE
// statement.
type ConstraintTableDef interface {
	TableDef
	// Placeholder function to ensure that only desired types
	// (*ConstraintTableDef) conform to the ConstraintTableDef interface.
	constraintTableDef()
}

func (*UniqueConstraintTableDef) constraintTableDef() {}

// UniqueConstraintTableDef represents a unique constraint within a CREATE
// TABLE statement.
type UniqueConstraintTableDef struct {
	IndexTableDef
	PrimaryKey bool
}

func (node *UniqueConstraintTableDef) String() string {
	var buf bytes.Buffer
	if node.Name != "" {
		fmt.Fprintf(&buf, "CONSTRAINT %s ", node.Name)
	}
	if node.PrimaryKey {
		buf.WriteString("PRIMARY KEY ")
	} else {
		buf.WriteString("UNIQUE ")
	}
	fmt.Fprintf(&buf, "(%s)", node.Columns)
	if node.Storing != nil {
		fmt.Fprintf(&buf, " STORING (%s)", node.Storing)
	}
	return buf.String()
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	IfNotExists bool
	Table       *QualifiedName
	Defs        TableDefs
}

func (node *CreateTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE")
	if node.IfNotExists {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s (%s)", node.Table, node.Defs)
	return buf.String()
}
