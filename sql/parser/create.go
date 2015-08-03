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
	_, _ = buf.WriteString("CREATE DATABASE ")
	if node.IfNotExists {
		_, _ = buf.WriteString("IF NOT EXISTS ")
	}
	_, _ = buf.WriteString(node.Name.String())
	return buf.String()
}

// TableDef represents a column or index definition within a CREATE TABLE
// statement.
type TableDef interface {
	// Placeholder function to ensure that only desired types (*TableDef) conform
	// to the TableDef interface.
	tableDef()
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

// ColumnTableDef represents a column dlefinition within a CREATE TABLE
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

func (node *ColumnTableDef) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %s", node.Name, node.Type)
	switch node.Nullable {
	case Null:
		_, _ = buf.WriteString(" NULL")
	case NotNull:
		_, _ = buf.WriteString(" NOT NULL")
	}
	if node.PrimaryKey {
		_, _ = buf.WriteString(" PRIMARY KEY")
	} else if node.Unique {
		_, _ = buf.WriteString(" UNIQUE")
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
	Name       Name
	PrimaryKey bool
	Unique     bool
	Columns    NameList
}

func (node *IndexTableDef) String() string {
	var buf bytes.Buffer
	if node.Name != "" {
		fmt.Fprintf(&buf, "CONSTRAINT %s ", node.Name)
	}
	if node.PrimaryKey {
		_, _ = buf.WriteString("PRIMARY KEY ")
	} else if node.Unique {
		_, _ = buf.WriteString("UNIQUE ")
	} else {
		_, _ = buf.WriteString("INDEX ")
	}
	fmt.Fprintf(&buf, "(%s)", node.Columns)
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
	_, _ = buf.WriteString("CREATE TABLE")
	if node.IfNotExists {
		_, _ = buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s (%s)", node.Table, node.Defs)
	return buf.String()
}
