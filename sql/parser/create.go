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
	Encoding    *StrVal
}

// Format implements the NodeFormatter interface.
func (node *CreateDatabase) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("CREATE DATABASE ")
	if node.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}
	FormatNode(buf, f, node.Name)
	if node.Encoding != nil {
		buf.WriteString(" ENCODING=")
		node.Encoding.Format(buf, f)
	}
}

// IndexElem represents a column with a direction in a CREATE INDEX statement.
type IndexElem struct {
	Column    Name
	Direction Direction
}

// Format implements the NodeFormatter interface.
func (node IndexElem) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Column)
	if node.Direction != DefaultDirection {
		buf.WriteByte(' ')
		buf.WriteString(node.Direction.String())
	}
}

// IndexElemList is list of IndexElem.
type IndexElemList []IndexElem

// Format pretty-prints the contained names separated by commas.
// Format implements the NodeFormatter interface.
func (l IndexElemList) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, indexElem := range l {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, indexElem)
	}
}

// CreateIndex represents a CREATE INDEX statement.
type CreateIndex struct {
	Name        Name
	Table       *QualifiedName
	Unique      bool
	IfNotExists bool
	Columns     IndexElemList
	// Extra columns to be stored together with the indexed ones as an optimization
	// for improved reading performance.
	Storing NameList
}

// Format implements the NodeFormatter interface.
func (node *CreateIndex) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("CREATE ")
	if node.Unique {
		buf.WriteString("UNIQUE ")
	}
	buf.WriteString("INDEX ")
	if node.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}
	if node.Name != "" {
		fmt.Fprintf(buf, "%s ", node.Name)
	}
	fmt.Fprintf(buf, "ON %s (", node.Table)
	FormatNode(buf, f, node.Columns)
	buf.WriteByte(')')
	if node.Storing != nil {
		fmt.Fprintf(buf, " STORING (%s)", node.Storing)
	}
}

// TableDef represents a column, index or constraint definition within a CREATE
// TABLE statement.
type TableDef interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types (*TableDef) conform
	// to the TableDef interface.
	tableDef()
	setName(name Name)
}

func (*ColumnTableDef) tableDef() {}
func (*IndexTableDef) tableDef()  {}

// TableDefs represents a list of table definitions.
type TableDefs []TableDef

// Format implements the NodeFormatter interface.
func (node TableDefs) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, n := range node {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, n)
	}
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
	Name        Name
	Type        ColumnType
	Nullable    Nullability
	PrimaryKey  bool
	Unique      bool
	DefaultExpr Expr
	CheckExpr   Expr
	References  struct {
		Table *QualifiedName
		Col   Name
	}
}

func newColumnTableDef(
	name Name, typ ColumnType, qualifications []ColumnQualification,
) *ColumnTableDef {
	d := &ColumnTableDef{
		Name:     name,
		Type:     typ,
		Nullable: SilentNull,
	}
	for _, c := range qualifications {
		switch t := c.(type) {
		case *ColumnDefault:
			d.DefaultExpr = t.Expr
		case NotNullConstraint:
			d.Nullable = NotNull
		case NullConstraint:
			d.Nullable = Null
		case PrimaryKeyConstraint:
			d.PrimaryKey = true
		case UniqueConstraint:
			d.Unique = true
		case *ColumnCheckConstraint:
			d.CheckExpr = t.Expr
		case *ColumnFKConstraint:
			d.References.Table = t.Table
			d.References.Col = t.Col
		default:
			panic(fmt.Sprintf("unexpected column qualification: %T", c))
		}
	}
	return d
}

func (node *ColumnTableDef) setName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *ColumnTableDef) Format(buf *bytes.Buffer, f FmtFlags) {
	fmt.Fprintf(buf, "%s ", node.Name)
	FormatNode(buf, f, node.Type)
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
	if node.DefaultExpr != nil {
		buf.WriteString(" DEFAULT ")
		FormatNode(buf, f, node.DefaultExpr)
	}
	if node.CheckExpr != nil {
		buf.WriteString(" CHECK (")
		FormatNode(buf, f, node.CheckExpr)
		buf.WriteByte(')')
	}
	if node.References.Table != nil {
		buf.WriteString(" REFERENCES ")
		FormatNode(buf, f, node.References.Table)
		if node.References.Col != "" {
			buf.WriteString(" (")
			FormatNode(buf, f, node.References.Col)
			buf.WriteByte(')')
		}
	}

}

// ColumnQualification represents a constraint on a column.
type ColumnQualification interface {
	columnQualification()
}

func (*ColumnDefault) columnQualification()         {}
func (NotNullConstraint) columnQualification()      {}
func (NullConstraint) columnQualification()         {}
func (PrimaryKeyConstraint) columnQualification()   {}
func (UniqueConstraint) columnQualification()       {}
func (*ColumnCheckConstraint) columnQualification() {}
func (*ColumnFKConstraint) columnQualification()    {}

// ColumnDefault represents a DEFAULT clause for a column.
type ColumnDefault struct {
	Expr Expr
}

// NotNullConstraint represents NOT NULL on a column.
type NotNullConstraint struct{}

// NullConstraint represents NULL on a column.
type NullConstraint struct{}

// PrimaryKeyConstraint represents NULL on a column.
type PrimaryKeyConstraint struct{}

// UniqueConstraint represents UNIQUE on a column.
type UniqueConstraint struct{}

// ColumnCheckConstraint represents either a check on a column.
type ColumnCheckConstraint struct {
	Expr Expr
}

// ColumnFKConstraint represents a FK-constaint on a column.
type ColumnFKConstraint struct {
	Table *QualifiedName
	Col   Name // empty-string means use PK
}

// NameListToIndexElems converts a NameList to an IndexElemList with all
// members using the `DefaultDirection`.
func NameListToIndexElems(lst NameList) IndexElemList {
	elems := make(IndexElemList, 0, len(lst))
	for _, n := range lst {
		elems = append(elems, IndexElem{Column: Name(n), Direction: DefaultDirection})
	}
	return elems
}

// IndexTableDef represents an index definition within a CREATE TABLE
// statement.
type IndexTableDef struct {
	Name    Name
	Columns IndexElemList
	Storing NameList
}

func (node *IndexTableDef) setName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *IndexTableDef) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("INDEX ")
	if node.Name != "" {
		FormatNode(buf, f, node.Name)
		buf.WriteByte(' ')
	}
	buf.WriteByte('(')
	FormatNode(buf, f, node.Columns)
	buf.WriteByte(')')
	if node.Storing != nil {
		buf.WriteString(" STORING (")
		FormatNode(buf, f, node.Storing)
		buf.WriteByte(')')
	}
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

// Format implements the NodeFormatter interface.
func (node *UniqueConstraintTableDef) Format(buf *bytes.Buffer, f FmtFlags) {
	if node.Name != "" {
		fmt.Fprintf(buf, "CONSTRAINT %s ", node.Name)
	}
	if node.PrimaryKey {
		buf.WriteString("PRIMARY KEY ")
	} else {
		buf.WriteString("UNIQUE ")
	}
	buf.WriteByte('(')
	FormatNode(buf, f, node.Columns)
	buf.WriteByte(')')
	if node.Storing != nil {
		buf.WriteString(" STORING (")
		FormatNode(buf, f, node.Storing)
		buf.WriteByte(')')
	}
}

func (*CheckConstraintTableDef) tableDef()           {}
func (*CheckConstraintTableDef) constraintTableDef() {}

// CheckConstraintTableDef represents a check constraint within a CREATE
// TABLE statement.
type CheckConstraintTableDef struct {
	Name Name
	Expr Expr
}

func (node *CheckConstraintTableDef) setName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *CheckConstraintTableDef) Format(buf *bytes.Buffer, f FmtFlags) {
	if node.Name != "" {
		fmt.Fprintf(buf, "CONSTRAINT %s ", node.Name)
	}
	fmt.Fprintf(buf, "CHECK (")
	FormatNode(buf, f, node.Expr)
	buf.WriteByte(')')
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	IfNotExists bool
	Table       *QualifiedName
	Defs        TableDefs
}

// Format implements the NodeFormatter interface.
func (node *CreateTable) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("CREATE TABLE ")
	if node.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}
	FormatNode(buf, f, node.Table)
	buf.WriteString(" (")
	FormatNode(buf, f, node.Defs)
	buf.WriteByte(')')
}
