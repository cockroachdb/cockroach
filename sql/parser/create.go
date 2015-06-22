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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
	"strings"
)

func (*CreateDatabase) statement() {}
func (*CreateIndex) statement()    {}
func (*CreateTable) statement()    {}
func (*CreateView) statement()     {}

// CreateDatabase represents a CREATE DATABASE statement.
type CreateDatabase struct {
	IfNotExists bool
	Name        string
}

func (node *CreateDatabase) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE DATABASE")
	if node.IfNotExists {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s", node.Name)
	return buf.String()
}

// CreateIndex represents a CREATE INDEX statement.
type CreateIndex struct {
	Name      string
	TableName string
	Unique    bool
}

func (node *CreateIndex) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE ")
	if node.Unique {
		buf.WriteString("UNIQUE ")
	}
	fmt.Fprintf(&buf, "INDEX %s ON %s", node.Name, node.TableName)
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
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// NullType represents either NULL, NOT NULL or an unspecified value (silent
// NULL).
type NullType int

// The values for NullType.
const (
	NotNull NullType = iota
	Null
	SilentNull
)

// ColumnTableDef represents a column dlefinition within a CREATE TABLE
// statement.
type ColumnTableDef struct {
	Name       string
	Type       ColumnType
	Null       NullType
	PrimaryKey bool
	Unique     bool
}

func (node *ColumnTableDef) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %s", node.Name, node.Type)
	switch node.Null {
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

// IndexTableDef represents an index definition within a CREATE TABLE
// statement.
type IndexTableDef struct {
	Name    string
	Unique  bool
	Columns []string
}

func (node *IndexTableDef) String() string {
	var buf bytes.Buffer
	if node.Unique {
		buf.WriteString("UNIQUE ")
	}
	fmt.Fprintf(&buf, "INDEX %s (%s)",
		node.Name, strings.Join(node.Columns, ", "))
	return buf.String()
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	IfNotExists bool
	Name        string
	Defs        TableDefs
}

func (node *CreateTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE")
	if node.IfNotExists {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s (%s)", node.Name, node.Defs)
	return buf.String()
}

// CreateView represents a CREATE VIEW statement.
type CreateView struct {
	Name string
}

func (node *CreateView) String() string {
	return fmt.Sprintf("CREATE VIEW %s", node.Name)
}
