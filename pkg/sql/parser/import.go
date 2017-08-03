// Copyright 2017 The Cockroach Authors.
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

package parser

import "bytes"

// Import represents a BACKUP statement.
type Import struct {
	Table      UnresolvedName
	CreateFile Expr
	CreateDefs TableDefs
	FileFormat string
	Files      Exprs
	Options    KVOptions
	Temp       Expr
}

// ImportCreate represents the source of the schema in an import.
// This is one of a literal CREATE TABLE statement, or the URI to a file that
// contains a CREATE TABLE statement.
type ImportCreate interface {
	importCreate()
}

// ImportCreateStmt implements ImportCreate via a CreateTable.
type ImportCreateStmt struct {
	*CreateTable
}

func (ImportCreateStmt) importCreate() {}

// ImportCreateFile implements ImportCreate, with a URI schema file.
type ImportCreateFile struct {
	Expr
}

func (ImportCreateFile) importCreate() {}

var _ Statement = &Import{}

// Format implements the NodeFormatter interface.
func (node *Import) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("IMPORT TABLE ")
	FormatNode(buf, f, node.Table)

	if node.CreateFile != nil {
		buf.WriteString(" CREATE USING ")
		FormatNode(buf, f, node.CreateFile)
		buf.WriteString(" ")
	} else {
		buf.WriteString(" (")
		FormatNode(buf, f, node.CreateDefs)
		buf.WriteString(") ")
	}

	buf.WriteString(node.FileFormat)
	buf.WriteString(" DATA (")
	FormatNode(buf, f, node.Files)
	buf.WriteString(") ")

	if node.Options != nil {
		buf.WriteString("WITH OPTIONS (")
		FormatNode(buf, f, node.Options)
		buf.WriteString(") ")
	}

	buf.WriteString("USING TEMP STORE ")
	FormatNode(buf, f, node.Temp)
}
