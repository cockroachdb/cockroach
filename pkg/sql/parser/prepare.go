// Copyright 2016 The Cockroach Authors.
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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

import "bytes"

// Prepare represents a PREPARE statement.
type Prepare struct {
	Name      Name
	Types     []ColumnType
	Statement Statement
}

// Format implements the NodeFormatter interface.
func (node *Prepare) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("PREPARE ")
	FormatNode(buf, f, node.Name)
	if len(node.Types) > 0 {
		buf.WriteString(" (")
		for i, t := range node.Types {
			if i > 0 {
				buf.WriteString(", ")
			}
			FormatNode(buf, f, t)
		}
		buf.WriteRune(')')
	}
	buf.WriteString(" AS ")
	FormatNode(buf, f, node.Statement)
}

// Execute represents an EXECUTE statement.
type Execute struct {
	Name   Name
	Params Exprs
}

// Format implements the NodeFormatter interface.
func (node *Execute) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("EXECUTE ")
	FormatNode(buf, f, node.Name)
	if len(node.Params) > 0 {
		buf.WriteString(" (")
		FormatNode(buf, f, node.Params)
		buf.WriteByte(')')
	}
}

// Deallocate represents a DEALLOCATE statement.
type Deallocate struct {
	Name Name // empty for ALL
}

// Format implements the NodeFormatter interface.
func (node *Deallocate) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DEALLOCATE ")
	if node.Name == "" {
		buf.WriteString("ALL")
	} else {
		FormatNode(buf, f, node.Name)
	}
}
