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
// Author: Matt Jibson

package parser

import "bytes"

// CopyFrom represents a COPY FROM statement.
type CopyFrom struct {
	Table   NormalizableTableName
	Columns UnresolvedNames
	Stdin   bool
}

// Format implements the NodeFormatter interface.
func (node *CopyFrom) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("COPY ")
	FormatNode(buf, f, node.Table)
	if len(node.Columns) > 0 {
		buf.WriteString(" (")
		FormatNode(buf, f, node.Columns)
		buf.WriteString(")")
	}
	buf.WriteString(" FROM ")
	if node.Stdin {
		buf.WriteString("STDIN")
	}
}
