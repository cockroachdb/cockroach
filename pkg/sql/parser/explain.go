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

package parser

import (
	"bytes"
	"strings"
)

// Explain represents an EXPLAIN statement.
type Explain struct {
	// Options defines how EXPLAIN should operate (VERBOSE, METADATA,
	// etc.) Which options are valid depends on the explain mode. See
	// sql/explain.go for details.
	Options []string

	// Statement is the statement being EXPLAINed.
	Statement Statement

	// Enclosed is set to true if the EXPLAIN syntax was used as a data
	// source, and thus enclosed in square brackets.
	Enclosed bool
}

// Format implements the NodeFormatter interface.
func (node *Explain) Format(buf *bytes.Buffer, f FmtFlags) {
	if node.Enclosed {
		buf.WriteByte('[')
	}
	buf.WriteString("EXPLAIN ")
	if len(node.Options) > 0 {
		buf.WriteByte('(')
		for i, opt := range node.Options {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(strings.ToUpper(opt))
		}
		buf.WriteString(") ")
	}
	FormatNode(buf, f, node.Statement)
	if node.Enclosed {
		buf.WriteByte(']')
	}
}
