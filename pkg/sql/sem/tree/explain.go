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

package tree

import (
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
}

// Format implements the NodeFormatter interface.
func (node *Explain) Format(ctx *FmtCtx) {
	ctx.WriteString("EXPLAIN ")
	if len(node.Options) > 0 {
		ctx.WriteByte('(')
		for i, opt := range node.Options {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.WriteString(strings.ToUpper(opt))
		}
		ctx.WriteString(") ")
	}
	ctx.FormatNode(node.Statement)
}
