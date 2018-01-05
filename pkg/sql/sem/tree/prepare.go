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

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
)

// Prepare represents a PREPARE statement.
type Prepare struct {
	Name      Name
	Types     []coltypes.T
	Statement Statement
}

// Format implements the NodeFormatter interface.
func (node *Prepare) Format(ctx *FmtCtx) {
	ctx.WriteString("PREPARE ")
	ctx.FormatNode(&node.Name)
	if len(node.Types) > 0 {
		ctx.WriteString(" (")
		for i, t := range node.Types {
			if i > 0 {
				ctx.WriteString(", ")
			}
			t.Format(ctx.Buffer, ctx.flags.EncodeFlags())
		}
		ctx.WriteRune(')')
	}
	ctx.WriteString(" AS ")
	ctx.FormatNode(node.Statement)
}

// Execute represents an EXECUTE statement.
type Execute struct {
	Name   Name
	Params Exprs
}

// Format implements the NodeFormatter interface.
func (node *Execute) Format(ctx *FmtCtx) {
	ctx.WriteString("EXECUTE ")
	ctx.FormatNode(&node.Name)
	if len(node.Params) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Params)
		ctx.WriteByte(')')
	}
}

// Deallocate represents a DEALLOCATE statement.
type Deallocate struct {
	Name Name // empty for ALL
}

// Format implements the NodeFormatter interface.
func (node *Deallocate) Format(ctx *FmtCtx) {
	ctx.WriteString("DEALLOCATE ")
	if node.Name == "" {
		ctx.WriteString("ALL")
	} else {
		ctx.FormatNode(&node.Name)
	}
}
