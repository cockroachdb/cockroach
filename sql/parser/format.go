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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
)

type fmtFlags struct {
	showTypes        bool
	showTableAliases bool
}

// FmtFlags enables conditional formatting in the pretty-printer.
type FmtFlags *fmtFlags

// FmtSimple instructs the pretty-printer to produce
// a straightforward representation, ideally using SQL
// syntax that makes prettyprint+parse idempotent.
var FmtSimple FmtFlags = &fmtFlags{showTypes: false}

// FmtQualify instructs the pretty-printer to qualify qnames with the
// table name.
var FmtQualify FmtFlags = &fmtFlags{showTableAliases: true}

// FmtShowTypes instructs the pretty-printer to
// annotate expressions with their resolved types.
var FmtShowTypes FmtFlags = &fmtFlags{showTypes: true}

// NodeFormatter is implemented by nodes that can be pretty-printed.
type NodeFormatter interface {
	// Format performs pretty-printing towards a bytes buffer. The
	// flags argument influences the results.
	Format(buf *bytes.Buffer, flags FmtFlags)
}

// FormatNode recurses into a node for pretty-printing.
// Flag-driven special cases can hook into this.
func FormatNode(buf *bytes.Buffer, f FmtFlags, n NodeFormatter) {
	if f.showTypes {
		if te, ok := n.(TypedExpr); ok {
			buf.WriteByte('(')
			n.Format(buf, f)
			buf.WriteString(")[")
			if rt := te.ReturnType(); rt == nil {
				// An attempt is made to pretty-print an expression that was
				// not assigned a type yet. This should not happen, so we make
				// it clear in the output this needs to be investigated
				// further.
				buf.WriteString(fmt.Sprintf("??? %v", te))
			} else {
				buf.WriteString(rt.Type())
			}
			buf.WriteByte(']')
			return
		}
	}
	n.Format(buf, f)
}

// AsStringWithFlags pretty prints a node to a string given specific flags.
func AsStringWithFlags(n NodeFormatter, f FmtFlags) string {
	var buf bytes.Buffer
	FormatNode(&buf, f, n)
	return buf.String()
}

// AsString pretty prints a node to a string.
func AsString(n NodeFormatter) string {
	return AsStringWithFlags(n, FmtSimple)
}
