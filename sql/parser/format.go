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

import "bytes"

const (
	// FmtSimple instructs the pretty-printer to produce
	// a straightforward representation, ideally using SQL
	// syntax that makes prettyprint+parse idempotent.
	FmtSimple = 0
)

// NodeFormatter is implemented by nodes that can be pretty-printed.
type NodeFormatter interface {
	// Format performs pretty-printing towards a bytes buffer.  The
	// flags argument influences the results.
	Format(buf *bytes.Buffer, flags int)
}

// FormatNode recurses into a node for pretty-printing.
// Flag-driven special cases can hook into this.
func FormatNode(buf *bytes.Buffer, f int, n NodeFormatter) {
	n.Format(buf, f)
}

// AsString pretty prints a node to a string.
func AsString(n NodeFormatter) string {
	var buf bytes.Buffer
	FormatNode(&buf, FmtSimple, n)
	return buf.String()
}
