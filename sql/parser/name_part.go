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

// NamePart is the interface for the sub-parts of an UnresolvedName or
// the Selector/Context members of ColumnItem and FunctionName.
type NamePart interface {
	NodeFormatter
	namePart()
}

var _ NamePart = Name("")
var _ NamePart = &ArraySubscript{}
var _ NamePart = UnqualifiedStar{}

// We reuse Name as an unresolved name part.
func (Name) namePart() {}

// We also reuse the UnqualifiedStar as an unresolved name part.
func (UnqualifiedStar) namePart() {}

// ArraySubscript corresponds to the syntax `<name>[ ... ]`.
type ArraySubscript struct {
	Begin Expr
	End   Expr
}

// Format implements the NodeFormatter interface.
func (a *ArraySubscript) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('[')
	FormatNode(buf, f, a.Begin)
	if a.End != nil {
		buf.WriteByte(':')
		FormatNode(buf, f, a.End)
	}
	buf.WriteByte(']')
}
func (a *ArraySubscript) namePart() {}

// NameParts represents a combination of names with array and
// sub-field subscripts.
type NameParts []NamePart

// Format implements the NodeFormatter interface.
func (l NameParts) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, p := range l {
		_, isArraySubscript := p.(*ArraySubscript)
		if !isArraySubscript && i > 0 {
			buf.WriteByte('.')
		}
		FormatNode(buf, f, p)
	}
}
