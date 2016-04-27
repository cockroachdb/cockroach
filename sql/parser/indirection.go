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

import "bytes"

// IndirectionElem is a single element in an indirection expression.
type IndirectionElem interface {
	NodeFormatter
	indirectionElem()
}

func (NameIndirection) indirectionElem()   {}
func (StarIndirection) indirectionElem()   {}
func (*ArrayIndirection) indirectionElem() {}

// Indirection represents an indirection expression composed of a series of
// indirection elements.
type Indirection []IndirectionElem

// Format implements the NodeFormatter interface.
func (i Indirection) Format(buf *bytes.Buffer, f FmtFlags) {
	for _, e := range i {
		FormatNode(buf, f, e)
	}
}

// NameIndirection represents ".<name>" in an indirection expression.
type NameIndirection Name

// Format implements the NodeFormatter interface.
func (n NameIndirection) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('.')
	FormatNode(buf, f, Name(n))
}

// StarIndirection represents ".*" in an indirection expression.
type StarIndirection string

const (
	qualifiedStar   StarIndirection = ".*"
	unqualifiedStar StarIndirection = "*"
)

// Format implements the NodeFormatter interface.
func (s StarIndirection) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(string(s))
}

// ArrayIndirection represents "[<begin>:<end>]" in an indirection expression.
type ArrayIndirection struct {
	Begin Expr
	End   Expr
}

// Format implements the NodeFormatter interface.
func (a *ArrayIndirection) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('[')
	FormatNode(buf, f, a.Begin)
	if a.End != nil {
		buf.WriteByte(':')
		FormatNode(buf, f, a.End)
	}
	buf.WriteByte(']')
}
