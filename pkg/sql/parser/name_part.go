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
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// Special case normalization rules for Turkish/Azeri lowercase dotless-i and
// uppercase dotted-i. Fold both dotted and dotless 'i' into the ascii i/I, so
// our case-insensitive comparison functions can be locale-invariant. This
// mapping implements case-insensitivity for Turkish and other latin-derived
// languages simultaneously, with the additional quirk that it is also
// insensitive to the dottedness of the i's
var normalize = unicode.SpecialCase{
	unicode.CaseRange{
		Lo: 0x0130,
		Hi: 0x0130,
		Delta: [unicode.MaxCase]rune{
			0x49 - 0x130, // Upper
			0x69 - 0x130, // Lower
			0x49 - 0x130, // Title
		},
	},
	unicode.CaseRange{
		Lo: 0x0131,
		Hi: 0x0131,
		Delta: [unicode.MaxCase]rune{
			0x49 - 0x131, // Upper
			0x69 - 0x131, // Lower
			0x49 - 0x131, // Title
		},
	},
}

func isASCII(s string) bool {
	for _, c := range s {
		if c > unicode.MaxASCII {
			return false
		}
	}
	return true
}

// A Name is an SQL identifier.
type Name string

// Format implements the NodeFormatter interface.
func (n Name) Format(buf *bytes.Buffer, f FmtFlags) {
	encodeSQLIdent(buf, string(n))
}

// Normalize normalizes to lowercase and Unicode Normalization Form C
// (NFC).
func (n Name) Normalize() string {
	lower := strings.Map(normalize.ToLower, string(n))
	if isASCII(lower) {
		return lower
	}
	return norm.NFC.String(lower)
}

// ReNormalizeName performs the same work as NormalizeName but when
// the string originates from the database. We define a different
// function so as to be able to track usage of this function (cf. #8200).
func ReNormalizeName(name string) string {
	return Name(name).Normalize()
}

// ToStrings converts the name list to an array of regular strings.
func (l NameList) ToStrings() []string {
	if l == nil {
		return nil
	}
	names := make([]string, len(l))
	for i, n := range l {
		names[i] = string(n)
	}
	return names
}

// A NameList is a list of identifiers.
type NameList []Name

// Format implements the NodeFormatter interface.
func (l NameList) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, n := range l {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, n)
	}
}

// UnqualifiedStar corresponds to a standalone '*' in an expression or
// a '*' as name part of an UnresolvedName.
type UnqualifiedStar struct{}

// Format implements the NodeFormatter interface.
func (UnqualifiedStar) Format(buf *bytes.Buffer, _ FmtFlags) { buf.WriteByte('*') }
func (u UnqualifiedStar) String() string                     { return AsString(u) }

// ArraySubscript corresponds to the syntax `<name>[ ... ]`.
type ArraySubscript struct {
	Begin Expr
	End   Expr
	Slice bool
}

// Format implements the NodeFormatter interface.
func (a *ArraySubscript) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('[')
	if a.Begin != nil {
		FormatNode(buf, f, a.Begin)
	}
	if a.Slice {
		buf.WriteByte(':')
		if a.End != nil {
			FormatNode(buf, f, a.End)
		}
	}
	buf.WriteByte(']')
}

// NamePart is the interface for the sub-parts of an UnresolvedName or
// the Selector/Context members of ColumnItem and FunctionName.
type NamePart interface {
	NodeFormatter
	namePart()
}

var _ NamePart = Name("")
var _ NamePart = &ArraySubscript{}
var _ NamePart = UnqualifiedStar{}

func (Name) namePart()              {}
func (a *ArraySubscript) namePart() {}
func (UnqualifiedStar) namePart()   {}

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

// UnresolvedName holds the initial syntax of a name as
// determined during parsing.
type UnresolvedName NameParts

// Format implements the NodeFormatter interface.
func (u UnresolvedName) Format(buf *bytes.Buffer, f FmtFlags) { NameParts(u).Format(buf, f) }
func (u UnresolvedName) String() string                       { return AsString(u) }

// UnresolvedNames corresponds to a comma-separate list of unresolved
// names.  Note: this should be treated as immutable when embedded in
// an Expr context, otherwise the Walk code must be updated to
// duplicate the array an Expr node is duplicated.
type UnresolvedNames []UnresolvedName

// Format implements the NodeFormatter interface.
func (u UnresolvedNames) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, n := range u {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, n)
	}
}
