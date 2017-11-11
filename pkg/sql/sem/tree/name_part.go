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
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
)

// A Name is an SQL identifier.
//
// In general, a Name is the result of parsing a name nonterminal, which is used
// in the grammar where reserved keywords cannot be distinguished from
// identifiers. A Name that matches a reserved keyword must thus be quoted when
// formatted. (Names also need quoting for a variety of other reasons; see
// isBareIdentifier.)
//
// For historical reasons, some Names are instead the result of parsing
// `unrestricted_name` nonterminals. See UnrestrictedName for details.
type Name string

// Format implements the NodeFormatter interface.
func (n Name) Format(buf *bytes.Buffer, f FmtFlags) {
	if f.anonymize {
		buf.WriteByte('_')
	} else {
		lex.EncodeRestrictedSQLIdent(buf, string(n), f.encodeFlags)
	}
}

// Normalize normalizes to lowercase and Unicode Normalization Form C
// (NFC).
func (n Name) Normalize() string {
	return lex.NormalizeName(string(n))
}

// An UnrestrictedName is a Name that does not need to be escaped when it
// matches a reserved keyword.
//
// In general, an UnrestrictedName is the result of parsing an unrestricted_name
// nonterminal, which is used in the grammar where reserved keywords can be
// unambiguously interpreted as identifiers. When formatted, an UnrestrictedName
// that matches a reserved keyword thus does not need to be quoted.
//
// For historical reasons, some unrestricted_name nonterminals are instead
// parsed as Names. The only user-visible impact of this is that we are too
// aggressive about quoting names in certain positions. New grammar rules should
// prefer to parse unrestricted_name nonterminals into UnrestrictedNames.
type UnrestrictedName string

// Format implements the NodeFormatter interface.
func (u UnrestrictedName) Format(buf *bytes.Buffer, f FmtFlags) {
	if f.anonymize {
		buf.WriteByte('_')
	} else {
		lex.EncodeUnrestrictedSQLIdent(buf, string(u), f.encodeFlags)
	}
}

// Normalize normalizes to lowercase and Unicode Normalization Form C
// (NFC).
func (u UnrestrictedName) Normalize() string {
	return Name(u).Normalize()
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
var _ NamePart = UnrestrictedName("")
var _ NamePart = &ArraySubscript{}
var _ NamePart = UnqualifiedStar{}

func (Name) namePart()              {}
func (UnrestrictedName) namePart()  {}
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
func (u UnresolvedName) Format(buf *bytes.Buffer, f FmtFlags) { FormatNode(buf, f, NameParts(u)) }
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
