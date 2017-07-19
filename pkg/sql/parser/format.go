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
	ShowTableAliases bool
	symbolicVars     bool
	hideConstants    bool
	// tableNameFormatter will be called on all NormalizableTableNames if it is
	// non-nil.
	tableNameFormatter func(*NormalizableTableName, *bytes.Buffer, FmtFlags)
	// indexedVarFormat is an optional interceptor for
	// IndexedVarContainer.IndexedVarFormat calls; it can be used to
	// customize the formatting of IndexedVars.
	indexedVarFormat func(buf *bytes.Buffer, f FmtFlags, c IndexedVarContainer, idx int)
	// starDatumFormat is an optional interceptor for StarDatum.Format calls,
	// can be used to customize the formatting of StarDatums.
	starDatumFormat func(buf *bytes.Buffer, f FmtFlags)
	// If true, non-function names are replaced by underscores.
	anonymize bool
	// If true, strings will be rendered without wrapping quotes if possible.
	bareStrings bool
	// If true, datums and placeholders will have type annotations (like
	// :::interval) as necessary to disambiguate between possible type
	// resolutions.
	disambiguateDatumTypes bool
	// If false, passwords are replaced by *****.
	showPasswords bool
}

// FmtFlags enables conditional formatting in the pretty-printer.
type FmtFlags *fmtFlags

// FmtSimple instructs the pretty-printer to produce
// a straightforward representation.
var FmtSimple FmtFlags = &fmtFlags{}

// FmtSimpleWithPasswords instructs the pretty-printer to produce a
// straightforward representation that does not suppress passwords.
var FmtSimpleWithPasswords FmtFlags = &fmtFlags{showPasswords: true}

// FmtShowTypes instructs the pretty-printer to
// annotate expressions with their resolved types.
var FmtShowTypes FmtFlags = &fmtFlags{showTypes: true}

// FmtSymbolicVars instructs the pretty-printer to
// print indexedVars using symbolic notation, to
// disambiguate columns.
var FmtSymbolicVars FmtFlags = &fmtFlags{symbolicVars: true}

// FmtBareStrings instructs the pretty-printer to print strings without
// wrapping quotes, if possible.
var FmtBareStrings FmtFlags = &fmtFlags{bareStrings: true}

// FmtParsable instructs the pretty-printer to produce a representation that
// can be parsed into an equivalent expression (useful for serialization of
// expressions).
var FmtParsable FmtFlags = &fmtFlags{disambiguateDatumTypes: true}

// FmtHideConstants instructs the pretty-printer to produce a
// representation that does not disclose query-specific data.
var FmtHideConstants FmtFlags = &fmtFlags{hideConstants: true}

// FmtAnonymize instructs the pretty-printer to remove
// any name but function names.
// TODO(knz): temporary until a better solution is found for #13968
var FmtAnonymize FmtFlags = &fmtFlags{anonymize: true}

// FmtReformatTableNames returns FmtFlags that instructs the pretty-printer
// to substitute the printing of table names using the provided function.
func FmtReformatTableNames(
	base FmtFlags, fn func(*NormalizableTableName, *bytes.Buffer, FmtFlags),
) FmtFlags {
	f := *base
	f.tableNameFormatter = fn
	return &f
}

// StripTypeFormatting removes the flag that extracts types from the format flags,
// so as to enable rendering expressions for which types have not been computed yet.
func StripTypeFormatting(f FmtFlags) FmtFlags {
	nf := *f
	nf.showTypes = false
	return &nf
}

// FmtExpr returns FmtFlags that indicate how the pretty-printer
// should format expressions.
func FmtExpr(base FmtFlags, showTypes bool, symbolicVars bool, showTableAliases bool) FmtFlags {
	f := *base
	f.showTypes = showTypes
	f.symbolicVars = symbolicVars
	f.ShowTableAliases = showTableAliases
	return &f
}

// FmtIndexedVarFormat returns FmtFlags that customizes the printing of
// IndexedVars using the provided function.
func FmtIndexedVarFormat(
	base FmtFlags, fn func(buf *bytes.Buffer, f FmtFlags, c IndexedVarContainer, idx int),
) FmtFlags {
	f := *base
	f.indexedVarFormat = fn
	return &f
}

// FmtStarDatumFormat returns FmtFlags that customizes the printing of
// StarDatums using the provided function.
func FmtStarDatumFormat(base FmtFlags, fn func(buf *bytes.Buffer, f FmtFlags)) FmtFlags {
	f := *base
	f.starDatumFormat = fn
	return &f
}

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
			formatNodeOrHideConstants(buf, f, n)
			buf.WriteString(")[")
			if rt := te.ResolvedType(); rt == nil {
				// An attempt is made to pretty-print an expression that was
				// not assigned a type yet. This should not happen, so we make
				// it clear in the output this needs to be investigated
				// further.
				buf.WriteString(fmt.Sprintf("??? %v", te))
			} else {
				buf.WriteString(rt.String())
			}
			buf.WriteByte(']')
			return
		}
	}
	formatNodeOrHideConstants(buf, f, n)
	if f.disambiguateDatumTypes {
		var typ Type
		if d, isDatum := n.(Datum); isDatum {
			if d.AmbiguousFormat() {
				typ = d.ResolvedType()
			}
		} else if p, isPlaceholder := n.(*Placeholder); isPlaceholder {
			typ = p.typ
		}
		if typ != nil {
			buf.WriteString(":::")
			colType, err := DatumTypeToColumnType(typ)
			if err != nil {
				panic(err)
			}
			FormatNode(buf, f, colType)
		}
	}
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

// Serialize pretty prints a node to a string using FmtParsable; it is
// appropriate when we store expressions into strings that are later parsed back
// into expressions.
func Serialize(n NodeFormatter) string {
	return AsStringWithFlags(n, FmtParsable)
}
