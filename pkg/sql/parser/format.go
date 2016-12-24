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
	// tableNameNormalizer will be called on all NormalizableTableNames if it is
	// non-nil. Its results will be used if they are non-nil, or ignored if they
	// are nil.
	tableNameNormalizer func(*NormalizableTableName) *TableName
	// indexedVarFormat is an optional interceptor for
	// IndexedVarContainer.IndexedVarFormat calls; it can be used to
	// customize the formatting of IndexedVars.
	indexedVarFormat func(buf *bytes.Buffer, f FmtFlags, c IndexedVarContainer, idx int)
	// starDatumFormat is an optional interceptor for StarDatum.Format calls,
	// can be used to customize the formatting of StarDatums.
	starDatumFormat func(buf *bytes.Buffer, f FmtFlags)
	// If true, strings will be rendered without wrapping quotes if possible.
	bareStrings bool
	// If true, constant values will not include type annotations like INTERVAL,
	// DATE; useful when formatting a single value and the type is known.
	noDatumTypes bool
}

// FmtFlags enables conditional formatting in the pretty-printer.
type FmtFlags *fmtFlags

// FmtSimple instructs the pretty-printer to produce
// a straightforward representation, ideally using SQL
// syntax that makes prettyprint+parse idempotent.
var FmtSimple FmtFlags = &fmtFlags{showTypes: false}

// FmtValue instructs the pretty-printer to produce
// a value representation, which omits type annotations like INTERVAL, DATE.
var FmtValue FmtFlags = &fmtFlags{noDatumTypes: true}

// FmtShowTypes instructs the pretty-printer to
// annotate expressions with their resolved types.
var FmtShowTypes FmtFlags = &fmtFlags{showTypes: true, noDatumTypes: true}

// FmtSymbolicVars instructs the pretty-printer to
// print indexedVars using symbolic notation, to
// disambiguate columns.
var FmtSymbolicVars FmtFlags = &fmtFlags{symbolicVars: true}

// FmtBareStrings instructs the pretty-printer to print strings without
// wrapping quotes, if possible.
var FmtBareStrings FmtFlags = &fmtFlags{bareStrings: true, noDatumTypes: true}

// FmtNormalizeTableNames returns FmtFlags that instructs the pretty-printer
// to normalize all table names using the provided function.
func FmtNormalizeTableNames(fn func(*NormalizableTableName) *TableName) FmtFlags {
	return &fmtFlags{tableNameNormalizer: fn}
}

// FmtExpr returns FmtFlags that indicate how the pretty-printer
// should format expressions.
func FmtExpr(showTypes, symbolicVars, showTableAliases bool) FmtFlags {
	return &fmtFlags{
		showTypes:        showTypes,
		noDatumTypes:     showTypes,
		symbolicVars:     symbolicVars,
		ShowTableAliases: showTableAliases,
	}
}

// FmtIndexedVarFormat returns FmtFlags that customizes the printing of
// IndexedVars using the provided function.
func FmtIndexedVarFormat(
	fn func(buf *bytes.Buffer, f FmtFlags, c IndexedVarContainer, idx int),
) FmtFlags {
	return &fmtFlags{indexedVarFormat: fn}
}

// FmtStarDatumFormat returns FmtFlags that customizes the printing of
// StarDatums using the provided function.
func FmtStarDatumFormat(fn func(buf *bytes.Buffer, f FmtFlags)) FmtFlags {
	return &fmtFlags{starDatumFormat: fn}
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
			n.Format(buf, f)
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
