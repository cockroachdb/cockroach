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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type fmtFlags struct {
	showTypes        bool
	ShowTableAliases bool
	symbolicVars     bool
	hideConstants    bool
	// tableNameFormatter will be called on all NormalizableTableNames if it is
	// non-nil.
	tableNameFormatter func(*FmtCtx, *NormalizableTableName)
	// indexedVarFormat is an optional interceptor for
	// IndexedVarContainer.IndexedVarFormat calls; it can be used to
	// customize the formatting of IndexedVars.
	indexedVarFormat func(ctx *FmtCtx, idx int)
	// placeholderFormat is an optional interceptor for Placeholder.Format calls;
	// it can be used to format placeholders differently than normal.
	placeholderFormat func(ctx *FmtCtx, p *Placeholder)
	// If true, non-function names are replaced by underscores.
	anonymize bool
	// If true, strings will be formatted for being contents of ARRAYs.
	withinArray bool
	// If true, datums and placeholders will have type annotations (like
	// :::interval) as necessary to disambiguate between possible type
	// resolutions.
	disambiguateDatumTypes bool
	// If false, passwords are replaced by *****.
	showPasswords bool
	// If true, always prints qualified names even if originally omitted.
	alwaysQualify bool
	// If true, grouping parentheses are always shown. Used for testing.
	alwaysParens bool
	// Flags that control the formatting of strings and identifiers.
	encodeFlags lex.EncodeFlags
}

// FmtCtx is suitable for passing to Format() methods.
// It also exposes the underlying bytes.Buffer interface for
// convenience.
type FmtCtx struct {
	*bytes.Buffer
	flags FmtFlags
}

// MakeFmtCtx creates a FmtCtx from an existing buffer and flags.
func MakeFmtCtx(buf *bytes.Buffer, f FmtFlags) FmtCtx {
	return FmtCtx{Buffer: buf, flags: f}
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

// FmtBareStrings instructs the pretty-printer to print strings without
// wrapping quotes, if the string contains no special characters.
var FmtBareStrings FmtFlags = &fmtFlags{encodeFlags: lex.EncodeFlags{BareStrings: true}}

// FmtArrays instructs the pretty-printer to print strings without
// wrapping quotes, if the string contains no special characters.
var FmtArrays FmtFlags = &fmtFlags{withinArray: true, encodeFlags: lex.EncodeFlags{BareStrings: true}}

// FmtBareIdentifiers instructs the pretty-printer to print
// identifiers without wrapping quotes in any case.
var FmtBareIdentifiers FmtFlags = &fmtFlags{encodeFlags: lex.EncodeFlags{BareIdentifiers: true}}

// FmtParsable instructs the pretty-printer to produce a representation that
// can be parsed into an equivalent expression (useful for serialization of
// expressions).
var FmtParsable FmtFlags = &fmtFlags{disambiguateDatumTypes: true}

// FmtCheckEquivalence instructs the pretty-printer to produce a representation
// that can be used to check equivalence of expressions. Specifically:
//  - IndexedVars are formatted using symbolic notation (to disambiguate
//    columns).
//  - datum types are disambiguated with explicit type
//    annotations. This is necessary because datums of different types
//    can otherwise be formatted to the same string: (for example the
//    DDecimal 1 and the DInt 1).
var FmtCheckEquivalence FmtFlags = &fmtFlags{symbolicVars: true, disambiguateDatumTypes: true}

// FmtHideConstants instructs the pretty-printer to produce a
// representation that does not disclose query-specific data.
var FmtHideConstants FmtFlags = &fmtFlags{hideConstants: true}

// FmtAnonymize instructs the pretty-printer to remove
// any name but function names.
// TODO(knz): temporary until a better solution is found for #13968
var FmtAnonymize FmtFlags = &fmtFlags{anonymize: true}

// FmtSimpleQualified instructs the pretty-printer to produce
// a straightforward representation that qualifies table names.
var FmtSimpleQualified FmtFlags = &fmtFlags{alwaysQualify: true}

// FmtAlwaysGroupExprs instructs the pretty-printer to enclose
// sub-expressions between parentheses.
var FmtAlwaysGroupExprs FmtFlags = &fmtFlags{alwaysParens: true}

// FmtReformatTableNames returns FmtFlags that instructs the pretty-printer
// to substitute the printing of table names using the provided function.
func FmtReformatTableNames(
	base FmtFlags, fn func(*FmtCtx, *NormalizableTableName),
) FmtFlags {
	f := *base
	f.tableNameFormatter = fn
	return &f
}

// StripTypeFormatting removes the flag that extracts types from the format flags,
// so as to enable rendering expressions for which types have not been computed yet.
func (ctx *FmtCtx) StripTypeFormatting() {
	nf := *ctx.flags
	nf.showTypes = false
	ctx.flags = &nf
}

// CopyWithFlags creates a new FmtCtx with different formatting flags
// to become those specified, but the same formatting target.
func (ctx *FmtCtx) CopyWithFlags(f FmtFlags) FmtCtx {
	ret := *ctx
	ret.flags = f
	return ret
}

// ShowTableAliases returns true iff the flags have the corresponding bit set.
func (ctx *FmtCtx) ShowTableAliases() bool {
	return ctx.flags.ShowTableAliases
}

// Printf calls fmt.Fprintf on the linked bytes.Buffer. It is provided
// for convenience, to avoid having to call fmt.Fprintf(ctx.Buffer, ...).
//
// Note: DO NOT USE THIS TO INTERPOLATE %s ON NodeFormatter OBJECTS.
// This would call the String() method on them and would fail to reuse
// the same bytes buffer (and waste allocations). Instead use
// ctx.FormatNode().
func (ctx *FmtCtx) Printf(f string, args ...interface{}) {
	fmt.Fprintf(ctx.Buffer, f, args...)
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
func FmtIndexedVarFormat(base FmtFlags, fn func(ctx *FmtCtx, idx int)) FmtFlags {
	f := *base
	f.indexedVarFormat = fn
	return &f
}

// FmtPlaceholderFormat returns FmtFlags that customizes the printing of
// StarDatums using the provided function.
func FmtPlaceholderFormat(
	base FmtFlags, placeholderFn func(_ *FmtCtx, _ *Placeholder),
) FmtFlags {
	f := *base
	f.placeholderFormat = placeholderFn
	return &f
}

// NodeFormatter is implemented by nodes that can be pretty-printed.
type NodeFormatter interface {
	// Format performs pretty-printing towards a bytes buffer. The flags member
	// of ctx influences the results. Most callers should use FormatNode instead.
	Format(ctx *FmtCtx)
}

// FormatNode recurses into a node for pretty-printing.
// Flag-driven special cases can hook into this.
func (ctx *FmtCtx) FormatNode(n NodeFormatter) {
	f := ctx.flags
	if f.showTypes {
		if te, ok := n.(TypedExpr); ok {
			ctx.WriteByte('(')
			ctx.formatNodeOrHideConstants(n)
			ctx.WriteString(")[")
			if rt := te.ResolvedType(); rt == nil {
				// An attempt is made to pretty-print an expression that was
				// not assigned a type yet. This should not happen, so we make
				// it clear in the output this needs to be investigated
				// further.
				fmt.Fprintf(ctx.Buffer, "??? %v", te)
			} else {
				ctx.WriteString(rt.String())
			}
			ctx.WriteByte(']')
			return
		}
	}
	if f.alwaysParens {
		if _, ok := n.(Expr); ok {
			ctx.WriteByte('(')
		}
	}
	ctx.formatNodeOrHideConstants(n)
	if f.alwaysParens {
		if _, ok := n.(Expr); ok {
			ctx.WriteByte(')')
		}
	}
	if f.disambiguateDatumTypes {
		var typ types.T
		if d, isDatum := n.(Datum); isDatum {
			if p, isPlaceholder := d.(*Placeholder); isPlaceholder {
				// p.typ will be nil if the placeholder has not been type-checked yet.
				typ = p.typ
			} else if d.AmbiguousFormat() {
				typ = d.ResolvedType()
			}
		}
		if typ != nil {
			ctx.WriteString(":::")
			colType, err := coltypes.DatumTypeToColumnType(typ)
			if err != nil {
				panic(err)
			}
			colType.Format(ctx.Buffer, f.encodeFlags)
		}
	}
}

// AsStringWithFlags pretty prints a node to a string given specific flags.
func AsStringWithFlags(n NodeFormatter, f FmtFlags) string {
	var buf bytes.Buffer
	ctx := FmtCtx{Buffer: &buf, flags: f}
	ctx.FormatNode(n)
	return buf.String()
}

// AsString pretty prints a node to a string.
func AsString(n NodeFormatter) string {
	return AsStringWithFlags(n, FmtSimple)
}

// ErrString pretty prints a node to a string. Identifiers are not quoted.
func ErrString(n NodeFormatter) string {
	return AsStringWithFlags(n, FmtBareIdentifiers)
}

// Serialize pretty prints a node to a string using FmtParsable; it is
// appropriate when we store expressions into strings that are later parsed back
// into expressions.
func Serialize(n NodeFormatter) string {
	return AsStringWithFlags(n, FmtParsable)
}
