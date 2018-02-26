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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// FmtFlags carries options for the pretty-printer.
type FmtFlags int

// HasFlags tests whether the given flags are all set.
func (f FmtFlags) HasFlags(subset FmtFlags) bool {
	return f&subset == subset
}

// SetFlags sets the given formatting flags.
func (f *FmtFlags) SetFlags(subset FmtFlags) {
	*f |= subset
}

// EncodeFlags returns the subset of the flags that are also lex encode flags.
func (f FmtFlags) EncodeFlags() lex.EncodeFlags {
	return lex.EncodeFlags(f) & (lex.EncFirstFreeFlagBit - 1)
}

// Basic bit definitions for the FmtFlags bitmask.
const (
	// FmtSimple instructs the pretty-printer to produce
	// a straightforward representation.
	FmtSimple FmtFlags = 0

	// FmtShowPasswords instructs the pretty-printer to not suppress passwords.
	// If not set, passwords are replaced by *****.
	FmtShowPasswords FmtFlags = FmtFlags(lex.EncFirstFreeFlagBit) << iota

	// FmtShowTypes instructs the pretty-printer to
	// annotate expressions with their resolved types.
	FmtShowTypes

	// FmtHideConstants instructs the pretty-printer to produce a
	// representation that does not disclose query-specific data.
	FmtHideConstants

	// FmtAnonymize instructs the pretty-printer to remove
	// any name but function names.
	// TODO(knz): temporary until a better solution is found for #13968
	FmtAnonymize

	// FmtAlwaysQualifyTableNames instructs the pretty-printer to
	// qualify table names, even if originally omitted.
	FmtAlwaysQualifyTableNames

	// FmtAlwaysGroupExprs instructs the pretty-printer to enclose
	// sub-expressions between parentheses.
	// Used for testing.
	FmtAlwaysGroupExprs

	// FmtShowTableAliases reveals the table aliases.
	FmtShowTableAliases

	// FmtSymbolicSubqueries indicates that subqueries must be pretty-printed
	// using numeric notation (@S123).
	FmtSymbolicSubqueries

	// If set, strings will be formatted for being contents of ARRAYs.
	// Used internally in combination with FmtArrays defined below.
	fmtWithinArray

	// If set, datums and placeholders will have type annotations (like
	// :::interval) as necessary to disambiguate between possible type
	// resolutions.
	fmtDisambiguateDatumTypes

	// fmtSymbolicVars indicates that IndexedVars must be pretty-printed
	// using numeric notation (@123).
	fmtSymbolicVars
)

// Composite/derived flag definitions follow.
const (
	// FmtBareStrings instructs the pretty-printer to print strings without
	// wrapping quotes, if the string contains no special characters.
	FmtBareStrings FmtFlags = FmtFlags(lex.EncBareStrings)

	// FmtBareIdentifiers instructs the pretty-printer to print
	// identifiers without wrapping quotes in any case.
	FmtBareIdentifiers FmtFlags = FmtFlags(lex.EncBareIdentifiers)

	// FmtArrays instructs the pretty-printer to print strings without
	// wrapping quotes, if the string contains no special characters.
	FmtArrays FmtFlags = fmtWithinArray | FmtFlags(lex.EncBareStrings)

	// FmtParsable instructs the pretty-printer to produce a representation that
	// can be parsed into an equivalent expression (useful for serialization of
	// expressions).
	FmtParsable FmtFlags = fmtDisambiguateDatumTypes

	// FmtCheckEquivalence instructs the pretty-printer to produce a representation
	// that can be used to check equivalence of expressions. Specifically:
	//  - IndexedVars are formatted using symbolic notation (to disambiguate
	//    columns).
	//  - datum types are disambiguated with explicit type
	//    annotations. This is necessary because datums of different types
	//    can otherwise be formatted to the same string: (for example the
	//    DDecimal 1 and the DInt 1).
	FmtCheckEquivalence FmtFlags = fmtSymbolicVars | fmtDisambiguateDatumTypes
)

// FmtCtx is suitable for passing to Format() methods.
// It also exposes the underlying bytes.Buffer interface for
// convenience.
type FmtCtx struct {
	*bytes.Buffer
	// The flags to use for pretty-printing.
	flags FmtFlags
	// indexedVarFormat is an optional interceptor for
	// IndexedVarContainer.IndexedVarFormat calls; it can be used to
	// customize the formatting of IndexedVars.
	indexedVarFormat func(ctx *FmtCtx, idx int)
	// tableNameFormatter will be called on all NormalizableTableNames if it is
	// non-nil.
	tableNameFormatter func(*FmtCtx, *NormalizableTableName)
	// placeholderFormat is an optional interceptor for Placeholder.Format calls;
	// it can be used to format placeholders differently than normal.
	placeholderFormat func(ctx *FmtCtx, p *Placeholder)
}

// MakeFmtCtx creates a FmtCtx from an existing buffer and flags.
func MakeFmtCtx(buf *bytes.Buffer, f FmtFlags) FmtCtx {
	return FmtCtx{Buffer: buf, flags: f}
}

// WithReformatTableNames modifies FmtCtx to instructs the pretty-printer
// to substitute the printing of table names using the provided function.
func (ctx *FmtCtx) WithReformatTableNames(fn func(*FmtCtx, *NormalizableTableName)) *FmtCtx {
	ctx.tableNameFormatter = fn
	return ctx
}

// CopyWithFlags creates a new FmtCtx with different formatting flags
// to become those specified, but the same formatting target.
func (ctx *FmtCtx) CopyWithFlags(f FmtFlags) FmtCtx {
	ret := *ctx
	ret.flags = f
	return ret
}

// HasFlags returns true iff the given flags are set in the formatter context.
func (ctx *FmtCtx) HasFlags(f FmtFlags) bool {
	return ctx.flags.HasFlags(f)
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
	if showTypes {
		base |= FmtShowTypes
	}
	if symbolicVars {
		base |= fmtSymbolicVars
	}
	if showTableAliases {
		base |= FmtShowTableAliases
	}
	return base
}

// WithIndexedVarFormat modifies FmtCtx to customize the printing of
// IndexedVars using the provided function.
func (ctx *FmtCtx) WithIndexedVarFormat(fn func(ctx *FmtCtx, idx int)) *FmtCtx {
	ctx.indexedVarFormat = fn
	return ctx
}

// WithPlaceholderFormat modifies FmtCtx to customizes the printing of
// StarDatums using the provided function.
func (ctx *FmtCtx) WithPlaceholderFormat(placeholderFn func(_ *FmtCtx, _ *Placeholder)) *FmtCtx {
	ctx.placeholderFormat = placeholderFn
	return ctx
}

// NodeFormatter is implemented by nodes that can be pretty-printed.
type NodeFormatter interface {
	// Format performs pretty-printing towards a bytes buffer. The flags member
	// of ctx influences the results. Most callers should use FormatNode instead.
	Format(ctx *FmtCtx)
}

// FormatName formats a string as a name.
//
// Note: prefer FormatNameP below when the string is already on the
// heap.
func (ctx *FmtCtx) FormatName(s string) {
	ctx.FormatNode((*Name)(&s))
}

// FormatNameP formats a string reference as a name.
func (ctx *FmtCtx) FormatNameP(s *string) {
	ctx.FormatNode((*Name)(s))
}

// FormatNode recurses into a node for pretty-printing.
// Flag-driven special cases can hook into this.
func (ctx *FmtCtx) FormatNode(n NodeFormatter) {
	f := ctx.flags
	if f.HasFlags(FmtShowTypes) {
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
	if f.HasFlags(FmtAlwaysGroupExprs) {
		if _, ok := n.(Expr); ok {
			ctx.WriteByte('(')
		}
	}
	ctx.formatNodeOrHideConstants(n)
	if f.HasFlags(FmtAlwaysGroupExprs) {
		if _, ok := n.(Expr); ok {
			ctx.WriteByte(')')
		}
	}
	if f.HasFlags(fmtDisambiguateDatumTypes) {
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
			colType.Format(ctx.Buffer, f.EncodeFlags())
		}
	}
}

// AsStringWithFlags pretty prints a node to a string given specific flags.
func AsStringWithFlags(n NodeFormatter, fl FmtFlags) string {
	ctx := NewFmtCtxWithBuf(fl)
	ctx.FormatNode(n)
	return ctx.CloseAndGetString()
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

// FmtCtxWithBuf is a combination of FmtCtx and bytes.Buffer, meant
// for use in the following pattern:
//
// f := NewFmtCtxWithBuf(flags)
// f.FormatNode(...)
// f.WriteString(...)
// ... etc ...
// return f.CloseAndGetString()
//
// Users must either call Close() or CloseAndGetString().
//
// It implements the interface of FmtCtx, which in turn implements
// that of bytes.Buffer. Therefore, the String() method works and can
// be used multiple times. The CloseAndGetString() method is meant to
// combine Close() and String().
type FmtCtxWithBuf struct {
	FmtCtx
	buf bytes.Buffer
}

var fmtCtxWithBufPool = sync.Pool{
	New: func() interface{} {
		ctx := &FmtCtxWithBuf{}
		ctx.Buffer = &ctx.buf
		return ctx
	},
}

// NewFmtCtxWithBuf returns a FmtCtxWithBuf ready for use.
func NewFmtCtxWithBuf(f FmtFlags) *FmtCtxWithBuf {
	ctx := fmtCtxWithBufPool.Get().(*FmtCtxWithBuf)
	ctx.flags = f
	return ctx
}

// Close releases the FmtCtxWithBuf.
func (f *FmtCtxWithBuf) Close() {
	f.Reset()
	f.FmtCtx = FmtCtx{Buffer: &f.buf}
	fmtCtxWithBufPool.Put(f)
}

// CloseAndGetString combines Close() and String().
func (f *FmtCtxWithBuf) CloseAndGetString() string {
	s := f.buf.String()
	f.Close()
	return s
}

func (ctx *FmtCtx) alwaysFormatTablePrefix() bool {
	return ctx.flags.HasFlags(FmtAlwaysQualifyTableNames) || ctx.tableNameFormatter != nil
}
