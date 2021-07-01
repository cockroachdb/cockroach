// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// FmtFlags carries options for the pretty-printer.
type FmtFlags int

// HasFlags tests whether the given flags are all set.
func (f FmtFlags) HasFlags(subset FmtFlags) bool {
	return f&subset == subset
}

// HasAnyFlags tests whether any of the given flags are all set.
func (f FmtFlags) HasAnyFlags(subset FmtFlags) bool {
	return f&subset != 0
}

// EncodeFlags returns the subset of the flags that are also lex encode flags.
func (f FmtFlags) EncodeFlags() lexbase.EncodeFlags {
	return lexbase.EncodeFlags(f) & (lexbase.EncFirstFreeFlagBit - 1)
}

// Basic bit definitions for the FmtFlags bitmask.
const (
	// FmtSimple instructs the pretty-printer to produce
	// a straightforward representation.
	FmtSimple FmtFlags = 0

	// FmtBareStrings instructs the pretty-printer to print strings and
	// other values without wrapping quotes. If the value is a SQL
	// string, the quotes will only be omitted if the string contains no
	// special characters. If it does contain special characters, the
	// string will be escaped and enclosed in e'...' regardless of
	// whether FmtBareStrings is specified. See FmtRawStrings below for
	// an alternative.
	FmtBareStrings FmtFlags = FmtFlags(lexbase.EncBareStrings)

	// FmtBareIdentifiers instructs the pretty-printer to print
	// identifiers without wrapping quotes in any case.
	FmtBareIdentifiers FmtFlags = FmtFlags(lexbase.EncBareIdentifiers)

	// FmtShowPasswords instructs the pretty-printer to not suppress passwords.
	// If not set, passwords are replaced by *****.
	FmtShowPasswords FmtFlags = FmtFlags(lexbase.EncFirstFreeFlagBit) << iota

	// FmtShowTypes instructs the pretty-printer to
	// annotate expressions with their resolved types.
	FmtShowTypes

	// FmtHideConstants instructs the pretty-printer to produce a
	// representation that does not disclose query-specific data. It
	// also shorten long lists in tuples, VALUES and array expressions.
	FmtHideConstants

	// FmtAnonymize instructs the pretty-printer to remove
	// any name but function names.
	// TODO(knz): temporary until a better solution is found for #13968
	FmtAnonymize

	// FmtAlwaysQualifyTableNames instructs the pretty-printer to
	// qualify table names, even if originally omitted.
	// Requires Annotations in the formatting context.
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

	// If set, strings will be formatted using the postgres datum-to-text
	// conversion. See comments in pgwire_encode.go.
	// Used internally in combination with FmtPgwireText defined below.
	fmtPgwireFormat

	// If set, datums and placeholders will have type annotations (like
	// :::interval) as necessary to disambiguate between possible type
	// resolutions.
	fmtDisambiguateDatumTypes

	// fmtSymbolicVars indicates that IndexedVars must be pretty-printed
	// using numeric notation (@123).
	fmtSymbolicVars

	// fmtUnicodeStrings prints strings and JSON using the Go string
	// formatter. This is used e.g. for emitting values to CSV files.
	fmtRawStrings

	// FmtParsableNumerics produces decimal and float representations
	// that are always parsable, even if they require a string
	// representation like -Inf. Negative values are preserved "inside"
	// the numeric by enclosing them within parentheses.
	FmtParsableNumerics

	// FmtPGCatalog is used to produce expressions formatted in a way that's as
	// close as possible to what clients expect to live in pg_catalog (e.g.
	// pg_attrdef.adbin, pg_constraint.condef and pg_indexes.indexdef columns).
	// Specifically, this strips type annotations (Postgres doesn't know what
	// those are), adds cast expressions for non-numeric constants, and formats
	// indexes in Postgres-specific syntax.
	FmtPGCatalog

	// If set, user defined types and datums of user defined types will be
	// formatted in a way that is stable across changes to the underlying type.
	// For type names, this means that they will be formatted as '@id'. For enum
	// members, this means that they will be serialized as their bytes physical
	// representations.
	fmtStaticallyFormatUserDefinedTypes

	// fmtFormatByteLiterals instructs bytes to be formatted as byte literals
	// rather than string literals. For example, the bytes \x40ab will be formatted
	// as x'40ab' rather than '\x40ab'.
	fmtFormatByteLiterals
)

// PasswordSubstitution is the string that replaces
// passwords unless FmtShowPasswords is specified.
const PasswordSubstitution = "'*****'"

// Composite/derived flag definitions follow.
const (
	// FmtPgwireText instructs the pretty-printer to use
	// a pg-compatible conversion to strings. See comments
	// in pgwire_encode.go.
	FmtPgwireText FmtFlags = fmtPgwireFormat | FmtFlags(lexbase.EncBareStrings)

	// FmtParsable instructs the pretty-printer to produce a representation that
	// can be parsed into an equivalent expression. If there is a chance that the
	// formatted data will be stored durably on disk or sent to other nodes,
	// then this formatting directive is not appropriate, and FmtSerializable
	// should be used instead.
	FmtParsable FmtFlags = fmtDisambiguateDatumTypes | FmtParsableNumerics

	// FmtSerializable instructs the pretty-printer to produce a representation
	// for expressions that can be serialized to disk. It serializes user defined
	// types using representations that are stable across changes of the type
	// itself. This should be used when serializing expressions that will be
	// stored on disk, like DEFAULT expressions of columns.
	FmtSerializable FmtFlags = FmtParsable | fmtStaticallyFormatUserDefinedTypes

	// FmtCheckEquivalence instructs the pretty-printer to produce a representation
	// that can be used to check equivalence of expressions. Specifically:
	//  - IndexedVars are formatted using symbolic notation (to disambiguate
	//    columns).
	//  - datum types are disambiguated with explicit type
	//    annotations. This is necessary because datums of different types
	//    can otherwise be formatted to the same string: (for example the
	//    DDecimal 1 and the DInt 1).
	//  - user defined types and datums of user defined types are formatted
	//    using static representations to avoid name resolution and invalidation
	//    due to changes in the underlying type.
	FmtCheckEquivalence FmtFlags = fmtSymbolicVars |
		fmtDisambiguateDatumTypes |
		FmtParsableNumerics |
		fmtStaticallyFormatUserDefinedTypes

	// FmtArrayToString is a special composite flag suitable
	// for the output of array_to_string(). This de-quotes
	// the strings enclosed in the array and skips the normal escaping
	// of strings. Special characters are hex-escaped.
	FmtArrayToString FmtFlags = FmtBareStrings | fmtRawStrings

	// FmtExport, if set, formats datums in a raw form suitable for
	// EXPORT, e.g. suitable for output into a CSV file. The intended
	// goal for this flag is to ensure values can be read back using the
	// ParseDatumStringAs() / ParseStringas() functions (IMPORT).
	//
	// We do not use FmtParsable for this purpose because FmtParsable
	// intends to preserve all the information useful to CockroachDB
	// internally, at the expense of readability by 3rd party tools.
	//
	// We also separate this set of flag from fmtArrayToString
	// because the behavior of array_to_string() is fixed for compatibility
	// with PostgreSQL, whereas EXPORT may evolve over time to support
	// other things (eg. fixing #33429).
	//
	// TODO(mjibson): Note that this is currently not suitable for
	// emitting arrays or tuples. See: #33429
	FmtExport FmtFlags = FmtBareStrings | fmtRawStrings
)

const flagsRequiringAnnotations FmtFlags = FmtAlwaysQualifyTableNames

// FmtCtx is suitable for passing to Format() methods.
// It also exposes the underlying bytes.Buffer interface for
// convenience.
//
// FmtCtx cannot be copied by value.
type FmtCtx struct {
	_ util.NoCopy

	bytes.Buffer

	dataConversionConfig sessiondatapb.DataConversionConfig

	// NOTE: if you add more flags to this structure, make sure to add
	// corresponding cleanup code in FmtCtx.Close().

	// The flags to use for pretty-printing.
	flags FmtFlags
	// AST Annotations (used by some flags). Can be unset if those flags are not
	// used.
	ann *Annotations
	// indexedVarFormat is an optional interceptor for
	// IndexedVarContainer.IndexedVarFormat calls; it can be used to
	// customize the formatting of IndexedVars.
	indexedVarFormat func(ctx *FmtCtx, idx int)
	// tableNameFormatter will be called on all TableNames if it is non-nil.
	tableNameFormatter func(*FmtCtx, *TableName)
	// placeholderFormat is an optional interceptor for Placeholder.Format calls;
	// it can be used to format placeholders differently than normal.
	placeholderFormat func(ctx *FmtCtx, p *Placeholder)
	// indexedTypeFormatter is an optional interceptor for formatting
	// IDTypeReferences differently than normal.
	indexedTypeFormatter func(*FmtCtx, *OIDTypeReference)
}

// FmtCtxOption is an option to pass into NewFmtCtx.
type FmtCtxOption func(*FmtCtx)

// FmtAnnotations adds annotations to the FmtCtx.
func FmtAnnotations(ann *Annotations) FmtCtxOption {
	return func(ctx *FmtCtx) {
		ctx.ann = ann
	}
}

// FmtIndexedVarFormat modifies FmtCtx to customize the printing of
// IndexedVars using the provided function.
func FmtIndexedVarFormat(fn func(ctx *FmtCtx, idx int)) FmtCtxOption {
	return func(ctx *FmtCtx) {
		ctx.indexedVarFormat = fn
	}
}

// FmtPlaceholderFormat modifies FmtCtx to customize the printing of
// StarDatums using the provided function.
func FmtPlaceholderFormat(placeholderFn func(_ *FmtCtx, _ *Placeholder)) FmtCtxOption {
	return func(ctx *FmtCtx) {
		ctx.placeholderFormat = placeholderFn
	}
}

// FmtReformatTableNames modifies FmtCtx to to substitute the printing of table
// naFmtParsable using the provided function.
func FmtReformatTableNames(tableNameFmt func(*FmtCtx, *TableName)) FmtCtxOption {
	return func(ctx *FmtCtx) {
		ctx.tableNameFormatter = tableNameFmt
	}
}

// FmtIndexedTypeFormat modifies FmtCtx to customize the printing of
// IDTypeReferences using the provided function.
func FmtIndexedTypeFormat(fn func(*FmtCtx, *OIDTypeReference)) FmtCtxOption {
	return func(ctx *FmtCtx) {
		ctx.indexedTypeFormatter = fn
	}
}

// FmtDataConversionConfig modifies FmtCtx to contain items relevant for the
// given DataConversionConfig.
func FmtDataConversionConfig(dcc sessiondatapb.DataConversionConfig) FmtCtxOption {
	return func(ctx *FmtCtx) {
		ctx.dataConversionConfig = dcc
	}
}

// NewFmtCtx creates a FmtCtx; only flags that don't require Annotations
// can be used.
func NewFmtCtx(f FmtFlags, opts ...FmtCtxOption) *FmtCtx {
	ctx := fmtCtxPool.Get().(*FmtCtx)
	ctx.flags = f
	for _, opts := range opts {
		opts(ctx)
	}
	if ctx.ann == nil && f&flagsRequiringAnnotations != 0 {
		panic(errors.AssertionFailedf("no Annotations provided"))
	}
	return ctx
}

// WithDataConversionConfig modifies FmtCtx to substitute the DataConversionConfig,
// calls fn, then restore the original session data.
func (ctx *FmtCtx) WithDataConversionConfig(dcc sessiondatapb.DataConversionConfig, fn func()) {
	old := ctx.dataConversionConfig
	FmtDataConversionConfig(dcc)(ctx)
	defer func() { ctx.dataConversionConfig = old }()
	fn()
}

// WithReformatTableNames modifies FmtCtx to to substitute the printing of table
// names using the provided function, calls fn, then restores the original table
// formatting.
func (ctx *FmtCtx) WithReformatTableNames(tableNameFmt func(*FmtCtx, *TableName), fn func()) {
	old := ctx.tableNameFormatter
	ctx.tableNameFormatter = tableNameFmt
	defer func() { ctx.tableNameFormatter = old }()

	fn()
}

// WithFlags changes the flags in the FmtCtx, runs the given function, then
// restores the old flags.
func (ctx *FmtCtx) WithFlags(flags FmtFlags, fn func()) {
	if ctx.ann == nil && flags&flagsRequiringAnnotations != 0 {
		panic(errors.AssertionFailedf("no Annotations provided"))
	}
	oldFlags := ctx.flags
	ctx.flags = flags
	defer func() { ctx.flags = oldFlags }()

	fn()
}

// HasFlags returns true iff the given flags are set in the formatter context.
func (ctx *FmtCtx) HasFlags(f FmtFlags) bool {
	return ctx.flags.HasFlags(f)
}

// Printf calls fmt.Fprintf on the linked bytes.Buffer. It is provided
// for convenience, to avoid having to call fmt.Fprintf(&ctx.Buffer, ...).
//
// Note: DO NOT USE THIS TO INTERPOLATE %s ON NodeFormatter OBJECTS.
// This would call the String() method on them and would fail to reuse
// the same bytes buffer (and waste allocations). Instead use
// ctx.FormatNode().
func (ctx *FmtCtx) Printf(f string, args ...interface{}) {
	fmt.Fprintf(&ctx.Buffer, f, args...)
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

// FormatUsername formats a username safely.
func (ctx *FmtCtx) FormatUsername(s security.SQLUsername) {
	ctx.FormatName(s.Normalized())
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
				ctx.Printf("??? %v", te)
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
	if f.HasAnyFlags(fmtDisambiguateDatumTypes | FmtPGCatalog) {
		var typ *types.T
		if d, isDatum := n.(Datum); isDatum {
			if p, isPlaceholder := d.(*Placeholder); isPlaceholder {
				// p.typ will be nil if the placeholder has not been type-checked yet.
				typ = p.typ
			} else if d.AmbiguousFormat() {
				typ = d.ResolvedType()
			} else if _, isArray := d.(*DArray); isArray && f.HasFlags(FmtPGCatalog) {
				typ = d.ResolvedType()
			}
		}
		if typ != nil {
			if f.HasFlags(fmtDisambiguateDatumTypes) {
				ctx.WriteString(":::")
				ctx.FormatTypeReference(typ)
			} else if f.HasFlags(FmtPGCatalog) && !typ.IsNumeric() {
				ctx.WriteString("::")
				ctx.FormatTypeReference(typ)
			}
		}
	}
}

// AsStringWithFlags pretty prints a node to a string given specific flags; only
// flags that don't require Annotations can be used.
func AsStringWithFlags(n NodeFormatter, fl FmtFlags, opts ...FmtCtxOption) string {
	ctx := NewFmtCtx(fl, opts...)
	ctx.FormatNode(n)
	return ctx.CloseAndGetString()
}

// AsStringWithFQNames pretty prints a node to a string with the
// FmtAlwaysQualifyTableNames flag (which requires annotations).
func AsStringWithFQNames(n NodeFormatter, ann *Annotations) string {
	ctx := NewFmtCtx(FmtAlwaysQualifyTableNames, FmtAnnotations(ann))
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

// Serialize pretty prints a node to a string using FmtSerializable; it is
// appropriate when we store expressions into strings that are stored on disk
// and may be later parsed back into expressions.
func Serialize(n NodeFormatter) string {
	return AsStringWithFlags(n, FmtSerializable)
}

// SerializeForDisplay pretty prints a node to a string using FmtParsable.
// It is appropriate when printing expressions that are visible to end users.
func SerializeForDisplay(n NodeFormatter) string {
	return AsStringWithFlags(n, FmtParsable)
}

var fmtCtxPool = sync.Pool{
	New: func() interface{} {
		return &FmtCtx{}
	},
}

// Close releases a FmtCtx for reuse. Closing a FmtCtx is not required, but is
// recommended for performance-sensitive paths.
func (ctx *FmtCtx) Close() {
	ctx.Buffer.Reset()
	ctx.flags = 0
	ctx.ann = nil
	ctx.indexedVarFormat = nil
	ctx.tableNameFormatter = nil
	ctx.placeholderFormat = nil
	ctx.dataConversionConfig = sessiondatapb.DataConversionConfig{}
	fmtCtxPool.Put(ctx)
}

// CloseAndGetString combines Close() and String().
func (ctx *FmtCtx) CloseAndGetString() string {
	s := ctx.String()
	ctx.Close()
	return s
}

func (ctx *FmtCtx) alwaysFormatTablePrefix() bool {
	return ctx.flags.HasFlags(FmtAlwaysQualifyTableNames) || ctx.tableNameFormatter != nil
}
