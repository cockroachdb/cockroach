// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cast defines the semantically allowed casts and their information.
//
// Note that it does not provide the mechanism to perform the evaluation of
// these casts.
package cast

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// Context represents the contexts in which a cast can be performed. There
// are three types of cast contexts: explicit, assignment, and implicit. Not all
// casts can be performed in all contexts. See the description of each context
// below for more details.
//
// The concept of cast contexts is taken directly from Postgres's cast behavior.
// More information can be found in the Postgres documentation on type
// conversion: https://www.postgresql.org/docs/current/typeconv.html
type Context uint8

const (
	_ Context = iota
	// ContextExplicit is a cast performed explicitly with the syntax
	// CAST(x AS T) or x::T.
	ContextExplicit
	// ContextAssignment is a cast implicitly performed during an INSERT,
	// UPSERT, or UPDATE statement.
	ContextAssignment
	// ContextImplicit is a cast performed implicitly. For example, the DATE
	// below is implicitly cast to a TIMESTAMPTZ so that the values can be
	// compared.
	//
	//   SELECT '2021-01-10'::DATE < now()
	//
	ContextImplicit
)

// String returns the representation of Context as a string.
func (cc Context) String() string {
	switch cc {
	case ContextExplicit:
		return "explicit"
	case ContextAssignment:
		return "assignment"
	case ContextImplicit:
		return "implicit"
	default:
		return "invalid"
	}
}

// PGString returns the representation of Context as an abbreviated string.
func (cc Context) PGString() string {
	switch cc {
	case ContextExplicit:
		return "e"
	case ContextAssignment:
		return "a"
	case ContextImplicit:
		return "i"
	}
	return ""
}

// ContextOrigin indicates the source of information for a cast's maximum
// context (see cast.MaxContext below). It is only used to annotate entries in
// castMap and to perform assertions on cast entries in the init function. It
// has no effect on the behavior of a cast.
type ContextOrigin uint8

const (
	_ ContextOrigin = iota
	// ContextOriginPgCast specifies that a cast's maximum context is based on
	// information in Postgres's pg_cast table.
	ContextOriginPgCast
	// ContextOriginAutomaticIOConversion specifies that a cast's maximum
	// context is not included in Postgres's pg_cast table. In Postgres's
	// internals, these casts are evaluated by each data type's input and output
	// functions.
	//
	// Automatic casts can only convert to or from string types [1]. Conversions
	// to string types are assignment casts and conversions from string types
	// are explicit casts [2]. These rules are asserted in the init function.
	//
	// [1] https://www.postgresql.org/docs/13/catalog-pg-cast.html#CATALOG-PG-CAST
	// [2] https://www.postgresql.org/docs/13/sql-createcast.html#SQL-CREATECAST-NOTES
	ContextOriginAutomaticIOConversion
	// ContextOriginLegacyConversion is used for casts that are not supported by
	// Postgres, but are supported by CockroachDB and continue to be supported
	// for backwards compatibility.
	ContextOriginLegacyConversion
)

// Cast includes details about a cast from one OID to another.
//
// TODO(mgartner, otan): Move PerformCast logic to this struct.
type Cast struct {
	// MaxContext is the maximum context in which the cast is allowed. A cast
	// can only be performed in a context that is at or below the specified
	// maximum context.
	//
	// ContextExplicit casts can only be performed in an explicit context.
	//
	// ContextAssignment casts can be performed in an explicit context or in
	// an assignment context in an INSERT, UPSERT, or UPDATE statement.
	//
	// ContextImplicit casts can be performed in any context.
	MaxContext Context
	// origin is the source of truth for the cast's context. It is used to
	// annotate entries in castMap and to perform assertions on cast entries in
	// the init function. It has no effect on the behavior of a cast.
	origin ContextOrigin
	// Volatility indicates whether the result of the cast is dependent only on
	// the source value, or dependent on outside factors (such as parameter
	// variables or table contents).
	Volatility volatility.V
	// VolatilityHint is an optional string for volatility.Stable casts. When
	// set, it is used as an error hint suggesting a possible workaround when
	// stable casts are not allowed.
	VolatilityHint string
}

// ForEachCast calls fn for every valid cast from a source type to a target
// type. Iteration order is deterministic.
func ForEachCast(
	fn func(
		src oid.Oid, tgt oid.Oid, castCtx Context, ctxOrigin ContextOrigin, v volatility.V,
	),
) {
	srcOids := make([]oid.Oid, 0, len(castMap))
	for src := range castMap {
		srcOids = append(srcOids, src)
	}
	sort.Slice(srcOids, func(i, j int) bool {
		return srcOids[i] < srcOids[j]
	})
	var tgtOids []oid.Oid
	for _, src := range srcOids {
		tgts := castMap[src]
		tgtOids = tgtOids[:0]
		for tgt := range tgts {
			tgtOids = append(tgtOids, tgt)
		}
		sort.Slice(tgtOids, func(i, j int) bool {
			return tgtOids[i] < tgtOids[j]
		})
		for _, tgt := range tgtOids {
			cast := tgts[tgt]
			fn(src, tgt, cast.MaxContext, cast.origin, cast.Volatility)
		}
	}
}

// ValidCast returns true if a valid cast exists from src to tgt in the given
// context.
func ValidCast(src, tgt *types.T, ctx Context) bool {
	srcFamily := src.Family()
	tgtFamily := tgt.Family()

	// If src and tgt are array types, check for a valid cast between their
	// content types.
	if srcFamily == types.ArrayFamily && tgtFamily == types.ArrayFamily {
		return ValidCast(src.ArrayContents(), tgt.ArrayContents(), ctx)
	}

	// If src and tgt are tuple types, check for a valid cast between each
	// corresponding tuple element.
	//
	// Casts from a tuple type to AnyTuple are a no-op so they are always valid.
	// If tgt is AnyTuple, we continue to LookupCast below which contains a
	// special case for these casts.
	if srcFamily == types.TupleFamily && tgtFamily == types.TupleFamily &&
		!tgt.Identical(types.AnyTuple) {
		srcTypes := src.TupleContents()
		tgtTypes := tgt.TupleContents()
		// The tuple types must have the same number of elements.
		if len(srcTypes) != len(tgtTypes) {
			return false
		}
		for i := range srcTypes {
			if ok := ValidCast(srcTypes[i], tgtTypes[i], ctx); !ok {
				return false
			}
		}
		return true
	}

	// If src and tgt are not both array or tuple types, check castMap for a
	// valid cast.
	c, ok := LookupCast(src, tgt)
	if ok {
		return c.MaxContext >= ctx
	}

	return false
}

// OIDInCastMap checks to see if the cast is in the cast map. This bypasses
// a few false equivalences found in LookupCast.
// You are more likely using to use LookupCast.
func OIDInCastMap(src, tgt oid.Oid) bool {
	_, ok := castMap[src][tgt]
	return ok
}

// LookupCast returns a cast that describes the cast from src to tgt if it
// exists. If it does not exist, ok=false is returned.
func LookupCast(src, tgt *types.T) (Cast, bool) {
	srcFamily := src.Family()
	tgtFamily := tgt.Family()

	// Unknown is the type given to an expression that statically evaluates
	// to NULL. NULL can be immutably cast to any type in any context.
	if srcFamily == types.UnknownFamily {
		return Cast{
			MaxContext: ContextImplicit,
			Volatility: volatility.Immutable,
		}, true
	}

	// Enums have dynamic OIDs, so they can't be populated in castMap. Instead,
	// we dynamically create cast structs for valid enum casts.
	if srcFamily == types.EnumFamily && tgtFamily == types.StringFamily {
		// Casts from enum types to strings are immutable and allowed in
		// assignment contexts.
		return Cast{
			MaxContext: ContextAssignment,
			Volatility: volatility.Immutable,
		}, true
	}
	if tgtFamily == types.EnumFamily {
		switch srcFamily {
		case types.StringFamily:
			// Casts from string types to enums are immutable and allowed in
			// explicit contexts.
			return Cast{
				MaxContext: ContextExplicit,
				Volatility: volatility.Immutable,
			}, true
		case types.UnknownFamily:
			// Casts from unknown to enums are immutable and allowed in implicit
			// contexts.
			return Cast{
				MaxContext: ContextImplicit,
				Volatility: volatility.Immutable,
			}, true
		}
	}

	// Casts from array types to string types are stable and allowed in
	// assignment contexts.
	if srcFamily == types.ArrayFamily && tgtFamily == types.StringFamily {
		return Cast{
			MaxContext: ContextAssignment,
			Volatility: volatility.Stable,
		}, true
	}

	if srcFamily == types.ArrayFamily && tgtFamily == types.PGVectorFamily {
		return Cast{
			MaxContext: ContextAssignment,
			Volatility: volatility.Stable,
		}, true
	}
	if srcFamily == types.PGVectorFamily && tgtFamily == types.ArrayFamily &&
		tgt.ArrayContents().Family() == types.FloatFamily {
		// Note that postgres only allows casts to FLOAT4[], but given that
		// under the hood FLOAT8 and FLOAT4 represented exactly the same way in
		// CRDB we'll allow both.
		return Cast{
			MaxContext: ContextAssignment,
			Volatility: volatility.Stable,
		}, true
	}

	// Casts from array and tuple types to string types are immutable and
	// allowed in assignment contexts.
	// TODO(mgartner): Tuple to string casts should be stable. They are
	// immutable to avoid backward incompatibility with previous versions, but
	// this is incorrect and can causes corrupt indexes, corrupt tables, and
	// incorrect query results.
	if srcFamily == types.TupleFamily && tgtFamily == types.StringFamily {
		return Cast{
			MaxContext: ContextAssignment,
			Volatility: volatility.Immutable,
		}, true
	}

	// Casts from any tuple type to AnyTuple are no-ops, so they are implicit
	// and immutable.
	if srcFamily == types.TupleFamily && tgt.Identical(types.AnyTuple) {
		return Cast{
			MaxContext: ContextImplicit,
			Volatility: volatility.Immutable,
		}, true
	}

	// Casts from string types to array and tuple types are stable and allowed
	// in explicit contexts.
	if srcFamily == types.StringFamily &&
		(tgtFamily == types.ArrayFamily || tgtFamily == types.TupleFamily) {
		return Cast{
			MaxContext: ContextExplicit,
			Volatility: volatility.Stable,
		}, true
	}

	// Casts from int types to bit and varbit types are allowed only if the the
	// length of the bit or varbit is defined
	if srcFamily == types.IntFamily &&
		tgtFamily == types.BitFamily &&
		tgt.Width() != 0 {
		return Cast{
			MaxContext: ContextExplicit,
			Volatility: volatility.Immutable,
		}, true
	}

	if tgts, ok := castMap[src.Oid()]; ok {
		if c, ok := tgts[tgt.Oid()]; ok {
			return c, true
		}
	}

	// If src and tgt are the same type, the immutable cast is valid in any
	// context. This logic is intentionally after the lookup into castMap so
	// that entries in castMap are preferred.
	if src.Oid() == tgt.Oid() {
		return Cast{
			MaxContext: ContextImplicit,
			Volatility: volatility.Immutable,
		}, true
	}

	return Cast{}, false
}

// LookupCastVolatility returns the Volatility of a valid cast.
func LookupCastVolatility(from, to *types.T) (_ volatility.V, ok bool) {
	fromFamily := from.Family()
	toFamily := to.Family()
	// Special case for casting between arrays.
	if fromFamily == types.ArrayFamily && toFamily == types.ArrayFamily {
		return LookupCastVolatility(from.ArrayContents(), to.ArrayContents())
	}
	// Special case for casting between tuples.
	if fromFamily == types.TupleFamily && toFamily == types.TupleFamily {
		fromTypes := from.TupleContents()
		toTypes := to.TupleContents()
		// Handle case where an overload makes a tuple get casted to tuple{}.
		if len(toTypes) == 1 && toTypes[0].Family() == types.AnyFamily {
			return volatility.Stable, true
		}
		if len(fromTypes) != len(toTypes) {
			return 0, false
		}
		maxVolatility := volatility.Leakproof
		for i := range fromTypes {
			v, lookupOk := LookupCastVolatility(fromTypes[i], toTypes[i])
			if !lookupOk {
				return 0, false
			}
			if v > maxVolatility {
				maxVolatility = v
			}
		}
		return maxVolatility, true
	}

	cast, ok := LookupCast(from, to)
	if !ok {
		return 0, false
	}
	return cast.Volatility, true
}
