// Copyright 2020 The Cockroach Authors.
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
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type TypeCategory string

var (
	typTypeBase      = TypeCategory("b")
	typTypeComposite = TypeCategory("c")
	typTypeDomain    = TypeCategory("d")
	typTypeEnum      = TypeCategory("e")
	typTypePseudo    = TypeCategory("p")
	typTypeRange     = TypeCategory("r")

	// Avoid unused warning for constants.
	_ = typTypeComposite
	_ = typTypeDomain
	_ = typTypeEnum
	_ = typTypePseudo
	_ = typTypeRange

	// See https://www.postgresql.org/docs/9.6/static/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE.
	typCategoryArray       = TypeCategory("A")
	typCategoryBoolean     = TypeCategory("B")
	typCategoryComposite   = TypeCategory("C")
	typCategoryDateTime    = TypeCategory("D")
	typCategoryEnum        = TypeCategory("E")
	typCategoryGeometric   = TypeCategory("G")
	typCategoryNetworkAddr = TypeCategory("I")
	typCategoryNumeric     = TypeCategory("N")
	typCategoryPseudo      = TypeCategory("P")
	typCategoryRange       = TypeCategory("R")
	typCategoryString      = TypeCategory("S")
	typCategoryTimespan    = TypeCategory("T")
	typCategoryUserDefined = TypeCategory("U")
	typCategoryBitString   = TypeCategory("V")
	typCategoryUnknown     = TypeCategory("X")

	// Avoid unused warning for constants.
	_ = typCategoryComposite
	_ = typCategoryEnum
	_ = typCategoryGeometric
	_ = typCategoryRange
)

// TODO: normalize with sql/pg_catalog.go.
var familyToTypeCategory = map[types.Family]TypeCategory{
	types.AnyFamily:            typCategoryPseudo,
	types.BitFamily:            typCategoryBitString,
	types.BoolFamily:           typCategoryBoolean,
	types.BytesFamily:          typCategoryUserDefined,
	types.DateFamily:           typCategoryDateTime,
	types.TimeFamily:           typCategoryDateTime,
	types.TimeTZFamily:         typCategoryDateTime,
	types.FloatFamily:          typCategoryNumeric,
	types.IntFamily:            typCategoryNumeric,
	types.IntervalFamily:       typCategoryTimespan,
	types.JsonFamily:           typCategoryUserDefined,
	types.DecimalFamily:        typCategoryNumeric,
	types.StringFamily:         typCategoryString,
	types.CollatedStringFamily: typCategoryString,
	types.TimestampFamily:      typCategoryDateTime,
	types.TimestampTZFamily:    typCategoryDateTime,
	types.ArrayFamily:          typCategoryArray,
	types.TupleFamily:          typCategoryPseudo,
	types.OidFamily:            typCategoryNumeric,
	types.UuidFamily:           typCategoryUserDefined,
	types.INetFamily:           typCategoryNetworkAddr,
	types.UnknownFamily:        typCategoryUnknown,
}

var typeCategoryToPreferredOID = map[TypeCategory]oid.Oid{
	typCategoryBoolean: oid.T_oid,
	//	typCategoryOid:  // TODO
	// TODO category for inet
	typCategoryString:    oid.T_text,
	typCategoryNumeric:   oid.T_float8,
	typCategoryDateTime:  oid.T_timestamptz,
	typCategoryTimespan:  oid.T_interval,
	typCategoryBitString: oid.T_varbit,
}

func isPreferredOidForCategory(o oid.Oid, cat TypeCategory) bool {
	ret, ok := typeCategoryToPreferredOID[cat]
	return !ok || ret == o
}

func pgFilterAttempt(
	ctx *SemaContext, s *typeCheckOverloadState, attempt func() error,
) (ok bool, _ int, _ error) {
	before := s.overloadIdxs
	err := attempt()
	if err != nil {
		return true, 0, err
	}
	if len(s.overloadIdxs) == 1 {
		return true, int(s.overloadIdxs[0]), nil
	}
	s.overloadIdxs = before
	return false, 0, nil
}

func pgFilterOverloads(
	overloads []overloadImpl, overloadIdxs []uint8, fn func(int, overloadImpl) (bool, error),
) ([]uint8, error) {
	newOverloads := make([]uint8, 0, len(overloadIdxs))
	for i := 0; i < len(overloadIdxs); i++ {
		include, err := fn(int(overloadIdxs[i]), overloads[overloadIdxs[i]])
		if err != nil {
			return nil, err
		}
		if include {
			newOverloads = append(newOverloads, overloadIdxs[i])
		}
	}
	return newOverloads, nil
}

func pgUseOverload(
	ctx *SemaContext, overloads []overloadImpl, exprs ...Expr,
) ([]TypedExpr, []overloadImpl, error) {
	fns, err := pgFindOverload(ctx, overloads, exprs...)
	if err != nil {
		return nil, nil, err
	}

	typedExprs := make([]TypedExpr, len(exprs))
	curType := types.Unknown
	allSameType := true
	for i, expr := range exprs {
		switch {
		case ctx.isUnresolvedPlaceholder(exprs[i]):
			typedExprs[i] = StripParens(exprs[i]).(*Placeholder)
		default:
			var err error
			typedExprs[i], err = expr.TypeCheck(ctx, types.Any)
			if err != nil {
				return nil, nil, err
			}
			if !isConstant(exprs[i]) {
				if curType == types.Unknown {
					curType = typedExprs[i].ResolvedType()
				} else if typedExprs[i].ResolvedType().Oid() != curType.Oid() {
					allSameType = false
				}
			}
		}
	}
	if len(fns) != 1 {
		return typedExprs, fns, err
	}
	// Try apply requisite implicit casts.
	for i, typ := range fns[0].params().Types() {
		if isConstant(exprs[i]) || ctx.isUnresolvedPlaceholder(exprs[i]) {
			typedExprs[i], err = exprs[i].TypeCheck(ctx, typ)
			if err != nil {
				if allSameType {
					typedExprs[i], err = exprs[i].TypeCheck(ctx, curType)
					if err != nil {
						return nil, nil, err
					}
					if curType.Oid() != typ.Oid() {
						typedExprs[i], err = NewTypedCastExpr(typedExprs[i], typ)
						if err != nil {
							return nil, nil, err
						}
					}
					continue
				}
				// Everything should be the same type.
				// TODO: upcast if necessary.
				return nil, nil, err
			}
		} else {
			if typedExprs[i].ResolvedType().Oid() != typ.Oid() && typedExprs[i].ResolvedType().Oid() != oid.T_unknown {
				typedExprs[i], err = NewTypedCastExpr(typedExprs[i], typ)
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}

	return typedExprs, fns, err
}

// TODO(otan): Since we're using oid == oid, I expect this to be broken
// when comparing float4/float8 and int2/int4/int8.
func pgFindOverload(
	ctx *SemaContext, overloads []overloadImpl, exprs ...Expr,
) ([]overloadImpl, error) {
	// Cockroach only: don't allow if there are too many overloads.
	// NOTE(otan): why?!
	if len(overloads) > math.MaxUint8 {
		return nil, errors.AssertionFailedf("too many overloads (%d > 255)", len(overloads))
	}

	// Initialize some state such that when things get returned,
	// we have some stat information filled in.
	var s typeCheckOverloadState

	// Cockroach only: if no overloads are provided, just type check parameters and return.
	// NOTE(otan): why?!
	if len(overloads) == 0 {
		return nil, nil
	}

	// If preferred overloads are available, only consider those.
	// This should go away as preferred overloads are only there
	// because of a lack of implicit casts.
	// NOTE(otan): this can go away if we get rid of preferred overloads.
	preferredOverloads := []overloadImpl{}
	for _, overload := range overloads {
		if overload.preferred() {
			preferredOverloads = append(preferredOverloads, overload)
		}
	}
	if len(preferredOverloads) > 0 {
		overloads = preferredOverloads
	}

	s.overloads = overloads
	s.overloadIdxs = make([]uint8, len(overloads))
	for i := 0; i < len(overloads); i++ {
		s.overloadIdxs[i] = uint8(i)
	}

	// Cockroach only: filter out incorrect parameter length overloads.
	// This only happens (slightly) later in postgres.
	// This simplifies all checks that come later.
	var err error
	if s.overloadIdxs, err = pgFilterOverloads(
		s.overloads,
		s.overloadIdxs,
		func(idx int, o overloadImpl) (bool, error) {
			return o.params().MatchLen(len(exprs)), nil
		},
	); err != nil {
		return nil, err
	}

	// Check if there is an exact match for the datatypes.
	// There should only ever be one of these.
	// In postgres, this only gets done for functions. But should work either way.
	if ok, idx, err := pgFilterAttempt(ctx, &s, func() error {
		var err error
		s.overloadIdxs, err = pgFilterOverloads(
			s.overloads,
			s.overloadIdxs,
			func(idx int, o overloadImpl) (bool, error) {
				found := true
				for i, t := range o.params().Types() {
					if isConstant(exprs[i]) || ctx.isUnresolvedPlaceholder(exprs[i]) {
						return false, err
					}
					typedExpr, err := exprs[i].TypeCheck(ctx, types.Any)
					if err != nil {
						return false, err
					}
					if typedExpr.ResolvedType().Oid() != t.Oid() {
						return false, err
					}
				}
				return found, nil
			},
		)
		return err
	}); err != nil {
		return nil, err
	} else if ok {
		return s.overloads[idx : idx+1], nil
	}

	// Functions only:
	// UNSUPPORTED: postgres can infer <type>(obj) to another type.
	// To combat this, we should probably just add function builtins
	// for each type we do support, which performs a PerformCast.

	// In postgres, we swap over live to `func_match_argtypes`.
	// This goes over each argument, and checks whether we can either
	// make it exactly match, or implicit cast it to match.
	if s.overloadIdxs, err = pgFilterOverloads(
		s.overloads,
		s.overloadIdxs,
		func(idx int, o overloadImpl) (bool, error) {
			for i, typ := range o.params().Types() {
				switch {
				case isConstant(exprs[i]), ctx.isUnresolvedPlaceholder(exprs[i]):
				default:
					// Try to implicitly cast the item if we can (or simply cast it if it's coa constant).
					// If we do, set typedExprs.
					typedExpr, err := exprs[i].TypeCheck(ctx, types.Any)
					if err != nil {
						return false, err
					}

					// NULL should always stay NULL.
					if typedExpr.ResolvedType().Oid() == oid.T_unknown {
						continue
					}
					// Same oid can move on.
					if typedExpr.ResolvedType().Oid() == typ.Oid() {
						continue
					}

					// Otherwise, attempt an implicit cast.
					if _, ok := FindCast(
						typedExpr.ResolvedType().Oid(),
						typ.Oid(),
						CastContextImplicit,
					); !ok {
						// If none can be cast, we should not consider this entry.
						return false, nil
					}
				}
			}
			return true, nil
		},
	); err != nil {
		return nil, err
	}
	if len(s.overloadIdxs) == 0 {
		return nil, nil
	}
	if len(s.overloadIdxs) == 1 {
		return s.overloads[s.overloadIdxs[0] : s.overloadIdxs[0]+1], nil
	}

	// Now we have multiple candidates we can use.
	// Use a heuristic to find the best one ("func_select_candidate").

	// Cockroach difference: I (don't think) we have domains, so ignore the
	// domain get base type step.

	// Postgres: choose highest exact same matches.
	// We count how many with exact matches there are, and choose the one
	// with the highest exact matches.
	numUnknown := 0
	{
		highestExactMatches := 0
		newOverloadIdxs := s.overloadIdxs[:0]
		for _, idx := range s.overloadIdxs {
			exactMatches := 0
			for i := range exprs {
				switch {
				case isConstant(exprs[i]), ctx.isUnresolvedPlaceholder(exprs[i]):
					numUnknown += 1
				default:
					typedExpr, err := exprs[i].TypeCheck(ctx, types.Any)
					if err != nil {
						return nil, err
					}
					if typedExpr.ResolvedType().Oid() == s.overloads[idx].params().GetAt(i).Oid() {
						exactMatches++
					}
				}
			}
			if exactMatches >= highestExactMatches {
				if exactMatches > highestExactMatches {
					newOverloadIdxs = s.overloadIdxs[:0]
				}
				highestExactMatches = exactMatches
				newOverloadIdxs = append(newOverloadIdxs, idx)
			}
		}
		s.overloadIdxs = newOverloadIdxs
		if len(s.overloadIdxs) == 1 {
			return s.overloads[s.overloadIdxs[0] : s.overloadIdxs[0]+1], nil
		}
	}

	// Postgres: choose the "preferred" category for each type.
	// Constants are annoying because they have a "natural" type, but
	// in postgres they remain an "unknown" type.
	{
		highestExactOrPreferredMatches := 0
		newOverloadIdxs := s.overloadIdxs[:0]
		for _, idx := range s.overloadIdxs {
			exactOrPreferredMatches := 0
			for i := range exprs {
				switch {
				case isConstant(exprs[i]), ctx.isUnresolvedPlaceholder(exprs[i]):
				default:
					typedExpr, err := exprs[i].TypeCheck(ctx, types.Any)
					if err != nil {
						return nil, err
					}
					exprOid := typedExpr.ResolvedType().Oid()
					paramT := s.overloads[idx].params().GetAt(i)
					paramOid := paramT.Oid()
					if exprOid != oid.T_unknown {
						if exprOid == paramOid ||
							isPreferredOidForCategory(exprOid, familyToTypeCategory[paramT.Family()]) {
							exactOrPreferredMatches++
						}
					}
				}
			}
			if exactOrPreferredMatches >= highestExactOrPreferredMatches {
				if exactOrPreferredMatches > highestExactOrPreferredMatches {
					newOverloadIdxs = s.overloadIdxs[:0]
				}
				highestExactOrPreferredMatches = exactOrPreferredMatches
				newOverloadIdxs = append(newOverloadIdxs, idx)
			}
		}
		s.overloadIdxs = newOverloadIdxs
		if len(s.overloadIdxs) == 1 {
			return s.overloads[s.overloadIdxs[0] : s.overloadIdxs[0]+1], nil
		}
	}

	// Postgres: everything below assumes there is at least one unknown input.
	if numUnknown > 0 {
		if ok, idx, err := pgFilterAttempt(ctx, &s, func() error {
			slotHasPreferredType := make([]bool, len(exprs))
			slotCategories := make([]TypeCategory, len(exprs))

			// Favour the same type.
			// If there are multiple, choose the one with the "preferred" category.
			unknownsResolved := true
			for i := range exprs {
				// Ignore unknowns.
				if !isConstant(exprs[i]) && !ctx.isUnresolvedPlaceholder(exprs[i]) {
					continue
				}
				haveConflict := false
				slotCategories[i] = TypeCategory("")

				for _, idx := range s.overloadIdxs {
					paramT := s.overloads[idx].params().GetAt(i)
					preferredCategory := familyToTypeCategory[paramT.Family()]
					isPreferredType := isPreferredOidForCategory(paramT.Oid(), preferredCategory)
					if slotCategories[i] == "" {
						slotCategories[i] = preferredCategory
						slotHasPreferredType[i] = isPreferredOidForCategory(paramT.Oid(), preferredCategory)
					} else if slotCategories[i] == preferredCategory {
						slotHasPreferredType[i] = slotHasPreferredType[i] || isPreferredType
					} else {
						if preferredCategory == typCategoryString {
							// STRING always wins
							slotCategories[i] = preferredCategory
							slotHasPreferredType[i] = isPreferredType
						} else {
							haveConflict = true
						}
					}
				}

				if haveConflict && slotCategories[i] != typCategoryString {
					unknownsResolved = false
					break
				}
			}

			if unknownsResolved {
				s.overloadIdxs, err = pgFilterOverloads(
					s.overloads,
					s.overloadIdxs,
					func(idx int, o overloadImpl) (bool, error) {
						for i := range exprs {
							if !isConstant(exprs[i]) && !ctx.isUnresolvedPlaceholder(exprs[i]) {
								continue
							}
							paramT := o.params().GetAt(i)
							preferredCategory := familyToTypeCategory[paramT.Family()]
							isPreferredType := isPreferredOidForCategory(paramT.Oid(), preferredCategory)
							if preferredCategory != slotCategories[i] {
								return false, nil
							}
							if slotHasPreferredType[i] && !isPreferredType {
								return false, nil
							}
						}
						return true, nil
					},
				)
				if err != nil {
					return err
				}
				if len(s.overloadIdxs) == 1 {
					return nil
				}
			}

			// Postgres!
			// Last gasp: if there are both known- and unknown-type inputs, and all
			// the known types are the same, assume the unknown inputs are also that
			// type, and see if that gives us a unique match.  If so, use that match.
			if numUnknown < len(exprs) {
				allSameType := true
				curType := types.Unknown
				for i := range exprs {
					// Ignore unknowns.
					if isConstant(exprs[i]) || ctx.isUnresolvedPlaceholder(exprs[i]) {
						continue
					}
					typedExpr, err := exprs[i].TypeCheck(ctx, types.Any)
					if err != nil {
						return err
					}
					nextTyp := typedExpr.ResolvedType()

					if curType == types.Unknown {
						curType = nextTyp
					} else if curType.Oid() != nextTyp.Oid() {
						allSameType = false
						break
					}
				}

				if allSameType {
					s.overloadIdxs, err = pgFilterOverloads(
						s.overloads,
						s.overloadIdxs,
						func(idx int, o overloadImpl) (bool, error) {
							for i := range exprs {
								// Take only unknowns.
								if !isConstant(exprs[i]) && !ctx.isUnresolvedPlaceholder(exprs[i]) {
									continue
								}
								paramT := o.params().GetAt(i)
								if paramT.Oid() == curType.Oid() {
									continue
								}
								if _, ok := FindCast(paramT.Oid(), curType.Oid(), CastContextImplicit); !ok {
									return false, nil
								}
							}
							return true, nil
						},
					)
				}
			}
			return nil
		}); err != nil {
			return nil, err
		} else if ok {
			return s.overloads[idx : idx+1], nil
		}
	}

	ret := make([]overloadImpl, len(s.overloadIdxs))
	for i, idx := range s.overloadIdxs {
		ret[i] = s.overloads[idx]
	}
	return ret, nil
}
