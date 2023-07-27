// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

//go:generate stringer -type=ColumnConversionKind -trimprefix ColumnConversion

// ColumnConversionKind represents "how hard" a semantic type conversion
// will be.
// TODO(bob): Should this be a type hierarchy to support conversion planning?
type ColumnConversionKind int

// These columnConversionKinds are ordered from easiest to hardest.
const (
	// ColumnConversionDangerous indicates that we could effect a type
	// conversion via built-in cast methods, but that the semantics are
	// not well-defined enough that a user would want to rely on this.
	// The use of this value should be accompanied by a link to an issue
	// to follow up.
	ColumnConversionDangerous ColumnConversionKind = iota - 1
	// ColumnConversionImpossible indicates that it is not possible to
	// effect the desired type conversion.  Additional information will
	// be present in the error returned from ClassifyConversion.
	ColumnConversionImpossible
	// ColumnConversionTrivial indicates that a conversion is
	// byte-for-byte compatible with existing data and there is no
	// possibility of introducing a constraint validation error.
	// This would include conversions such as STRING -> BYTES, or
	// a change in which only the visible type of a column is changing.
	ColumnConversionTrivial
	// ColumnConversionValidate indicates that a conversion is
	// byte-for-byte compatible with existing data, but that we need
	// to validate the existing data against the new rule.
	// This would include BYTE -> STRING, where we need to validate
	// that all of the existing bytes are valid UTF-8 sequences.
	ColumnConversionValidate
	// ColumnConversionGeneral indicates that we will end up rewriting
	// the existing data into a new format.
	ColumnConversionGeneral
)

// classifier returns a classifier function that simply returns the
// target ColumnConversionKind.
func (i ColumnConversionKind) classifier() classifier {
	return func(_ *types.T, _ *types.T) ColumnConversionKind {
		return i
	}
}

// TODO(bob): Once we support non-trivial conversions, perhaps this should
// also construct the conversion plan?
type classifier func(oldType *types.T, newType *types.T) ColumnConversionKind

// classifiers contains the logic for looking up conversions which
// don't require a fully-generalized approach.
var classifiers = map[types.Family]map[types.Family]classifier{
	types.BytesFamily: {
		types.BytesFamily:  classifierWidth,
		types.StringFamily: ColumnConversionValidate.classifier(),
		types.UuidFamily:   ColumnConversionValidate.classifier(),
	},
	types.DecimalFamily: {
		// Decimals are always encoded as an apd.Decimal
		types.DecimalFamily: classifierHardestOf(classifierDecimalPrecision, classifierWidth),
	},
	types.FloatFamily: {
		// Floats are always encoded as 64-bit values on disk and we don't
		// actually care about scale or precision.
		types.FloatFamily: ColumnConversionTrivial.classifier(),
	},
	types.IntFamily: {
		types.IntFamily: func(from *types.T, to *types.T) ColumnConversionKind {
			return classifierWidth(from, to)
		},
	},
	types.BitFamily: {
		types.BitFamily: func(from *types.T, to *types.T) ColumnConversionKind {
			return classifierWidth(from, to)
		},
	},
	types.StringFamily: {
		types.BytesFamily: func(s *types.T, b *types.T) ColumnConversionKind {
			return ColumnConversionValidate
		},
		types.StringFamily: classifierWidth,
	},
	types.TimestampFamily: {
		types.TimestampTZFamily: classifierTimePrecision,
		types.TimestampFamily:   classifierTimePrecision,
	},
	types.TimestampTZFamily: {
		types.TimestampFamily:   classifierTimePrecision,
		types.TimestampTZFamily: classifierTimePrecision,
	},
	types.TimeFamily: {
		types.TimeFamily: classifierTimePrecision,
	},
	types.TimeTZFamily: {
		types.TimeTZFamily: classifierTimePrecision,
	},
}

// classifierHardestOf creates a composite classifier that returns the
// hardest kind of the enclosed classifiers.  If any of the
// classifiers report impossible, impossible will be returned.
func classifierHardestOf(classifiers ...classifier) classifier {
	return func(oldType *types.T, newType *types.T) ColumnConversionKind {
		ret := ColumnConversionTrivial

		for _, c := range classifiers {
			next := c(oldType, newType)
			switch {
			case next == ColumnConversionImpossible:
				return ColumnConversionImpossible
			case next > ret:
				ret = next
			}
		}

		return ret
	}
}

// classifierTimePrecision returns trivial only if the new type has a precision
// greater than the existing precision.  If they are the same, it returns
// no-op.  Otherwise, it returns ColumnConversionOverwrite.
func classifierTimePrecision(oldType *types.T, newType *types.T) ColumnConversionKind {
	oldPrecision := oldType.Precision()
	newPrecision := newType.Precision()

	switch {
	case newPrecision >= oldPrecision:
		return ColumnConversionTrivial
	default:
		return ColumnConversionGeneral
	}
}

// classifierDecimalPrecision returns trivial only if the new type has a precision
// greater than the existing precision.  If they are the same, it returns
// no-op.  Otherwise, it returns validate.
func classifierDecimalPrecision(oldType *types.T, newType *types.T) ColumnConversionKind {
	oldPrecision := oldType.Precision()
	newPrecision := newType.Precision()

	switch {
	case oldPrecision == newPrecision:
		return ColumnConversionTrivial
	case oldPrecision == 0:
		return ColumnConversionValidate
	case newPrecision == 0 || newPrecision > oldPrecision:
		return ColumnConversionTrivial
	default:
		return ColumnConversionValidate
	}
}

// classifierWidth returns trivial only if the new type has a width
// greater than the existing width.  If they are the same, it returns
// no-op.  Otherwise, it returns validate.
func classifierWidth(oldType *types.T, newType *types.T) ColumnConversionKind {
	switch {
	case oldType.Width() == newType.Width():
		return ColumnConversionTrivial
	case oldType.Width() == 0 && newType.Width() < 64:
		return ColumnConversionValidate
	case newType.Width() == 0 || newType.Width() > oldType.Width():
		return ColumnConversionTrivial
	default:
		return ColumnConversionValidate
	}
}

// ClassifyConversion takes two ColumnTypes and determines "how hard"
// the conversion is.  Note that this function will return
// ColumnConversionTrivial if the two types are equal.
func ClassifyConversion(
	ctx context.Context, oldType *types.T, newType *types.T,
) (ColumnConversionKind, error) {
	if oldType.Identical(newType) {
		return ColumnConversionTrivial, nil
	}

	// Use custom logic for classifying a conversion.
	if mid, ok := classifiers[oldType.Family()]; ok {
		if fn, ok := mid[newType.Family()]; ok {
			ret := fn(oldType, newType)
			if ret != ColumnConversionImpossible {
				return ret, nil
			}
		}
	}

	// See if there's existing cast logic.  If so, return general.
	semaCtx := tree.MakeSemaContext()
	if err := semaCtx.Placeholders.Init(1 /* numPlaceholders */, nil /* typeHints */); err != nil {
		return ColumnConversionImpossible, err
	}

	// Use a placeholder just to sub in the original type.
	fromPlaceholder, err := (&tree.Placeholder{Idx: 0}).TypeCheck(ctx, &semaCtx, oldType)
	if err != nil {
		return ColumnConversionImpossible, err
	}

	// Cook up a cast expression using the placeholder.
	cast := tree.NewTypedCastExpr(fromPlaceholder, newType)
	if _, err := cast.TypeCheck(ctx, &semaCtx, nil); err == nil {
		return ColumnConversionGeneral, nil
	}

	return ColumnConversionImpossible,
		pgerror.Newf(pgcode.CannotCoerce, "cannot convert %s to %s", oldType.SQLString(), newType.SQLString())
}
