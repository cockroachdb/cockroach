// Copyright 2018 The Cockroach Authors.
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

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

//go:generate stringer -type=columnConversionKind -trimprefix columnConversion
type columnConversionKind int

// These columnConversionKinds are ordered from easiest to hardest.
// TODO(bob): Should this be a type hierarchy to support conversion planning?
const (
	// columnConversionDangerous indicates that we could effect a type
	// conversion via built-in cast methods, but that the semantics are
	// not well-defined enough that a user would want to rely on this.
	// The use of this value should be accompanied by a link to an issue
	// to follow up.
	columnConversionDangerous columnConversionKind = iota - 1
	// columnConversionImpossible indicates that it is not possible to
	// effect the desired type conversion.  Additional information will
	// be present in the error returned from classifyConversion.
	columnConversionImpossible
	// columnConversionNoOp indicates that the requested change is
	// meaningless.
	columnConversionNoOp
	// columnConversionTrivial indicates that a conversion is
	// byte-for-byte compatible with existing data and there is no
	// possibility of introducing a constraint validation error.
	// This would include conversions such as STRING -> BYTES
	columnConversionTrivial
	// columnConversionValidate indicates that a conversion is
	// byte-for-byte compatible with existing data, but that we need
	// to validate the existing data against the new rule.
	// This would include BYTE -> STRING, where we need to validate
	// that all of the existing bytes are valid UTF-8 sequences.
	columnConversionValidate
	// columnConversionGeneral indicates that we will end up rewriting
	// the existing data into a new format.
	columnConversionGeneral
)

// classifier returns a classifier function that simply returns the
// target columnConversionKind.
func (i columnConversionKind) classifier() classifier {
	return func(_ *sqlbase.ColumnType, _ *sqlbase.ColumnType) columnConversionKind {
		return i
	}
}

// TODO(bob): Once we support non-trivial conversions, perhaps this should
// also construct the conversion plan?
type classifier func(oldType *sqlbase.ColumnType, newType *sqlbase.ColumnType) columnConversionKind

// classifiers contains the logic for looking up conversions which
// don't require a fully-generalized approach.
var classifiers = map[sqlbase.ColumnType_SemanticType]map[sqlbase.ColumnType_SemanticType]classifier{
	sqlbase.ColumnType_BYTES: {
		sqlbase.ColumnType_BYTES:  classifierWidth,
		sqlbase.ColumnType_STRING: columnConversionValidate.classifier(),
		sqlbase.ColumnType_UUID:   columnConversionValidate.classifier(),
	},
	sqlbase.ColumnType_DECIMAL: {
		// Decimals are always encoded as an apd.Decimal
		sqlbase.ColumnType_DECIMAL: classifierHardestOf(classifierPrecision, classifierWidth),
	},
	sqlbase.ColumnType_FLOAT: {
		// Floats are always encoded as 64-bit values on disk and we don't
		// actually care about scale or precision.
		sqlbase.ColumnType_FLOAT: columnConversionTrivial.classifier(),
	},
	sqlbase.ColumnType_INT: {
		// Right now, our behavior with respect to handling BIT(n) doesn't
		// match pgsql behavior.  Converting between INT(n) and BIT(n),
		// especially regarding negative values, is somewhat dodgy.
		// The safest thing to do at the moment is to deny this change,
		// unless the user provides a USING expression.  We can revisit
		// this once the following issue is worked out:
		// https://github.com/cockroachdb/cockroach/issues/20991
		sqlbase.ColumnType_INT: func(from *sqlbase.ColumnType, to *sqlbase.ColumnType) columnConversionKind {
			fromBit := from.VisibleType == sqlbase.ColumnType_BIT
			toBit := to.VisibleType == sqlbase.ColumnType_BIT

			if fromBit == toBit {
				return classifierWidth(from, to)
			}
			return columnConversionDangerous
		},
	},
	sqlbase.ColumnType_STRING: {
		// If we want to convert string -> bytes, we need to know that the
		// bytes type has an unlimited width or that we have at least
		// 4x the number of bytes as known-maximum characters.
		sqlbase.ColumnType_BYTES: func(s *sqlbase.ColumnType, b *sqlbase.ColumnType) columnConversionKind {
			switch {
			case b.Width == 0:
				return columnConversionTrivial
			case s.Width == 0:
				return columnConversionValidate
			case b.Width >= s.Width*4:
				return columnConversionTrivial
			default:
				return columnConversionValidate
			}
		},
		sqlbase.ColumnType_STRING: classifierWidth,
	},
	sqlbase.ColumnType_TIME: {
		sqlbase.ColumnType_TIMETZ: columnConversionTrivial.classifier(),
	},
	sqlbase.ColumnType_TIMETZ: {
		sqlbase.ColumnType_TIME: columnConversionTrivial.classifier(),
	},
	sqlbase.ColumnType_TIMESTAMP: {
		sqlbase.ColumnType_TIMESTAMPTZ: columnConversionTrivial.classifier(),
	},
	sqlbase.ColumnType_TIMESTAMPTZ: {
		sqlbase.ColumnType_TIMESTAMP: columnConversionTrivial.classifier(),
	},
}

// classifierHardestOf creates a composite classifier that returns the
// hardest kind of the enclosed classifiers.  If any of the
// classifiers report impossible, impossible will be returned.
func classifierHardestOf(classifiers ...classifier) classifier {
	return func(oldType *sqlbase.ColumnType, newType *sqlbase.ColumnType) columnConversionKind {
		ret := columnConversionNoOp

		for _, c := range classifiers {
			next := c(oldType, newType)
			switch {
			case next == columnConversionImpossible:
				return columnConversionImpossible
			case next > ret:
				ret = next
			}
		}

		return ret
	}
}

// classifierPrecision returns trivial only if the new type has a precision
// greater than the existing precision.  If they are the same, it returns
// no-op.  Otherwise, it returns validate.
func classifierPrecision(
	oldType *sqlbase.ColumnType, newType *sqlbase.ColumnType,
) columnConversionKind {
	switch {
	case oldType.Precision == newType.Precision:
		return columnConversionNoOp
	case oldType.Precision == 0:
		return columnConversionValidate
	case newType.Precision == 0 || newType.Precision > oldType.Precision:
		return columnConversionTrivial
	default:
		return columnConversionValidate
	}
}

// classifierWidth returns trivial only if the new type has a width
// greater than the existing width.  If they are the same, it returns
// no-op.  Otherwise, it returns validate.
func classifierWidth(
	oldType *sqlbase.ColumnType, newType *sqlbase.ColumnType,
) columnConversionKind {
	switch {
	case oldType.Width == newType.Width:
		return columnConversionNoOp
	case oldType.Width == 0:
		return columnConversionValidate
	case newType.Width == 0 || newType.Width > oldType.Width:
		return columnConversionTrivial
	default:
		return columnConversionValidate
	}
}

// classifyConversion takes two ColumnTypes and determines "how hard"
// the conversion is.
func classifyConversion(
	oldType *sqlbase.ColumnType, newType *sqlbase.ColumnType,
) (columnConversionKind, error) {
	if oldType.Equal(newType) {
		return columnConversionNoOp, nil
	}

	// Use custom logic for classifying a conversion.
	if mid, ok := classifiers[oldType.SemanticType]; ok {
		if fn, ok := mid[newType.SemanticType]; ok {
			ret := fn(oldType, newType)
			if ret != columnConversionImpossible {
				return ret, nil
			}
		}
	}

	// See if there's existing cast logic.  If so, return general.
	ctx := tree.MakeSemaContext(false)

	// Use a placeholder just to sub in the original type.
	fromPlaceholder, err := (&tree.Placeholder{}).TypeCheck(&ctx, oldType.ToDatumType())
	if err != nil {
		return columnConversionImpossible, err
	}

	// Cook up a cast expression using the placeholder.
	if cast, err := tree.NewTypedCastExpr(fromPlaceholder, newType.ToDatumType()); err == nil {
		if _, err := cast.TypeCheck(&ctx, nil); err == nil {
			return columnConversionGeneral, nil
		}
	}

	return columnConversionImpossible,
		pgerror.NewErrorf(pgerror.CodeCannotCoerceError, "cannot convert %s to %s", oldType.SQLString(), newType.SQLString())
}
