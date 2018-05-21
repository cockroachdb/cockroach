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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type columnConversionKind int

// These columnConversionKinds are ordered from easiest to hardest.
// TODO: Should this be a type hierarchy to support conversion planning?
const (
	// columnConversionImpossible indicates that it is not possible to
	// effect the desired type conversion.  Additional information will
	// be present in the error returned from classifyConversion.
	columnConversionImpossible columnConversionKind = iota
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
func (k columnConversionKind) classifier() classifier {
	return func(_ *sqlbase.ColumnType, _ *sqlbase.ColumnType) columnConversionKind {
		return k
	}
}

var columnConversionKindNames = map[columnConversionKind]string{
	columnConversionGeneral:    "General",
	columnConversionImpossible: "Impossible",
	columnConversionNoOp:       "NoOp",
	columnConversionTrivial:    "Trivial",
	columnConversionValidate:   "Validate",
}

func (k columnConversionKind) String() string {
	if name, ok := columnConversionKindNames[k]; ok {
		return name
	}
	return fmt.Sprintf("unknown kind: %d", k)
}

// TODO: Once we support non-trivial conversions, perhaps this should
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
		// Floats are always encoded as 64-bit values on disk
		sqlbase.ColumnType_FLOAT: classifierHardestOf(classifierPrecision, classifierWidth),
	},
	sqlbase.ColumnType_INT: {
		sqlbase.ColumnType_INT: classifierWidth,
	},
	sqlbase.ColumnType_STRING: {
		// If we want to convert string -> bytes, we need to know that the
		// bytes type has an unlimited width or that we have at least
		// 4x the number of bytes as known-maximum characters.
		sqlbase.ColumnType_BYTES: func(string *sqlbase.ColumnType, bytes *sqlbase.ColumnType) columnConversionKind {
			switch {
			case bytes.Width == 0:
				return columnConversionTrivial
			case string.Width == 0:
				return columnConversionValidate
			case bytes.Width >= string.Width*4:
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
