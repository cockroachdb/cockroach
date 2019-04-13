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

package schemachange

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
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
var classifiers = map[types.SemanticType]map[types.SemanticType]classifier{
	types.BYTES: {
		types.BYTES:  classifierWidth,
		types.STRING: ColumnConversionValidate.classifier(),
		types.UUID:   ColumnConversionValidate.classifier(),
	},
	types.DECIMAL: {
		// Decimals are always encoded as an apd.Decimal
		types.DECIMAL: classifierHardestOf(classifierPrecision, classifierWidth),
	},
	types.FLOAT: {
		// Floats are always encoded as 64-bit values on disk and we don't
		// actually care about scale or precision.
		types.FLOAT: ColumnConversionTrivial.classifier(),
	},
	types.INT: {
		types.INT: func(from *types.T, to *types.T) ColumnConversionKind {
			return classifierWidth(from, to)
		},
	},
	types.BIT: {
		types.BIT: func(from *types.T, to *types.T) ColumnConversionKind {
			return classifierWidth(from, to)
		},
	},
	types.STRING: {
		// If we want to convert string -> bytes, we need to know that the
		// bytes type has an unlimited width or that we have at least
		// 4x the number of bytes as known-maximum characters.
		types.BYTES: func(s *types.T, b *types.T) ColumnConversionKind {
			switch {
			case b.Width() == 0:
				return ColumnConversionTrivial
			case s.Width() == 0:
				return ColumnConversionValidate
			case b.Width() >= s.Width()*4:
				return ColumnConversionTrivial
			default:
				return ColumnConversionValidate
			}
		},
		types.STRING: classifierWidth,
	},
	types.TIMESTAMP: {
		types.TIMESTAMPTZ: ColumnConversionTrivial.classifier(),
	},
	types.TIMESTAMPTZ: {
		types.TIMESTAMP: ColumnConversionTrivial.classifier(),
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

// classifierPrecision returns trivial only if the new type has a precision
// greater than the existing precision.  If they are the same, it returns
// no-op.  Otherwise, it returns validate.
func classifierPrecision(oldType *types.T, newType *types.T) ColumnConversionKind {
	switch {
	case oldType.Precision() == newType.Precision():
		return ColumnConversionTrivial
	case oldType.Precision() == 0:
		return ColumnConversionValidate
	case newType.Precision() == 0 || newType.Precision() > oldType.Precision():
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
func ClassifyConversion(oldType *types.T, newType *types.T) (ColumnConversionKind, error) {
	if oldType.Identical(newType) {
		return ColumnConversionTrivial, nil
	}

	// Use custom logic for classifying a conversion.
	if mid, ok := classifiers[oldType.SemanticType()]; ok {
		if fn, ok := mid[newType.SemanticType()]; ok {
			ret := fn(oldType, newType)
			if ret != ColumnConversionImpossible {
				return ret, nil
			}
		}
	}

	// See if there's existing cast logic.  If so, return general.
	ctx := tree.MakeSemaContext()
	if err := ctx.Placeholders.Init(1 /* numPlaceholders */, nil /* typeHints */); err != nil {
		return ColumnConversionImpossible, err
	}

	// Use a placeholder just to sub in the original type.
	fromPlaceholder, err := (&tree.Placeholder{Idx: 0}).TypeCheck(&ctx, oldType)
	if err != nil {
		return ColumnConversionImpossible, err
	}

	// Cook up a cast expression using the placeholder.
	if cast, err := tree.NewTypedCastExpr(fromPlaceholder, newType); err == nil {
		if _, err := cast.TypeCheck(&ctx, nil); err == nil {
			return ColumnConversionGeneral, nil
		}
	}

	return ColumnConversionImpossible,
		pgerror.NewErrorf(pgerror.CodeCannotCoerceError, "cannot convert %s to %s", oldType.SQLString(), newType.SQLString())
}
