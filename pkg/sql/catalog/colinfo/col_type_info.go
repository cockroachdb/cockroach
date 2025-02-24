// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colinfo

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"golang.org/x/text/language"
)

// ColTypeInfo is a type that allows multiple representations of column type
// information (to avoid conversions and allocations).
type ColTypeInfo struct {
	// Only one of these fields can be set.
	resCols  ResultColumns
	colTypes []*types.T
}

// ColTypeInfoFromResCols creates a ColTypeInfo from ResultColumns.
func ColTypeInfoFromResCols(resCols ResultColumns) ColTypeInfo {
	return ColTypeInfo{resCols: resCols}
}

// ColTypeInfoFromColTypes creates a ColTypeInfo from []ColumnType.
func ColTypeInfoFromColTypes(colTypes []*types.T) ColTypeInfo {
	return ColTypeInfo{colTypes: colTypes}
}

// ColTypeInfoFromColumns creates a ColTypeInfo from []catalog.Column.
func ColTypeInfoFromColumns(columns []catalog.Column) ColTypeInfo {
	colTypes := make([]*types.T, len(columns))
	for i, col := range columns {
		colTypes[i] = col.GetType()
	}
	return ColTypeInfoFromColTypes(colTypes)
}

// NumColumns returns the number of columns in the type.
func (ti ColTypeInfo) NumColumns() int {
	if ti.resCols != nil {
		return len(ti.resCols)
	}
	return len(ti.colTypes)
}

// Type returns the datum type of the i-th column.
func (ti ColTypeInfo) Type(idx int) *types.T {
	if ti.resCols != nil {
		return ti.resCols[idx].Typ
	}
	return ti.colTypes[idx]
}

// ValidateColumnDefType returns an error if the type of a column definition is
// not valid. It is checked when a column is created or altered.
func ValidateColumnDefType(ctx context.Context, st *cluster.Settings, t *types.T) error {
	switch t.Family() {
	case types.StringFamily, types.CollatedStringFamily:
		if t.Family() == types.CollatedStringFamily {
			if _, err := language.Parse(t.Locale()); err != nil {
				return pgerror.Newf(pgcode.Syntax, `invalid locale %s`, t.Locale())
			}
		}

	case types.DecimalFamily:
		switch {
		case t.Precision() == 0 && t.Scale() > 0:
			// TODO (seif): Find right range for error message.
			return errors.New("invalid NUMERIC precision 0")
		case t.Precision() < t.Scale():
			return fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				t.Scale(), t.Precision())
		}

	case types.ArrayFamily:
		if t.ArrayContents().Family() == types.ArrayFamily {
			// Nested arrays are not supported as a column type.
			return errors.Errorf("nested array unsupported as column type: %s", t.String())
		}
		if t.ArrayContents().Family() == types.JsonFamily {
			// JSON arrays are not supported as a column type.
			return unimplemented.NewWithIssueDetailf(23468, t.String(),
				"arrays of JSON unsupported as column type")
		}
		if err := types.CheckArrayElementType(t.ArrayContents()); err != nil {
			return err
		}
		return ValidateColumnDefType(ctx, st, t.ArrayContents())

	case types.BitFamily, types.IntFamily, types.FloatFamily, types.BoolFamily, types.BytesFamily, types.DateFamily,
		types.INetFamily, types.IntervalFamily, types.JsonFamily, types.OidFamily, types.TimeFamily,
		types.TimestampFamily, types.TimestampTZFamily, types.UuidFamily, types.TimeTZFamily,
		types.GeographyFamily, types.GeometryFamily, types.EnumFamily, types.Box2DFamily,
		types.TSQueryFamily, types.TSVectorFamily, types.PGLSNFamily, types.PGVectorFamily, types.RefCursorFamily:
	// These types are OK.

	case types.TupleFamily:
		if !t.UserDefined() {
			return pgerror.New(pgcode.InvalidTableDefinition, "cannot use anonymous record type as table column")
		}
		if t.TypeMeta.ImplicitRecordType {
			return unimplemented.NewWithIssue(70099, "cannot use table record type as table column")
		}

	default:
		return pgerror.Newf(pgcode.InvalidTableDefinition,
			"value type %s cannot be used for table columns", t.String())
	}

	return nil
}

// ColumnTypeIsIndexable returns whether the type t is valid as an indexed
// column in a regular FORWARD index.
func ColumnTypeIsIndexable(t *types.T) bool {
	// NB: .IsAmbiguous checks the content type of array types.
	if t.IsAmbiguous() {
		return false
	}

	switch t.Family() {
	case types.TupleFamily, types.RefCursorFamily, types.JsonpathFamily:
		return false
	}

	// If the type is an array, check its content type as well.
	if unwrapped := t.ArrayContents(); unwrapped != nil {
		switch unwrapped.Family() {
		case types.TupleFamily, types.RefCursorFamily, types.JsonpathFamily:
			return false
		}
	}

	// Some inverted index types also have a key encoding, but we don't
	// want to support those yet. See #50659.
	return !MustBeValueEncoded(t) && !ColumnTypeIsOnlyInvertedIndexable(t)
}

// ColumnTypeIsInvertedIndexable returns whether the type t is valid to be indexed
// using an inverted index.
func ColumnTypeIsInvertedIndexable(t *types.T) bool {
	switch t.Family() {
	case types.ArrayFamily:
		switch t.ArrayContents().Family() {
		case types.RefCursorFamily, types.JsonpathFamily:
			return false
		default:
			return true
		}
	case types.JsonFamily, types.StringFamily:
		return true
	}
	return ColumnTypeIsOnlyInvertedIndexable(t)
}

// ColumnTypeIsOnlyInvertedIndexable returns true if the type t is only
// indexable via an inverted index.
func ColumnTypeIsOnlyInvertedIndexable(t *types.T) bool {
	if t.IsAmbiguous() || t.Family() == types.TupleFamily {
		return false
	}
	if t.Family() == types.ArrayFamily {
		t = t.ArrayContents()
	}
	switch t.Family() {
	case types.GeographyFamily:
	case types.GeometryFamily:
	case types.TSVectorFamily:
	default:
		return false
	}
	return true
}

// ColumnTypeIsVectorIndexable returns true if the type t can be indexed using a
// vector index.
func ColumnTypeIsVectorIndexable(t *types.T) bool {
	return t.Family() == types.PGVectorFamily
}

// MustBeValueEncoded returns true if columns of the given kind can only be value
// encoded.
func MustBeValueEncoded(semanticType *types.T) bool {
	switch semanticType.Family() {
	case types.ArrayFamily:
		switch semanticType.Oid() {
		case oid.T_int2vector, oid.T_oidvector:
			return true
		default:
			return MustBeValueEncoded(semanticType.ArrayContents())
		}
	case types.TupleFamily, types.GeographyFamily, types.GeometryFamily:
		return true
	case types.TSVectorFamily, types.TSQueryFamily:
		return true
	case types.PGVectorFamily:
		return true
	}
	return false
}

// ValidateColumnForIndex checks that the given column type is allowed in the
// given index. If "isLastCol" is true, then it is the last column in the index.
// "colDesc" can either be a column name or an expression in the form (a + b).
func ValidateColumnForIndex(
	indexType idxtype.T, colDesc string, colType *types.T, isLastCol bool,
) error {
	// Inverted and vector indexes only allow certain types for the last column.
	if isLastCol {
		switch indexType {
		case idxtype.INVERTED:
			if !ColumnTypeIsInvertedIndexable(colType) {
				return sqlerrors.NewInvalidLastColumnError(colDesc, colType.Name(), indexType)
			}
			return nil

		case idxtype.VECTOR:
			if !ColumnTypeIsVectorIndexable(colType) {
				return sqlerrors.NewInvalidLastColumnError(colDesc, colType.Name(), indexType)
			} else if colType.Width() <= 0 {
				return errors.WithDetail(
					pgerror.Newf(
						pgcode.FeatureNotSupported,
						"%s column %s does not have a fixed number of dimensions, so it cannot be indexed",
						colType.Name(), colDesc,
					),
					"specify the number of dimensions in the type, like VECTOR(128) for 128 dimensions",
				)
			}
			return nil
		}
	}

	if !ColumnTypeIsIndexable(colType) {
		// If the column is indexable as the last column in the right kind of
		// index, then use a more descriptive error message.
		if ColumnTypeIsInvertedIndexable(colType) {
			if indexType == idxtype.INVERTED {
				// Column type is allowed, but only as the last column.
				return sqlerrors.NewColumnOnlyIndexableError(colDesc, colType.Name(), idxtype.INVERTED)
			}

			// Column type is allowed in an inverted index.
			return errors.WithHint(pgerror.Newf(
				pgcode.FeatureNotSupported,
				"column %s has type %s, which is not indexable in a non-inverted index",
				colDesc, colType),
				"you may want to create an inverted index instead. See the documentation for inverted indexes: "+docs.URL("inverted-indexes.html"))
		}

		if ColumnTypeIsVectorIndexable(colType) {
			if indexType == idxtype.VECTOR {
				// Column type is allowed, but only as the last column.
				return sqlerrors.NewColumnOnlyIndexableError(colDesc, colType.Name(), idxtype.VECTOR)
			}

			// Column type is allowed in a vector index.
			return errors.WithHint(pgerror.Newf(
				pgcode.FeatureNotSupported,
				"column %s has type %s, which is not indexable in a non-vector index",
				colDesc, colType),
				"you may want to create a vector index instead")
		}

		return sqlerrors.NewColumnNotIndexableError(colDesc, colType.Name(), colType.DebugString())
	}

	return nil
}
