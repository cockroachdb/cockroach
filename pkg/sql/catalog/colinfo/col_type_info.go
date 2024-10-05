// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colinfo

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
func ValidateColumnDefType(ctx context.Context, version clusterversion.Handle, t *types.T) error {
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
		return ValidateColumnDefType(ctx, version, t.ArrayContents())

	case types.BitFamily, types.IntFamily, types.FloatFamily, types.BoolFamily, types.BytesFamily, types.DateFamily,
		types.INetFamily, types.IntervalFamily, types.JsonFamily, types.OidFamily, types.TimeFamily,
		types.TimestampFamily, types.TimestampTZFamily, types.UuidFamily, types.TimeTZFamily,
		types.GeographyFamily, types.GeometryFamily, types.EnumFamily, types.Box2DFamily:
	// These types are OK.

	case types.TupleFamily:
		if !t.UserDefined() {
			return pgerror.New(pgcode.InvalidTableDefinition, "cannot use anonymous record type as table column")
		}
		if t.TypeMeta.ImplicitRecordType {
			return unimplemented.NewWithIssue(70099, "cannot use table record type as table column")
		}

	case types.TSQueryFamily, types.TSVectorFamily:
		if !version.IsActive(ctx, clusterversion.V23_1) {
			return pgerror.Newf(pgcode.FeatureNotSupported,
				"TSVector/TSQuery not supported until version 23.1")
		}

	case types.PGLSNFamily:
		if !version.IsActive(ctx, clusterversion.V23_2) {
			return pgerror.Newf(
				pgcode.FeatureNotSupported,
				"pg_lsn not supported until version 23.2",
			)
		}

	case types.RefCursorFamily:
		if !version.IsActive(ctx, clusterversion.V23_2) {
			return pgerror.Newf(
				pgcode.FeatureNotSupported,
				"refcursor not supported until version 23.2",
			)
		}

	default:
		return pgerror.Newf(pgcode.InvalidTableDefinition,
			"value type %s cannot be used for table columns", t.String())
	}

	return nil
}

// ColumnTypeIsIndexable returns whether the type t is valid as an indexed column.
func ColumnTypeIsIndexable(t *types.T) bool {
	// NB: .IsAmbiguous checks the content type of array types.
	if t.IsAmbiguous() || t.Family() == types.TupleFamily || t.Family() == types.RefCursorFamily {
		return false
	}

	// If the type is an array, check its content type as well.
	if unwrapped := t.ArrayContents(); unwrapped != nil {
		if unwrapped.Family() == types.TupleFamily || unwrapped.Family() == types.RefCursorFamily {
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
		return t.ArrayContents().Family() != types.RefCursorFamily
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
	}
	return false
}
