// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colinfo

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
func ValidateColumnDefType(t *types.T) error {
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
		if err := types.CheckArrayElementType(t.ArrayContents()); err != nil {
			return err
		}
		return ValidateColumnDefType(t.ArrayContents())

	case types.BitFamily, types.IntFamily, types.FloatFamily, types.BoolFamily, types.BytesFamily, types.DateFamily,
		types.INetFamily, types.IntervalFamily, types.JsonFamily, types.OidFamily, types.TimeFamily,
		types.TimestampFamily, types.TimestampTZFamily, types.UuidFamily, types.TimeTZFamily,
		types.GeographyFamily, types.GeometryFamily, types.EnumFamily, types.Box2DFamily:
		// These types are OK.

	default:
		return pgerror.Newf(pgcode.InvalidTableDefinition,
			"value type %s cannot be used for table columns", t.String())
	}

	return nil
}

// ColumnTypeIsIndexable returns whether the type t is valid as an indexed column.
func ColumnTypeIsIndexable(t *types.T) bool {
	if t.IsAmbiguous() || t.Family() == types.TupleFamily {
		return false
	}
	// Some inverted index types also have a key encoding, but we don't
	// want to support those yet. See #50659.
	return !MustBeValueEncoded(t) && !ColumnTypeIsInvertedIndexable(t)
}

// ColumnTypeIsInvertedIndexable returns whether the type t is valid to be indexed
// using an inverted index.
func ColumnTypeIsInvertedIndexable(t *types.T) bool {
	if t.IsAmbiguous() || t.Family() == types.TupleFamily {
		return false
	}
	family := t.Family()
	return family == types.JsonFamily || family == types.ArrayFamily ||
		family == types.GeographyFamily || family == types.GeometryFamily
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
	case types.JsonFamily, types.TupleFamily, types.GeographyFamily, types.GeometryFamily:
		return true
	}
	return false
}

// GetColumnTypes populates the types of the columns with the given IDs into the
// outTypes slice, returning it. You must use the returned slice, as this
// function might allocate a new slice.
func GetColumnTypes(
	desc catalog.TableDescriptor, columnIDs []descpb.ColumnID, outTypes []*types.T,
) ([]*types.T, error) {
	if cap(outTypes) < len(columnIDs) {
		outTypes = make([]*types.T, len(columnIDs))
	} else {
		outTypes = outTypes[:len(columnIDs)]
	}
	for i, id := range columnIDs {
		col, err := desc.FindColumnWithID(id)
		if err != nil {
			return nil, err
		}
		if !col.Public() {
			return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
		}
		outTypes[i] = col.GetType()
	}
	return outTypes, nil
}

// GetColumnTypesFromColDescs populates the types of the columns with the given
// IDs into the outTypes slice, returning it. You must use the returned slice,
// as this function might allocate a new slice.
func GetColumnTypesFromColDescs(
	cols []catalog.Column, columnIDs []descpb.ColumnID, outTypes []*types.T,
) []*types.T {
	if cap(outTypes) < len(columnIDs) {
		outTypes = make([]*types.T, len(columnIDs))
	} else {
		outTypes = outTypes[:len(columnIDs)]
	}
	for i, id := range columnIDs {
		for j := range cols {
			if id == cols[j].GetID() {
				outTypes[i] = cols[j].GetType()
				break
			}
		}
	}
	return outTypes
}
