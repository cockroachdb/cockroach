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

package sqlbase

import (
	"fmt"
	"unicode/utf8"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// This file provides facilities to support the correspondence
// between:
//
// - types.T, also called "datum types", for in-memory representations
//   via tree.Datum.
//
// - coltypes.T, also called "cast target types", which are specified
//   types in CREATE TABLE, ALTER TABLE and the CAST/:: syntax.
//
// - ColumnType, used in table descriptors.
//
// - the string representations thereof, for use in SHOW CREATE,
//   information_schema.columns and other introspection facilities.
//
// As a general rule of thumb, we are aiming for a 1-1 mapping between
// coltypes.T and ColumnType. Eventually we should even consider having
// just one implementation for both.
//
// Some notional complexity arises from the fact there are fewer
// different types.T than different coltypes.T/ColumnTypes. This is
// because some distinctions which are important at the time of data
// persistence (or casts) are not useful to keep for in-flight values;
// for example the final required precision for DECIMAL values.
//

// DatumTypeToColumnType converts a types.T (datum type) to a
// ColumnType.
//
// When working from a coltypes.T (i.e. a type in CREATE/ALTER TABLE)
// this must be used in combination with PopulateTypeAttrs() below.
// For example:
//
//	coltyp := <coltypes.T>
//	colDatumType := coltypes.CastTargetToDatumType(coltyp)
//	columnTyp, _ := DatumTypeToColumnType(colDatumType)
//	columnTyp, _ = PopulateTypeAttrs(columnTyp, coltyp)
//
func DatumTypeToColumnType(ptyp types.T) (ColumnType, error) {
	var ctyp ColumnType
	switch t := ptyp.(type) {
	case types.TCollatedString:
		ctyp.SemanticType = ColumnType_COLLATEDSTRING
		ctyp.Locale = &t.Locale
	case types.TArray:
		ctyp.SemanticType = ColumnType_ARRAY
		contents, err := datumTypeToColumnSemanticType(t.Typ)
		if err != nil {
			return ColumnType{}, err
		}
		ctyp.ArrayContents = &contents
		if t.Typ.FamilyEqual(types.FamCollatedString) {
			cs := t.Typ.(types.TCollatedString)
			ctyp.Locale = &cs.Locale
		}
	case types.TTuple:
		ctyp.SemanticType = ColumnType_TUPLE
		ctyp.TupleContents = make([]ColumnType, len(t.Types))
		for i, tc := range t.Types {
			var err error
			ctyp.TupleContents[i], err = DatumTypeToColumnType(tc)
			if err != nil {
				return ColumnType{}, err
			}
		}
		ctyp.TupleLabels = t.Labels
		return ctyp, nil
	default:
		semanticType, err := datumTypeToColumnSemanticType(ptyp)
		if err != nil {
			return ColumnType{}, err
		}
		ctyp.SemanticType = semanticType
	}
	return ctyp, nil
}

// PopulateTypeAttrs set other attributes of the ColumnType from a
// coltypes.T and performs type-specific verifications.
//
// This must be used on ColumnTypes produced from
// DatumTypeToColumnType if the origin of the type was a coltypes.T
// (e.g. via CastTargetToDatumType).
func PopulateTypeAttrs(base ColumnType, typ coltypes.T) (ColumnType, error) {
	switch t := typ.(type) {
	case *coltypes.TInt:
		base.Width = int32(t.Width)
		if val, present := aliasToVisibleTypeMap[t.Name]; present {
			base.VisibleType = val
		}

	case *coltypes.TFloat:
		base.VisibleType = ColumnType_NONE
		if t.Short {
			base.VisibleType = ColumnType_REAL
		}

	case *coltypes.TDecimal:
		base.Width = int32(t.Scale)
		base.Precision = int32(t.Prec)
		switch {
		case base.Precision == 0 && base.Width > 0:
			// TODO (seif): Find right range for error message.
			return ColumnType{}, errors.New("invalid NUMERIC precision 0")
		case base.Precision < base.Width:
			return ColumnType{}, fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				base.Width, base.Precision)
		}

	case *coltypes.TString:
		base.Width = int32(t.N)

	case *coltypes.TCollatedString:
		base.Width = int32(t.N)

	case *coltypes.TArray:
		base.ArrayDimensions = t.Bounds
		var err error
		base, err = PopulateTypeAttrs(base, t.ParamType)
		if err != nil {
			return ColumnType{}, err
		}

	case *coltypes.TVector:
		switch t.ParamType.(type) {
		case *coltypes.TInt, *coltypes.TOid:
		default:
			return ColumnType{}, errors.Errorf("vectors of type %s are unsupported", t.ParamType)
		}

	case *coltypes.TBool:
	case *coltypes.TBytes:
	case *coltypes.TDate:
	case *coltypes.TIPAddr:
	case *coltypes.TInterval:
	case *coltypes.TJSON:
	case *coltypes.TName:
	case *coltypes.TOid:
	case *coltypes.TTime:
	case *coltypes.TTimestamp:
	case *coltypes.TTimestampTZ:
	case *coltypes.TUUID:
	default:
		return ColumnType{}, errors.Errorf("unexpected type %T", t)
	}
	return base, nil
}

// aliasToVisibleTypeMap maps type aliases to ColumnType_VisibleType variants
// so that the alias is persisted. When adding new column type aliases or new
// VisibleType variants, consider adding to this mapping as well.
//
// This is used by PopulateTypeAttrs when propagating a coltypes.T
// specification to a ColumnType suitable in a descriptor.
var aliasToVisibleTypeMap = map[string]ColumnType_VisibleType{
	coltypes.Int2.Name:     ColumnType_SMALLINT,
	coltypes.Int4.Name:     ColumnType_INTEGER,
	coltypes.Int8.Name:     ColumnType_BIGINT,
	coltypes.Int64.Name:    ColumnType_BIGINT,
	coltypes.Integer.Name:  ColumnType_INTEGER,
	coltypes.SmallInt.Name: ColumnType_SMALLINT,
	coltypes.BigInt.Name:   ColumnType_BIGINT,
}

// SQLString returns the CockroachDB native SQL string that can be
// used to reproduce the ColumnType (via parsing -> coltypes.T ->
// CastTargetToColumnType -> PopulateAttrs).
//
// Is is used in error messages and also to produce the output
// of SHOW CREATE.
//
// TODO(knz): This is currently also used for
// information_schema.columns and produces invalid/incompatible values
// in that context. Either the standard names of the strings produced
// by SQLString() here must become those expected by users of
// information_schema.columns, or another function must be provided
// for the information schema instead.
func (c *ColumnType) SQLString() string {
	switch c.SemanticType {
	case ColumnType_STRING:
		if c.Width > 0 {
			return fmt.Sprintf("%s(%d)", c.SemanticType.String(), c.Width)
		}
	case ColumnType_FLOAT:
		const realName = "FLOAT4"
		const doubleName = "FLOAT8"

		switch c.VisibleType {
		case ColumnType_REAL:
			return realName
		default:
			// NONE now means double precision.
			// Pre-2.1 there were 3 cases:
			// - VisibleType = DOUBLE PRECISION, Width = 0 -> now clearly FLOAT8
			// - VisibleType = NONE, Width = 0 -> now clearly FLOAT8
			// - VisibleType = NONE, Width > 0 -> we need to derive the precision.
			if c.Precision >= 1 && c.Precision <= 24 {
				return realName
			}
			return doubleName
		}
	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			if c.Width > 0 {
				return fmt.Sprintf("%s(%d,%d)", c.SemanticType.String(), c.Precision, c.Width)
			}
			return fmt.Sprintf("%s(%d)", c.SemanticType.String(), c.Precision)
		}
	case ColumnType_TIMESTAMPTZ:
		return "TIMESTAMP WITH TIME ZONE"
	case ColumnType_COLLATEDSTRING:
		if c.Locale == nil {
			panic("locale is required for COLLATEDSTRING")
		}
		if c.Width > 0 {
			return fmt.Sprintf("%s(%d) COLLATE %s", ColumnType_STRING.String(), c.Width, *c.Locale)
		}
		return fmt.Sprintf("%s COLLATE %s", ColumnType_STRING.String(), *c.Locale)
	case ColumnType_ARRAY:
		return c.elementColumnType().SQLString() + "[]"
	}
	if c.VisibleType != ColumnType_NONE {
		return c.VisibleType.String()
	}
	return c.SemanticType.String()
}

// MaxCharacterLength returns the declared maximum length of
// characters if the ColumnType is a character or bit string data
// type. Returns false if the data type is not a character or bit
// string, or if the string's length is not bounded.
//
// This is used to populate information_schema.columns.character_maximum_length;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) MaxCharacterLength() (int32, bool) {
	switch c.SemanticType {
	case ColumnType_INT, ColumnType_STRING, ColumnType_COLLATEDSTRING:
		if c.Width > 0 {
			return c.Width, true
		}
	}
	return 0, false
}

// MaxOctetLength returns the maximum possible length in
// octets of a datum if the ColumnType is a character string. Returns
// false if the data type is not a character string, or if the
// string's length is not bounded.
//
// This is used to populate information_schema.columns.character_octet_length;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) MaxOctetLength() (int32, bool) {
	switch c.SemanticType {
	case ColumnType_STRING, ColumnType_COLLATEDSTRING:
		if c.Width > 0 {
			return c.Width * utf8.UTFMax, true
		}
	}
	return 0, false
}

// NumericPrecision returns the declared or implicit precision of numeric
// data types. Returns false if the data type is not numeric, or if the precision
// of the numeric type is not bounded.
//
// This is used to populate information_schema.columns.numeric_precision;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) NumericPrecision() (int32, bool) {
	switch c.SemanticType {
	case ColumnType_INT:
		return 64, true
	case ColumnType_FLOAT:
		switch c.VisibleType {
		case ColumnType_REAL:
			return 24, true
		default:
			// NONE now means double precision.
			// Pre-2.1 there were 3 cases:
			// - VisibleType = DOUBLE PRECISION, Width = 0 -> now clearly FLOAT8
			// - VisibleType = NONE, Width = 0 -> now clearly FLOAT8
			// - VisibleType = NONE, Width > 0 -> we need to derive the precision.
			if c.Precision >= 1 && c.Precision <= 24 {
				return 24, true
			}
			return 53, true
		}
	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			return c.Precision, true
		}
	}
	return 0, false
}

// NumericPrecisionRadix returns the implicit precision radix of
// numeric data types. Returns false if the data type is not numeric.
//
// This is used to populate information_schema.columns.numeric_precision_radix;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) NumericPrecisionRadix() (int32, bool) {
	switch c.SemanticType {
	case ColumnType_INT:
		return 2, true
	case ColumnType_FLOAT:
		return 2, true
	case ColumnType_DECIMAL:
		return 10, true
	}
	return 0, false
}

// NumericScale returns the declared or implicit precision of exact numeric
// data types. Returns false if the data type is not an exact numeric, or if the
// scale of the exact numeric type is not bounded.
//
// This is used to populate information_schema.columns.numeric_scale;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) NumericScale() (int32, bool) {
	switch c.SemanticType {
	case ColumnType_INT:
		return 0, true
	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			return c.Width, true
		}
	}
	return 0, false
}

// datumTypeToColumnSemanticType converts a types.T to a SemanticType.
//
// This is mainly used by DatumTypeToColumnType() above; it is also
// used to derive the semantic type of array elements and the
// determination of DatumTypeHasCompositeKeyEncoding().
func datumTypeToColumnSemanticType(ptyp types.T) (ColumnType_SemanticType, error) {
	switch ptyp {
	case types.Bool:
		return ColumnType_BOOL, nil
	case types.Int:
		return ColumnType_INT, nil
	case types.Float:
		return ColumnType_FLOAT, nil
	case types.Decimal:
		return ColumnType_DECIMAL, nil
	case types.Bytes:
		return ColumnType_BYTES, nil
	case types.String:
		return ColumnType_STRING, nil
	case types.Name:
		return ColumnType_NAME, nil
	case types.Date:
		return ColumnType_DATE, nil
	case types.Time:
		return ColumnType_TIME, nil
	case types.Timestamp:
		return ColumnType_TIMESTAMP, nil
	case types.TimestampTZ:
		return ColumnType_TIMESTAMPTZ, nil
	case types.Interval:
		return ColumnType_INTERVAL, nil
	case types.UUID:
		return ColumnType_UUID, nil
	case types.INet:
		return ColumnType_INET, nil
	case types.Oid, types.RegClass, types.RegNamespace, types.RegProc, types.RegType, types.RegProcedure:
		return ColumnType_OID, nil
	case types.Unknown:
		return ColumnType_NULL, nil
	case types.IntVector:
		return ColumnType_INT2VECTOR, nil
	case types.OidVector:
		return ColumnType_OIDVECTOR, nil
	case types.JSON:
		return ColumnType_JSON, nil
	default:
		if ptyp.FamilyEqual(types.FamCollatedString) {
			return ColumnType_COLLATEDSTRING, nil
		}
		if ptyp.FamilyEqual(types.FamTuple) {
			return ColumnType_TUPLE, nil
		}
		if wrapper, ok := ptyp.(types.TOidWrapper); ok {
			return datumTypeToColumnSemanticType(wrapper.T)
		}
		return -1, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError, "unsupported result type: %s, %T, %+v", ptyp, ptyp, ptyp)
	}
}

// columnSemanticTypeToDatumType determines a types.T that can be used
// to instantiate an in-memory representation of values for the given
// column type.
func columnSemanticTypeToDatumType(c *ColumnType, k ColumnType_SemanticType) types.T {
	switch k {
	case ColumnType_BOOL:
		return types.Bool
	case ColumnType_INT:
		return types.Int
	case ColumnType_FLOAT:
		return types.Float
	case ColumnType_DECIMAL:
		return types.Decimal
	case ColumnType_STRING:
		return types.String
	case ColumnType_BYTES:
		return types.Bytes
	case ColumnType_DATE:
		return types.Date
	case ColumnType_TIME:
		return types.Time
	case ColumnType_TIMESTAMP:
		return types.Timestamp
	case ColumnType_TIMESTAMPTZ:
		return types.TimestampTZ
	case ColumnType_INTERVAL:
		return types.Interval
	case ColumnType_UUID:
		return types.UUID
	case ColumnType_INET:
		return types.INet
	case ColumnType_JSON:
		return types.JSON
	case ColumnType_TUPLE:
		return types.FamTuple
	case ColumnType_COLLATEDSTRING:
		if c.Locale == nil {
			panic("locale is required for COLLATEDSTRING")
		}
		return types.TCollatedString{Locale: *c.Locale}
	case ColumnType_NAME:
		return types.Name
	case ColumnType_OID:
		return types.Oid
	case ColumnType_NULL:
		return types.Unknown
	case ColumnType_INT2VECTOR:
		return types.IntVector
	case ColumnType_OIDVECTOR:
		return types.OidVector
	}
	return nil
}

// ToDatumType converts the ColumnType to a types.T (type of in-memory
// representations). It returns nil if there is no such type.
//
// This is a lossy conversion: some type attributes are not preserved.
func (c *ColumnType) ToDatumType() types.T {
	switch c.SemanticType {
	case ColumnType_ARRAY:
		return types.TArray{Typ: columnSemanticTypeToDatumType(c, *c.ArrayContents)}
	case ColumnType_TUPLE:
		datums := types.TTuple{
			Types:  make([]types.T, len(c.TupleContents)),
			Labels: c.TupleLabels,
		}
		for i := range c.TupleContents {
			datums.Types[i] = c.TupleContents[i].ToDatumType()
		}
		return datums
	default:
		return columnSemanticTypeToDatumType(c, c.SemanticType)
	}
}

// ColumnTypesToDatumTypes converts a slice of ColumnTypes to a slice of
// datum types.
func ColumnTypesToDatumTypes(colTypes []ColumnType) []types.T {
	res := make([]types.T, len(colTypes))
	for i, t := range colTypes {
		res[i] = t.ToDatumType()
	}
	return res
}

// CheckValueWidth checks that the width (for strings, byte arrays,
// and bit strings) and scale (for decimals) of the value fits the
// specified column type. Used by INSERT and UPDATE.
func CheckValueWidth(typ ColumnType, val tree.Datum, name string) error {
	switch typ.SemanticType {
	case ColumnType_STRING:
		if v, ok := tree.AsDString(val); ok {
			if typ.Width > 0 && utf8.RuneCountInString(string(v)) > int(typ.Width) {
				return fmt.Errorf("value too long for type %s (column %q)",
					typ.SQLString(), name)
			}
		}
	case ColumnType_INT:
		if v, ok := tree.AsDInt(val); ok {
			if typ.Width == 32 || typ.Width == 64 || typ.Width == 16 {
				// Width is defined in bits.
				width := uint(typ.Width - 1)

				// We're performing bounds checks inline with Go's implementation of min and max ints in Math.go.
				shifted := v >> width
				if (v >= 0 && shifted > 0) || (v < 0 && shifted < -1) {
					return fmt.Errorf("integer out of range for type %s (column %q)", typ.VisibleType, name)
				}
			}
		}
	case ColumnType_DECIMAL:
		if v, ok := val.(*tree.DDecimal); ok {
			if err := tree.LimitDecimalWidth(&v.Decimal, int(typ.Precision), int(typ.Width)); err != nil {
				return errors.Wrapf(err, "type %s (column %q)", typ.SQLString(), name)
			}
		}
	case ColumnType_ARRAY:
		if v, ok := val.(*tree.DArray); ok {
			elementType := *typ.elementColumnType()
			for i := range v.Array {
				if err := CheckValueWidth(elementType, v.Array[i], name); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// elementColumnType works on a ColumnType with semantic type ARRAY
// and retrieves the ColumnType of the elements of the array.
//
// This is used by CheckValueWidth() and SQLType().
//
// TODO(knz): make this return a bool and avoid a heap allocation.
func (c *ColumnType) elementColumnType() *ColumnType {
	if c.SemanticType != ColumnType_ARRAY {
		return nil
	}
	result := *c
	result.SemanticType = *c.ArrayContents
	result.ArrayContents = nil
	return &result
}

// CheckColumnType verifies that a given value is compatible
// with the type requested by the column. If the value is a
// placeholder, the type of the placeholder gets populated.
func CheckColumnType(col ColumnDescriptor, typ types.T, pmap *tree.PlaceholderInfo) error {
	if typ == types.Unknown {
		return nil
	}

	// If the value is a placeholder, then the column check above has
	// populated 'colTyp' with a type to assign to it.
	colTyp := col.Type.ToDatumType()
	if p, pok := typ.(types.TPlaceholder); pok {
		if err := pmap.SetType(p.Name, colTyp); err != nil {
			return fmt.Errorf("cannot infer type for placeholder %s from column %q: %s",
				p.Name, col.Name, err)
		}
	} else if !typ.Equivalent(colTyp) {
		// Not a placeholder; check that the value cast has succeeded.
		return fmt.Errorf("value type %s doesn't match type %s of column %q",
			typ, col.Type.SemanticType, col.Name)
	}
	return nil
}
