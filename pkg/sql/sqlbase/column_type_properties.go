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
	"math"
	"unicode/utf8"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
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
func DatumTypeToColumnType(ptyp types.T) (types.ColumnType, error) {
	var ctyp types.ColumnType
	switch t := ptyp.(type) {
	case types.TCollatedString:
		ctyp.SemanticType = types.COLLATEDSTRING
		ctyp.Locale = &t.Locale
	case types.TArray:
		ctyp.SemanticType = types.ARRAY
		contents, err := DatumTypeToColumnType(t.Typ)
		if err != nil {
			return types.ColumnType{}, err
		}
		ctyp.ArrayContents = &contents
	case types.TTuple:
		ctyp.SemanticType = types.TUPLE
		ctyp.TupleContents = make([]types.ColumnType, len(t.Types))
		for i, tc := range t.Types {
			var err error
			ctyp.TupleContents[i], err = DatumTypeToColumnType(tc)
			if err != nil {
				return types.ColumnType{}, err
			}
		}
		ctyp.TupleLabels = t.Labels
		return ctyp, nil
	case types.TOidWrapper:
		ctyp.SemanticType = ptyp.SemanticType()
		ctyp.ZZZ_Oid = t.Oid()
	default:
		ctyp.SemanticType = ptyp.SemanticType()
	}
	return ctyp, nil
}

// PopulateTypeAttrs set other attributes of the ColumnType from a
// coltypes.T and performs type-specific verifications.
//
// This must be used on ColumnTypes produced from
// DatumTypeToColumnType if the origin of the type was a coltypes.T
// (e.g. via CastTargetToDatumType).
func PopulateTypeAttrs(base types.ColumnType, typ coltypes.T) (types.ColumnType, error) {
	switch t := typ.(type) {
	case *coltypes.TBitArray:
		if t.Width > math.MaxInt32 {
			return types.ColumnType{}, fmt.Errorf("bit width too large: %d", t.Width)
		}
		base.Width = int32(t.Width)
		if t.Variable {
			base.ZZZ_Oid = oid.T_varbit
		}

	case *coltypes.TInt:
		// Ensure that "naked" INT types are promoted to INT8 to preserve
		// compatibility with previous versions.
		if t.Width == 0 {
			base.Width = 64
		} else {
			base.Width = int32(t.Width)
		}

	case *coltypes.TFloat:
		if t.Short {
			base.Width = 32
		} else {
			base.Width = 64
		}

	case *coltypes.TDecimal:
		base.Width = int32(t.Scale)
		base.Precision = int32(t.Prec)
		switch {
		case base.Precision == 0 && base.Width > 0:
			// TODO (seif): Find right range for error message.
			return types.ColumnType{}, errors.New("invalid NUMERIC precision 0")
		case base.Precision < base.Width:
			return types.ColumnType{}, fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				base.Width, base.Precision)
		}

	case *coltypes.TString:
		base.Width = int32(t.N)
		base.ZZZ_Oid = coltypeStringVariantToOid(t.Variant)

	case *coltypes.TCollatedString:
		base.Width = int32(t.N)
		base.ZZZ_Oid = coltypeStringVariantToOid(t.Variant)

	case *coltypes.TArray:
		var err error
		*base.ArrayContents, err = PopulateTypeAttrs(*base.ArrayContents, t.ParamType)
		if err != nil {
			return types.ColumnType{}, err
		}

	case *coltypes.TVector:
		switch t.ParamType.(type) {
		case *coltypes.TInt:
			base.ArrayContents = &types.ColumnType{SemanticType: types.INT}
			base.ZZZ_Oid = oid.T_int2vector
		case *coltypes.TOid:
			base.ArrayContents = &types.ColumnType{SemanticType: types.OID}
			base.ZZZ_Oid = oid.T_oidvector
		default:
			return types.ColumnType{}, errors.Errorf("vectors of type %s are unsupported", t.ParamType)
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
		return types.ColumnType{}, errors.Errorf("unexpected type %T", t)
	}
	return base, nil
}

// coltypeStringVariantToOid returns the Oid of a coltypes.TString or
// coltypes.TCollatedString variant.
func coltypeStringVariantToOid(c coltypes.TStringVariant) oid.Oid {
	switch c {
	case coltypes.TStringVariantVARCHAR:
		return oid.T_varchar
	case coltypes.TStringVariantCHAR:
		return oid.T_bpchar
	case coltypes.TStringVariantQCHAR:
		return oid.T_char
	}
	return 0
}

// LimitValueWidth checks that the width (for strings, byte arrays, and bit
// strings) and scale (for decimals) of the value fits the specified column
// type. In case of decimals, it can truncate fractional digits in the input
// value in order to fit the target column. If the input value fits the target
// column, it is returned unchanged. If the input value can be truncated to fit,
// then a truncated copy is returned. Otherwise, an error is returned. This
// method is used by INSERT and UPDATE.
func LimitValueWidth(
	typ *types.ColumnType, inVal tree.Datum, name *string,
) (outVal tree.Datum, err error) {
	switch typ.SemanticType {
	case types.STRING, types.COLLATEDSTRING:
		var sv string
		if v, ok := tree.AsDString(inVal); ok {
			sv = string(v)
		} else if v, ok := inVal.(*tree.DCollatedString); ok {
			sv = v.Contents
		}

		if typ.Width > 0 && utf8.RuneCountInString(sv) > int(typ.Width) {
			return nil, pgerror.NewErrorf(pgerror.CodeStringDataRightTruncationError,
				"value too long for type %s (column %q)",
				typ.SQLString(), tree.ErrNameStringP(name))
		}
	case types.INT:
		if v, ok := tree.AsDInt(inVal); ok {
			if typ.Width == 32 || typ.Width == 64 || typ.Width == 16 {
				// Width is defined in bits.
				width := uint(typ.Width - 1)

				// We're performing bounds checks inline with Go's implementation of min and max ints in Math.go.
				shifted := v >> width
				if (v >= 0 && shifted > 0) || (v < 0 && shifted < -1) {
					return nil, pgerror.NewErrorf(pgerror.CodeNumericValueOutOfRangeError,
						"integer out of range for type %s (column %q)",
						oid.TypeName[typ.Oid()], tree.ErrNameStringP(name))
				}
			}
		}
	case types.BIT:
		if v, ok := tree.AsDBitArray(inVal); ok {
			if typ.Width > 0 {
				bitLen := v.BitLen()
				switch typ.Oid() {
				case oid.T_varbit:
					if bitLen > uint(typ.Width) {
						return nil, pgerror.NewErrorf(pgerror.CodeStringDataRightTruncationError,
							"bit string length %d too large for type %s", bitLen, typ.SQLString())
					}
				default:
					if bitLen != uint(typ.Width) {
						return nil, pgerror.NewErrorf(pgerror.CodeStringDataLengthMismatchError,
							"bit string length %d does not match type %s", bitLen, typ.SQLString())
					}
				}
			}
		}
	case types.DECIMAL:
		if inDec, ok := inVal.(*tree.DDecimal); ok {
			if inDec.Form != apd.Finite || typ.Precision == 0 {
				// Non-finite form or unlimited target precision, so no need to limit.
				break
			}
			if int64(typ.Precision) >= inDec.NumDigits() && typ.Width == inDec.Exponent {
				// Precision and scale of target column are sufficient.
				break
			}

			var outDec tree.DDecimal
			outDec.Set(&inDec.Decimal)
			err := tree.LimitDecimalWidth(&outDec.Decimal, int(typ.Precision), int(typ.Width))
			if err != nil {
				return nil, pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
					"type %s (column %q)",
					typ.SQLString(), tree.ErrNameStringP(name))
			}
			return &outDec, nil
		}
	case types.ARRAY:
		if inArr, ok := inVal.(*tree.DArray); ok {
			var outArr *tree.DArray
			elementType := typ.ElementColumnType()
			for i, inElem := range inArr.Array {
				outElem, err := LimitValueWidth(elementType, inElem, name)
				if err != nil {
					return nil, err
				}
				if outElem != inElem {
					if outArr == nil {
						outArr = &tree.DArray{
							ParamTyp: inArr.ParamTyp,
							Array:    make(tree.Datums, len(inArr.Array)),
							HasNulls: inArr.HasNulls,
						}
						copy(outArr.Array, inArr.Array[:i])
					}
				}
				if outArr != nil {
					outArr.Array[i] = inElem
				}
			}
			if outArr != nil {
				return outArr, nil
			}
		}
	}
	return inVal, nil
}

// CheckDatumTypeFitsColumnType verifies that a given scalar value
// type is valid to be stored in a column of the given column type. If
// the scalar value is a placeholder, the type of the placeholder gets
// populated. NULL values are considered to fit every target type.
//
// For the purpose of this analysis, column type aliases are not
// considered to be different (eg. TEXT and VARCHAR will fit the same
// scalar type String).
//
// This is used by the UPDATE, INSERT and UPSERT code.
func CheckDatumTypeFitsColumnType(
	col ColumnDescriptor, typ types.T, pmap *tree.PlaceholderInfo,
) error {
	if typ.SemanticType() == types.NULL {
		return nil
	}
	// If the value is a placeholder, then the column check above has
	// populated 'colTyp' with a type to assign to it.
	colTyp := col.Type.ToDatumType()
	if p, pok := typ.(types.TPlaceholder); pok {
		if err := pmap.SetType(p.Idx, colTyp); err != nil {
			return pgerror.NewErrorf(pgerror.CodeIndeterminateDatatypeError,
				"cannot infer type for placeholder %s from column %q: %s",
				p.Idx, tree.ErrNameString(col.Name), err)
		}
	} else if !typ.Equivalent(colTyp) {
		// Not a placeholder; check that the value cast has succeeded.
		return pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError,
			"value type %s doesn't match type %s of column %q",
			typ, col.Type.SQLString(), tree.ErrNameString(col.Name))
	}
	return nil
}
