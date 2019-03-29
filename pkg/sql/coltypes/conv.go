// Copyright 2017 The Cockroach Authors.
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

package coltypes

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/lib/pq/oid"
)

// TOidToType produces a Datum type equivalent to the given
// TOid.
func TOidToType(ct *TOid) *types.T {
	switch ct {
	case Oid:
		return types.Oid
	case RegClass:
		return types.RegClass
	case RegNamespace:
		return types.RegNamespace
	case RegProc:
		return types.RegProc
	case RegProcedure:
		return types.RegProcedure
	case RegType:
		return types.RegType
	default:
		panic(fmt.Sprintf("unexpected *TOid: %v", ct))
	}
}

// OidTypeToColType produces an TOid equivalent to the given
// Datum type.
func OidTypeToColType(t *types.T) *TOid {
	if t.SemanticType == types.OID {
		switch t.Oid() {
		case oid.T_oid:
			return Oid
		case oid.T_regclass:
			return RegClass
		case oid.T_regnamespace:
			return RegNamespace
		case oid.T_regproc:
			return RegProc
		case oid.T_regprocedure:
			return RegProcedure
		case oid.T_regtype:
			return RegType
		}
	}
	panic(fmt.Sprintf("unexpected type: %v", t))
}

// DatumTypeToColumnType produces a SQL column type equivalent to the
// given Datum type. Used to generate CastExpr nodes during
// normalization.
func DatumTypeToColumnType(t *types.T) (T, error) {
	switch t.Oid() {
	case oid.T_name:
		return Name, nil
	case oid.T_int2vector:
		return Int2vector, nil
	case oid.T_oidvector:
		return OidVector, nil
	}

	switch t.SemanticType {
	case types.BOOL:
		return Bool, nil
	case types.BIT:
		return VarBit, nil
	case types.INT:
		return Int8, nil
	case types.FLOAT:
		return Float8, nil
	case types.DECIMAL:
		return Decimal, nil
	case types.TIMESTAMP:
		return Timestamp, nil
	case types.TIMESTAMPTZ:
		return TimestampWithTZ, nil
	case types.INTERVAL:
		return Interval, nil
	case types.JSON:
		return JSON, nil
	case types.UUID:
		return UUID, nil
	case types.INET:
		return INet, nil
	case types.DATE:
		return Date, nil
	case types.TIME:
		return Time, nil
	case types.STRING:
		return String, nil
	case types.BYTES:
		return Bytes, nil
	case types.OID:
		return OidTypeToColType(t), nil
	case types.COLLATEDSTRING:
		return &TCollatedString{
			TString: TString{Variant: TStringVariantSTRING},
			Locale:  *t.Locale,
		}, nil
	case types.ARRAY:
		elemTyp, err := DatumTypeToColumnType(t.ArrayContents)
		if err != nil {
			return nil, err
		}
		return ArrayOf(elemTyp, nil)
	case types.TUPLE:
		colTyp := make(TTuple, len(t.TupleContents))
		for i := range t.TupleContents {
			elemTyp, err := DatumTypeToColumnType(&t.TupleContents[i])
			if err != nil {
				return nil, err
			}
			colTyp[i] = elemTyp
		}
		return colTyp, nil
	}

	return nil, pgerror.NewErrorf(pgerror.CodeInvalidTableDefinitionError,
		"value type %s cannot be used for table columns", t)
}

// CastTargetToDatumType produces the types.T that is closest to the given SQL
// cast target type. The resulting type might not be exactly equivalent. For
// example, the following source and destination types are not equivalent,
// because the destination type allows strings that are longer than two
// characters. If a string having three characters were converted to VARCHAR(2),
// the extra character would be truncated (i.e. it's a lossy conversion).
//
//   VARCHAR(2) => STRING
//
func CastTargetToDatumType(t CastTargetType) *types.T {
	switch ct := t.(type) {
	case *TBool:
		return types.Bool
	case *TBitArray:
		if ct.Width != 0 || ct.Variable {
			typ := types.T{SemanticType: types.BIT, Width: int32(ct.Width)}
			if ct.Variable {
				typ.ZZZ_Oid = oid.T_varbit
			}
			return &typ
		}
		return types.VarBit
	case *TInt:
		if ct.Width != 0 {
			return &types.T{SemanticType: types.INT, Width: int32(ct.Width)}
		}
		return types.Int
	case *TSerial:
		return types.Int
	case *TFloat:
		if ct.Short {
			return &types.T{SemanticType: types.FLOAT, Width: 32}
		}
		return types.Float
	case *TDecimal:
		if ct.Prec != 0 || ct.Scale != 0 {
			return &types.T{
				SemanticType: types.DECIMAL,
				Precision:    int32(ct.Prec),
				Width:        int32(ct.Scale),
			}
		}
		return types.Decimal
	case *TString:
		if ct.N != 0 || ct.Variant != TStringVariantSTRING {
			typ := types.T{SemanticType: types.STRING, Width: int32(ct.N)}
			populateStringVariant(ct.Variant, &typ)
			return &typ
		}
		return types.String
	case *TName:
		return types.Name
	case *TBytes:
		return types.Bytes
	case *TDate:
		return types.Date
	case *TTime:
		return types.Time
	case *TTimestamp:
		return types.Timestamp
	case *TTimestampTZ:
		return types.TimestampTZ
	case *TInterval:
		return types.Interval
	case *TJSON:
		return types.Jsonb
	case *TUUID:
		return types.Uuid
	case *TIPAddr:
		return types.INet
	case *TCollatedString:
		typ := types.T{SemanticType: types.COLLATEDSTRING, Locale: &ct.Locale, Width: int32(ct.N)}
		populateStringVariant(ct.Variant, &typ)
		return &typ
	case *TArray:
		return &types.T{SemanticType: types.ARRAY, ArrayContents: CastTargetToDatumType(ct.ParamType)}
	case *TVector:
		switch ct.ParamType.(type) {
		case *TInt:
			return types.IntVector
		case *TOid:
			return types.OidVector
		default:
			panic(fmt.Sprintf("unexpected CastTarget %T[%T]", t, ct.ParamType))
		}
	case TTuple:
		ret := &types.T{SemanticType: types.TUPLE, TupleContents: make([]types.T, len(ct))}
		for i := range ct {
			ret.TupleContents[i] = *CastTargetToDatumType(ct[i])
		}
		return ret
	case *TOid:
		return TOidToType(ct)
	default:
		panic(fmt.Sprintf("unexpected CastTarget %T", t))
	}
}

func populateStringVariant(variant TStringVariant, typ *types.T) {
	switch variant {
	case TStringVariantVARCHAR:
		typ.ZZZ_Oid = oid.T_varchar
	case TStringVariantCHAR:
		typ.ZZZ_Oid = oid.T_bpchar
	case TStringVariantQCHAR:
		typ.ZZZ_Oid = oid.T_char
	}
}
