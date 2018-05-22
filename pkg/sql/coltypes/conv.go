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
)

// TOidToType produces a Datum type equivalent to the given
// TOid.
func TOidToType(ct *TOid) types.T {
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
func OidTypeToColType(t types.T) *TOid {
	switch t {
	case types.Oid:
		return Oid
	case types.RegClass:
		return RegClass
	case types.RegNamespace:
		return RegNamespace
	case types.RegProc:
		return RegProc
	case types.RegProcedure:
		return RegProcedure
	case types.RegType:
		return RegType
	default:
		panic(fmt.Sprintf("unexpected type: %v", t))
	}
}

// DatumTypeToColumnType produces a SQL column type equivalent to the
// given Datum type. Used to generate CastExpr nodes during
// normalization.
func DatumTypeToColumnType(t types.T) (T, error) {
	switch t {
	case types.Bool:
		return Bool, nil
	case types.Int:
		return Int, nil
	case types.Float:
		return Float, nil
	case types.Decimal:
		return Decimal, nil
	case types.Timestamp:
		return Timestamp, nil
	case types.TimestampTZ:
		return TimestampWithTZ, nil
	case types.Interval:
		return Interval, nil
	case types.JSON:
		return JSON, nil
	case types.UUID:
		return UUID, nil
	case types.INet:
		return INet, nil
	case types.Date:
		return Date, nil
	case types.Time:
		return Time, nil
	case types.TimeTZ:
		return TimeTZ, nil
	case types.String:
		return String, nil
	case types.Name:
		return Name, nil
	case types.Bytes:
		return Bytes, nil
	case types.Oid,
		types.RegClass,
		types.RegNamespace,
		types.RegProc,
		types.RegProcedure,
		types.RegType:
		return OidTypeToColType(t), nil
	}

	switch typ := t.(type) {
	case types.TCollatedString:
		return &TCollatedString{Name: "STRING", Locale: typ.Locale}, nil
	case types.TArray:
		elemTyp, err := DatumTypeToColumnType(typ.Typ)
		if err != nil {
			return nil, err
		}
		return ArrayOf(elemTyp, nil)
	case types.TTuple:
		colTyp := make(TTuple, len(typ.Types))
		for i := range typ.Types {
			elemTyp, err := DatumTypeToColumnType(typ.Types[i])
			if err != nil {
				return nil, err
			}
			colTyp[i] = elemTyp
		}
		return colTyp, nil
	case types.TOidWrapper:
		return DatumTypeToColumnType(typ.T)
	}

	return nil, pgerror.NewErrorf(pgerror.CodeInvalidTableDefinitionError,
		"value type %s cannot be used for table columns", t)
}

// CastTargetToDatumType produces the types.T that is closest to the given SQL
// cast target type. The resulting type might not be "tight", meaning that it
// may allow values that the original type would not allow. See the comment for
// TryCastTargetToDatumType for more details.
func CastTargetToDatumType(t CastTargetType) types.T {
	res, _ := TryCastTargetToDatumType(t)
	return res
}

// TryCastTargetToDatumType produces the types.T that is closest to the given
// SQL cast target type. It returns the resulting type, as well as a boolean
// indicating whether the conversion is "tight". A tight conversion means that
// the destination type allows exactly the same set of values that the source
// type allows. For example, the following conversion is not tight, because the
// destination type allows strings that are longer than two characters:
//
//   VARCHAR(2) => STRING
//
func TryCastTargetToDatumType(src CastTargetType) (dst types.T, tight bool) {
	switch ct := src.(type) {
	case *TBool:
		return types.Bool, true
	case *TInt:
		return types.Int, ct.Width == 0
	case *TFloat:
		return types.Float, ct.Width == 64 && ct.Prec == 0 && !ct.PrecSpecified
	case *TDecimal:
		return types.Decimal, ct.Prec == 0 && ct.Scale == 0
	case *TString:
		return types.String, ct.N == 0
	case *TName:
		return types.Name, true
	case *TBytes:
		return types.Bytes, true
	case *TDate:
		return types.Date, true
	case *TTime:
		return types.Time, true
	case *TTimeTZ:
		return types.TimeTZ, true
	case *TTimestamp:
		return types.Timestamp, true
	case *TTimestampTZ:
		return types.TimestampTZ, true
	case *TInterval:
		return types.Interval, true
	case *TJSON:
		return types.JSON, true
	case *TUUID:
		return types.UUID, true
	case *TIPAddr:
		return types.INet, true
	case *TCollatedString:
		return types.TCollatedString{Locale: ct.Locale}, ct.N == 0
	case *TArray:
		typ, equiv := TryCastTargetToDatumType(ct.ParamType)
		return types.TArray{Typ: typ}, equiv
	case *TVector:
		switch ct.ParamType.(type) {
		case *TInt:
			return types.IntVector, true
		case *TOid:
			return types.OidVector, true
		default:
			panic(fmt.Sprintf("unexpected CastTarget %T[%T]", src, ct.ParamType))
		}
	case TTuple:
		tight = true
		ret := types.TTuple{Types: make([]types.T, len(ct))}
		for i := range ct {
			var tightVal bool
			ret.Types[i], tightVal = TryCastTargetToDatumType(ct[i])
			tight = tight && tightVal
		}
		return ret, tight
	case *TOid:
		return TOidToType(ct), true
	default:
		panic(fmt.Sprintf("unexpected CastTarget %T", src))
	}
}
