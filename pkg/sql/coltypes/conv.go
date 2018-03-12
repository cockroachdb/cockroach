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
	case types.TOidWrapper:
		return DatumTypeToColumnType(typ.T)
	}

	return nil, pgerror.NewErrorf(pgerror.CodeInvalidTableDefinitionError,
		"value type %s cannot be used for table columns", t)
}

// CastTargetToDatumType produces a types.T equivalent to the given
// SQL cast target type.
func CastTargetToDatumType(t CastTargetType) types.T {
	switch ct := t.(type) {
	case *TBool:
		return types.Bool
	case *TInt:
		return types.Int
	case *TFloat:
		return types.Float
	case *TDecimal:
		return types.Decimal
	case *TString:
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
		return types.JSON
	case *TUUID:
		return types.UUID
	case *TIPAddr:
		return types.INet
	case *TCollatedString:
		return types.TCollatedString{Locale: ct.Locale}
	case *TArray:
		return types.TArray{Typ: CastTargetToDatumType(ct.ParamType)}
	case *TVector:
		switch ct.ParamType.(type) {
		case *TInt:
			return types.IntVector
		case *TOid:
			return types.OidVector
		default:
			panic(fmt.Sprintf("unexpected CastTarget %T[%T]", t, ct.ParamType))
		}
	case *TOid:
		return TOidToType(ct)
	default:
		panic(fmt.Sprintf("unexpected CastTarget %T", t))
	}
}
