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

// OidColTypeToType produces a Datum type equivalent to the given
// OidColType.
func OidColTypeToType(ct *OidColType) types.T {
	switch ct {
	case OidColTypeOid:
		return types.Oid
	case OidColTypeRegClass:
		return types.RegClass
	case OidColTypeRegNamespace:
		return types.RegNamespace
	case OidColTypeRegProc:
		return types.RegProc
	case OidColTypeRegProcedure:
		return types.RegProcedure
	case OidColTypeRegType:
		return types.RegType
	default:
		panic(fmt.Sprintf("unexpected *OidColType: %v", ct))
	}
}

// OidTypeToColType produces an OidColType equivalent to the given
// Datum type.
func OidTypeToColType(t types.T) *OidColType {
	switch t {
	case types.Oid:
		return OidColTypeOid
	case types.RegClass:
		return OidColTypeRegClass
	case types.RegNamespace:
		return OidColTypeRegNamespace
	case types.RegProc:
		return OidColTypeRegProc
	case types.RegProcedure:
		return OidColTypeRegProcedure
	case types.RegType:
		return OidColTypeRegType
	default:
		panic(fmt.Sprintf("unexpected type: %v", t))
	}
}

// DatumTypeToColumnType produces a SQL column type equivalent to the
// given Datum type. Used to generate CastExpr nodes during
// normalization.
func DatumTypeToColumnType(t types.T) (ColumnType, error) {
	switch t {
	case types.Bool:
		return BoolColTypeBool, nil
	case types.Int:
		return IntColTypeInt, nil
	case types.Float:
		return FloatColTypeFloat, nil
	case types.Decimal:
		return DecimalColTypeDecimal, nil
	case types.Timestamp:
		return TimestampColTypeTimestamp, nil
	case types.TimestampTZ:
		return TimestampTzColTypeTimestampWithTZ, nil
	case types.Interval:
		return IntervalColTypeInterval, nil
	case types.JSON:
		return JsonColType, nil
	case types.UUID:
		return UuidColTypeUUID, nil
	case types.INet:
		return IpnetColTypeINet, nil
	case types.Date:
		return DateColTypeDate, nil
	case types.String:
		return StringColTypeString, nil
	case types.Name:
		return NameColTypeName, nil
	case types.Bytes:
		return BytesColTypeBytes, nil
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
		return &CollatedStringColType{Name: "STRING", Locale: typ.Locale}, nil
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
	case *BoolColType:
		return types.Bool
	case *IntColType:
		return types.Int
	case *FloatColType:
		return types.Float
	case *DecimalColType:
		return types.Decimal
	case *StringColType:
		return types.String
	case *NameColType:
		return types.Name
	case *BytesColType:
		return types.Bytes
	case *DateColType:
		return types.Date
	case *TimestampColType:
		return types.Timestamp
	case *TimestampTZColType:
		return types.TimestampTZ
	case *IntervalColType:
		return types.Interval
	case *JSONColType:
		return types.JSON
	case *UUIDColType:
		return types.UUID
	case *IPAddrColType:
		return types.INet
	case *CollatedStringColType:
		return types.TCollatedString{Locale: ct.Locale}
	case *ArrayColType:
		return types.TArray{Typ: CastTargetToDatumType(ct.ParamType)}
	case *VectorColType:
		return types.IntVector
	case *OidColType:
		return OidColTypeToType(ct)
	default:
		panic(fmt.Sprintf("unexpected CastTarget %T", t))
	}
}
