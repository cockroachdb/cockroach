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

package parser

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// oidColTypeToType produces a Datum type equivalent to the given
// OidColType.
func oidColTypeToType(ct *OidColType) types.T {
	switch ct {
	case oidColTypeOid:
		return types.Oid
	case oidColTypeRegClass:
		return types.RegClass
	case oidColTypeRegNamespace:
		return types.RegNamespace
	case oidColTypeRegProc:
		return types.RegProc
	case oidColTypeRegProcedure:
		return types.RegProcedure
	case oidColTypeRegType:
		return types.RegType
	default:
		panic(fmt.Sprintf("unexpected *OidColType: %v", ct))
	}
}

// oidTypeToColType produces an OidColType equivalent to the given
// Datum type.
func oidTypeToColType(t types.T) *OidColType {
	switch t {
	case types.Oid:
		return oidColTypeOid
	case types.RegClass:
		return oidColTypeRegClass
	case types.RegNamespace:
		return oidColTypeRegNamespace
	case types.RegProc:
		return oidColTypeRegProc
	case types.RegProcedure:
		return oidColTypeRegProcedure
	case types.RegType:
		return oidColTypeRegType
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
		return boolColTypeBool, nil
	case types.Int:
		return intColTypeInt, nil
	case types.Float:
		return floatColTypeFloat, nil
	case types.Decimal:
		return decimalColTypeDecimal, nil
	case types.Timestamp:
		return timestampColTypeTimestamp, nil
	case types.TimestampTZ:
		return timestampTzColTypeTimestampWithTZ, nil
	case types.Interval:
		return intervalColTypeInterval, nil
	case types.JSON:
		return jsonColType, nil
	case types.UUID:
		return uuidColTypeUUID, nil
	case types.INet:
		return ipnetColTypeINet, nil
	case types.Date:
		return dateColTypeDate, nil
	case types.String:
		return stringColTypeString, nil
	case types.Name:
		return nameColTypeName, nil
	case types.Bytes:
		return bytesColTypeBytes, nil
	case types.Oid,
		types.RegClass,
		types.RegNamespace,
		types.RegProc,
		types.RegProcedure,
		types.RegType:
		return oidTypeToColType(t), nil
	}

	switch typ := t.(type) {
	case types.TCollatedString:
		return &CollatedStringColType{Name: "STRING", Locale: typ.Locale}, nil
	case types.TArray:
		elemTyp, err := DatumTypeToColumnType(typ.Typ)
		if err != nil {
			return nil, err
		}
		return arrayOf(elemTyp, nil)
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
		return oidColTypeToType(ct)
	default:
		panic(fmt.Sprintf("unexpected CastTarget %T", t))
	}
}
