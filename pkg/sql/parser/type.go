// Copyright 2015 The Cockroach Authors.
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
	types "github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

var (
	// TypeNull is a transitionary alias.
	TypeNull = types.TypeNull
	// TypeBool is a transitionary alias.
	TypeBool = types.TypeBool
	// TypeInt  is a transitionary alias.
	TypeInt = types.TypeInt
	// TypeFloat is a transitionary alias.
	TypeFloat = types.TypeFloat
	// TypeDecimal is a transitionary alias.
	TypeDecimal = types.TypeDecimal
	// TypeString is a transitionary alias.
	TypeString = types.TypeString
	// TypeCollatedString is a transitionary alias.
	TypeCollatedString = types.TypeCollatedString
	// TypeByte is a transitionary alias.
	TypeBytes = types.TypeBytes
	// TypeDate is a transitionary alias.
	TypeDate = types.TypeDate
	// TypeTimestamp is a transitionary alias.
	TypeTimestamp = types.TypeTimestamp
	// TypeTimestampTZ is a transitionary alias.
	TypeTimestampTZ = types.TypeTimestampTZ
	// TypeInterval is a transitionary alias.
	TypeInterval = types.TypeInterval
	// TypeJSON is a transitionary alias.
	TypeJSON = types.TypeJSON
	// TypeUUID is a transitionary alias.
	TypeUUID = types.TypeUUID
	// TypeINet is a transitionary alias.
	TypeINet = types.TypeINet
	// TypeTuple is a transitionary alias.
	TypeTuple = types.TypeTuple
	// TypeArray is a transitionary alias.
	TypeArray = types.TypeArray
	// TypeTable is a transitionary alias.
	TypeTable = types.TypeTable
	// TypePlaceholder is a transitionary alias.
	TypePlaceholder = types.TypePlaceholder
	// TypeAnyArray is a transitionary alias.
	TypeAnyArray = types.TypeAnyArray
	// TypeAny  is a transitionary alias.
	TypeAny = types.TypeAny
	// TypeOid  is a transitionary alias.
	TypeOid = types.TypeOid
	// TypeRegClass is a transitionary alias.
	TypeRegClass = types.TypeRegClass
	// TypeRegNamespace is a transitionary alias.
	TypeRegNamespace = types.TypeRegNamespace
	// TypeRegProc is a transitionary alias.
	TypeRegProc = types.TypeRegProc
	// TypeRegProcedure is a transitionary alias.
	TypeRegProcedure = types.TypeRegProcedure
	// TypeRegType is a transitionary alias.
	TypeRegType = types.TypeRegType
	// TypeName is a transitionary alias.
	TypeName = types.TypeName
	// TypeIntVector is a transitionary alias.
	TypeIntVector = types.TypeIntVector
	// TypeName is a transitionary alias.
	TypeNameArray = types.TypeNameArray
)

// TCollatedString is a transitionary alias.
type TCollatedString = types.TCollatedString

// TTuple is a transitionary alias.
type TTuple = types.TTuple

// TPlaceholder is a transitionary alias.
type TPlaceholder = types.TPlaceholder

// TArray is a transitionary alias.
type TArray = types.TArray

// TTable is a transitionary alias.
type TTable = types.TTable
