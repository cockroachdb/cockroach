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

package types

import (
	"fmt"

	"github.com/cockroachdb/apd"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// T represents an exec physical type - a bytes representation of a particular
// column type.
type T int

//go:generate stringer -type=T
const (
	// Bool is a column of type bool
	Bool T = iota
	// Bytes is a column of type []byte
	Bytes
	// Decimal is a column of type apd.Decimal
	Decimal
	// Int8 is a column of type int8
	Int8
	// Int16 is a column of type int16
	Int16
	// Int32 is a column of type int32
	Int32
	// Int64 is a column of type int64
	Int64
	// Float32 is a column of type float32
	Float32
	// Float64 is a column of type float64
	Float64

	// Unhandled is a temporary value that represents an unhandled type.
	// TODO(jordan): this should be replaced by a panic once all types are
	// handled.
	Unhandled
)

// FromColumnType returns the T that corresponds to the input ColumnType.
func FromColumnType(ct sqlbase.ColumnType) T {
	switch ct.SemanticType {
	case sqlbase.ColumnType_BOOL:
		return Bool
	case sqlbase.ColumnType_BYTES, sqlbase.ColumnType_STRING, sqlbase.ColumnType_NAME:
		return Bytes
	case sqlbase.ColumnType_DATE, sqlbase.ColumnType_OID:
		return Int64
	case sqlbase.ColumnType_DECIMAL:
		return Decimal
	case sqlbase.ColumnType_INT:
		switch ct.Width {
		case 8:
			return Int8
		case 16:
			return Int16
		case 32:
			return Int32
		case 0, 64:
			return Int64
		}
		panic(fmt.Sprintf("integer with unknown width %d", ct.Width))
	case sqlbase.ColumnType_FLOAT:
		return Float64
	}
	return Unhandled
}

// FromColumnTypes calls FromColumnType on each element of cts, returning the
// resulting slice.
func FromColumnTypes(cts []sqlbase.ColumnType) []T {
	typs := make([]T, len(cts))
	for i := range typs {
		typs[i] = FromColumnType(cts[i])
	}
	return typs
}

// FromGoType returns the type for a Go value, if applicable. Shouldn't be used at
// runtime.
func FromGoType(v interface{}) T {
	switch t := v.(type) {
	case int8:
		return Int8
	case int16:
		return Int16
	case int32:
		return Int32
	case int, int64:
		return Int64
	case bool:
		return Bool
	case float32:
		return Float32
	case float64:
		return Float64
	case []byte:
		return Bytes
	case string:
		return Bytes
	case apd.Decimal:
		return Decimal
	default:
		panic(fmt.Sprintf("type %T not supported yet", t))
	}
}

// GoTypeName returns the stringified Go type for an exec type.
func (t T) GoTypeName() string {
	switch t {
	case Bool:
		return "bool"
	case Bytes:
		return "[]byte"
	case Int8:
		return "int8"
	case Int16:
		return "int16"
	case Int32:
		return "int32"
	case Int64:
		return "int64"
	case Float32:
		return "float32"
	case Float64:
		return "float64"
	default:
		panic(fmt.Sprintf("unhandled type %d", t))
	}
}
