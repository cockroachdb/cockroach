// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package types

import (
	"fmt"

	"github.com/cockroachdb/apd"
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

// AllTypes is slice of all exec types.
var AllTypes []T

func init() {
	for i := Bool; i < Unhandled; i++ {
		AllTypes = append(AllTypes, i)
	}
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
	case Decimal:
		return "apd.Decimal"
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
