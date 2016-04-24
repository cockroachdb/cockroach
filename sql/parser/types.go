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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
)

// ColumnType represents a type in a column definition.
type ColumnType interface {
	fmt.Stringer
	columnType()
}

func (*BoolType) columnType()        {}
func (*IntType) columnType()         {}
func (*FloatType) columnType()       {}
func (*DecimalType) columnType()     {}
func (*DateType) columnType()        {}
func (*TimestampType) columnType()   {}
func (*TimestampTZType) columnType() {}
func (*IntervalType) columnType()    {}
func (*StringType) columnType()      {}
func (*BytesType) columnType()       {}

// Pre-allocated immutable boolean column types.
var (
	boolTypeBool    = &BoolType{Name: "BOOL"}
	boolTypeBoolean = &BoolType{Name: "BOOLEAN"}
)

// BoolType represents a BOOLEAN type.
type BoolType struct {
	Name string
}

func (node *BoolType) String() string {
	return node.Name
}

// Pre-allocated immutable integer column types.
var (
	intTypeBit      = &IntType{Name: "BIT"}
	intTypeInt      = &IntType{Name: "INT"}
	intTypeInt64    = &IntType{Name: "INT64"}
	intTypeInteger  = &IntType{Name: "INTEGER"}
	intTypeSmallInt = &IntType{Name: "SMALLINT"}
	intTypeBigInt   = &IntType{Name: "BIGINT"}
)

// IntType represents an INT, INTEGER, SMALLINT or BIGINT type.
type IntType struct {
	Name string
	N    int
}

func (node *IntType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	return buf.String()
}

// Pre-allocated immutable float column types.
var (
	floatTypeReal   = &FloatType{Name: "REAL"}
	floatTypeFloat  = &FloatType{Name: "FLOAT"}
	floatTypeDouble = &FloatType{Name: "DOUBLE PRECISION"}
)

// FloatType represents a REAL, DOUBLE or FLOAT type.
type FloatType struct {
	Name string
	Prec int
}

func newFloatType(prec int) *FloatType {
	if prec == 0 {
		return floatTypeFloat
	}
	return &FloatType{Name: "FLOAT", Prec: prec}
}

func (node *FloatType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(&buf, "(%d)", node.Prec)
	}
	return buf.String()
}

// Pre-allocated immutable decimal column types.
var (
	decimalTypeDec     = &DecimalType{Name: "DEC"}
	decimalTypeDecimal = &DecimalType{Name: "DECIMAL"}
	decimalTypeNumeric = &DecimalType{Name: "NUMERIC"}
)

// DecimalType represents a DECIMAL or NUMERIC type.
type DecimalType struct {
	Name  string
	Prec  int
	Scale int
}

func (node *DecimalType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(&buf, "(%d", node.Prec)
		if node.Scale > 0 {
			fmt.Fprintf(&buf, ",%d", node.Scale)
		}
		buf.WriteString(")")
	}
	return buf.String()
}

// Pre-allocated immutable date column type.
var dateTypeDate = &DateType{}

// DateType represents a DATE type.
type DateType struct {
}

func (node *DateType) String() string {
	return "DATE"
}

// Pre-allocated immutable timestamp column type.
var timestampTypeTimestamp = &TimestampType{}

// TimestampType represents a TIMESTAMP type.
type TimestampType struct {
}

func (node *TimestampType) String() string {
	return "TIMESTAMP"
}

// Pre-allocated immutable timestamp with time zone column type.
var timestampTzTypeTimestampWithTZ = &TimestampTZType{}

// TimestampTZType represents a TIMESTAMP type.
type TimestampTZType struct {
}

func (node *TimestampTZType) String() string {
	return "TIMESTAMP WITH TIME ZONE"
}

// Pre-allocated immutable interval column type.
var intervalTypeInterval = &IntervalType{}

// IntervalType represents an INTERVAL type
type IntervalType struct {
}

func (node *IntervalType) String() string {
	return "INTERVAL"
}

// Pre-allocated immutable string column types.
var (
	stringTypeChar    = &StringType{Name: "CHAR"}
	stringTypeVarChar = &StringType{Name: "VARCHAR"}
	stringTypeString  = &StringType{Name: "STRING"}
	stringTypeText    = &StringType{Name: "TEXT"}
)

// StringType represents a STRING, CHAR or VARCHAR type.
type StringType struct {
	Name string
	N    int
}

func (node *StringType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	return buf.String()
}

// Pre-allocated immutable bytes column types.
var (
	bytesTypeBlob  = &BytesType{Name: "BLOB"}
	bytesTypeBytes = &BytesType{Name: "BYTES"}
	bytesTypeBytea = &BytesType{Name: "BYTEA"}
)

// BytesType represents a BYTES or BLOB type.
type BytesType struct {
	Name string
}

func (node *BytesType) String() string {
	return node.Name
}
