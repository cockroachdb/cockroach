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

	"github.com/pkg/errors"
)

// ColumnType represents a type in a column definition.
type ColumnType interface {
	fmt.Stringer
	NodeFormatter
	columnType()
}

func (*BoolColType) columnType()        {}
func (*IntColType) columnType()         {}
func (*FloatColType) columnType()       {}
func (*DecimalColType) columnType()     {}
func (*DateColType) columnType()        {}
func (*TimestampColType) columnType()   {}
func (*TimestampTZColType) columnType() {}
func (*IntervalColType) columnType()    {}
func (*StringColType) columnType()      {}
func (*BytesColType) columnType()       {}

// Pre-allocated immutable boolean column types.
var (
	boolColTypeBool    = &BoolColType{Name: "BOOL"}
	boolColTypeBoolean = &BoolColType{Name: "BOOLEAN"}
)

// BoolColType represents a BOOLEAN type.
type BoolColType struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (node *BoolColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Name)
}

// Pre-allocated immutable integer column types.
var (
	intColTypeBit         = &IntColType{Name: "BIT", N: 1, ImplicitWidth: true}
	intColTypeInt         = &IntColType{Name: "INT"}
	intColTypeInt64       = &IntColType{Name: "INT64"}
	intColTypeInteger     = &IntColType{Name: "INTEGER"}
	intColTypeSmallInt    = &IntColType{Name: "SMALLINT"}
	intColTypeBigInt      = &IntColType{Name: "BIGINT"}
	intColTypeSerial      = &IntColType{Name: "SERIAL"}
	intColTypeSmallSerial = &IntColType{Name: "SMALLSERIAL"}
	intColTypeBigSerial   = &IntColType{Name: "BIGSERIAL"}
)

var errBitLengthNotPositive = errors.New("length for type bit must be at least 1")

func newIntBitType(n int) (*IntColType, error) {
	if n < 1 {
		return nil, errBitLengthNotPositive
	}
	return &IntColType{Name: "BIT", N: n}, nil
}

// IntColType represents an INT, INTEGER, SMALLINT or BIGINT type.
type IntColType struct {
	Name          string
	N             int
	ImplicitWidth bool
}

// Format implements the NodeFormatter interface.
func (node *IntColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 && !node.ImplicitWidth {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
}

// IsSerial returns true when this column should be given a DEFAULT of a unique,
// incrementing function.
func (node *IntColType) IsSerial() bool {
	return node.Name == intColTypeSerial.Name || node.Name == intColTypeSmallSerial.Name ||
		node.Name == intColTypeBigSerial.Name
}

// Pre-allocated immutable float column types.
var (
	floatColTypeReal   = &FloatColType{Name: "REAL"}
	floatColTypeFloat  = &FloatColType{Name: "FLOAT"}
	floatColTypeDouble = &FloatColType{Name: "DOUBLE PRECISION"}
)

// FloatColType represents a REAL, DOUBLE or FLOAT type.
type FloatColType struct {
	Name string
	Prec int
}

func newFloatColType(prec int) *FloatColType {
	if prec == 0 {
		return floatColTypeFloat
	}
	return &FloatColType{Name: "FLOAT", Prec: prec}
}

// Format implements the NodeFormatter interface.
func (node *FloatColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(buf, "(%d)", node.Prec)
	}
}

// Pre-allocated immutable decimal column types.
var (
	decimalColTypeDec     = &DecimalColType{Name: "DEC"}
	decimalColTypeDecimal = &DecimalColType{Name: "DECIMAL"}
	decimalColTypeNumeric = &DecimalColType{Name: "NUMERIC"}
)

// DecimalColType represents a DECIMAL or NUMERIC type.
type DecimalColType struct {
	Name  string
	Prec  int
	Scale int
}

// Format implements the NodeFormatter interface.
func (node *DecimalColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(buf, "(%d", node.Prec)
		if node.Scale > 0 {
			fmt.Fprintf(buf, ",%d", node.Scale)
		}
		buf.WriteByte(')')
	}
}

// Pre-allocated immutable date column type.
var dateColTypeDate = &DateColType{}

// DateColType represents a DATE type.
type DateColType struct {
}

// Format implements the NodeFormatter interface.
func (node *DateColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DATE")
}

// Pre-allocated immutable timestamp column type.
var timestampColTypeTimestamp = &TimestampColType{}

// TimestampColType represents a TIMESTAMP type.
type TimestampColType struct {
}

// Format implements the NodeFormatter interface.
func (node *TimestampColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("TIMESTAMP")
}

// Pre-allocated immutable timestamp with time zone column type.
var timestampTzColTypeTimestampWithTZ = &TimestampTZColType{}

// TimestampTZColType represents a TIMESTAMP type.
type TimestampTZColType struct {
}

// Format implements the NodeFormatter interface.
func (node *TimestampTZColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("TIMESTAMP WITH TIME ZONE")
}

// Pre-allocated immutable interval column type.
var intervalColTypeInterval = &IntervalColType{}

// IntervalColType represents an INTERVAL type
type IntervalColType struct {
}

// Format implements the NodeFormatter interface.
func (node *IntervalColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("INTERVAL")
}

// Pre-allocated immutable string column types.
var (
	stringColTypeChar    = &StringColType{Name: "CHAR"}
	stringColTypeVarChar = &StringColType{Name: "VARCHAR"}
	stringColTypeString  = &StringColType{Name: "STRING"}
	stringColTypeText    = &StringColType{Name: "TEXT"}
)

// StringColType represents a STRING, CHAR or VARCHAR type.
type StringColType struct {
	Name string
	N    int
}

// Format implements the NodeFormatter interface.
func (node *StringColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
}

// Pre-allocated immutable bytes column types.
var (
	bytesColTypeBlob  = &BytesColType{Name: "BLOB"}
	bytesColTypeBytes = &BytesColType{Name: "BYTES"}
	bytesColTypeBytea = &BytesColType{Name: "BYTEA"}
)

// BytesColType represents a BYTES or BLOB type.
type BytesColType struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (node *BytesColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Name)
}

func (node *BoolColType) String() string        { return AsString(node) }
func (node *IntColType) String() string         { return AsString(node) }
func (node *FloatColType) String() string       { return AsString(node) }
func (node *DecimalColType) String() string     { return AsString(node) }
func (node *DateColType) String() string        { return AsString(node) }
func (node *TimestampColType) String() string   { return AsString(node) }
func (node *TimestampTZColType) String() string { return AsString(node) }
func (node *IntervalColType) String() string    { return AsString(node) }
func (node *StringColType) String() string      { return AsString(node) }
func (node *BytesColType) String() string       { return AsString(node) }

// DatumTypeToColumnType produces a SQL column type equivalent to the
// given Datum type. Used to generate CastExpr nodes during
// normalization.
func DatumTypeToColumnType(d Datum) (ColumnType, error) {
	switch d.(type) {
	case *DInt:
		return intColTypeInt, nil
	case *DFloat:
		return floatColTypeFloat, nil
	case *DDecimal:
		return decimalColTypeDecimal, nil
	case *DTimestamp:
		return timestampColTypeTimestamp, nil
	case *DTimestampTZ:
		return timestampTzColTypeTimestampWithTZ, nil
	case *DInterval:
		return intervalColTypeInterval, nil
	case *DDate:
		return dateColTypeDate, nil
	case *DString:
		return stringColTypeString, nil
	case *DBytes:
		return bytesColTypeBytes, nil
	}
	return nil, errors.Errorf("internal error: unknown Datum type %T", d)
}
