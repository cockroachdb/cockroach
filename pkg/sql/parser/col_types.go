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
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// ColTypeFormatter knows how to format a ColType to a bytes.Buffer.
type ColTypeFormatter interface {
	fmt.Stringer
	Format(buf *bytes.Buffer, flags lex.EncodeFlags)
}

// ColTypeAsString print a ColumnType to a string.
func ColTypeAsString(n ColTypeFormatter) string {
	var buf bytes.Buffer
	n.Format(&buf, lex.EncodeFlags{})
	return buf.String()
}

// CastTargetType represents a type that is a valid cast target.
type CastTargetType interface {
	ColTypeFormatter
	castTargetType()
}

// ColumnType represents a type in a column definition.
type ColumnType interface {
	CastTargetType

	columnType()
}

func (*BoolColType) columnType()           {}
func (*IntColType) columnType()            {}
func (*FloatColType) columnType()          {}
func (*DecimalColType) columnType()        {}
func (*DateColType) columnType()           {}
func (*TimestampColType) columnType()      {}
func (*TimestampTZColType) columnType()    {}
func (*IntervalColType) columnType()       {}
func (*JSONColType) columnType()           {}
func (*UUIDColType) columnType()           {}
func (*IPAddrColType) columnType()         {}
func (*StringColType) columnType()         {}
func (*NameColType) columnType()           {}
func (*BytesColType) columnType()          {}
func (*CollatedStringColType) columnType() {}
func (*ArrayColType) columnType()          {}
func (*VectorColType) columnType()         {}
func (*OidColType) columnType()            {}

// All ColumnTypes also implement CastTargetType.
func (*BoolColType) castTargetType()           {}
func (*IntColType) castTargetType()            {}
func (*FloatColType) castTargetType()          {}
func (*DecimalColType) castTargetType()        {}
func (*DateColType) castTargetType()           {}
func (*TimestampColType) castTargetType()      {}
func (*TimestampTZColType) castTargetType()    {}
func (*IntervalColType) castTargetType()       {}
func (*JSONColType) castTargetType()           {}
func (*UUIDColType) castTargetType()           {}
func (*IPAddrColType) castTargetType()         {}
func (*StringColType) castTargetType()         {}
func (*NameColType) castTargetType()           {}
func (*BytesColType) castTargetType()          {}
func (*CollatedStringColType) castTargetType() {}
func (*ArrayColType) castTargetType()          {}
func (*VectorColType) castTargetType()         {}
func (*OidColType) castTargetType()            {}

// BoolColType represents a BOOLEAN type.
type BoolColType struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *BoolColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

var (
	errScaleOutOfRange = pgerror.NewError(pgerror.CodeNumericValueOutOfRangeError, "scale out of range")
)

// IntColType represents an INT, INTEGER, SMALLINT or BIGINT type.
type IntColType struct {
	Name          string
	Width         int
	ImplicitWidth bool
}

// Format implements the ColTypeFormatter interface.
func (node *IntColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.Width > 0 && !node.ImplicitWidth {
		fmt.Fprintf(buf, "(%d)", node.Width)
	}
}

// IsSerial returns true when this column should be given a DEFAULT of a unique,
// incrementing function.
func (node *IntColType) IsSerial() bool {
	return node.Name == intColTypeSerial.Name || node.Name == intColTypeSmallSerial.Name ||
		node.Name == intColTypeBigSerial.Name
}

// FloatColType represents a REAL, DOUBLE or FLOAT type.
type FloatColType struct {
	Name          string
	Prec          int
	Width         int
	PrecSpecified bool // true if the value of Prec is not the default
}

// Format implements the ColTypeFormatter interface.
func (node *FloatColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(buf, "(%d)", node.Prec)
	}
}

// DecimalColType represents a DECIMAL or NUMERIC type.
type DecimalColType struct {
	Name  string
	Prec  int
	Scale int
}

// Format implements the ColTypeFormatter interface.
func (node *DecimalColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(buf, "(%d", node.Prec)
		if node.Scale > 0 {
			fmt.Fprintf(buf, ",%d", node.Scale)
		}
		buf.WriteByte(')')
	}
}

// LimitDecimalWidth limits d's precision (total number of digits) and scale
// (number of digits after the decimal point).
func LimitDecimalWidth(d *apd.Decimal, precision, scale int) error {
	if d.Form != apd.Finite || precision <= 0 {
		return nil
	}
	// Use +1 here because it is inverted later.
	if scale < math.MinInt32+1 || scale > math.MaxInt32 {
		return errScaleOutOfRange
	}
	if scale > precision {
		return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "scale (%d) must be between 0 and precision (%d)", scale, precision)
	}

	// http://www.postgresql.org/docs/9.5/static/datatype-numeric.html
	// "If the scale of a value to be stored is greater than
	// the declared scale of the column, the system will round the
	// value to the specified number of fractional digits. Then,
	// if the number of digits to the left of the decimal point
	// exceeds the declared precision minus the declared scale, an
	// error is raised."

	c := DecimalCtx.WithPrecision(uint32(precision))
	c.Traps = apd.InvalidOperation

	if _, err := c.Quantize(d, d, -int32(scale)); err != nil {
		var lt string
		switch v := precision - scale; v {
		case 0:
			lt = "1"
		default:
			lt = fmt.Sprintf("10^%d", v)
		}
		return pgerror.NewErrorf(pgerror.CodeNumericValueOutOfRangeError, "value with precision %d, scale %d must round to an absolute value less than %s", precision, scale, lt)
	}
	return nil
}

// DateColType represents a DATE type.
type DateColType struct {
}

// Format implements the ColTypeFormatter interface.
func (node *DateColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("DATE")
}

// TimestampColType represents a TIMESTAMP type.
type TimestampColType struct {
}

// Format implements the ColTypeFormatter interface.
func (node *TimestampColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("TIMESTAMP")
}

// TimestampTZColType represents a TIMESTAMP type.
type TimestampTZColType struct {
}

// Format implements the ColTypeFormatter interface.
func (node *TimestampTZColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("TIMESTAMP WITH TIME ZONE")
}

// IntervalColType represents an INTERVAL type
type IntervalColType struct {
}

// Format implements the ColTypeFormatter interface.
func (node *IntervalColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("INTERVAL")
}

// UUIDColType represents a UUID type.
type UUIDColType struct {
}

// Format implements the ColTypeFormatter interface.
func (node *UUIDColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("UUID")
}

// IPAddrColType represents an INET or CIDR type.
type IPAddrColType struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *IPAddrColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

// StringColType represents a STRING, CHAR or VARCHAR type.
type StringColType struct {
	Name string
	N    int
}

// Format implements the ColTypeFormatter interface.
func (node *StringColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
}

// NameColType represents a a NAME type.
type NameColType struct{}

// Format implements the ColTypeFormatter interface.
func (node *NameColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("NAME")
}

// BytesColType represents a BYTES or BLOB type.
type BytesColType struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *BytesColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

// CollatedStringColType represents a STRING, CHAR or VARCHAR type with a
// collation locale.
type CollatedStringColType struct {
	Name   string
	N      int
	Locale string
}

// Format implements the ColTypeFormatter interface.
func (node *CollatedStringColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
	buf.WriteString(" COLLATE ")
	lex.EncodeUnrestrictedSQLIdent(buf, node.Locale, f)
}

// ArrayColType represents an ARRAY column type.
type ArrayColType struct {
	Name string
	// ParamTyp is the type of the elements in this array.
	ParamType ColumnType
	Bounds    []int32
}

// Format implements the ColTypeFormatter interface.
func (node *ArrayColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if collation, ok := node.ParamType.(*CollatedStringColType); ok {
		buf.WriteString(" COLLATE ")
		lex.EncodeUnrestrictedSQLIdent(buf, collation.Locale, f)
	}
}

// VectorColType is the base for VECTOR column types, which are Postgres's
// older, limited version of ARRAYs. These are not meant to be persisted,
// because ARRAYs are a strict superset.
type VectorColType struct {
	Name      string
	ParamType ColumnType
}

// Format implements the ColTypeFormatter interface.
func (node *VectorColType) Format(buf *bytes.Buffer, _ lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

// JSONColType represents the JSON column type.
type JSONColType struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *JSONColType) Format(buf *bytes.Buffer, _ lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

// OidColType represents an OID type, which is the type of system object
// identifiers. There are several different OID types: the raw OID type, which
// can be any integer, and the reg* types, each of which corresponds to the
// particular system table that contains the system object identified by the
// OID itself.
//
// See https://www.postgresql.org/docs/9.6/static/datatype-oid.html.
type OidColType struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *OidColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

func (node *BoolColType) String() string           { return ColTypeAsString(node) }
func (node *IntColType) String() string            { return ColTypeAsString(node) }
func (node *FloatColType) String() string          { return ColTypeAsString(node) }
func (node *DecimalColType) String() string        { return ColTypeAsString(node) }
func (node *DateColType) String() string           { return ColTypeAsString(node) }
func (node *TimestampColType) String() string      { return ColTypeAsString(node) }
func (node *TimestampTZColType) String() string    { return ColTypeAsString(node) }
func (node *IntervalColType) String() string       { return ColTypeAsString(node) }
func (node *JSONColType) String() string           { return ColTypeAsString(node) }
func (node *UUIDColType) String() string           { return ColTypeAsString(node) }
func (node *IPAddrColType) String() string         { return ColTypeAsString(node) }
func (node *StringColType) String() string         { return ColTypeAsString(node) }
func (node *NameColType) String() string           { return ColTypeAsString(node) }
func (node *BytesColType) String() string          { return ColTypeAsString(node) }
func (node *CollatedStringColType) String() string { return ColTypeAsString(node) }
func (node *ArrayColType) String() string          { return ColTypeAsString(node) }
func (node *VectorColType) String() string         { return ColTypeAsString(node) }
func (node *OidColType) String() string            { return ColTypeAsString(node) }
