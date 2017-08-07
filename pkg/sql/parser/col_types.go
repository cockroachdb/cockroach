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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/pkg/errors"
)

// CastTargetType represents a type that is a valid cast target.
type CastTargetType interface {
	fmt.Stringer
	NodeFormatter

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
func (*UUIDColType) columnType()           {}
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
func (*UUIDColType) castTargetType()           {}
func (*StringColType) castTargetType()         {}
func (*NameColType) castTargetType()           {}
func (*BytesColType) castTargetType()          {}
func (*CollatedStringColType) castTargetType() {}
func (*ArrayColType) castTargetType()          {}
func (*VectorColType) castTargetType()         {}
func (*OidColType) castTargetType()            {}

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
	intColTypeBit         = &IntColType{Name: "BIT", Width: 1, ImplicitWidth: true}
	intColTypeInt         = &IntColType{Name: "INT"}
	intColTypeInt2        = &IntColType{Name: "INT2", Width: 16, ImplicitWidth: true}
	intColTypeInt4        = &IntColType{Name: "INT4", Width: 32, ImplicitWidth: true}
	intColTypeInt8        = &IntColType{Name: "INT8"}
	intColTypeInt64       = &IntColType{Name: "INT64"}
	intColTypeInteger     = &IntColType{Name: "INTEGER"}
	intColTypeSmallInt    = &IntColType{Name: "SMALLINT", Width: 16, ImplicitWidth: true}
	intColTypeBigInt      = &IntColType{Name: "BIGINT"}
	intColTypeSerial      = &IntColType{Name: "SERIAL"}
	intColTypeSmallSerial = &IntColType{Name: "SMALLSERIAL"}
	intColTypeBigSerial   = &IntColType{Name: "BIGSERIAL"}
)

var errBitLengthNotPositive = errors.New("length for type bit must be at least 1")

func newIntBitType(width int) (*IntColType, error) {
	if width < 1 {
		return nil, errBitLengthNotPositive
	}
	return &IntColType{Name: "BIT", Width: width}, nil
}

// IntColType represents an INT, INTEGER, SMALLINT or BIGINT type.
type IntColType struct {
	Name          string
	Width         int
	ImplicitWidth bool
}

// Format implements the NodeFormatter interface.
func (node *IntColType) Format(buf *bytes.Buffer, f FmtFlags) {
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

// Pre-allocated immutable float column types.
var (
	floatColTypeReal   = &FloatColType{Name: "REAL", Width: 32}
	floatColTypeFloat  = &FloatColType{Name: "FLOAT", Width: 64}
	floatColTypeFloat4 = &FloatColType{Name: "FLOAT4", Width: 32}
	floatColTypeFloat8 = &FloatColType{Name: "FLOAT8", Width: 64}
	floatColTypeDouble = &FloatColType{Name: "DOUBLE PRECISION", Width: 64}
)

// FloatColType represents a REAL, DOUBLE or FLOAT type.
type FloatColType struct {
	Name          string
	Prec          int
	Width         int
	PrecSpecified bool // true if the value of Prec is not the default
}

// NewFloatColType creates a type representing a FLOAT, optionally with a
// precision.
func NewFloatColType(prec int, precSpecified bool) *FloatColType {
	if prec == 0 && !precSpecified {
		return floatColTypeFloat
	}
	return &FloatColType{Name: "FLOAT", Width: 64, Prec: prec, PrecSpecified: precSpecified}
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

// LimitDecimalWidth limits d's precision (total number of digits) and scale
// (number of digits after the decimal point).
func LimitDecimalWidth(d *apd.Decimal, precision, scale int) error {
	if d.Form != apd.Finite || precision <= 0 {
		return nil
	}
	// Use +1 here because it is inverted later.
	if scale < math.MinInt32+1 || scale > math.MaxInt32 {
		return errors.New("scale out of range")
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

// Pre-allocated immutable uuid column type.
var uuidColTypeUUID = &UUIDColType{}

// UUIDColType represents a UUID type.
type UUIDColType struct {
}

// Format implements the NodeFormatter interface.
func (node *UUIDColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("UUID")
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

// Pre-allocated immutable name column type.
var nameColTypeName = &NameColType{}

// NameColType represents a a NAME type.
type NameColType struct{}

// Format implements the NodeFormatter interface.
func (node *NameColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("NAME")
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

// CollatedStringColType represents a STRING, CHAR or VARCHAR type with a
// collation locale.
type CollatedStringColType struct {
	Name   string
	N      int
	Locale string
}

// Format implements the NodeFormatter interface.
func (node *CollatedStringColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
	buf.WriteString(" COLLATE ")
	encodeSQLIdent(buf, node.Locale, f)
}

// ArrayColType represents an ARRAY column type.
type ArrayColType struct {
	Name string
	// ParamTyp is the type of the elements in this array.
	ParamType   ColumnType
	BoundsExprs Exprs
}

// Format implements the NodeFormatter interface.
func (node *ArrayColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Name)
	if collation, ok := node.ParamType.(*CollatedStringColType); ok {
		buf.WriteString(" COLLATE ")
		encodeSQLIdent(buf, collation.Locale, f)
	}
}

func arrayOf(colType ColumnType, boundsExprs Exprs) (ColumnType, error) {
	return &ArrayColType{Name: colType.String() + "[]", ParamType: colType, BoundsExprs: boundsExprs}, nil
}

// VectorColType is the base for VECTOR column types, which are Postgres's
// older, limited version of ARRAYs. These are not meant to be persisted,
// because ARRAYs are a strict superset.
type VectorColType struct {
	Name      string
	ParamType ColumnType
}

// Format implements the NodeFormatter interface.
func (node *VectorColType) Format(buf *bytes.Buffer, _ FmtFlags) {
	buf.WriteString(node.Name)
}

// Int2VectorColType represents an INT2VECTOR column type.
var int2vectorColType = &VectorColType{
	Name:      "INT2VECTOR",
	ParamType: intColTypeInt,
}

// Pre-allocated immutable postgres oid column types.
var (
	oidColTypeOid          = &OidColType{Name: "OID"}
	oidColTypeRegClass     = &OidColType{Name: "REGCLASS"}
	oidColTypeRegNamespace = &OidColType{Name: "REGNAMESPACE"}
	oidColTypeRegProc      = &OidColType{Name: "REGPROC"}
	oidColTypeRegProcedure = &OidColType{Name: "REGPROCEDURE"}
	oidColTypeRegType      = &OidColType{Name: "REGTYPE"}
)

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

// Format implements the NodeFormatter interface.
func (node *OidColType) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Name)
}

func oidColTypeToType(ct *OidColType) Type {
	switch ct {
	case oidColTypeOid:
		return TypeOid
	case oidColTypeRegClass:
		return TypeRegClass
	case oidColTypeRegNamespace:
		return TypeRegNamespace
	case oidColTypeRegProc:
		return TypeRegProc
	case oidColTypeRegProcedure:
		return TypeRegProcedure
	case oidColTypeRegType:
		return TypeRegType
	default:
		panic(fmt.Sprintf("unexpected *OidColType: %v", ct))
	}
}

func oidTypeToColType(t Type) *OidColType {
	switch t {
	case TypeOid:
		return oidColTypeOid
	case TypeRegClass:
		return oidColTypeRegClass
	case TypeRegNamespace:
		return oidColTypeRegNamespace
	case TypeRegProc:
		return oidColTypeRegProc
	case TypeRegProcedure:
		return oidColTypeRegProcedure
	case TypeRegType:
		return oidColTypeRegType
	default:
		panic(fmt.Sprintf("unexpected type: %v", t))
	}
}

func (node *BoolColType) String() string           { return AsString(node) }
func (node *IntColType) String() string            { return AsString(node) }
func (node *FloatColType) String() string          { return AsString(node) }
func (node *DecimalColType) String() string        { return AsString(node) }
func (node *DateColType) String() string           { return AsString(node) }
func (node *TimestampColType) String() string      { return AsString(node) }
func (node *TimestampTZColType) String() string    { return AsString(node) }
func (node *IntervalColType) String() string       { return AsString(node) }
func (node *UUIDColType) String() string           { return AsString(node) }
func (node *StringColType) String() string         { return AsString(node) }
func (node *NameColType) String() string           { return AsString(node) }
func (node *BytesColType) String() string          { return AsString(node) }
func (node *CollatedStringColType) String() string { return AsString(node) }
func (node *ArrayColType) String() string          { return AsString(node) }
func (node *VectorColType) String() string         { return AsString(node) }
func (node *OidColType) String() string            { return AsString(node) }

// DatumTypeToColumnType produces a SQL column type equivalent to the
// given Datum type. Used to generate CastExpr nodes during
// normalization.
func DatumTypeToColumnType(t Type) (ColumnType, error) {
	switch t {
	case TypeBool:
		return boolColTypeBool, nil
	case TypeInt:
		return intColTypeInt, nil
	case TypeFloat:
		return floatColTypeFloat, nil
	case TypeDecimal:
		return decimalColTypeDecimal, nil
	case TypeTimestamp:
		return timestampColTypeTimestamp, nil
	case TypeTimestampTZ:
		return timestampTzColTypeTimestampWithTZ, nil
	case TypeInterval:
		return intervalColTypeInterval, nil
	case TypeUUID:
		return uuidColTypeUUID, nil
	case TypeDate:
		return dateColTypeDate, nil
	case TypeString:
		return stringColTypeString, nil
	case TypeName:
		return nameColTypeName, nil
	case TypeBytes:
		return bytesColTypeBytes, nil
	case TypeOid,
		TypeRegClass,
		TypeRegNamespace,
		TypeRegProc,
		TypeRegProcedure,
		TypeRegType:
		return oidTypeToColType(t), nil
	}

	switch typ := t.(type) {
	case TCollatedString:
		return &CollatedStringColType{Name: "STRING", Locale: typ.Locale}, nil
	case TArray:
		elemTyp, err := DatumTypeToColumnType(typ.Typ)
		if err != nil {
			return nil, err
		}
		return arrayOf(elemTyp, Exprs(nil))
	case tOidWrapper:
		return DatumTypeToColumnType(typ.Type)
	}

	return nil, errors.Errorf("value type %s cannot be used for table columns", t)
}

// CastTargetToDatumType produces a Type equivalent to the given
// SQL cast target type.
func CastTargetToDatumType(t CastTargetType) Type {
	switch ct := t.(type) {
	case *BoolColType:
		return TypeBool
	case *IntColType:
		return TypeInt
	case *FloatColType:
		return TypeFloat
	case *DecimalColType:
		return TypeDecimal
	case *StringColType:
		return TypeString
	case *NameColType:
		return TypeName
	case *BytesColType:
		return TypeBytes
	case *DateColType:
		return TypeDate
	case *TimestampColType:
		return TypeTimestamp
	case *TimestampTZColType:
		return TypeTimestampTZ
	case *IntervalColType:
		return TypeInterval
	case *UUIDColType:
		return TypeUUID
	case *CollatedStringColType:
		return TCollatedString{Locale: ct.Locale}
	case *ArrayColType:
		return TArray{CastTargetToDatumType(ct.ParamType)}
	case *VectorColType:
		return TypeIntVector
	case *OidColType:
		return oidColTypeToType(ct)
	default:
		panic(errors.Errorf("unexpected CastTarget %T", t))
	}
}
