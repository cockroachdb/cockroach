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
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// BoolColType represents a BOOLEAN type.
type BoolColType struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *BoolColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

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

var errScaleOutOfRange = pgerror.NewError(pgerror.CodeNumericValueOutOfRangeError, "scale out of range")

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
