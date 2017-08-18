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

package base

import (
	"bytes"
	"fmt"
)

type SexprFormatter interface {
	FormatSExpr(buf *bytes.Buffer)
}

func FormatSExprBool(buf *bytes.Buffer, x bool)       { fmt.Fprintf(buf, "%v", x) }
func FormatSExprInt64(buf *bytes.Buffer, x int64)     { fmt.Fprintf(buf, "%v", x) }
func FormatSExprInt32(buf *bytes.Buffer, x int32)     { fmt.Fprintf(buf, "%v", x) }
func FormatSExprInt16(buf *bytes.Buffer, x int16)     { fmt.Fprintf(buf, "%v", x) }
func FormatSExprRune(buf *bytes.Buffer, x rune)       { fmt.Fprintf(buf, "%q", x) }
func FormatSExprInt8(buf *bytes.Buffer, x int8)       { fmt.Fprintf(buf, "%v", x) }
func FormatSExprUint64(buf *bytes.Buffer, x uint64)   { fmt.Fprintf(buf, "%v", x) }
func FormatSExprUint32(buf *bytes.Buffer, x uint32)   { fmt.Fprintf(buf, "%v", x) }
func FormatSExprUint16(buf *bytes.Buffer, x uint16)   { fmt.Fprintf(buf, "%v", x) }
func FormatSExprUint8(buf *bytes.Buffer, x uint8)     { fmt.Fprintf(buf, "%v", x) }
func FormatSExprByte(buf *bytes.Buffer, x byte)       { fmt.Fprintf(buf, "%q", x) }
func FormatSExprString(buf *bytes.Buffer, x string)   { fmt.Fprintf(buf, "%q", x) }
func FormatSExprFloat32(buf *bytes.Buffer, x float32) { fmt.Fprintf(buf, "%v", x) }
func FormatSExprFloat64(buf *bytes.Buffer, x float64) { fmt.Fprintf(buf, "%v", x) }

// @for enum

func (x Enum) FormatSExpr(buf *bytes.Buffer) {
	buf.WriteString(x.String())
}

// @done enum

// @for struct

func (x Struct) FormatSExpr(buf *bytes.Buffer) {
	buf.WriteString("(Struct")

	// @for item

	buf.WriteString(" Name: ")
	// @if isNotPrimitive
	x.Name().FormatSExpr(buf)
	// @fi isNotPrimitive
	// @if isPrimitive
	FormatSExprTypName(buf, x.Name())
	// @fi isPrimitive

	// @done item

	buf.WriteByte(')')
}

// @done struct

// @for sum

func (x Sum) FormatSExpr(buf *bytes.Buffer) {
	switch x.Tag() {
	// @for item
	case SumType:
		x.MustBeType().FormatSExpr(buf)
		// @done item
	}
}

// @done sum
