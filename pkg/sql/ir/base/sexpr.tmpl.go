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
	"strconv"
)

// SexprFormatter is implemented by any type that can write its own string
// representation into the provided buffer.
type SexprFormatter interface {
	FormatSExpr(*bytes.Buffer)
}

// FormatSExprInt64 writes the base 10 string representation of x into buf.
func FormatSExprInt64(buf *bytes.Buffer, x int64) {
	buf.WriteString(strconv.FormatInt(x, 10))
}

// @for enum

// FormatSExpr implements the SexprFormatter interface.
func (x Enum) FormatSExpr(buf *bytes.Buffer) {
	buf.WriteString(x.String())
}

// @done enum

// @for struct

// FormatSExpr implements the SexprFormatter interface.
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

// FormatSExpr implements the SexprFormatter interface.
func (x Sum) FormatSExpr(buf *bytes.Buffer) {
	switch x.Tag() {
	// @for item
	case SumType:
		x.MustBeType().FormatSExpr(buf)
		// @done item
	}
}

// @done sum
