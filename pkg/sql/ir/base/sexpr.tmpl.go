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

type SexprFormatter interface {
	FormatSExpr(buf *bytes.Buffer)
}

func FormatSExprInt64(buf *bytes.Buffer, x int64) {
	buf.WriteString(strconv.FormatInt(x, 10))
}

// @for enum

func (x ºEnum) FormatSExpr(buf *bytes.Buffer) {
	buf.WriteString(x.String())
}

// @done enum

// @for struct

func (x ºStruct) FormatSExpr(buf *bytes.Buffer) {
	buf.WriteString("(ºStruct")

	// @for item

	buf.WriteString(" ºItem: ")
	// @if isNotPrimitive
	x.ºItem().FormatSExpr(buf)
	// @fi isNotPrimitive
	// @if isPrimitive
	FormatSExprºType(buf, x.ºItem())
	// @fi isPrimitive

	// @done item

	buf.WriteByte(')')
}

// @done struct

// @for sum

func (x ºSum) FormatSExpr(buf *bytes.Buffer) {
	switch x.Tag() {
	// @for item
	case ºSumºtype:
		x.MustBeºtype().FormatSExpr(buf)
		// @done item
	}
}

// @done sum
