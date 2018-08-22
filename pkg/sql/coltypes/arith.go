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

package coltypes

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
)

// TBool represents a BOOLEAN type.
type TBool struct{}

// TypeName implements the ColTypeFormatter interface.
func (node *TBool) TypeName() string { return "BOOL" }

// Format implements the ColTypeFormatter interface.
func (node *TBool) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}

// TInt represents an INT, INTEGER, SMALLINT or BIGINT type.
type TInt struct {
	Width int
}

// IntegerTypeNames maps a TInt data width to a canonical type name.
var IntegerTypeNames = map[int]string{
	0:  "INT",
	16: "INT2",
	32: "INT4",
	64: "INT8",
}

// TypeName implements the ColTypeFormatter interface.
func (node *TInt) TypeName() string { return IntegerTypeNames[node.Width] }

// Format implements the ColTypeFormatter interface.
func (node *TInt) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}

// TSerial represents a SERIAL type.
type TSerial struct{ *TInt }

var serialNames = map[int]string{
	0:  "SERIAL",
	16: "SERIAL2",
	32: "SERIAL4",
	64: "SERIAL8",
}

// TypeName implements the ColTypeFormatter interface.
func (node *TSerial) TypeName() string { return serialNames[node.Width] }

// Format implements the ColTypeFormatter interface.
func (node *TSerial) Format(buf *bytes.Buffer, _ lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}

// TFloat represents a REAL or DOUBLE type.
type TFloat struct{ Short bool }

// TypeName implements the ColTypeFormatter interface.
func (node *TFloat) TypeName() string {
	if node.Short {
		return "FLOAT4"
	}
	return "FLOAT8"
}

// Format implements the ColTypeFormatter interface.
func (node *TFloat) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}

// TDecimal represents a DECIMAL or NUMERIC type.
type TDecimal struct {
	Prec  int
	Scale int
}

// TypeName implements the ColTypeFormatter interface.
func (node *TDecimal) TypeName() string { return "DECIMAL" }

// Format implements the ColTypeFormatter interface.
func (node *TDecimal) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
	if node.Prec > 0 {
		fmt.Fprintf(buf, "(%d", node.Prec)
		if node.Scale > 0 {
			fmt.Fprintf(buf, ",%d", node.Scale)
		}
		buf.WriteByte(')')
	}
}
