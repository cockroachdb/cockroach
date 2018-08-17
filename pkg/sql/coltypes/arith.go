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
	Name          string
	Width         int
	ImplicitWidth bool
}

// TypeName implements the ColTypeFormatter interface.
func (node *TInt) TypeName() string { return node.Name }

// Format implements the ColTypeFormatter interface.
func (node *TInt) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.Width > 0 && !node.ImplicitWidth {
		fmt.Fprintf(buf, "(%d)", node.Width)
	}
}

// TSerial represents a SERIAL type.
type TSerial struct {
	IntType *TInt
}

var serialNames = map[*TInt]string{
	Int:  "SERIAL",
	Int2: "SERIAL2",
	Int4: "SERIAL4",
	Int8: "SERIAL8",
}

// TypeName implements the ColTypeFormatter interface.
func (node *TSerial) TypeName() string { return serialNames[node.IntType] }

// Format implements the ColTypeFormatter interface.
func (node *TSerial) Format(buf *bytes.Buffer, _ lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}

// TFloat represents a REAL, DOUBLE or FLOAT type.
type TFloat struct {
	Name          string
	Prec          int
	Width         int
	PrecSpecified bool // true if the value of Prec is not the default
}

// TypeName implements the ColTypeFormatter interface.
func (node *TFloat) TypeName() string { return node.Name }

// Format implements the ColTypeFormatter interface.
func (node *TFloat) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(buf, "(%d)", node.Prec)
	}
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
