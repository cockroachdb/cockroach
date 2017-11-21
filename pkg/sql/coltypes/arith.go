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
type TBool struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *TBool) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

// TInt represents an INT, INTEGER, SMALLINT or BIGINT type.
type TInt struct {
	Name          string
	Width         int
	ImplicitWidth bool
}

// Format implements the ColTypeFormatter interface.
func (node *TInt) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.Width > 0 && !node.ImplicitWidth {
		fmt.Fprintf(buf, "(%d)", node.Width)
	}
}

// IsSerial returns true when this column should be given a DEFAULT of a unique,
// incrementing function.
func (node *TInt) IsSerial() bool {
	return node.Name == Serial.Name || node.Name == SmallSerial.Name ||
		node.Name == BigSerial.Name
}

// TFloat represents a REAL, DOUBLE or FLOAT type.
type TFloat struct {
	Name          string
	Prec          int
	Width         int
	PrecSpecified bool // true if the value of Prec is not the default
}

// Format implements the ColTypeFormatter interface.
func (node *TFloat) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(buf, "(%d)", node.Prec)
	}
}

// TDecimal represents a DECIMAL or NUMERIC type.
type TDecimal struct {
	Name  string
	Prec  int
	Scale int
}

// Format implements the ColTypeFormatter interface.
func (node *TDecimal) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(buf, "(%d", node.Prec)
		if node.Scale > 0 {
			fmt.Fprintf(buf, ",%d", node.Scale)
		}
		buf.WriteByte(')')
	}
}
