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

// TString represents a STRING, CHAR or VARCHAR type.
type TString struct {
	Name string
	N    int
}

// TypeName implements the ColTypeFormatter interface.
func (node *TString) TypeName() string { return node.Name }

// Format implements the ColTypeFormatter interface.
func (node *TString) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
}

// TName represents a a NAME type.
type TName struct{}

// TypeName implements the ColTypeFormatter interface.
func (node *TName) TypeName() string { return "NAME" }

// Format implements the ColTypeFormatter interface.
func (node *TName) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}

// TBytes represents a BYTES or BLOB type.
type TBytes struct{}

// TypeName implements the ColTypeFormatter interface.
func (node *TBytes) TypeName() string { return "BYTES" }

// Format implements the ColTypeFormatter interface.
func (node *TBytes) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}

// TBitArray represents a BIT or VARBIT type.
type TBitArray struct {
	// Width is the maximum number of bits.
	// Always specified for BIT, may be 0 for VARBIT.
	Width uint
	// Variable distinguishes between BIT and VARBIT.
	Variable bool
}

// TypeName implements the ColTypeFormatter interface.
func (node *TBitArray) TypeName() string {
	if node.Variable {
		return "VARBIT"
	}
	return "BIT"
}

// Format implements the ColTypeFormatter interface.
func (node *TBitArray) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
	if node.Width > 0 {
		if node.Width == 1 && !node.Variable {
			// BIT(1) pretty-prints as just BIT.
			return
		}
		fmt.Fprintf(buf, "(%d)", node.Width)
	}
}

// TCollatedString represents a STRING, CHAR or VARCHAR type with a
// collation locale.
type TCollatedString struct {
	Name   string
	N      int
	Locale string
}

// TypeName implements the ColTypeFormatter interface.
func (node *TCollatedString) TypeName() string { return node.Name }

// Format implements the ColTypeFormatter interface.
func (node *TCollatedString) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
	buf.WriteString(" COLLATE ")
	lex.EncodeUnrestrictedSQLIdent(buf, node.Locale, f)
}
