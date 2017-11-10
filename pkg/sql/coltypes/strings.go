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

// Format implements the ColTypeFormatter interface.
func (node *TString) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
}

// TName represents a a NAME type.
type TName struct{}

// Format implements the ColTypeFormatter interface.
func (node *TName) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("NAME")
}

// TBytes represents a BYTES or BLOB type.
type TBytes struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *TBytes) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

// TCollatedString represents a STRING, CHAR or VARCHAR type with a
// collation locale.
type TCollatedString struct {
	Name   string
	N      int
	Locale string
}

// Format implements the ColTypeFormatter interface.
func (node *TCollatedString) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
	buf.WriteString(" COLLATE ")
	lex.EncodeUnrestrictedSQLIdent(buf, node.Locale, f)
}
