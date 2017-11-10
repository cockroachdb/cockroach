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

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
)

// StringColType represents a STRING, CHAR or VARCHAR type.
type StringColType struct {
	Name string
	N    int
}

// Format implements the ColTypeFormatter interface.
func (node *StringColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
}

// NameColType represents a a NAME type.
type NameColType struct{}

// Format implements the ColTypeFormatter interface.
func (node *NameColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("NAME")
}

// BytesColType represents a BYTES or BLOB type.
type BytesColType struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *BytesColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

// CollatedStringColType represents a STRING, CHAR or VARCHAR type with a
// collation locale.
type CollatedStringColType struct {
	Name   string
	N      int
	Locale string
}

// Format implements the ColTypeFormatter interface.
func (node *CollatedStringColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
	buf.WriteString(" COLLATE ")
	lex.EncodeUnrestrictedSQLIdent(buf, node.Locale, f)
}
