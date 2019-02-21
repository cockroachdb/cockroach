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

// TDate represents a DATE type.
type TDate struct{}

// TypeName implements the ColTypeFormatter interface.
func (node *TDate) TypeName() string { return "DATE" }

// Format implements the ColTypeFormatter interface.
func (node *TDate) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}

// TTime represents a TIME type.
type TTime struct {
	// PrecisionSet indicates whether the Precision type has been set.
	// Note that this information is only used to round-trip the types
	// during pretty-printing of an AST. It is not further used
	// in CockroachDB. (See issue #32565)
	PrecisionSet bool
	Precision    int
}

// TypeName implements the ColTypeFormatter interface.
func (node *TTime) TypeName() string { return "TIME" }

// Format implements the ColTypeFormatter interface.
func (node *TTime) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
	if node.PrecisionSet {
		fmt.Fprintf(buf, "(%d)", node.Precision)
	}
}

// TTimestamp represents a TIMESTAMP type.
type TTimestamp struct {
	// PrecisionSet indicates whether the Precision type has been set.
	// Note that this information is only used to round-trip the types
	// during pretty-printing of an AST. It is not further used
	// in CockroachDB. (See issue #32098)
	PrecisionSet bool
	Precision    int
}

// TypeName implements the ColTypeFormatter interface.
func (node *TTimestamp) TypeName() string { return "TIMESTAMP" }

// Format implements the ColTypeFormatter interface.
func (node *TTimestamp) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
	if node.PrecisionSet {
		fmt.Fprintf(buf, "(%d)", node.Precision)
	}
}

// TTimestampTZ represents a TIMESTAMP type.
type TTimestampTZ struct {
	// PrecisionSet indicates whether the Precision type has been set.
	// Note that this information is only used to round-trip the types
	// during pretty-printing of an AST. It is not further used
	// in CockroachDB. (See issue #32098)
	PrecisionSet bool
	Precision    int
}

// TypeName implements the ColTypeFormatter interface.
func (node *TTimestampTZ) TypeName() string { return "TIMESTAMPTZ" }

// Format implements the ColTypeFormatter interface.
func (node *TTimestampTZ) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
	if node.PrecisionSet {
		fmt.Fprintf(buf, "(%d)", node.Precision)
	}
}

// TInterval represents an INTERVAL type
type TInterval struct{}

// TypeName implements the ColTypeFormatter interface.
func (node *TInterval) TypeName() string { return "INTERVAL" }

// Format implements the ColTypeFormatter interface.
func (node *TInterval) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}
