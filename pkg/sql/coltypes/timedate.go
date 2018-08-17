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
type TTime struct{}

// TypeName implements the ColTypeFormatter interface.
func (node *TTime) TypeName() string { return "TIME" }

// Format implements the ColTypeFormatter interface.
func (node *TTime) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}

// TTimestamp represents a TIMESTAMP type.
type TTimestamp struct{}

// TypeName implements the ColTypeFormatter interface.
func (node *TTimestamp) TypeName() string { return "TIMESTAMP" }

// Format implements the ColTypeFormatter interface.
func (node *TTimestamp) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}

// TTimestampTZ represents a TIMESTAMP type.
type TTimestampTZ struct{}

// TypeName implements the ColTypeFormatter interface.
func (node *TTimestampTZ) TypeName() string { return "TIMESTAMPTZ" }

// Format implements the ColTypeFormatter interface.
func (node *TTimestampTZ) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("TIMESTAMP WITH TIME ZONE")
}

// TInterval represents an INTERVAL type
type TInterval struct{}

// TypeName implements the ColTypeFormatter interface.
func (node *TInterval) TypeName() string { return "INTERVAL" }

// Format implements the ColTypeFormatter interface.
func (node *TInterval) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}
