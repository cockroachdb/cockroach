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

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
)

// DateColType represents a DATE type.
type DateColType struct{}

// Format implements the ColTypeFormatter interface.
func (node *DateColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("DATE")
}

// TimestampColType represents a TIMESTAMP type.
type TimestampColType struct{}

// Format implements the ColTypeFormatter interface.
func (node *TimestampColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("TIMESTAMP")
}

// TimestampTZColType represents a TIMESTAMP type.
type TimestampTZColType struct{}

// Format implements the ColTypeFormatter interface.
func (node *TimestampTZColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("TIMESTAMP WITH TIME ZONE")
}

// IntervalColType represents an INTERVAL type
type IntervalColType struct{}

// Format implements the ColTypeFormatter interface.
func (node *IntervalColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("INTERVAL")
}
