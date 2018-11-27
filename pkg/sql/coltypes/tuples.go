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

// TTuple represents tuple column types. Tuples aren't writable to disk, but
// all types still need ColType representations.
type TTuple []T

// TypeName implements the ColTypeFormatter interface.
func (node TTuple) TypeName() string { return "" }

// Format implements the ColTypeFormatter interface.
func (node TTuple) Format(buf *bytes.Buffer, flags lex.EncodeFlags) {
	buf.WriteString("(")
	for i := range node {
		if i != 0 {
			buf.WriteString(", ")
		}
		node[i].Format(buf, flags)
	}
	buf.WriteString(")")
}
