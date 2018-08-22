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

// TArray represents an ARRAY column type.
type TArray struct {
	// ParamTyp is the type of the elements in this array.
	ParamType T
	Bounds    []int32
}

// TypeName implements the ColTypeFormatter interface.
func (node *TArray) TypeName() string {
	return node.ParamType.TypeName() + "[]"
}

// Format implements the ColTypeFormatter interface.
func (node *TArray) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	if collation, ok := node.ParamType.(*TCollatedString); ok {
		// We cannot use node.ParamType.Format() directly here (and DRY
		// across the two branches of the if) because if we have an array
		// of collated strings, the COLLATE string must appear after the
		// square brackets.
		collation.TString.Format(buf, f)
		buf.WriteString("[] COLLATE ")
		lex.EncodeUnrestrictedSQLIdent(buf, collation.Locale, f)
	} else {
		node.ParamType.Format(buf, f)
		buf.WriteString("[]")
	}
}

// canBeInArrayColType returns true if the given T is a valid
// element type for an array column type.
func canBeInArrayColType(t T) bool {
	switch t.(type) {
	case *TJSON:
		return false
	default:
		return true
	}
}

// TVector is the base for VECTOR column types, which are Postgres's
// older, limited version of ARRAYs. These are not meant to be persisted,
// because ARRAYs are a strict superset.
type TVector struct {
	Name      string
	ParamType T
}

// TypeName implements the ColTypeFormatter interface.
func (node *TVector) TypeName() string { return node.Name }

// Format implements the ColTypeFormatter interface.
func (node *TVector) Format(buf *bytes.Buffer, _ lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
}
