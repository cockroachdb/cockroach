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

// TStringVariant distinguishes between flavors of string
// types. Even though values of string types of different variants
// have the same on-disk and in-memory repr, the type metadata for
// them has different behavior wrt pretty-printing and introspection.
type TStringVariant int

const (
	// TStringVariantSTRING is the canonical CockroachDB string type.
	//
	// It is reported as STRING in SHOW CREATE but "text" in
	// introspection for compatibility with PostgreSQL.
	//
	// It has no default maximum width.
	TStringVariantSTRING TStringVariant = iota

	// TStringVariantCHAR is the "standard SQL" string type of maximum length.
	//
	// It is reported as CHAR in SHOW CREATE and "character" in
	// introspection for compatibility with PostgreSQL.
	//
	// Its default maximum with is 1. It always has a maximum width.
	TStringVariantCHAR

	// TStringVariantVARCHAR is the "standard SQL" string type of varying
	// length.
	//
	// It is reported as VARCHAR in SHOW CREATE and "character varying"
	// in introspection for compatibility with PostgreSQL.
	//
	// It has no default maximum length but can be associated with one
	// in the syntax.
	TStringVariantVARCHAR

	// TStringVariantQCHAR is a special PostgreSQL-only type supported for
	// compatibility. It behaves like VARCHAR, its maximum width cannot
	// be modified, and has a peculiar name in the syntax and
	// introspection.
	//
	// It is reported as "char" (with double quotes included) in SHOW
	// CREATE and "char" in introspection for compatibility
	// with PostgreSQL.
	TStringVariantQCHAR
)

// String implements the fmt.Stringer interface.
func (v TStringVariant) String() string {
	switch v {
	case TStringVariantCHAR:
		return "CHAR"
	case TStringVariantVARCHAR:
		return "VARCHAR"
	case TStringVariantQCHAR:
		return `"char"`
	default:
		return "STRING"
	}
}

// TString represents a STRING, CHAR or VARCHAR type.
type TString struct {
	Variant TStringVariant
	N       uint
}

// TypeName implements the ColTypeFormatter interface.
func (node *TString) TypeName() string { return node.Variant.String() }

// Format implements the ColTypeFormatter interface.
func (node *TString) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
	if !(node.Variant == TStringVariantCHAR && node.N == 1) && node.N > 0 {
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

// TCollatedString represents a STRING, CHAR, QCHAR or VARCHAR type
// with a collation locale.
type TCollatedString struct {
	TString
	Locale string
}

// Format implements the ColTypeFormatter interface.
func (node *TCollatedString) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.TypeName())
	// In general, if there is a specified width we want to print it next
	// to the type. However, in the specific case of CHAR, the default
	// is 1 and the width should be omitted in that case.
	if node.N > 0 && !(node.Variant == TStringVariantCHAR && node.N == 1) {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
	buf.WriteString(" COLLATE ")
	lex.EncodeUnrestrictedSQLIdent(buf, node.Locale, f)
}
