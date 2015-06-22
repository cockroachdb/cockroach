// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
	"strings"
)

const (
	astInt        = "INT"
	astInteger    = "INTEGER"
	astTinyInt    = "TINYINT"
	astSmallInt   = "SMALLINT"
	astMediumInt  = "MEDIUMINT"
	astBigInt     = "BIGINT"
	astReal       = "REAL"
	astDouble     = "DOUBLE"
	astFloat      = "FLOAT"
	astDecimal    = "DECIMAL"
	astNumeric    = "NUMERIC"
	astChar       = "CHAR"
	astVarChar    = "VARCHAR"
	astBinary     = "BINARY"
	astVarBinary  = "VARBINARY"
	astText       = "TEXT"
	astTinyText   = "TINYTEXT"
	astMediumText = "MEDIUMTEXT"
	astLongText   = "LONGTEXT"
	astBlob       = "BLOB"
	astTinyBlob   = "TINYBLOB"
	astMediumBlob = "MEDIUMBLOB"
	astLongBlob   = "LONGBLOB"
)

// ColumnType represents a type in a column definition.
type ColumnType interface {
	fmt.Stringer
	columnType()
}

// BitType represents a BIT type.
type BitType struct {
	N int
}

func (*BitType) columnType() {}

func (node *BitType) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "BIT")
	if node.N >= 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	return buf.String()
}

// IntType represents an INT, TINYINT, SMALLINT, MEDIUMINT, BIGINT or INTEGER
// type.
type IntType struct {
	Name     string
	N        int
	Unsigned bool
}

func (*IntType) columnType() {}

func (node *IntType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.N >= 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	if node.Unsigned {
		buf.WriteString(" UNSIGNED")
	}
	return buf.String()
}

// FloatType represents a REAL, DOUBLE or FLOAT type.
type FloatType struct {
	Name     string
	N        int
	Prec     int
	Unsigned bool
}

func (*FloatType) columnType() {}

func (node *FloatType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.N >= 0 {
		fmt.Fprintf(&buf, "(%d,%d)", node.N, node.Prec)
	}
	if node.Unsigned {
		buf.WriteString(" UNSIGNED")
	}
	return buf.String()
}

// DecimalType represents a DECIMAL or NUMERIC type.
type DecimalType struct {
	Name string
	N    int
	Prec int
}

func (*DecimalType) columnType() {}

func (node *DecimalType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.N >= 0 {
		fmt.Fprintf(&buf, "(%d", node.N)
		if node.Prec >= 0 {
			fmt.Fprintf(&buf, ",%d", node.Prec)
		}
		buf.WriteString(")")
	}
	return buf.String()
}

// DateType represents a DATE type.
type DateType struct {
}

func (*DateType) columnType() {}

func (node *DateType) String() string {
	return "DATE"
}

// TimeType represents a TIME type.
type TimeType struct {
}

func (*TimeType) columnType() {}

func (node *TimeType) String() string {
	return "TIME"
}

// DateTimeType represents a DATETIME type.
type DateTimeType struct {
}

func (*DateTimeType) columnType() {}

func (node *DateTimeType) String() string {
	return "DATETIME"
}

// TimestampType represents a TIMESTAMP type.
type TimestampType struct {
}

func (*TimestampType) columnType() {}

func (node *TimestampType) String() string {
	return "TIMESTAMP"
}

// CharType represents a CHAR or VARCHAR type.
type CharType struct {
	Name string
	N    int
}

func (*CharType) columnType() {}

func (node *CharType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.N >= 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	return buf.String()
}

// BinaryType represents a BINARY or VARBINARY type.
type BinaryType struct {
	Name string
	N    int
}

func (*BinaryType) columnType() {}

func (node *BinaryType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.N >= 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	return buf.String()
}

// TextType represents a TEXT, TINYTEXT, MEDIUMTEXT or LONGTEXT type.
type TextType struct {
	Name string
}

func (*TextType) columnType() {}

func (node *TextType) String() string {
	return node.Name
}

// BlobType represents a BLOB, TINYBLOB, MEDIUMBLOB or LONGBLOB type.
type BlobType struct {
	Name string
}

func (*BlobType) columnType() {}

func (node *BlobType) String() string {
	return node.Name
}

// EnumType represents an ENUM type.
type EnumType struct {
	Vals []string
}

func (*EnumType) columnType() {}

func (node *EnumType) String() string {
	return fmt.Sprintf("ENUM(%s)", strings.Join(node.Vals, ","))
}

// SetType represents a SET type.
type SetType struct {
	Vals []string
}

func (*SetType) columnType() {}

func (node *SetType) String() string {
	return fmt.Sprintf("SET(%s)", strings.Join(node.Vals, ","))
}
