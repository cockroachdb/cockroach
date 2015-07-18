// Copyright 2015 The Cockroach Authors.
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
)

const (
	astInt      = "INT"
	astInteger  = "INTEGER"
	astSmallInt = "SMALLINT"
	astBigInt   = "BIGINT"
	astReal     = "REAL"
	astDouble   = "DOUBLE PRECISION"
	astFloat    = "FLOAT"
	astDecimal  = "DECIMAL"
	astNumeric  = "NUMERIC"
	astChar     = "CHAR"
	astVarChar  = "VARCHAR"
)

// ColumnType represents a type in a column definition.
type ColumnType interface {
	fmt.Stringer
	columnType()
}

func (*BitType) columnType()       {}
func (*BoolType) columnType()      {}
func (*IntType) columnType()       {}
func (*FloatType) columnType()     {}
func (*DecimalType) columnType()   {}
func (*DateType) columnType()      {}
func (*TimeType) columnType()      {}
func (*TimestampType) columnType() {}
func (*CharType) columnType()      {}
func (*TextType) columnType()      {}
func (*BlobType) columnType()      {}

// BitType represents a BIT type.
type BitType struct {
	N int
}

func (node *BitType) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "BIT")
	if node.N > 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	return buf.String()
}

// BoolType represents a BOOLEAN type.
type BoolType struct{}

func (node *BoolType) String() string {
	return "BOOLEAN"
}

// IntType represents an INT, INTEGER, SMALLINT or BIGINT type.
type IntType struct {
	Name string
	N    int
}

func (node *IntType) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	return buf.String()
}

// FloatType represents a REAL, DOUBLE or FLOAT type.
type FloatType struct {
	Name string
	Prec int
}

func (node *FloatType) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(&buf, "(%d)", node.Prec)
	}
	return buf.String()
}

// DecimalType represents a DECIMAL or NUMERIC type.
type DecimalType struct {
	Name  string
	Prec  int
	Scale int
}

func (node *DecimalType) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(node.Name)
	if node.Prec > 0 {
		fmt.Fprintf(&buf, "(%d", node.Prec)
		if node.Scale > 0 {
			fmt.Fprintf(&buf, ",%d", node.Scale)
		}
		_, _ = buf.WriteString(")")
	}
	return buf.String()
}

// DateType represents a DATE type.
type DateType struct {
}

func (node *DateType) String() string {
	return "DATE"
}

// TimeType represents a TIME type.
type TimeType struct {
}

func (node *TimeType) String() string {
	return "TIME"
}

// TimestampType represents a TIMESTAMP type.
type TimestampType struct {
}

func (node *TimestampType) String() string {
	return "TIMESTAMP"
}

// CharType represents a CHAR or VARCHAR type.
type CharType struct {
	Name string
	N    int
}

func (node *CharType) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	return buf.String()
}

// TextType represents a TEXT type.
type TextType struct {
	Name string
}

func (node *TextType) String() string {
	return "TEXT"
}

// BlobType represents a BLOB type.
type BlobType struct {
	Name string
}

func (node *BlobType) String() string {
	return "BLOB"
}
