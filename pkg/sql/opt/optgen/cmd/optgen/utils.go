// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

func unTitle(name string) string {
	rune, size := utf8.DecodeRuneInString(name)
	return fmt.Sprintf("%c%s", unicode.ToLower(rune), name[size:])
}

func mapType(typ string) string {
	switch typ {
	case "Expr":
		return "GroupID"

	case "ExprList":
		return "ListID"

	default:
		return "PrivateID"
	}
}

// isListType returns true if the given type is ExprList. An expression may
// have at most one field with a list type.
func isListType(typ string) bool {
	return typ == "ExprList"
}

// isPrivateType returns true if the given type is anything besides Expr or
// ExprList. An expression may have at most one field with a private type.
func isPrivateType(typ string) bool {
	return typ != "Expr" && typ != "ExprList"
}

// listField returns the field definition expression for the given define's
// list field, or nil if it does not have a list field.
func listField(d *lang.DefineExpr) *lang.DefineFieldExpr {
	// If list-typed field is present, it will be the last field, or the second
	// to last field if a private field is present.
	index := len(d.Fields) - 1
	if privateField(d) != nil {
		index--
	}

	if index < 0 {
		return nil
	}

	defineField := d.Fields[index]
	if isListType(string(defineField.Type)) {
		return defineField
	}

	return nil
}

// privateField returns the field definition expression for the given define's
// private field, or nil if it does not have a private field.
func privateField(d *lang.DefineExpr) *lang.DefineFieldExpr {
	// If private is present, it will be the last field.
	index := len(d.Fields) - 1
	if index < 0 {
		return nil
	}

	defineField := d.Fields[index]
	if isPrivateType(string(defineField.Type)) {
		return defineField
	}

	return nil
}
