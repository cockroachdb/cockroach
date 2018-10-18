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
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

func title(name string) string {
	rune, size := utf8.DecodeRuneInString(name)
	return fmt.Sprintf("%c%s", unicode.ToUpper(rune), name[size:])
}

func unTitle(name string) string {
	rune, size := utf8.DecodeRuneInString(name)
	return fmt.Sprintf("%c%s", unicode.ToLower(rune), name[size:])
}

// isExportedField is true if the name of a define field starts with an upper-
// case letter.
func isExportedField(field *lang.DefineFieldExpr) bool {
	return unicode.IsUpper(rune(field.Name[0]))
}

// isEmbeddedField is true if the name of a define field is "_". All such fields
// are generated as Go embedded structs.
func isEmbeddedField(field *lang.DefineFieldExpr) bool {
	return field.Name == "_"
}

// expandFields returns all fields from the given define, with the fields of any
// embedded structs recursively expanded into a flat list.
func expandFields(compiled *lang.CompiledExpr, define *lang.DefineExpr) lang.DefineFieldsExpr {
	var fields lang.DefineFieldsExpr
	for _, field := range define.Fields {
		if isEmbeddedField(field) {
			embedded := expandFields(compiled, compiled.LookupDefine(string(field.Type)))
			fields = append(fields, embedded...)
		} else {
			fields = append(fields, field)
		}
	}
	return fields
}

// generateDefineComments is a helper function that generates a block of
// op definition comments by converting the Optgen comment to a Go comment.
// The comments are assumed to start with the name of the op and follow with a
// description of the op, like this:
//   # <opname> <description of what this op does>
//   # ...
//
// The initial opname is replaced with the given replaceName, in order to adapt
// it to different enums and structs that are generated.
func generateDefineComments(w io.Writer, define *lang.DefineExpr, replaceName string) {
	for i, comment := range define.Comments {
		// Replace the # comment character used in Optgen with the Go
		// comment character.
		s := strings.Replace(string(comment), "#", "//", 1)

		// Replace the definition name if it is the first word in the first
		// comment.
		if i == 0 && strings.HasPrefix(string(comment), "# "+string(define.Name)) {
			s = strings.Replace(s, string(define.Name), replaceName, 1)
		}

		fmt.Fprintf(w, "  %s\n", s)
	}
}
