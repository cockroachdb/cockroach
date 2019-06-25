// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// generateComments is a helper function that generates a block of comments by
// converting an Optgen comment to a Go comment. The comments are assumed to
// start with the name of an op or field and follow with a description, similar
// to this:
//   # <name> <description of what this op or field does>
//   # ...
//
// The initial name is replaced with the given replaceName, in order to adapt
// it to different enums and structs that are generated.
func generateComments(w io.Writer, comments lang.CommentsExpr, findName, replaceName string) {
	for i, comment := range comments {
		// Replace the # comment character used in Optgen with the Go comment
		// character.
		s := strings.Replace(string(comment), "#", "//", 1)

		// Replace the findName if it is the first word in the first comment.
		if i == 0 && strings.HasPrefix(string(comment), "# "+findName) {
			s = strings.Replace(s, findName, replaceName, 1)
		}

		fmt.Fprintf(w, "  %s\n", s)
	}
}
