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
	"bytes"
	"fmt"
	"io"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

func generateOps(compiled *lang.CompiledExpr, w io.Writer) {
	fmt.Fprintf(w, "const (\n")
	fmt.Fprintf(w, "  UnknownOp Operator = iota\n\n")

	for _, define := range compiled.Defines {
		fmt.Fprintf(w, "  %sOp\n", define.Name)
	}

	fmt.Fprintf(w, ")\n\n")

	// Generate op names and indexes.
	var names bytes.Buffer
	var indexes bytes.Buffer

	fmt.Fprint(&names, "unknown")
	fmt.Fprint(&indexes, "0, ")

	for _, define := range compiled.Defines {
		fmt.Fprintf(&indexes, "%d, ", names.Len())

		// Trim the Op suffix and convert to "dash case".
		fmt.Fprint(&names, dashCase(string(define.Name)))
	}

	fmt.Fprintf(w, "const opNames = \"%s\"\n\n", names.String())

	fmt.Fprintf(w, "var opIndexes = [...]uint32{%s%d}\n\n", indexes.String(), names.Len())
}

// dashCase converts camel-case identifiers into "dash case", where uppercase
// letters in the middle of the identifier are replaced by a dash followed
// by the lowercase version the letter. Example:
//   InnerJoinApply => inner-join-apply
func dashCase(s string) string {
	var buf bytes.Buffer

	for i, ch := range s {
		if unicode.IsUpper(ch) {
			if i != 0 {
				buf.WriteByte('-')
			}

			buf.WriteRune(unicode.ToLower(ch))
		} else {
			buf.WriteRune(ch)
		}
	}

	return buf.String()
}
