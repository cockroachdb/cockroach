// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import "strings"

type Jsonpath struct {
	Strict bool
	Path   Path
}

func (j Jsonpath) String() string {
	var sb strings.Builder
	if j.Strict {
		sb.WriteString("strict ")
	}
	j.Path.ToString(&sb, false /* inKey */, true /* printBrackets */)
	return sb.String()
}

// Validate walks the Jsonpath AST. It returns an error if the AST is invalid
// (last not in array subscripts, @ in root expressions).
func (j Jsonpath) Validate() error {
	return j.Path.Validate(0 /* nestingLevel */, false /* insideArraySubscript */)
}
