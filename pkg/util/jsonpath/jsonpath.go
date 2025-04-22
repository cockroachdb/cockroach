// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

type Jsonpath struct {
	Strict bool
	Path   Path
}

func (j Jsonpath) String() string {
	var mode string
	if j.Strict {
		mode = "strict "
	}
	return mode + j.Path.String()
}

// Validate walks the Jsonpath AST. It returns an error if the AST is invalid
// (last not in array subscripts, @ in root expressions).
func (j Jsonpath) Validate() error {
	return j.Path.Validate(0 /* nestingLevel */, false /* insideArraySubscript */)
}
