// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import "sort"

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

// ValidateAndCollectVariables walks the Jsonpath AST and collects all variable
// names. It returns an error if the AST is invalid (last not in array subscripts,
// @ in root expressions).
func (j Jsonpath) ValidateAndCollectVariables() ([]string, error) {
	vars := map[string]struct{}{}
	if err := j.Path.Validate(vars, 0 /* nestingLevel */, false /* insideArraySubscript */); err != nil {
		return nil, err
	}
	varsList := make([]string, 0, len(vars))
	for v := range vars {
		varsList = append(varsList, v)
	}
	// Iteration order over maps is non-deterministic, so sort the variables to
	// make error messages deterministic.
	sort.Strings(varsList)
	return varsList, nil
}
