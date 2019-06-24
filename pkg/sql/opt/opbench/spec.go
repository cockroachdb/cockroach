// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opbench

import (
	"regexp"
	"sort"
	"strings"
)

// Spec defines a single parameterized Plan which we would like to
// benchmark.
// A Spec has "parameters", which are values that get substituted into
// the Plan, and "inputs", which are the values from which the parameters
// are derived. These might differ in cases like where a Spec
// must scan tables of different widths, and the "input" is the width
// of the table, and the "parameter" is the name of the table that is
// scanned.
type Spec struct {
	// Name is the title of spec. It is what determines the filename of
	// this spec's results.
	Name string

	// Plan is the parameterized exprgen Plan, with each parameter prefixed with
	// a $.
	Plan string

	// Inputs is the set of possible inputs, along with a set of potential values
	// for each. This allows enumerating all combinations.
	Inputs []Options

	// GetParam maps the name of a parameter to its value, given
	// the configuration.
	GetParam func(string, Configuration) string
}

// paramRegex matches parameters in a Plan (words prefixed with a $).
var paramRegex = regexp.MustCompile(`\$[a-zA-Z_]+`)

// getParams extracts all the $dollarsign prefixed parameters from the spec's
// Plan, sorting and deduplicating them.
func (s *Spec) getParams() []string {
	paramRefs := paramRegex.FindAllString(s.Plan, -1)
	// Sort them for deduplication.
	sort.Strings(paramRefs)
	result := make([]string, 0, len(paramRefs))
	last := ""
	for i := 0; i < len(paramRefs); i++ {
		if last != paramRefs[i] {
			// Get rid of the leading $.
			next := paramRefs[i][1:]
			result = append(result, next)
			last = paramRefs[i]
		}
	}
	return result
}

// InputNames returns a slice of the names of the inputs to the spec.
func (s *Spec) InputNames() []string {
	var result []string
	for _, i := range s.Inputs {
		result = append(result, i.Field)
	}
	return result
}

// FillInParams returns the Spec's Plan with parameters filled
// in with respect to the given configuration.
func (s *Spec) FillInParams(c Configuration) string {
	// Replace all the parameters in the Plan with their values.
	text := s.Plan
	params := s.getParams()
	for _, k := range params {
		text = strings.Replace(text, "$"+k, s.GetParam(k, c), -1)
	}

	return text
}
