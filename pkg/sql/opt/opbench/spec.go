// Copyright 2019 The Cockroach Authors.
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

package opbench

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Spec defines a single parameterized plan which we would like to
// benchmark.
// A Spec has "parameters", which are values that get substituted into
// the plan, and "inputs", which are the values from which the parameters
// are derived. These might differ in cases like where a Spec
// must scan tables of different widths, and the "input" is the width
// of the table, and the "parameter" is the name of the table that is
// scanned.
type Spec struct {
	// Name is the title of spec. It is what determines the filename of
	// this spec's results.
	Name string

	// plan is the parameterized exprgen plan, with each parameter prefixed with
	// a $.
	plan string

	// inputs is the set of possible inputs, along with a set of potential values
	// for each. This allows enumerating all combinations of possible inputs.
	inputs []choice

	// getParam maps the name of a parameter to its value, given
	// the configuration.
	getParam func(string, Configuration) string
}

// paramRegex matches parameters in a plan (words prefixed with a $).
var paramRegex = regexp.MustCompile(`\$[a-zA-Z_]+`)

// getParams extracts all the $dollarsign prefixed parameters from the spec's
// plan, sorting and deduplicating them.
func (s *Spec) getParams() []string {
	paramRefs := paramRegex.FindAllString(s.plan, -1)
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

// Inputs returns a slice of the names of the inputs to the spec.
func (s *Spec) Inputs() []string {
	var result []string
	for _, i := range s.inputs {
		result = append(result, i.field)
	}
	return result
}

// FillInParams returns the Spec's plan with parameters filled
// in with respect to the given configuration.
func (s *Spec) FillInParams(c Configuration) string {
	// Replace all the parameters in the plan with their values.
	text := s.plan
	params := s.getParams()
	for _, k := range params {
		text = strings.Replace(text, "$"+k, s.getParam(k, c), -1)
	}

	return text
}

// Benches is the set of benchmarks we run.
var Benches = []*Spec{
	HashJoinSpec,
	MergeJoinSpec,
	LookupJoinSpec,
}

// HashJoinSpec does a hash join between supplier and lineitem.
var HashJoinSpec = &Spec{
	Name: "tpch-hash-join",
	plan: `
(Root
	(InnerJoin
		(Scan
			[
				(Table "supplier")
				(Cols "s_suppkey")
				(Index "supplier@s_nk")
				(HardLimit $supplier_rows)
			]
		)
		(Scan
			[
				(Table "lineitem")
				(Cols "l_suppkey")
				(Index "lineitem@l_sk")
				(HardLimit $lineitem_rows)
			]
		)
		[
			(Eq (Var "l_suppkey") (Var "s_suppkey"))
		]
		[ ]
	)
	(Presentation "l_suppkey")
	(NoOrdering)
)`,

	inputs: []choice{
		{"lineitem_rows", []float64{1000000, 2000000, 3000000, 4000000, 5000000, 6000000}},
		{"supplier_rows", []float64{1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000}},
	},

	getParam: func(paramName string, config Configuration) string {
		switch paramName {
		case "lineitem_rows":
			return fmt.Sprintf("%d", int(config.Get("lineitem_rows")))
		case "supplier_rows":
			return fmt.Sprintf("%d", int(config.Get("supplier_rows")))
		}
		panic(fmt.Sprintf("can't handle %q", paramName))
	},
}

// MergeJoinSpec does a merge join between supplier and lineitem.
var MergeJoinSpec = &Spec{
	Name: "tpch-merge-join",
	plan: `
(Root
	(MergeJoin
		(Scan
			[
				(Table "lineitem")
				(Cols "l_suppkey")
				(Index "lineitem@l_sk")
				(HardLimit $lineitem_rows)
			]
		)
		(Scan
			[
				(Table "supplier")
				(Cols "s_suppkey")
				(HardLimit $supplier_rows)
			]
		)
		[ ]
		[
			(JoinType "inner-join")
			(LeftEq "+l_suppkey")
			(RightEq "+s_suppkey")
			(LeftOrdering "+l_suppkey")
			(RightOrdering "+s_suppkey")
		]
	)
	(Presentation "l_suppkey")
	(NoOrdering)
)`,

	inputs: []choice{
		{"lineitem_rows", []float64{1000000, 2000000, 3000000, 4000000, 5000000, 6000000}},
		{"supplier_rows", []float64{1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000}},
	},

	getParam: func(paramName string, config Configuration) string {
		switch paramName {
		case "lineitem_rows":
			return fmt.Sprintf("%d", int(config.Get("lineitem_rows")))
		case "supplier_rows":
			return fmt.Sprintf("%d", int(config.Get("supplier_rows")))
		}
		panic(fmt.Sprintf("can't handle %q", paramName))
	},
}

// LookupJoinSpec does a lookup join between supplier and lineitem.
var LookupJoinSpec = &Spec{
	Name: "tpch-lookup-join",
	plan: `
(Root
	(MakeLookupJoin
		(Scan
			[
				(Table "supplier")
				(Index "supplier@s_nk")
				(Cols "s_suppkey")
				(HardLimit $supplier_rows)
			]
		)
		[
			(JoinType "inner-join")
			(Table "lineitem")
			(Index "lineitem@l_sk")
			(KeyCols "s_suppkey")
			(Cols "l_suppkey")
		]
		[
		]
	)
	(Presentation "l_suppkey")
	(NoOrdering)
)`,

	inputs: []choice{
		{"supplier_rows", []float64{1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000}},
	},

	getParam: func(paramName string, config Configuration) string {
		switch paramName {
		case "supplier_rows":
			return fmt.Sprintf("%d", int(config.Get("supplier_rows")))
		}
		panic(fmt.Sprintf("can't handle %q", paramName))
	},
}
