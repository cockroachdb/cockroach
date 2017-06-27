// Copyright 2017 The Cockroach Authors.
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
//

package analyzer

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/ir"
	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/parser"
)

func TestAnalyze(t *testing.T) {
	testData := []struct {
		input  string
		exp    string
		expErr string
	}{
		{`prim a; prim a`, ``, `<input>:1:14: type "a" was defined previously at <input>:1:6`},

		{`struct blah{}`, `(typedef
    :name blah
    :type (struct))
`, ``},

		{`struct blah{int x = 3}`, ``, `<input>:1:13: type "int" was not defined`},
		{`prim int; struct blah{int x = 3}`, `(typedef :name int)
(typedef
    :name blah
    :type 
        (struct
            (field :name x :type int :isslice false :tag 3)
        ))
`, ``},

		{`enum x{a=1;a=2}`, ``, `<input>:1:12: enumeration constant or struct field "a" appeared previously in this definition at <input>:1:8`},
		{`enum x{a=1;b=1}`, ``, `<input>:1:14: tag 1 appeared previously in this definition at <input>:1:10`},
		{`enum x{a=1;b=2}`, `(typedef
    :name x
    :type 
        (enum
            (const :name a :tag 1)
            (const :name b :tag 2)
        ))
`, ``},

		{`sum x{a=1;b=2}`, ``,
			`<input>:1:7: type "a" was not defined`},
		{`prim a;prim b;sum x{a=1;b=2}`, ``,
			`<input>:1:21: sum alternative "a" is not a struct`},
		{`struct a{}sum x{a=1;a=2}`, ``, `<input>:1:21: sum alternative "a" appeared previously in this definition at <input>:1:17`},
		{`struct a{}struct b{}sum x{a=1;b=2}`, `(typedef
    :name a
    :type (struct))
(typedef
    :name b
    :type (struct))
(typedef
    :name x
    :type 
        (sum
            (alt :type a :tag 1)
            (alt :type b :tag 2)
        ))
`, ``},
	}

	for _, test := range testData {
		t.Run(test.input, func(t *testing.T) {
			defs, err := parser.Parse("", strings.NewReader(test.input))
			if err != nil {
				t.Fatalf("parse: expected success, got %v", err)
			}

			tir, err := Analyze(defs)
			if err != nil {
				if test.expErr == "" {
					t.Fatalf("expected success, got %v", err)
				}
				if test.expErr != err.Error() {
					t.Fatalf("expected %s, got %v", test.expErr, err)
				}
				return
			}

			tirStr := ir.ToString(tir)
			if tirStr != test.exp {
				t.Fatalf("expected:\n%s\ngot:\n%s\n", test.exp, tirStr)
			}
		})
	}
}
