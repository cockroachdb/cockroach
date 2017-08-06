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
		{`prim A; prim A`, ``, `<input>:1:14: type "A" was defined previously at <input>:1:6`},

		{`struct Blah{}`, `(typedef
    :name Blah
    :type (struct))
`, ``},

		{`struct Blah{int X = 3}`, ``, `<input>:1:13: type "int" was not defined`},
		{`prim int; struct Blah{int X = 3}`, `(typedef :name int)
(typedef
    :name Blah
    :type
        (struct
            (field :name X :type int :isslice false :tag 3)
        ))
`, ``},

		{`enum X{A=1;A=2}`, ``, `<input>:1:12: enumeration constant or struct field "A" appeared previously in this definition at <input>:1:8`},
		{`enum X{A=1;B=1}`, ``, `<input>:1:14: tag 1 appeared previously in this definition at <input>:1:10`},
		{`enum X{A=1;B=2}`, `(typedef
    :name X
    :type
        (enum
            (const :name A :tag 1)
            (const :name B :tag 2)
        ))
`, ``},

		{`sum X{A=1;B=2}`, ``,
			`<input>:1:7: type "A" was not defined`},
		{`prim A;prim B;sum X{A=1;B=2}`, ``,
			`<input>:1:21: sum alternative "A" is not a struct`},
		{`struct A{}sum X{A=1;A=2}`, ``, `<input>:1:21: sum alternative "A" appeared previously in this definition at <input>:1:17`},
		{`struct A{}struct B{}sum X{A=1;B=2}`, `(typedef
    :name A
    :type (struct))
(typedef
    :name B
    :type (struct))
(typedef
    :name X
    :type
        (sum
            (alt :type A :tag 1)
            (alt :type B :tag 2)
        ))
`, ``},

		{`sum foo{}`, ``,
			`<input>:1:5: name "foo" is not exported`},
		{`enum Foo{bar=1}`, ``,
			`<input>:1:10: name "bar" is not exported`},
		{`struct Foo{Foo bar=1}`, ``,
			`<input>:1:16: name "bar" is not exported`},
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
