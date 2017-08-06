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

package parser

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	testData := []struct {
		input  string
		exp    string
		expErr string
	}{
		{"prim blah", `prim blah;
`, ""},
		{"struct blah{}", `struct blah {
}
`, ""},
		{"struct blah{foo x = 1; reserved 3;bar[]y=2}", `struct blah {
 foo x = 1;
 reserved 3;
 bar[] y = 2;
}
`, ""},
		{"enum blah{x=3;y=4;reserved 5}", `enum blah {
 x = 3;
 y = 4;
 reserved 5;
}
`, ""},
		{"sum blah{x=3;y=4;reserved 5}", `sum blah {
 x = 3;
 y = 4;
 reserved 5;
}
`, ""},
		{"sum x {y=1} sum a{b=2}", `sum x {
 y = 1;
}

sum a {
 b = 2;
}
`, ""},
	}

	for _, test := range testData {
		t.Run(test.input, func(t *testing.T) {
			defs, err := Parse("", strings.NewReader(test.input))
			if err != nil {
				if test.expErr == "" {
					t.Fatalf("expected success, got %v", err)
				}
				if test.expErr != err.Error() {
					t.Fatalf("expected %s, got %v", test.expErr, err)
				}
				return
			}
			if test.expErr != "" {
				t.Fatalf("expected %s, got success", test.expErr)
			}
			defStr := ToString(defs)
			if defStr != test.exp {
				t.Fatalf("expected %s, got %s", test.exp, defStr)
			}

			// Test idempotence of Parser-ToString
			defs, err = Parse("", strings.NewReader(defStr))
			if err != nil {
				t.Fatalf("2nd pass: expected success, got %v", err)
			}
			defStr2 := ToString(defs)
			if defStr != defStr2 {
				t.Fatalf("2nd pass: expected %s, got %s", defStr, defStr2)
			}
		})
	}
}
