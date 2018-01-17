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

package treeprinter

import (
	"strings"
	"testing"
)

func TestTreePrinter(t *testing.T) {
	n := New()

	r := n.Child("root")
	r.AddEmptyLine()
	n1 := r.Childf("%d", 1)
	n1.Child("1.1")
	n12 := n1.Child("1.2")
	r.AddEmptyLine()
	r.AddEmptyLine()
	n12.Child("1.2.1")
	r.AddEmptyLine()
	n12.Child("1.2.2")
	n13 := n1.Child("1.3")
	n13.AddEmptyLine()
	n13.Child("1.3.1")
	n13.AddEmptyLine()
	n1.Child("1.4")
	r.Child("2")

	res := n.String()
	exp := `
root
 │
 ├── 1
 │    ├── 1.1
 │    ├── 1.2
 │    │    │
 │    │    │
 │    │    ├── 1.2.1
 │    │    │
 │    │    └── 1.2.2
 │    ├── 1.3
 │    │    │
 │    │    └── 1.3.1
 │    │
 │    └── 1.4
 └── 2
`
	exp = strings.TrimLeft(exp, "\n")
	if res != exp {
		t.Errorf("incorrect result:\n%s", res)
	}
}
