// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	n131 := n13.Child("1.3.1\n1.3.1a")
	n13.Child("1.3.2\n1.3.2a")
	n13.AddEmptyLine()
	n131.Child("1.3.1.1\n1.3.1.1a")
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
 │    │    ├── 1.3.1
 │    │    │   1.3.1a
 │    │    └── 1.3.2
 │    │        1.3.2a
 │    │         │
 │    │         └── 1.3.1.1
 │    │             1.3.1.1a
 │    └── 1.4
 └── 2
`
	exp = strings.TrimLeft(exp, "\n")
	if res != exp {
		t.Errorf("incorrect result:\n%s", res)
	}
}
