// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package extract

import (
	"fmt"
	"strings"
	"testing"
)

func TestSplitGroup(t *testing.T) {
	g, err := ParseGrammar(strings.NewReader(`
a ::=
	'A' b

b ::=
	c
	| b ',' c

c ::=
	'B'
	| 'C'
`))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Inline("b", "c"); err != nil {
		t.Fatal(err)
	}
	b, err := g.ExtractProduction("a", true, false, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(b))
}

func TestSplitOpt(t *testing.T) {
	g, err := ParseGrammar(strings.NewReader(`
a ::=
	'A' b

b ::=
	'B'
	|
`))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Inline("b"); err != nil {
		t.Fatal(err)
	}
	b, err := g.ExtractProduction("a", true, false, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(b))
}
