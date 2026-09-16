// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package extract

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// TestGenerateBNFSkipMarkers verifies that branches annotated with
// "unimplemented" or "SKIP DOC" are omitted from the generated BNF, and that
// "FORCE DOC" overrides the "unimplemented" exclusion.
func TestGenerateBNFSkipMarkers(t *testing.T) {
	const grammar = `
%%

stmt:
  documented_stmt
| unimplemented_stmt
| skipped_stmt
| forced_stmt

documented_stmt:
  DOCUMENTED { }

unimplemented_stmt:
  UNIMPL { return unimplemented(sqllex, "unimpl") }

skipped_stmt:
  SKIPPED { /* SKIP DOC */ }

forced_stmt:
  FORCED { /* FORCE DOC */ return unimplementedWithIssue(sqllex, 1) }

%%
`
	path := filepath.Join(t.TempDir(), "test.y")
	if err := os.WriteFile(path, []byte(grammar), 0644); err != nil {
		t.Fatal(err)
	}
	bnf, err := GenerateBNF(path, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	out := string(bnf)
	for _, want := range []string{"documented_stmt", "forced_stmt"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in generated BNF, got:\n%s", want, out)
		}
	}
	for _, notWant := range []string{"unimplemented_stmt", "skipped_stmt"} {
		if strings.Contains(out, notWant) {
			t.Errorf("expected %q to be excluded from generated BNF, got:\n%s", notWant, out)
		}
	}
}
