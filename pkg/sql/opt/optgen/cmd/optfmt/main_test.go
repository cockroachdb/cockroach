package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
	"github.com/cockroachdb/datadriven"
)

func TestPretty(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, prettyTest)
	})
}

func prettyTest(t *testing.T, d *datadriven.TestData) string {
	switch d.Cmd {
	case "pretty":
		var n int
		d.ScanArgs(t, "n", &n)
		s, err := prettyify(strings.NewReader(d.Input), n)
		if err != nil {
			t.Fatal(err)
		}

		// Verify we round trip correctly by ensuring non-whitespace
		// scanner tokens are encountered in the same order.
		{
			orig_toks := toTokens(d.Input)
			pretty_toks := toTokens(s)
			for i, tok := range orig_toks {
				if i >= len(pretty_toks) {
					t.Fatalf("pretty ended early after %d tokens", i+1)
				}
				if pretty_toks[i] != tok {
					t.Fatalf("token %d didn't match", i+1)
				}
			}
			if len(pretty_toks) > len(orig_toks) {
				t.Fatalf("orig ended early after %d tokens", len(orig_toks))
			}
		}

		return s
	default:
		t.Fatal("unknown command")
		return ""
	}
}

func toTokens(input string) []string {
	scanner := lang.NewScanner(strings.NewReader(input))
	var ret []string
	for {
		tok := scanner.Scan()
		lit := scanner.Literal()
		switch tok {
		case lang.WHITESPACE:
			// ignore
		case lang.EOF, lang.ILLEGAL, lang.ERROR:
			ret = append(ret, fmt.Sprintf("%s: %q", tok, lit))
			return ret
		default:
			ret = append(ret, fmt.Sprintf("%s: %s", tok, lit))
		}
	}
}
