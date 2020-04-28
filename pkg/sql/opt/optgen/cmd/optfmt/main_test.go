// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		n := defaultWidth
		ignoreTokOrder := d.HasArg("ignoreTokOrder")
		if d.HasArg("n") {
			d.ScanArgs(t, "n", &n)
		}
		s, err := prettyify(strings.NewReader(d.Input), n)
		if err != nil {
			return fmt.Sprintf("ERROR: %s", err)
		}

		// Verify we round trip correctly by ensuring non-whitespace
		// scanner tokens are encountered in the same order.
		{
			origToks := toTokens(d.Input)
			prettyToks := toTokens(s)
			for i, tok := range origToks {
				if i >= len(prettyToks) {
					t.Fatalf("pretty ended early after %d tokens", i+1)
				}
				if !ignoreTokOrder && prettyToks[i] != tok {
					t.Log(s)
					t.Fatalf("token %d didn't match", i+1)
				}
			}
			if len(prettyToks) > len(origToks) {
				t.Fatalf("orig ended early after %d tokens", len(origToks))
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
