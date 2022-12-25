// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randident

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"golang.org/x/text/unicode/norm"
)

// TestNameGen verifies that1 the name generation algorithm produces
// noise probabilistically (i.e. some generated names have the noise
// included and some don't).
func TestNameGen(t *testing.T) {
	type pred func(n string) bool
	validate := func(t *testing.T, names []string,
		always []pred,
		never []pred,
		probs []pred) {
		// Check that the always predicates apply all the time.
		for pnum, p := range always {
			for _, n := range names {
				if !p(n) {
					t.Errorf("always predicate at position %d failed to apply on %q", pnum, n)
					break
				}
			}
		}
		// Check that the never predicates apply never.
		for pnum, p := range never {
			for _, n := range names {
				if p(n) {
					t.Errorf("never predicate at position %d succeeded on %q", pnum, n)
					break
				}
			}
		}
		// Check that all the probability predicates apply at least once: the properties
		// are present at least some of the time.
		for pnum, p := range probs {
			applies := false
			for _, n := range names {
				if p(n) {
					applies = true
					break
				}
			}
			if !applies {
				t.Logf("%q", names)
				t.Errorf("prob predicate at position %d fails to apply at least once", pnum)
			}
		}
		// Check that all the predicates fail to apply at least once.
		// The properties must not be present at least some of the time.
		for pnum, p := range probs {
			fails := false
			for _, n := range names {
				if !p(n) {
					fails = true
					break
				}
			}
			if !fails {
				t.Logf("%q", names)
				t.Errorf("prob predicate at position %d always succeeds", pnum)
			}
		}
	}

	testNameGen := func(tname string, cfg *NameGeneratorConfig, pattern string,
		always []pred,
		never []pred,
		probs []pred) {
		t.Run(tname, func(t *testing.T) {
			rand, _ := randutil.NewTestRand()
			g := NewNameGenerator(cfg, rand, pattern)
			t.Run(tname+"/one", func(t *testing.T) {
				names := make([]string, 100)
				for i := 0; i < 100; i++ {
					names[i] = g.GenerateOne(i)
				}
				validate(t, names, always, never, probs)
			})
			t.Run(tname+"/multiple", func(t *testing.T) {
				conflicts := make(map[string]struct{})
				names, _ := g.GenerateMultiple(context.Background(), 100, conflicts)
				validate(t, names, always, never, probs)
			})
		})
	}

	cfg := NameGeneratorConfig{Number: true}
	testNameGen("base", &cfg, "hello",
		// always
		[]pred{func(n string) bool { return strings.HasPrefix(n, "hello") }},
		// never
		[]pred{func(n string) bool { return !strings.HasPrefix(n, "hello") }},
		// sometimes
		[]pred{func(n string) bool { return strings.HasSuffix(n, "0") }},
	)

	cfg = NameGeneratorConfig{Capitals: 0.5}
	testNameGen("capitals", &cfg, "hellouniverse",
		// always
		[]pred{func(n string) bool { return strings.ToLower(n) == "hellouniverse" }},
		// never
		nil,
		// sometimes
		[]pred{func(n string) bool { return strings.ToLower(n) != n }},
	)

	removeSpecial := func(n string) string {
		return strings.Map(func(r rune) rune {
			if r < 'a' || r > 'z' {
				return -1
			}
			return r
		}, n)
	}

	cfg = NameGeneratorConfig{Punctuate: 10}
	testNameGen("punct", &cfg, "hellouniverse",
		// always
		[]pred{func(n string) bool { return removeSpecial(n) == "hellouniverse" }},
		// never
		nil,
		// sometimes
		[]pred{
			func(n string) bool { return strings.Contains(n, ".") },
			func(n string) bool { return strings.Contains(n, "/") },
			func(n string) bool { return strings.Contains(n, "-") },
		},
	)

	cfg = NameGeneratorConfig{Quote: 0.5}
	testNameGen("quote", &cfg, "hellouniverse",
		// always
		[]pred{func(n string) bool { return removeSpecial(n) == "hellouniverse" }},
		// never
		nil,
		// sometimes
		[]pred{
			func(n string) bool { return strings.Contains(n, "'") },
			func(n string) bool { return strings.Contains(n, "\"") },
		},
	)

	cfg = NameGeneratorConfig{Space: 0.5}
	testNameGen("space", &cfg, "hellouniverse",
		// always
		[]pred{func(n string) bool { return removeSpecial(n) == "hellouniverse" }},
		// never
		nil,
		// sometimes
		[]pred{func(n string) bool { return strings.Contains(n, " ") }},
	)

	cfg = NameGeneratorConfig{Whitespace: 1}
	testNameGen("whitespace", &cfg, "hellouniverse",
		// always
		[]pred{func(n string) bool { return removeSpecial(n) == "hellouniverse" }},
		// never
		nil,
		// sometimes
		[]pred{
			func(n string) bool { return strings.Contains(n, " ") },
			func(n string) bool { return strings.Contains(n, "\t") },
			func(n string) bool { return strings.Contains(n, "\n") },
			func(n string) bool { return strings.Contains(n, "\f") },
			func(n string) bool { return strings.Contains(n, "\v") },
		},
	)

	cfg = NameGeneratorConfig{Emote: 0.5}
	testNameGen("emote", &cfg, "hellouniverse",
		// always
		[]pred{func(n string) bool { return removeSpecial(n) == "hellouniverse" }},
		// never
		nil,
		// sometimes
		[]pred{
			func(n string) bool {
				return strings.IndexFunc(n, func(r rune) bool {
					return r >= '😀' && r <= '🙄'
				}) >= 0
			},
		},
	)

	cfg = NameGeneratorConfig{Diacritics: 0.5, DiacriticDepth: 1}
	aPlusDiacritics := []string{}
	for i := 0x300; i <= 0x36f; i++ {
		aPlusDiacritics = append(aPlusDiacritics, norm.NFC.String(string([]rune{'a', rune(i)})))
	}
	testNameGen("diacritics", &cfg, "aaaaaaaaaaaaa",
		// always
		nil,
		// never
		nil,
		// sometimes
		[]pred{func(n string) bool {
			for _, ad := range aPlusDiacritics {
				if strings.Contains(n, ad) {
					return true
				}
			}
			return false
		}},
	)
}
