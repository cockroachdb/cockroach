// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randident

import (
	"context"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/text/unicode/norm"
)

// TestNameGen verifies that1 the name generation algorithm produces
// noise probabilistically (i.e. some generated names have the noise
// included and some don't).
func TestNameGen(t *testing.T) {
	type pred func(n string) bool
	type preds struct {
		always []pred
		never  []pred
		probs  []pred
	}

	validate := func(t *testing.T, names []string, ps preds) {
		// Check that the always predicates apply all the time.
		for pnum, p := range ps.always {
			for _, n := range names {
				if !p(n) {
					t.Errorf("always predicate at position %d failed to apply on %q", pnum, n)
					break
				}
			}
		}
		// Check that the never predicates apply never.
		for pnum, p := range ps.never {
			for _, n := range names {
				if p(n) {
					t.Errorf("never predicate at position %d succeeded on %q", pnum, n)
					break
				}
			}
		}
		// Check that all the probability predicates apply at least once: the properties
		// are present at least some of the time.
		for pnum, p := range ps.probs {
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
		for pnum, p := range ps.probs {
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

	// We fix the seed to avoid flakes in tests. The checks
	// below have a tiny but non-zero probability to fail.
	rng := rand.New(rand.NewSource(1212312321))

	testNameGen := func(tname string, cfg *NameGeneratorConfig, pattern string,
		ps preds) {
		t.Run(tname, func(t *testing.T) {
			g := NewNameGenerator(cfg, rng, pattern)
			t.Run(tname+"/one", func(t *testing.T) {
				names := make([]string, 100)
				for i := 0; i < 100; i++ {
					names[i] = g.GenerateOne(strconv.Itoa(i))
				}
				validate(t, names, ps)
			})
			t.Run(tname+"/multiple", func(t *testing.T) {
				conflicts := make(map[string]struct{})
				names, _ := g.GenerateMultiple(context.Background(), 100, conflicts)
				validate(t, names, ps)
			})
		})
	}

	cfg := NameGeneratorConfig{Suffix: true}
	testNameGen("base", &cfg, "hello",
		preds{
			always: []pred{func(n string) bool { return strings.HasPrefix(n, "hello") }},
			never:  []pred{func(n string) bool { return !strings.HasPrefix(n, "hello") }},
			probs:  []pred{func(n string) bool { return strings.HasSuffix(n, "0") }},
		},
	)

	cfg = NameGeneratorConfig{Capitals: 0.5}
	testNameGen("capitals", &cfg, "hellouniverse",
		preds{
			always: []pred{func(n string) bool { return strings.ToLower(n) == "hellouniverse" }},
			probs:  []pred{func(n string) bool { return strings.ToLower(n) != n }},
		},
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
		preds{
			always: []pred{func(n string) bool { return removeSpecial(n) == "hellouniverse" }},
			probs: []pred{
				func(n string) bool { return strings.Contains(n, ".") },
				func(n string) bool { return strings.Contains(n, "/") },
				func(n string) bool { return strings.Contains(n, "-") },
			},
		},
	)

	cfg = NameGeneratorConfig{Quote: 0.5}
	testNameGen("quote", &cfg, "hellouniverse",
		preds{
			always: []pred{func(n string) bool { return removeSpecial(n) == "hellouniverse" }},
			probs: []pred{
				func(n string) bool { return strings.Contains(n, "'") },
				func(n string) bool { return strings.Contains(n, "\"") },
			},
		},
	)

	cfg = NameGeneratorConfig{Fmt: 0.5}
	testNameGen("fmt", &cfg, "hellouniverse",
		preds{
			probs: []pred{
				func(n string) bool { return strings.Contains(n, "%") },
			},
		},
	)

	cfg = NameGeneratorConfig{Escapes: 0.5}
	testNameGen("fmt", &cfg, "hellouniverse",
		preds{
			probs: []pred{
				func(n string) bool { return strings.Contains(n, `\`) },
				func(n string) bool { return strings.Contains(n, `%`) },
			},
		},
	)

	cfg = NameGeneratorConfig{Space: 0.5}
	testNameGen("space", &cfg, "hellouniverse",
		preds{
			always: []pred{func(n string) bool { return removeSpecial(n) == "hellouniverse" }},
			probs:  []pred{func(n string) bool { return strings.Contains(n, " ") }},
		},
	)

	cfg = NameGeneratorConfig{Whitespace: 1}
	testNameGen("whitespace", &cfg, "hellouniverse",
		preds{
			always: []pred{func(n string) bool { return removeSpecial(n) == "hellouniverse" }},
			probs: []pred{
				func(n string) bool { return strings.Contains(n, " ") },
				func(n string) bool { return strings.Contains(n, "\t") },
				func(n string) bool { return strings.Contains(n, "\n") },
				func(n string) bool { return strings.Contains(n, "\f") },
				func(n string) bool { return strings.Contains(n, "\v") },
			},
		},
	)

	cfg = NameGeneratorConfig{Emote: 0.5}
	testNameGen("emote", &cfg, "hellouniverse",
		preds{
			always: []pred{func(n string) bool { return removeSpecial(n) == "hellouniverse" }},
			probs: []pred{
				func(n string) bool {
					return strings.IndexFunc(n, func(r rune) bool {
						return r >= '😀' && r <= '🙄'
					}) >= 0
				},
			},
		},
	)

	cfg = NameGeneratorConfig{Diacritics: 0.5, DiacriticDepth: 1}
	aPlusDiacritics := []string{}
	for i := 0x300; i <= 0x36f; i++ {
		aPlusDiacritics = append(aPlusDiacritics, norm.NFC.String(string([]rune{'a', rune(i)})))
	}
	testNameGen("diacritics", &cfg, "aaaaaaaaaaaaa",
		preds{
			probs: []pred{func(n string) bool {
				for _, ad := range aPlusDiacritics {
					if strings.Contains(n, ad) {
						return true
					}
				}
				return false
			}},
		},
	)
}
