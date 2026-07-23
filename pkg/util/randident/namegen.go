// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randident

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// NewNameGenerator instantiates a NameGenerator for the given
// generation pattern. The configuration is captured by reference and
// can be updated after the generator has started producing names.
func NewNameGenerator(cfg *NameGeneratorConfig, rand *rand.Rand, pattern string) NameGenerator {
	if rand == nil {
		rand, _ = randutil.NewPseudoRand()
	}
	return &nameGenerator{
		cfg:           cfg,
		rand:          rand,
		pattern:       pattern,
		suffixReplace: strings.Contains(pattern, "#"),
	}
}

type nameGenerator struct {
	cfg           *NameGeneratorConfig
	rand          *rand.Rand
	pattern       string
	suffixReplace bool
}

var _ NameGenerator = (*nameGenerator)(nil)

var escGenerators = []func(s *strings.Builder, r *rand.Rand){
	// C/Go hex escape sequences.
	func(s *strings.Builder, r *rand.Rand) { fmt.Fprintf(s, `\\x%02x`, r.Int31n(256)) },
	// C/Go special sequences.
	func(s *strings.Builder, r *rand.Rand) {
		const special = "rgfnv"
		s.WriteByte('\\')
		s.WriteByte(special[r.Intn(len(special))])
	},
	// HTTP escape sequences.
	func(s *strings.Builder, r *rand.Rand) { fmt.Fprintf(s, `%%%02x`, r.Int31n(256)) },
	// SQL escape sequences.
	func(s *strings.Builder, r *rand.Rand) { fmt.Fprintf(s, `\\u%04X`, r.Int31n(65536)) },
	func(s *strings.Builder, r *rand.Rand) { fmt.Fprintf(s, `\\U%08X`, r.Int31n(utf8.MaxRune+1)) },
}

// GenerateOne generates one random name.
func (g *nameGenerator) GenerateOne(suffix string) string {
	var s strings.Builder

	// We want to consider every character position, including the start
	// and end of the string, as a possible position to add some noise.
	// However, we don't want the length of the string to
	// increase/decrease the probability of some noise to appear. So we
	// divide each probability below by the number of positions, so that
	// the cumulative probability for the entire string is the one
	// requested by the configuration.
	l := float32(len(g.pattern) + 1)

	insertNoise := func() {
		// Add punctuation if requested.
		if g.cfg.Punctuate > 0 && g.rand.Float32() <= g.cfg.Punctuate/l {
			const punct = "./-!?&%|_{}()*,"
			c := punct[g.rand.Intn(len(punct))]
			s.WriteByte(c)
		}
		// Add special SQL quotes if requested.
		if g.cfg.Quote > 0 && g.rand.Float32() <= g.cfg.Quote/l {
			const punct = `'"`
			c := punct[g.rand.Intn(len(punct))]
			s.WriteByte(c)
		}
		// Add simple spaces if requested.
		if g.cfg.Space > 0 && g.rand.Float32() <= g.cfg.Space/l {
			s.WriteByte(' ')
		}
		// Add formatting directives if requested.
		if g.cfg.Fmt > 0 && g.rand.Float32() <= g.cfg.Fmt/l {
			s.WriteByte('%')
			const verb = "pvq"
			c := verb[g.rand.Intn(len(verb))]
			s.WriteByte(c)
		}
		// Add escape sequences if requested.
		if g.cfg.Escapes > 0 && g.rand.Float32() <= g.cfg.Escapes/l {
			fn := escGenerators[g.rand.Intn(len(escGenerators))]
			fn(&s, g.rand)
		}
		// Add complex whitespace if requested.
		if g.cfg.Whitespace > 0 && g.rand.Float32() <= g.cfg.Whitespace/l {
			const punct = " \t\n\r\f\v"
			c := punct[g.rand.Intn(len(punct))]
			s.WriteByte(c)
		}
		// Add emojis if requested.
		if g.cfg.Emote > 0 && g.rand.Float32() <= g.cfg.Emote/l {
			const emojiStart = 0x1F600 // 😀
			const emojiEnd = 0x1F644   // 🙄
			c := rune(randutil.RandIntInRange(g.rand, emojiStart, emojiEnd+1))
			s.WriteRune(c)
		}
	}

	for i, r := range g.pattern {
		if i == 0 {
			insertNoise()
		}
		if g.cfg.Capitals > 0 && ((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) && g.rand.Float32() < g.cfg.Capitals/l {
			if r >= 'a' {
				r = r - 'a' + 'A'
			}
		}
		s.WriteRune(r)

		// Add diacritics if requested.
		// We only add diacritics after a character in the pattern.
		if g.cfg.Diacritics > 0 && g.rand.Float32() <= g.cfg.Diacritics/l {
			const combiStart = 0x300
			const combiEnd = 0x36f
			numDias := 1
			if g.cfg.DiacriticDepth > 1 {
				numDias = randutil.RandIntInRange(g.rand, 1, int(g.cfg.DiacriticDepth)-1)
			}
			for i := 0; i < numDias; i++ {
				c := rune(randutil.RandIntInRange(g.rand, combiStart, combiEnd+1))
				s.WriteRune(c)
			}
		}
		insertNoise()
	}

	if g.cfg.Suffix {
		if g.suffixReplace {
			r := strings.ReplaceAll(s.String(), "#", suffix)
			r = lexbase.NormalizeString(r)
			return r
		}
		fmt.Fprintf(&s, "_%s", suffix)
	}
	// The introduction of special unicode characters above may result
	// in combined runes that should be reduced to NFC to form a valid
	// SQL identifier. Do it now.
	return lexbase.NormalizeString(s.String())
}

// GenerateMultiple generates multiple names.
func (g *nameGenerator) GenerateMultiple(
	ctx context.Context, count int, conflictNames map[string]struct{},
) (res []string, err error) {
	res = make([]string, 0, count)
	for i := 1; len(res) < count; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		candidateName := g.GenerateOne(strconv.Itoa(i))
		if _, ok := conflictNames[candidateName]; ok {
			continue
		}
		conflictNames[candidateName] = struct{}{}
		res = append(res, candidateName)
	}
	return res, nil
}
