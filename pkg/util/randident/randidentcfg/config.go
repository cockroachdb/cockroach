// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package randidentcfg is defined separately from randident so that
// client packages can avoid a direct dependency on randident, and
// randident can be rebuilt without rebuilding the entire sql package.
package randidentcfg

// ConfigDoc explains how to use the name generation configuration.
const ConfigDoc = `
- "number": whether to add a number to the generated names (default true).
  When enabled, occurrences of the character '#' in the name pattern are
  replaced by the number. If '#' is not present, the number is added at the end.
- "noise": whether to add noise to the generated names (default true).
  It adds a non-zero probability for each of the probability options below left to zero.
  (To enable noise generally but disable one type of noise, set its probability to -1.)
- "punctuate": probability of adding punctuation.
- "fmt": probability of adding random Go/C formatting directives.
- "escapes": probability of adding random escape sequences.
- "quote": probabiltiy of adding single or double quotes.
- "emote": probability of adding emojis.
- "space": probability of adding simple spaces.
- "whitespace": probability of adding complex whitespace.
- "capitals": probability of using capital letters.
  Note: the name pattern must contain ASCII letters already for capital letters to be used.
- "diacritics": probability of adding diacritics.
- "diacritic_depth": max number of diacritics to add at a time (default 1).
- "zalgo": special option that overrides diacritics and diacritic_depth (default false).
`

// Config configures name generation.
//
// The caller is responsible for calling the Finalize() method
// before using a config object.
type Config struct {
	// Suffix adds an identifying suffix to the name.
	// The pattern name can contain '#' to indicate where the suffix should go.
	Suffix bool `json:"suffix"`

	// Noise indicates that non-zero name generation options get
	// a non-zero value.
	Noise bool `json:"noise"`

	// Punctuate adds punctuation in the generated names.
	Punctuate float32 `json:"punctuate,omitempty"`

	// Quote adds quotes in the generate names.
	Quote float32 `json:"quote,omitempty"`

	// Emote adds emojis in the generated names.
	Emote float32 `json:"emote,omitempty"`

	// Space adds simple spaces in the generated names.
	Space float32 `json:"space,omitempty"`

	// Fmt adds Go/C formatting directives in the generated names.
	Fmt float32 `json:"fmt,omitempty"`

	// Escapes adds escape sequences in the generated names.
	Escapes float32 `json:"escapes,omitempty"`

	// Whitespace adds extra whitespace in the generated names.
	Whitespace float32 `json:"whitespace,omitempty"`

	// Capitals adds capitals in the generated names.
	Capitals float32 `json:"capitals,omitempty"`

	// Diacritics	adds diacritical marks in the generated names.
	Diacritics float32 `json:"diacritics,omitempty"`

	// DiacriticDepth is the max number of diacritic to add per character.
	DiacriticDepth int16 `json:"diacritic_depth,omitempty"`

	// Zalgo generates many diacritics.
	// See: https://en.wikipedia.org/wiki/Zalgo_text
	// The Zalgo config is not just a gimmick or an easter egg; it can
	// be used to exercise how well the display of names (e.g. in a web
	// UI) respects diacritics.
	Zalgo bool `json:"zalgo,omitempty"`
}

// Finalize populates all configuration fields after some of them have
// been customized.
func (cfg *Config) Finalize() {
	if cfg.Noise {
		// We want a majority of names (but not all) to contain noise.
		//
		// Let us reserve ~1/3rd of names without noise. We have 9
		// sources of noise below. The probability of each source being
		// activated for a given name is cumulative. So we divide the
		// probability we want there be some noise (2/3rd) by the number of
		// sources (9). Hence ~8%. This number should be tweaked when
		// adding/removing noise sources.
		//
		// If the starting value is nonzero, that's a sign it was
		// customized, in which case we keep that customization.
		const noiseProbabilityPerSource = 0.08
		if cfg.Punctuate == 0 {
			cfg.Punctuate = noiseProbabilityPerSource
		}
		if cfg.Quote == 0 {
			cfg.Quote = noiseProbabilityPerSource
		}
		if cfg.Emote == 0 {
			cfg.Emote = noiseProbabilityPerSource
		}
		if cfg.Space == 0 {
			cfg.Space = noiseProbabilityPerSource
		}
		if cfg.Fmt == 0 {
			cfg.Fmt = noiseProbabilityPerSource
		}
		if cfg.Escapes == 0 {
			cfg.Escapes = noiseProbabilityPerSource
		}
		if cfg.Whitespace == 0 {
			cfg.Whitespace = noiseProbabilityPerSource
		}
		if cfg.Capitals == 0 {
			cfg.Capitals = noiseProbabilityPerSource
		}
		if cfg.Diacritics == 0 {
			cfg.Diacritics = noiseProbabilityPerSource
		}
	}
	if cfg.Zalgo {
		// Add diacritics everywhere.
		cfg.Diacritics = 1000.0
		// Add many at a time.
		cfg.DiacriticDepth = 20
	}
	if cfg.Diacritics <= 0 {
		// Silence the field when pretty-printing.
		cfg.DiacriticDepth = 0
	}
}

// HasVariability returns true to indicate that name generation
// is guaranteed to return different names with the same config
// and name number.
func (cfg *Config) HasVariability() bool {
	return cfg.Suffix ||
		cfg.Punctuate > 0 ||
		cfg.Quote > 0 ||
		cfg.Emote > 0 ||
		cfg.Space > 0 ||
		cfg.Fmt > 0 ||
		cfg.Escapes > 0 ||
		cfg.Whitespace > 0 ||
		cfg.Capitals > 0 ||
		cfg.Diacritics > 0
}
