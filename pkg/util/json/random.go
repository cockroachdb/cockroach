// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package json

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// Some issues will only be revealed if we have duplicate strings, so we
// include a pool of common strings that we occasionally pull from rather than
// generating a completely random string.
var staticStrings = []string{
	"a",
	"b",
	"c",
	"foo",
	"bar",
	"baz",
	"foobar",
}

type randConfig struct {
	maxLen     int
	complexity int
	escapeProb float32
}

// RandOption is an option to control generation of random json.
type RandOption interface {
	apply(*randConfig)
}

type randFuncOpt func(cfg *randConfig)

func (fn randFuncOpt) apply(cfg *randConfig) {
	fn(cfg)
}

// WithMaxStrLen returns an option to set maximum length of random JSON string objects.
func WithMaxStrLen(l int) RandOption {
	return randFuncOpt(func(cfg *randConfig) {
		cfg.maxLen = l
	})
}

// WithComplexity returns an option to set maximum complexity of JSON objects.
func WithComplexity(c int) RandOption {
	return randFuncOpt(func(cfg *randConfig) {
		cfg.complexity = c
	})
}

// WithEscapeProb returns an option that configures the probability
// of producing escaped character in JSON.
// Setting to 0 produces JSON strings consisting of printable characters only.
func WithEscapeProb(p float32) RandOption {
	return randFuncOpt(func(cfg *randConfig) {
		cfg.escapeProb = p
	})
}

// defaultRandConfig is the default configuration for generating
// random JSON objects.
var defaultRandConfig = func() randConfig {
	return randConfig{
		maxLen:     defaultRandStrLen,
		complexity: 20,
		escapeProb: 0.01, // ~100 chars in our alphabet, one of which is \.
	}
}()

// objectKeyConfig is the configuration for generating object keys.
// Object keys are usually short, and are alphanumeric.
var objectKeyConfig = func() randConfig {
	return randConfig{
		maxLen:     defaultRandStrLen,
		escapeProb: 0,
	}
}()

// RandGen generates a random JSON value configured with specified options.
func RandGen(rng *rand.Rand, opts ...RandOption) (JSON, error) {
	cfg := defaultRandConfig
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return MakeJSON(doRandomJSON(rng, cfg))
}

// Random generates a random JSON value.
func Random(complexity int, rng *rand.Rand) (JSON, error) {
	cfg := randConfig{
		maxLen:     defaultRandStrLen,
		complexity: complexity,
	}
	return MakeJSON(doRandomJSON(rng, cfg))
}

const defaultRandStrLen = 10
const hexAlphabetUpperAndLower = "0123456789abcdefABCDEF"
const escapeAlphabet = `"\/'bfnrtu`

func randomJSONString(rng *rand.Rand, cfg randConfig) string {
	if cfg.maxLen <= defaultRandStrLen && rng.Intn(2) == 0 {
		return staticStrings[rng.Intn(len(staticStrings))]
	}

	// generate string of this size, biased to produce larger strings.
	l := rng.Intn(cfg.maxLen-cfg.maxLen/4) + cfg.maxLen>>2
	if cfg.escapeProb == 0 {
		return randutil.RandString(rng, l, randutil.PrintableKeyAlphabet)
	}

	result := make([]byte, 0)

	for i := 0; i < l; i++ {
		if rng.Float32() < cfg.escapeProb {
			c := escapeAlphabet[rng.Intn(len(escapeAlphabet))]
			result = append(result, '\\', c)
			i++
			if c == 'u' {
				// Generate random unicode sequence.
				result = append(result, []byte(randutil.RandString(rng, 4, hexAlphabetUpperAndLower))...)
				i += 4
			}
		} else {
			c := byte(rng.Intn(0x7f-0x20) + 0x20)
			if c == '\\' {
				// Retry -- escape handled above.
				i--
				continue
			}
			result = append(result, c)
		}
	}
	return string(result)
}

func randomJSONNumber(rng *rand.Rand) interface{} {
	return json.Number(fmt.Sprintf("%v", rng.ExpFloat64()))
}

func doRandomJSON(rng *rand.Rand, cfg randConfig) interface{} {
	if cfg.complexity <= 0 || rng.Intn(cfg.complexity) == 0 {
		switch rng.Intn(5) {
		case 0:
			return randomJSONString(rng, cfg)
		case 1:
			return randomJSONNumber(rng)
		case 2:
			return true
		case 3:
			return false
		case 4:
			return nil
		}
	}
	cfg.complexity--
	switch rng.Intn(3) {
	case 0:
		result := make([]interface{}, 0)
		for cfg.complexity > 0 {
			amount := 1 + rng.Intn(cfg.complexity)
			cfg.complexity -= amount
			result = append(result, doRandomJSON(rng, cfg))
		}
		return result
	case 1:
		result := make(map[string]interface{})
		for cfg.complexity > 0 {
			amount := 1 + rng.Intn(cfg.complexity)
			cfg.complexity -= amount
			result[randomJSONString(rng, objectKeyConfig)] = doRandomJSON(rng, cfg)
		}
		return result
	default:
		j, _ := MakeJSON(doRandomJSON(rng, cfg))
		encoding, _ := EncodeJSON(nil, j)
		encoded, _ := newEncodedFromRoot(encoding)
		return encoded
	}
}
