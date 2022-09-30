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
	strLen     int
	complexity int
}

// RandOption is an option to control generation of random json.
type RandOption interface {
	apply(*randConfig)
}

type funcOpt func(cfg *randConfig)

func (fn funcOpt) apply(cfg *randConfig) {
	fn(cfg)
}

// WithMaxStrLen returns an option to set maximum length of random JSON string objects.
func WithMaxStrLen(l int) RandOption {
	return funcOpt(func(cfg *randConfig) {
		cfg.strLen = l
	})
}

// WithComplexity returns an option to set maximum complexity of JSON objection.
func WithComplexity(c int) RandOption {
	return funcOpt(func(cfg *randConfig) {
		cfg.complexity = c
	})
}

func defaultRandConfig(complexity int) randConfig {
	return randConfig{
		strLen:     defaultRandStrLen,
		complexity: complexity,
	}
}

// RandGen generates a random JSON value configured with specified options.
func RandGen(rng *rand.Rand, opts ...RandOption) (JSON, error) {
	cfg := defaultRandConfig(20)
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return MakeJSON(doRandomJSON(rng, &cfg))
}

// Random generates a random JSON value.
func Random(complexity int, rng *rand.Rand) (JSON, error) {
	cfg := defaultRandConfig(complexity)
	return MakeJSON(doRandomJSON(rng, &cfg))
}

const defaultRandStrLen = 10

func randomJSONString(rng *rand.Rand, maxLen int) interface{} {
	if maxLen <= defaultRandStrLen && rng.Intn(2) == 0 {
		return staticStrings[rng.Intn(len(staticStrings))]
	}
	result := make([]byte, 0)
	l := rng.Intn(maxLen) + 3
	for i := 0; i < l+maxLen/4 && i < maxLen; i++ { // Bias to generate larger strings.
		result = append(result, byte(rng.Intn(0x7f-0x20)+0x20))
	}
	return string(result)
}

func randomJSONNumber(rng *rand.Rand) interface{} {
	return json.Number(fmt.Sprintf("%v", rng.ExpFloat64()))
}

func doRandomJSON(rng *rand.Rand, cfg *randConfig) interface{} {
	if cfg.complexity <= 0 || rng.Intn(cfg.complexity) == 0 {
		switch rng.Intn(5) {
		case 0:
			return randomJSONString(rng, cfg.strLen)
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
			result[randomJSONString(rng, defaultRandStrLen).(string)] = doRandomJSON(rng, cfg)
		}
		return result
	default:
		j, _ := Random(cfg.complexity, rng)
		encoding, _ := EncodeJSON(nil, j)
		encoded, _ := newEncodedFromRoot(encoding)
		return encoded
	}
}
