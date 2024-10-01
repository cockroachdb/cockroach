// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randident

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/randident/randidentcfg"
)

// NameGeneratorConfig is an alias for randidentcfg.Config.
type NameGeneratorConfig = randidentcfg.Config

// NameGenerator can be used to generate random SQL identifiers that
// resemble a base identifier. See NewNameGenerator().
type NameGenerator interface {
	// GenerateOne generates one name.
	//
	// The specified suffix is included in the name if Suffix is true
	// in the attached NameGeneratorConfig.
	GenerateOne(suffix string) string

	// GenerateMultiple generates count names, guaranteed to not be
	// present in the conflictNames map.
	//
	// ctx is checked for context cancellation, so the operation can be
	// interrupted when the algorithm has a hard time avoiding
	// conflicts.
	GenerateMultiple(ctx context.Context, count int, conflictNames map[string]struct{}) ([]string, error)
}

// DefaultNameGeneratorConfig returns a recommended default
// for NameGeneratorConfig. It generates names that are valid
// SQL identifiers but containing special characters.
func DefaultNameGeneratorConfig() NameGeneratorConfig {
	return NameGeneratorConfig{
		// When diacritics are enabled, just add one diacritic per
		// character in the pattern by default.
		DiacriticDepth: 1,

		// Number the objects by default and add some noise.
		Suffix: true,
		Noise:  true,
	}
}
