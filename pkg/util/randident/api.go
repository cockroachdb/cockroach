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

	"github.com/cockroachdb/cockroach/pkg/util/randident/randidentcfg"
)

// NameGeneratorConfig is an alias for randidentcfg.Config.
type NameGeneratorConfig = randidentcfg.Config

// NameGenerator can be used to generate random SQL identifiers that
// resemble a base identifier. See NewNameGenerator().
type NameGenerator interface {
	// GenerateOne generates one name.
	//
	// The specified number is included in the name if Number is true
	// in the attached NameGeneratorConfig.
	GenerateOne(number int) string

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
		Number: true,
		Noise:  true,
	}
}
