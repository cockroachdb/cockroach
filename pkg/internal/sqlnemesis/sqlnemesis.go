// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlnemesis

import (
	gosql "database/sql"
	"math/rand"
)

func RunNemesis(db *gosql.DB, rnd *rand.Rand, config GeneratorConfig) ([]error, error) {
	g, err := MakeGenerator(rnd, config)
	if err != nil {
		return nil, err
	}

	// not sure if this should be multi-threaded or single-threaded?
	// start with single thread for now
	for i := 0; i < config.numSteps; i++ {
		step := g.RandStep(rng)
		err := applyStep(step)
		if err != nil {
			return nil, err
		}
	}

	// we don't think our validator needs to know which statements have run
	// we only need the generator to know which validations to apply
	failures := g.Validate()
	return failures, nil
}

func applyStep(step Step) error {
	return nil
}
