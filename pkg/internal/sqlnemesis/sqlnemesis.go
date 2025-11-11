// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlnemesis

import (
	gosql "database/sql"
	"fmt"
	"math/rand"
)

// TODO: figure out observability / reproducibility story.
func log(s string) {
	fmt.Printf("%s\n", s)
}

func RunNemesis(db *gosql.DB, rng *rand.Rand, config GeneratorConfig) ([]error, error) {
	g, err := MakeGenerator(config)
	if err != nil {
		return nil, err
	}

	// Initialize the validators (which will also initialize the DB state).
	for _, validator := range g.validators {
		err = validator.Init(db, rng)
		if err != nil {
			return nil, err
		}
	}

	for iteration := 0; iteration < config.NumIterations; iteration++ {
		log(fmt.Sprintf("-- iteration %d", iteration))
		// TODO: not sure if this should be multi-threaded or single-threaded?
		// start with single thread for now
		for i := 0; i < config.OpsPerIteration; i++ {
			op, ignoreErrors := g.RandOperation(rng)
			err = apply(db, op)
			if err != nil {
				if !ignoreErrors {
					log(op)
					return nil, err
				}
			} else {
				log(op)
			}
		}

		// we don't think our validator needs to know which statements have run
		// we only need the generator to know which validations to apply
		failures := g.Validate(db)
		if len(failures) > 0 {
			return failures, nil
		}
	}
	return nil, nil
}

func apply(db *gosql.DB, stmt string) error {
	_, err := db.Exec(stmt)
	return err
}
