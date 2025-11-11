// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlnemesis

import (
	gosql "database/sql"
	"math/rand"

	"github.com/cockroachdb/errors"
)

type GeneratorConfig struct {
	NumIterations   int
	OpsPerIteration int
	Validators      []Validator
}

type Validator interface {
	Init(*gosql.DB, *rand.Rand) error
	GenerateRandomOperation(*rand.Rand) (_ string, ignoreErrors bool)
	Validate(*gosql.DB) error
}

// responsible for picking types of queries, types of validation
type Generator struct {
	validators []Validator
}

func MakeGenerator(config GeneratorConfig) (*Generator, error) {
	if len(config.Validators) == 0 {
		return nil, errors.Newf("no validators provided")
	}

	// TODO: perhaps pick a subset of validators.
	return &Generator{validators: config.Validators}, nil
}

func (g *Generator) RandOperation(rng *rand.Rand) (_ string, ignoreErrors bool) {
	// pick a random validator and pick an operation from that
	return g.validators[rng.Intn(len(g.validators))].GenerateRandomOperation(rng)
}

func (g *Generator) Validate(db *gosql.DB) []error {
	var failures []error
	for _, validator := range g.validators {
		if err := validator.Validate(db); err != nil {
			failures = append(failures, err)
		}
	}
	return failures
}
