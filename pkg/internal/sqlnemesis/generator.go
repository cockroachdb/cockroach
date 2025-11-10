// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlnemesis

import "math/rand"

type GeneratorConfig struct {
	numSteps int
}

type Validator interface {
	GenerateRandomSetup(rng *rand.Rand)
	GenerateRandomOperation(rng *rand.Rand) Operation
	Validate() error
}

// responsible for picking types of queries, types of validation
type Generator struct {
	validators []Validator
}

func MakeGenerator(config GeneratorConfig) (*Generator, error) {
	return &Generator{
		validators: []Validator(nil),
	}, nil
}

func (g *Generator) RandStep(rng *rand.Rand) Step {
	// pick a random validator and pick an operation from that
	return Step{g.validators[rng.Intn(len(g.validators))].GenerateRandomOperation(rng)}
}

func (g *Generator) Validate() []error {
	var failures []error
	for _, validator := range g.validators {
		if err := validator.Validate(); err != nil {
			failures = append(failures, err)
		}
	}
	return failures
}
