// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const (
	flagName        = "seed"
	flagDescription = "Random seed. Must be the same in 'init' and 'run'. Default changes in each run"
)

// RandomSeed is the interface used by the workload runner to print
// the seed used in a run.
type RandomSeed interface {
	LogMessage() string
}

// The following structs implement the same logic for int64 and uint64
// seeds, respectively. The distinction exists because currently some
// workloads use the `math/rand` package, while others use
// `golang.org/x/exp/rand`: the former uses int64 seeds and the latter
// uses uint64 seeds.

// Int64RandomSeed implements the RandomSeed interface for workloads
// that take an int64 seed.
type Int64RandomSeed struct {
	seed int64
}

// NewInt64RandomSeed creates a new Int64RandomSeed.
func NewInt64RandomSeed() *Int64RandomSeed {
	_, seed := randutil.NewPseudoRand()
	return &Int64RandomSeed{seed: seed}
}

// Set is a test-only function.
func (rs *Int64RandomSeed) Set(val int64) {
	rs.seed = val
}

// Seed returns the underlying rng seed.
func (rs *Int64RandomSeed) Seed() int64 {
	return rs.seed
}

// AddFlag adds a `seed` command line flag to a command that can be
// used by the caller to specify a custom random seed; if no seed is
// passed, a random one is used. Particularly useful in workloads that
// implement `init` and `run`, where the same seed needs to be passed.
func (rs *Int64RandomSeed) AddFlag(flags *Flags) {
	flags.Int64Var(&rs.seed, flagName, rs.Seed(), flagDescription)
}

// LogMessage returns a string that can be used for logging the chosen
// random seed.
func (rs *Int64RandomSeed) LogMessage() string {
	return fmt.Sprintf("random seed: %d", rs.seed)
}

// Uint64RandomSeed implements the RandomSeed interface for workloads
// that take an int64 seed.
type Uint64RandomSeed struct {
	seed uint64
}

// NewUint64RandomSeed creates a new Uint64RandomSeed.
func NewUint64RandomSeed() *Uint64RandomSeed {
	_, seed := randutil.NewPseudoRand()
	return &Uint64RandomSeed{seed: uint64(seed)}
}

// Set is a test-only function.
func (rs *Uint64RandomSeed) Set(val uint64) {
	rs.seed = val
}

// Seed returns the underlying rng seed.
func (rs *Uint64RandomSeed) Seed() uint64 {
	return rs.seed
}

// AddFlag has the same semantics as `AddFlag` for Int64RandomSeed.
func (rs *Uint64RandomSeed) AddFlag(flags *Flags) {
	flags.Uint64Var(&rs.seed, flagName, rs.Seed(), flagDescription)
}

// LogMessage returns a string that can be used for logging the chosen
// random seed.
func (rs *Uint64RandomSeed) LogMessage() string {
	return fmt.Sprintf("random seed: %d", rs.seed)
}
