// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const (
	flagName        = "seed"
	flagDescription = "Random seed. Default changes in each run"
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

type Int64RandomSeed struct {
	seed int64
}

func NewInt64RandomSeed() *Int64RandomSeed {
	_, seed := randutil.NewPseudoRand()
	return &Int64RandomSeed{seed: seed}
}

// test-only
func (rs *Int64RandomSeed) Set(val int64) {
	rs.seed = val
}

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

func (rs *Int64RandomSeed) LogMessage() string {
	return fmt.Sprintf("random seed: %d", rs.seed)
}

type Uint64RandomSeed struct {
	seed uint64
}

func NewUint64RandomSeed() *Uint64RandomSeed {
	_, seed := randutil.NewPseudoRand()
	return &Uint64RandomSeed{seed: uint64(seed)}
}

// test-only
func (rs *Uint64RandomSeed) Set(val uint64) {
	rs.seed = val
}

func (rs *Uint64RandomSeed) Seed() uint64 {
	return rs.seed
}

func (rs *Uint64RandomSeed) AddFlag(flags *Flags) {
	flags.Uint64Var(&rs.seed, flagName, rs.Seed(), flagDescription)
}

func (rs *Uint64RandomSeed) LogMessage() string {
	return fmt.Sprintf("random seed: %d", rs.seed)
}
