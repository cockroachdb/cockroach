// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// randomClusterInfoGen returns a randomly picked predefined configuration.
func (f randTestingFramework) randomClusterInfoGen(randSource *rand.Rand) gen.LoadedCluster {
	switch t := f.s.clusterGen.clusterGenType; t {
	case singleRegion:
		chosenIndex := randSource.Intn(len(state.SingleRegionClusterOptions))
		chosenType := state.SingleRegionClusterOptions[chosenIndex]
		return loadClusterInfo(chosenType)
	case multiRegion:
		chosenIndex := randSource.Intn(len(state.MultiRegionClusterOptions))
		chosenType := state.MultiRegionClusterOptions[chosenIndex]
		return loadClusterInfo(chosenType)
	case anyRegion:
		chosenIndex := randSource.Intn(len(state.AllClusterOptions))
		chosenType := state.AllClusterOptions[chosenIndex]
		return loadClusterInfo(chosenType)
	default:
		panic("unknown cluster gen type")
	}
}

// RandomizedBasicRanges implements the RangeGen interface, supporting random
// range info distribution.
type RandomizedBasicRanges struct {
	gen.BaseRanges
	placementType gen.PlacementType
	randSource    *rand.Rand
}

var _ gen.RangeGen = &RandomizedBasicRanges{}

func (r RandomizedBasicRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	if r.placementType != gen.Random {
		panic("RandomizedBasicRanges generate only randomized distributions")
	}
	rangesInfo := r.GetRangesInfo(r.placementType, len(s.Stores()), r.randSource, []float64{})
	r.LoadRangeInfo(s, rangesInfo)
	return s
}

// WeightedRandomizedBasicRanges implements the RangeGen interface, supporting
// weighted random range info distribution.
type WeightedRandomizedBasicRanges struct {
	gen.BaseRanges
	placementType gen.PlacementType
	randSource    *rand.Rand
	weightedRand  []float64
}

var _ gen.RangeGen = &WeightedRandomizedBasicRanges{}

func (wr WeightedRandomizedBasicRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	if wr.placementType != gen.WeightedRandom || len(wr.weightedRand) == 0 {
		panic("RandomizedBasicRanges generate only weighted randomized distributions with non-empty weightedRand")
	}
	rangesInfo := wr.GetRangesInfo(wr.placementType, len(s.Stores()), wr.randSource, wr.weightedRand)
	wr.LoadRangeInfo(s, rangesInfo)
	return s
}

// TODO(wenyihu6): Instead of duplicating the key generator logic in simulators,
// we should directly reuse the code from the repo pkg/workload/(kv|ycsb) to
// ensure consistent testing.

// generator generates both ranges and keyspace parameters for ranges
// generations.
type generator interface {
	key() int64
}

type uniformKeyGenerator struct {
	min, max int64
	random   *rand.Rand
}

// newUniformKeyGen returns a generator that generates number∈[min, max] with a
// uniform distribution.
func newUniformKeyGen(min, max int64, rand *rand.Rand) generator {
	if max <= min {
		panic(fmt.Sprintf("max (%d) must be greater than min (%d)", max, min))
	}
	return &uniformKeyGenerator{
		min:    min,
		max:    max,
		random: rand,
	}
}

func (g *uniformKeyGenerator) key() int64 {
	return g.random.Int63n(g.max-g.min) + g.min
}

type zipfianKeyGenerator struct {
	min, max int64
	random   *rand.Rand
	zipf     *rand.Zipf
}

// newZipfianKeyGen returns a generator that generates number ∈[min, max] with a
// zipfian distribution.
func newZipfianKeyGen(min, max int64, s float64, v float64, random *rand.Rand) generator {
	if max <= min {
		panic(fmt.Sprintf("max (%d) must be greater than min (%d)", max, min))
	}
	return &zipfianKeyGenerator{
		min:    min,
		max:    max,
		random: random,
		zipf:   rand.NewZipf(random, s, v, uint64(max-min)),
	}
}

func (g *zipfianKeyGenerator) key() int64 {
	return int64(g.zipf.Uint64()) + g.min
}

type generatorType int

const (
	uniformGenerator generatorType = iota
	zipfGenerator
)

func (g generatorType) String() string {
	switch g {
	case uniformGenerator:
		return "uniform"
	case zipfGenerator:
		return "zipf"
	default:
		panic("unknown cluster type")
	}
}

func (g generatorType) getGeneratorType(s string) generatorType {
	switch s {
	case "uniform":
		return uniformGenerator
	case "zipf":
		return zipfGenerator
	default:
		panic(fmt.Sprintf("unknown generator type: %s", s))
	}
}

// newGenerator returns a generator that generates number ∈[min, max] following
// a distribution based on gType.
func newGenerator(randSource *rand.Rand, iMin int64, iMax int64, gType generatorType) generator {
	switch gType {
	case uniformGenerator:
		return newUniformKeyGen(iMin, iMax, randSource)
	case zipfGenerator:
		return newZipfianKeyGen(iMin, iMax, 1.1, 1, randSource)
	default:
		panic(fmt.Sprintf("unexpected generator type %v", gType))
	}
}

type clusterConfigType int

const (
	singleRegion clusterConfigType = iota
	multiRegion
	anyRegion
)

func (c clusterConfigType) String() string {
	switch c {
	case singleRegion:
		return "single_region"
	case multiRegion:
		return "multi_region"
	case anyRegion:
		return "any_region"
	default:
		panic("unknown cluster type")
	}
}

func (c clusterConfigType) getClusterConfigType(s string) clusterConfigType {
	switch s {
	case "single_region":
		return singleRegion
	case "multi_region":
		return multiRegion
	case "any_region":
		return anyRegion
	default:
		panic(fmt.Sprintf("unknown cluster type: %s", s))
	}
}
