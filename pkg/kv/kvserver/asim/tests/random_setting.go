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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"math"
	"math/rand"
)

type generator interface {
	Num() int64
}

type generatorType int

const (
	uniformGenerator generatorType = iota
	zipfGenerator
)

func convertInt64ToInt(num int64) int {
	// Should be impossible since we have set imax, imin to something smaller to imax32
	if num < math.MinInt32 {
		return math.MinInt32
	}
	if num > math.MaxUint32 {
		return math.MaxUint32
	}
	return int(num)
}

func newGenerator(randSource *rand.Rand, iMin int64, iMax int64, gType generatorType) generator {
	// 1. uniform 2. zipf 3. skewedlatest
	// TODO: change this so that it pick one for all iterations and stuck with it -> act its fine
	switch gType {
	case uniformGenerator:
		return workload.NewUniformKeyGen(iMin, iMax, randSource)
	case zipfGenerator:
		return workload.NewZipfianKeyGen(iMin, iMax, 1.1, 1, randSource)
	default:
		panic(fmt.Sprintf("unexpected generator type %v", gType))
	}
}

type RandomizedType int

var randOptions = [...]RandomizedType{UniformNonRandom, SkewedNonRandom, UnweightedRandom, WeightedRandom}

const (
	UniformNonRandom RandomizedType = iota
	SkewedNonRandom
	UnweightedRandom
	WeightedRandom
)

type RandomizedGenerators struct {
	rangeGenerator    generator
	keySpaceGenerator generator
}

func newRandomizedGenerator(randSource *rand.Rand, iMin int64, iMax int64) generator {
	// Randomly pick a generator type: used for this iteration
	randBool := randSource.Intn(2) == 0
	if randBool {
		return newGenerator(randSource, iMin, iMax, uniformGenerator)
	} else {
		return newGenerator(randSource, iMin, iMax, zipfGenerator)
	}
}

const (
	defaultMinRange    = 1
	defaultMaxRange    = 1000
	defaultMinKeySpace = 1000
	defaultMaxKeySpace = 200000
)

func NewRandomizedGenerators(randSource *rand.Rand) RandomizedGenerators {
	return RandomizedGenerators{
		rangeGenerator:    newRandomizedGenerator(randSource, defaultMinRange, defaultMaxRange),
		keySpaceGenerator: newRandomizedGenerator(randSource, defaultMinKeySpace, defaultMaxKeySpace),
	}
}

func (rg RandomizedGenerators) randomBasicRangesGen(randSource *rand.Rand, weightedRand []float64) RandomizedBasicRanges {
	chosenIndex := randSource.Intn(len(randOptions))
	chosenType := randOptions[chosenIndex]
	return newRandomizedBasicRanges(
		convertInt64ToInt(rg.rangeGenerator.Num()),
		convertInt64ToInt(rg.keySpaceGenerator.Num()),
		defaultReplicationFactor,
		defaultBytes,
		chosenType,
		weightedRand)
}

// RandomizedBasicRanges implements the RangeGen interface.
type RandomizedBasicRanges struct {
	br           gen.BasicRanges
	randType     RandomizedType
	weightedRand []float64
}

var _ gen.RangeGen = &RandomizedBasicRanges{}

func newRandomizedBasicRanges(
	ranges int,
	keySpace int,
	replicationFactor int,
	bytes int64,
	randType RandomizedType,
	weightedRand []float64,
) RandomizedBasicRanges {
	var placementType gen.PlacementType
	if randType == UniformNonRandom {
		placementType = gen.Uniform
	}
	if randType == SkewedNonRandom {
		placementType = gen.Skewed
	}
	return RandomizedBasicRanges{
		br:           gen.NewBasicRanges(ranges, placementType, keySpace, replicationFactor, bytes),
		randType:     randType,
		weightedRand: weightedRand,
	}
}

func (r RandomizedBasicRanges) getRangesInfo(randSource *rand.Rand, stores int) (rangesInfo state.RangesInfo) {
	br := r.br
	switch r.randType {
	case UnweightedRandom:
		rangesInfo = state.RangesInfoRandomDistribution(randSource, stores, br.Ranges, br.KeySpace, br.ReplicationFactor, br.Bytes)
	case WeightedRandom:
		rangesInfo = state.RangesInfoWeightedRandomDistribution(randSource, r.weightedRand, br.Ranges, br.KeySpace, br.ReplicationFactor, br.Bytes)
	case UniformNonRandom, SkewedNonRandom:
		rangesInfo = br.GetRangesInfo(stores)
	default:
		panic(fmt.Sprintf("unexpected randomized type %v", r.randType))
	}
	return rangesInfo
}

// Generate returns an updated simulator state, where the cluster is loaded
// with ranges based on the parameters of basic ranges.
func (r RandomizedBasicRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	stores := len(s.Stores())
	randSource := rand.New(rand.NewSource(seed))
	rangesInfo := r.getRangesInfo(randSource, stores)
	return r.br.LoadRangeInfo(s, rangesInfo)
}

func randomClusterInfoGen(randSource *rand.Rand) gen.LoadedCluster {
	chosenIndex := randSource.Intn(len(state.ClusterOptions))
	chosenType := state.ClusterOptions[chosenIndex]
	return loadClusterInfoGen(chosenType)
}
