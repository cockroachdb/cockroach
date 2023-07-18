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
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
)

type randTestingFramework struct {
	randSource        *rand.Rand
	rangeGenerator    generator
	keySpaceGenerator generator
	randOptions       map[string]bool
	weightedRand      []float64
}

func newRandTestingFramework(
	randSource *rand.Rand, randOptions map[string]bool,
) randTestingFramework {
	rangeGenerator := newRandomizedGenerator(randSource, defaultMinRange, defaultMaxRange)
	keySpaceGenerator := newRandomizedGenerator(randSource, defaultMinKeySpace, defaultMaxKeySpace)
	return randTestingFramework{
		randSource:        randSource,
		rangeGenerator:    rangeGenerator,
		keySpaceGenerator: keySpaceGenerator,
		randOptions:       randOptions,
	}
}

func (r randTestingFramework) getCluster() gen.ClusterGen {
	if !r.randOptions["cluster"] {
		return defaultBasicClusterGen()
	}
	return randomClusterInfoGen(r.randSource)
}

func (r randTestingFramework) getRanges() gen.RangeGen {
	if !r.randOptions["ranges"] {
		return defaultBasicRangesGen()
	}
	return r.randomBasicRangesGen()
}

func (r randTestingFramework) getLoad() gen.LoadGen {
	if !r.randOptions["load"] {
		return defaultLoadGen()
	}
	return gen.BasicLoad{}
}

func (r randTestingFramework) getStaticSettings() gen.StaticSettings {
	if !r.randOptions["static_settings"] {
		return defaultStaticSettingsGen()
	}
	return gen.StaticSettings{}
}

func (r randTestingFramework) getStaticEvents() gen.StaticEvents {
	if !r.randOptions["static_events"] {
		return defaultStaticEventsGen()
	}
	return gen.StaticEvents{}
}

func (r randTestingFramework) getAssertions() []SimulationAssertion {
	if !r.randOptions["assertions"] {
		return defaultAssertions()
	}
	return []SimulationAssertion{}
}

type generator interface {
	Num() int64
}

type generatorType int

const (
	uniformGenerator generatorType = iota
	zipfGenerator
)

func newGenerator(randSource *rand.Rand, iMin int64, iMax int64, gType generatorType) generator {
	switch gType {
	case uniformGenerator:
		return workload.NewUniformKeyGen(iMin, iMax, randSource)
	case zipfGenerator:
		return workload.NewZipfianKeyGen(iMin, iMax, 1.1, 1, randSource)
	default:
		panic(fmt.Sprintf("unexpected generator type %v", gType))
	}
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

func (r randTestingFramework) randomBasicRangesGen() gen.RangeGen {
	randOptions := gen.GetAvailablePlacementTypes()
	chosenIndex := r.randSource.Intn(len(randOptions))
	chosenType := randOptions[chosenIndex]
	if len(r.weightedRand) == 0 {
		return NewRandomizedBasicRanges(
			r.randSource,
			convertInt64ToInt(r.rangeGenerator.Num()),
			convertInt64ToInt(r.keySpaceGenerator.Num()),
			chosenType,
			defaultReplicationFactor,
			defaultBytes,
		)
	} else {
		return NewWeightedRandomizedBasicRanges(
			r.randSource,
			r.weightedRand,
			convertInt64ToInt(r.rangeGenerator.Num()),
			convertInt64ToInt(r.keySpaceGenerator.Num()),
			defaultReplicationFactor,
			defaultBytes,
		)
	}

}
