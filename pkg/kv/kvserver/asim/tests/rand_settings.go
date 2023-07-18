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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// randomClusterInfoGen randomly picks a predefined configuration.
func randomClusterInfoGen(randSource *rand.Rand) gen.LoadedCluster {
	chosenIndex := randSource.Intn(len(state.ClusterOptions))
	chosenType := state.ClusterOptions[chosenIndex]
	return loadClusterInfo(chosenType)
}

type RandomizedBasicRanges struct {
	gen.BaseRanges
	placementType gen.PlacementType
	randSource    *rand.Rand
}

func NewRandomizedBasicRanges(
	randSource *rand.Rand,
	ranges int,
	keySpace int,
	placementType gen.PlacementType,
	replicationFactor int,
	bytes int64,
) RandomizedBasicRanges {
	if placementType == gen.WeightedRandom {
		// BETTER WARNING
		panic("cannot use randomized basic ranges")
	}
	return RandomizedBasicRanges{
		BaseRanges: gen.BaseRanges{
			Ranges:            ranges,
			KeySpace:          keySpace,
			ReplicationFactor: replicationFactor,
			Bytes:             bytes,
		},
		placementType: placementType,
		randSource:    randSource,
	}
}

var _ gen.RangeGen = &RandomizedBasicRanges{}

func (r RandomizedBasicRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	rangesInfo := r.GetRangesInfo(r.placementType, len(s.Stores()), r.randSource, []float64{})
	r.LoadRangeInfo(s, rangesInfo)
	return s
}

type WeightedRandomizedBasicRanges struct {
	RandomizedBasicRanges
	weightedRand []float64
}

var _ gen.RangeGen = &WeightedRandomizedBasicRanges{}

func NewWeightedRandomizedBasicRanges(
	randSource *rand.Rand,
	weightedRand []float64,
	ranges int,
	keySpace int,
	replicationFactor int,
	bytes int64,
) WeightedRandomizedBasicRanges {
	return WeightedRandomizedBasicRanges{
		RandomizedBasicRanges: NewRandomizedBasicRanges(randSource, ranges, keySpace, gen.WeightedRandom, replicationFactor, bytes),
		weightedRand:          weightedRand,
	}
}

func (wr WeightedRandomizedBasicRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	rangesInfo := wr.GetRangesInfo(wr.placementType, len(s.Stores()), wr.randSource, wr.weightedRand)
	wr.LoadRangeInfo(s, rangesInfo)
	return s
}
