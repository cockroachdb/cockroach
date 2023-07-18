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

// randomClusterInfoGen returns a randomly picked predefined configuration.
func (f randTestingFramework) randomClusterInfoGen(randSource *rand.Rand) gen.LoadedCluster {
	chosenIndex := randSource.Intn(len(state.ClusterOptions))
	chosenType := state.ClusterOptions[chosenIndex]
	return loadClusterInfo(chosenType)
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
