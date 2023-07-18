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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// randomClusterInfoGen randomly picks a predefined configuration.
func randomClusterInfoGen(randSource *rand.Rand) gen.LoadedCluster {
	chosenIndex := randSource.Intn(len(state.ClusterOptions))
	chosenType := state.ClusterOptions[chosenIndex]
	return loadClusterInfo(chosenType)
}

func getCluster(randSource *rand.Rand, useRandom bool) gen.ClusterGen {
	if !useRandom {
		return defaultBasicClusterGen()
	}
	return randomClusterInfoGen(randSource)
}

func getRanges(useRandom bool) gen.RangeGen {
	if !useRandom {
		return defaultBasicRangesGen()
	}
	return gen.BasicRanges{}
}

func getLoad(useRandom bool) gen.LoadGen {
	if !useRandom {
		return defaultLoadGen()
	}
	return gen.BasicLoad{}
}

func getStaticSettings(useRandom bool) gen.StaticSettings {
	if !useRandom {
		return defaultStaticSettingsGen()
	}
	return gen.StaticSettings{}
}

func getStaticEvents(useRandom bool) gen.StaticEvents {
	if !useRandom {
		return defaultStaticEventsGen()
	}
	return gen.StaticEvents{}
}

func getAssertions(useRandom bool) []SimulationAssertion {
	if !useRandom {
		return defaultAssertions()
	}
	return []SimulationAssertion{}
}
