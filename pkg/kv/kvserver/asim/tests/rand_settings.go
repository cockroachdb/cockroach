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

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"

func getCluster(useRandom bool) gen.ClusterGen {
	if !useRandom {
		return defaultBasicClusterGen()
	}
	return gen.BasicCluster{}
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
