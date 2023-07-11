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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// Cluster default setting.
const (
	defaultNodes         = 3
	defaultStoresPerNode = 1
)

func defaultBasicClusterGen() gen.BasicCluster {
	return gen.BasicCluster{
		Nodes:         defaultNodes,
		StoresPerNode: defaultStoresPerNode,
	}
}

func loadClusterInfoGen(configName string) gen.LoadedCluster {
	var clusterInfo state.ClusterInfo
	switch configName {
	case "single_region":
		clusterInfo = state.SingleRegionConfig
	case "single_region_multi_store":
		clusterInfo = state.SingleRegionMultiStoreConfig
	case "multi_region":
		clusterInfo = state.MultiRegionConfig
	case "complex":
		clusterInfo = state.ComplexConfig
	default:
		panic(fmt.Sprintf("unknown cluster config %s", configName))
	}
	return gen.LoadedCluster{
		Info: clusterInfo,
	}
}

func defaultSettingsGen() gen.StaticSettings {
	return gen.StaticSettings{Settings: config.DefaultSimulationSettings()}
}

func defaultEventGen() gen.StaticEvents {
	return gen.StaticEvents{DelayedEvents: event.DelayedEventList{}}
}

const defaultKeyspace = 200000

// Load default setting.
const (
	defaultRwRatio, defaultRate      = 0.0, 0.0
	defaultMinBlock, defaultMaxBlock = 1, 1
	defaultMinKey, defaultMaxKey     = int64(1), int64(defaultKeyspace)
	defaultSkewedAccess              = false
)

func defaultLoadGen() gen.BasicLoad {
	return gen.BasicLoad{
		RWRatio:      defaultRwRatio,
		Rate:         defaultRate,
		SkewedAccess: defaultSkewedAccess,
		MinBlockSize: defaultMinBlock,
		MaxBlockSize: defaultMaxBlock,
		MinKey:       defaultMinKey,
		MaxKey:       defaultMaxKey,
	}
}

// range default setting
const (
	defaultRanges            = 1
	defaultPlacementType     = gen.Uniform
	defaultReplicationFactor = 3
	defaultBytes             = 0
)

func defaultBasicRangesGen() gen.BasicRanges {
	// No WeightedRand needed since the defaultPlacementType is uniform.
	return gen.BasicRanges{
		Ranges:            defaultRanges,
		PlacementType:     defaultPlacementType,
		KeySpace:          defaultKeyspace,
		ReplicationFactor: defaultReplicationFactor,
		Bytes:             defaultBytes,
	}
}

func defaultAssertions() []SimulationAssertion {
	return []SimulationAssertion{
		conformanceAssertion{
			underreplicated: 0,
			overreplicated:  0,
			violating:       0,
			unavailable:     0,
		},
	}
}

const (
	defaultStat                 = "replicas"
	defaultHeight, defaultWidth = 15, 80
)

type plotSettings struct {
	stat          string
	height, width int
}

func defaultPlotSettings() plotSettings {
	return plotSettings{
		stat:   defaultStat,
		height: defaultHeight,
		width:  defaultWidth,
	}
}
