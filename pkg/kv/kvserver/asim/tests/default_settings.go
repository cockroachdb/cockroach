// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/assertion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
)

// This file defines settings for default generations where randomization is
// disabled. For instance, defaultBasicRangesGen is only used if
// randOption.range is false.
const (
	defaultNodes             = 3
	defaultStoresPerNode     = 1
	defaultStoreByteCapacity = 256 << 30 // 256 GiB
)

const (
	defaultRwRatio, defaultRate      = 0.0, 0.0
	defaultMinBlock, defaultMaxBlock = 1, 1
	defaultMinKey, defaultMaxKey     = int64(0), int64(200000)
	defaultSkewedAccess              = false
)

const (
	defaultRanges            = 10
	defaultPlacementType     = gen.Even
	defaultReplicationFactor = 3
	defaultBytes             = int64(0)
)

const (
	defaultStat                 = "replicas"
	defaultHeight, defaultWidth = 15, 80
)

const (
	defaultRequestCPUPerAccess, defaultRaftCPUPerWrite = int64(0), int64(0)
)

type staticOptionSettings struct {
	nodes               int
	storesPerNode       int
	storeByteCapacity   int64
	rwRatio             float64
	rate                float64
	minBlock            int
	maxBlock            int
	minKey              int64
	maxKey              int64
	skewedAccess        bool
	ranges              int
	placementType       gen.PlacementType
	replicationFactor   int
	bytes               int64
	stat                string
	height              int
	width               int
	requestCPUPerAccess int64
	raftCPUPerWrite     int64
}

func getDefaultStaticOptionSettings() staticOptionSettings {
	return staticOptionSettings{
		nodes:               defaultNodes,
		storesPerNode:       defaultStoresPerNode,
		storeByteCapacity:   defaultStoreByteCapacity,
		rwRatio:             defaultRwRatio,
		rate:                defaultRate,
		minBlock:            defaultMinBlock,
		maxBlock:            defaultMaxBlock,
		minKey:              defaultMinKey,
		maxKey:              defaultMaxKey,
		skewedAccess:        defaultSkewedAccess,
		ranges:              defaultRanges,
		placementType:       defaultPlacementType,
		replicationFactor:   defaultReplicationFactor,
		bytes:               defaultBytes,
		stat:                defaultStat,
		height:              defaultHeight,
		width:               defaultWidth,
		requestCPUPerAccess: defaultRequestCPUPerAccess,
		raftCPUPerWrite:     defaultRaftCPUPerWrite,
	}
}

func (f randTestingFramework) defaultBasicClusterGen() gen.BasicCluster {
	return gen.BasicCluster{
		Nodes:             f.defaultStaticSettings.nodes,
		StoresPerNode:     f.defaultStaticSettings.storesPerNode,
		StoreByteCapacity: f.defaultStaticSettings.storeByteCapacity,
	}
}

func (f randTestingFramework) defaultStaticSettingsGen() gen.StaticSettings {
	return gen.StaticSettings{Settings: config.DefaultSimulationSettings()}
}

func (f randTestingFramework) defaultStaticEventsGen() gen.StaticEvents {
	return gen.NewStaticEventsWithNoEvents()
}

func (f randTestingFramework) defaultLoadGen() gen.BasicLoad {
	return gen.BasicLoad{
		RWRatio:             f.defaultStaticSettings.rwRatio,
		Rate:                f.defaultStaticSettings.rate,
		SkewedAccess:        f.defaultStaticSettings.skewedAccess,
		MinBlockSize:        f.defaultStaticSettings.minBlock,
		MaxBlockSize:        f.defaultStaticSettings.maxBlock,
		MinKey:              f.defaultStaticSettings.minKey,
		MaxKey:              f.defaultStaticSettings.maxKey,
		RequestCPUPerAccess: f.defaultStaticSettings.requestCPUPerAccess,
		RaftCPUPerWrite:     f.defaultStaticSettings.raftCPUPerWrite,
	}
}

func (f randTestingFramework) defaultBasicRangesGen() gen.BasicRanges {
	return gen.BasicRanges{
		BaseRanges: gen.BaseRanges{
			Ranges:            f.defaultStaticSettings.ranges,
			MinKey:            f.defaultStaticSettings.minKey,
			MaxKey:            f.defaultStaticSettings.maxKey,
			ReplicationFactor: f.defaultStaticSettings.replicationFactor,
			Bytes:             f.defaultStaticSettings.bytes,
		},
		PlacementType: f.defaultStaticSettings.placementType,
	}
}

func defaultAssertions() []assertion.SimulationAssertion {
	return []assertion.SimulationAssertion{
		assertion.ConformanceAssertion{
			Underreplicated:           0,
			Overreplicated:            0,
			ViolatingConstraints:      0,
			Unavailable:               0,
			ViolatingLeasePreferences: 0,
			LessPreferredLeases:       0,
		},
	}
}
