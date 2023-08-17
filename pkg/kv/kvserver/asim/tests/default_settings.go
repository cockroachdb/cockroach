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
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
)

// This file defines settings for default generations where randomization is
// disabled. For instance, defaultBasicRangesGen is only used if
// randOption.range is false.
const (
	defaultNodes         = 3
	defaultStoresPerNode = 1
)

const defaultKeyspace = 200000

const (
	defaultRwRatio, defaultRate      = 0.0, 0.0
	defaultMinBlock, defaultMaxBlock = 1, 1
	defaultMinKey, defaultMaxKey     = int64(1), int64(defaultKeyspace)
	defaultSkewedAccess              = false
)

const (
	defaultRanges            = 1
	defaultPlacementType     = gen.Even
	defaultReplicationFactor = 3
	defaultBytes             = int64(0)
)

const (
	defaultStat                 = "replicas"
	defaultHeight, defaultWidth = 15, 80
)

type staticOptionSettings struct {
	nodes             int
	storesPerNode     int
	rwRatio           float64
	rate              float64
	minBlock          int
	maxBlock          int
	minKey            int64
	maxKey            int64
	skewedAccess      bool
	ranges            int
	keySpace          int
	placementType     gen.PlacementType
	replicationFactor int
	bytes             int64
	stat              string
	height            int
	width             int
}

func (d staticOptionSettings) printStaticOptionSettings(w *tabwriter.Writer) {
	_, _ = fmt.Fprintln(w, "STATIC OPTION:")
	_, _ = fmt.Fprintf(w, "nodes=%d\t", d.nodes)
	_, _ = fmt.Fprintf(w, "stores_per_node=%d\t", d.storesPerNode)
	_, _ = fmt.Fprintf(w, "rw_ratio=%0.2f\t", d.rwRatio)
	_, _ = fmt.Fprintf(w, "rate=%0.2f\t", d.rate)
	_, _ = fmt.Fprintf(w, "min_block=%d\t", d.minBlock)
	_, _ = fmt.Fprintf(w, "max_block=%d\t", d.maxBlock)
	_, _ = fmt.Fprintf(w, "min_key=%d\t", d.minKey)
	_, _ = fmt.Fprintf(w, "max_key=%d\t", d.maxKey)
	_, _ = fmt.Fprintf(w, "skewed_access=%t\t\n", d.skewedAccess)
	_, _ = fmt.Fprintf(w, "ranges=%d\t", d.ranges)
	_, _ = fmt.Fprintf(w, "key_space=%d\t\n", d.keySpace)
	_, _ = fmt.Fprintf(w, "placement_type=%v\t", d.placementType)
	_, _ = fmt.Fprintf(w, "replication_factor=%d\t", d.replicationFactor)
	_, _ = fmt.Fprintf(w, "bytes=%d\t", d.bytes)
	_, _ = fmt.Fprintf(w, "stat=%s\t", d.stat)
	_, _ = fmt.Fprintf(w, "height=%d\t", d.height)
	_, _ = fmt.Fprintf(w, "width=%d\t\n", d.width)
}

func getDefaultStaticOptionSettings() staticOptionSettings {
	return staticOptionSettings{
		nodes:             defaultNodes,
		storesPerNode:     defaultStoresPerNode,
		rwRatio:           defaultRwRatio,
		rate:              defaultRate,
		minBlock:          defaultMinBlock,
		maxBlock:          defaultMaxBlock,
		minKey:            defaultMinKey,
		maxKey:            defaultMaxKey,
		skewedAccess:      defaultSkewedAccess,
		ranges:            defaultRanges,
		keySpace:          defaultKeyspace,
		placementType:     defaultPlacementType,
		replicationFactor: defaultReplicationFactor,
		bytes:             defaultBytes,
		stat:              defaultStat,
		height:            defaultHeight,
		width:             defaultWidth,
	}
}

func (f randTestingFramework) defaultBasicClusterGen() gen.BasicCluster {
	return gen.BasicCluster{
		Nodes:         f.staticSettings.nodes,
		StoresPerNode: f.staticSettings.storesPerNode,
	}
}

func (f randTestingFramework) defaultStaticSettingsGen() gen.StaticSettings {
	return gen.StaticSettings{Settings: config.DefaultSimulationSettings()}
}

func (f randTestingFramework) defaultStaticEventsGen() gen.StaticEvents {
	return gen.StaticEvents{DelayedEvents: event.DelayedEventList{}}
}

func (f randTestingFramework) defaultLoadGen() gen.BasicLoad {
	return gen.BasicLoad{
		RWRatio:      f.staticSettings.rwRatio,
		Rate:         f.staticSettings.rate,
		SkewedAccess: f.staticSettings.skewedAccess,
		MinBlockSize: f.staticSettings.minBlock,
		MaxBlockSize: f.staticSettings.maxBlock,
		MinKey:       f.staticSettings.minKey,
		MaxKey:       f.staticSettings.maxKey,
	}
}

func (f randTestingFramework) defaultBasicRangesGen() gen.BasicRanges {
	return gen.BasicRanges{
		BaseRanges: gen.BaseRanges{
			Ranges:            f.staticSettings.ranges,
			KeySpace:          f.staticSettings.keySpace,
			ReplicationFactor: f.staticSettings.replicationFactor,
			Bytes:             f.staticSettings.bytes,
		},
		PlacementType: f.staticSettings.placementType,
	}
}

type plotSettings struct {
	stat          string
	height, width int
}

func (f randTestingFramework) defaultPlotSettings() plotSettings {
	return plotSettings{
		stat:   f.staticSettings.stat,
		height: f.staticSettings.height,
		width:  f.staticSettings.width,
	}
}
