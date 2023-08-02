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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
)

const (
	defaultNumIterations = 3
	defaultSeed          = int64(42)
	defaultDuration      = 10 * time.Minute
	defaultVerbosity     = false
)

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
	defaultPlacementType     = gen.Uniform
	defaultReplicationFactor = 3
	defaultBytes             = int64(0)
)

const (
	defaultStat                 = "replicas"
	defaultHeight, defaultWidth = 15, 80
)

type defaultSettings struct {
	numIterations     int
	seed              int64
	duration          time.Duration
	verbose           bool
	nodes             int
	storesPerNode     int
	keySpace          int
	rwRatio           float64
	rate              float64
	minBlock          int
	maxBlock          int
	minKey            int64
	maxKey            int64
	skewedAccess      bool
	ranges            int
	placementType     gen.PlacementType
	replicationFactor int
	bytes             int64
	stat              string
	height            int
	width             int
}

func (d defaultSettings) printDefaultSettings(w *tabwriter.Writer) {
	if _, err := fmt.Fprintln(w, "default_settings"); err != nil {
		panic(err)
	}
	_, _ = fmt.Fprintf(w, "numIterat8ions=%d\t", d.numIterations)
	_, _ = fmt.Fprintf(w, "seed=%d\t", d.seed)
	_, _ = fmt.Fprintf(w, "duration=%s\t", d.duration)
	_, _ = fmt.Fprintf(w, "verbose=%t\t", d.verbose)
	_, _ = fmt.Fprintf(w, "nodes=%d\t", d.nodes)
	_, _ = fmt.Fprintf(w, "storesPerNode=%d\t", d.storesPerNode)
	_, _ = fmt.Fprintf(w, "keySpace=%d\t\n", d.keySpace)
	_, _ = fmt.Fprintf(w, "rwRatio=%0.2f\t", d.rwRatio)
	_, _ = fmt.Fprintf(w, "rate=%0.2f\t", d.rate)
	_, _ = fmt.Fprintf(w, "minBlock=%d\t", d.minBlock)
	_, _ = fmt.Fprintf(w, "maxBlock=%d\t", d.maxBlock)
	_, _ = fmt.Fprintf(w, "minKey=%d\t", d.minKey)
	_, _ = fmt.Fprintf(w, "maxKey=%d\t", d.maxKey)
	_, _ = fmt.Fprintf(w, "skewedAccess=%t\t\n", d.skewedAccess)
	_, _ = fmt.Fprintf(w, "ranges=%d\t", d.ranges)
	_, _ = fmt.Fprintf(w, "placementType=%v\t", d.placementType)
	_, _ = fmt.Fprintf(w, "replicationFactor=%d\t", d.replicationFactor)
	_, _ = fmt.Fprintf(w, "bytes=%d\t", d.bytes)
	_, _ = fmt.Fprintf(w, "stat=%s\t", d.stat)
	_, _ = fmt.Fprintf(w, "height=%d\t", d.height)
	_, _ = fmt.Fprintf(w, "width=%d\t\n", d.width)
}

func getDefaultSettings() defaultSettings {
	return defaultSettings{
		numIterations:     defaultNumIterations,
		seed:              defaultSeed,
		duration:          defaultDuration,
		verbose:           defaultVerbosity,
		nodes:             defaultNodes,
		storesPerNode:     defaultStoresPerNode,
		keySpace:          defaultKeyspace,
		rwRatio:           defaultRwRatio,
		rate:              defaultRate,
		minBlock:          defaultMinBlock,
		maxBlock:          defaultMaxBlock,
		minKey:            defaultMinKey,
		maxKey:            defaultMaxKey,
		skewedAccess:      defaultSkewedAccess,
		ranges:            defaultRanges,
		placementType:     defaultPlacementType,
		replicationFactor: defaultReplicationFactor,
		bytes:             defaultBytes,
		stat:              defaultStat,
		height:            defaultHeight,
		width:             defaultWidth,
	}
}

// This file defines the default parameters for allocator simulator testing,
// including configurations for the cluster, ranges, load, static settings,
// static events, assertions, and plot settings.
func (f randTestingFramework) defaultBasicClusterGen() gen.BasicCluster {
	return gen.BasicCluster{
		Nodes:         f.defaultSettings.nodes,
		StoresPerNode: f.defaultSettings.storesPerNode,
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
		RWRatio:      f.defaultSettings.rwRatio,
		Rate:         f.defaultSettings.rate,
		SkewedAccess: f.defaultSettings.skewedAccess,
		MinBlockSize: f.defaultSettings.minBlock,
		MaxBlockSize: f.defaultSettings.maxBlock,
		MinKey:       f.defaultSettings.minKey,
		MaxKey:       f.defaultSettings.maxKey,
	}
}

func (f randTestingFramework) defaultBasicRangesGen() gen.BasicRanges {
	return gen.BasicRanges{
		BaseRanges: gen.BaseRanges{
			Ranges:            f.defaultSettings.ranges,
			KeySpace:          f.defaultSettings.keySpace,
			ReplicationFactor: f.defaultSettings.replicationFactor,
			Bytes:             f.defaultSettings.bytes,
		},
		PlacementType: f.defaultSettings.placementType,
	}
}

type plotSettings struct {
	stat          string
	height, width int
}

func (f randTestingFramework) defaultPlotSettings() plotSettings {
	return plotSettings{
		stat:   f.defaultSettings.stat,
		height: f.defaultSettings.height,
		width:  f.defaultSettings.width,
	}
}
