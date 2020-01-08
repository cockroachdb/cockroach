// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

// demoTelemetry corresponds to different sources of telemetry we are recording from cockroach demo.
type demoTelemetry int

const (
	_ demoTelemetry = iota
	// demo represents when cockroach demo is used at all.
	demo
	// nodes represents when cockroach demo is started with multiple nodes.
	nodes
	// demoLocality represents when cockroach demo is started with user defined localities.
	demoLocality
	// withLoad represents when cockroach demo is used with a background workload
	withLoad
	// geoPartitionedReplicas is used when cockroach demo is started with the geo-partitioned-replicas topology.
	geoPartitionedReplicas
)

var demoTelemetryMap = map[demoTelemetry]string{
	demo:                   "demo",
	nodes:                  "nodes",
	demoLocality:           "demo-locality",
	withLoad:               "withload",
	geoPartitionedReplicas: "geo-partitioned-replicas",
}

var demoTelemetryCounters map[demoTelemetry]telemetry.Counter

func init() {
	demoTelemetryCounters = make(map[demoTelemetry]telemetry.Counter)
	for ty, s := range demoTelemetryMap {
		demoTelemetryCounters[ty] = telemetry.GetCounterOnce(fmt.Sprintf("cli.demo.%s", s))
	}
}

func incrementDemoCounter(d demoTelemetry) {
	telemetry.Inc(demoTelemetryCounters[d])
}
