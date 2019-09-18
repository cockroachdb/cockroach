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

type demoTelemetry int

const (
	_ demoTelemetry = iota
	nodes
	demoLocality
	withLoad
	geoPartitionedReplicas
)

var demoTelemetryMap = map[demoTelemetry]string{
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
