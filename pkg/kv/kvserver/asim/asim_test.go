// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
)

func TestRunAllocatorSimulator(t *testing.T) {
	ctx := context.Background()
	rwg := make([]workload.Generator, 1)
	rwg[0] = &workload.RandomGenerator{}
	start := state.TestingStartTime()
	end := start.Add(1000 * time.Second)
	interval := 10 * time.Second

	exchange := state.NewFixedDelayExhange(start, interval, interval)
	s := state.LoadConfig(state.ComplexConfig)

	sim := asim.NewSimulator(start, end, interval, rwg, s, exchange)
	sim.RunSim(ctx)
}
