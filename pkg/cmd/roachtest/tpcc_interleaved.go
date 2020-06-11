// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"time"
)

type tpccInterleavedSpec struct {
	Nodes      int
	CPUs       int
	Warehouses int
	Duration   time.Duration
}

func (s tpccInterleavedSpec) run(ctx context.Context, t *test, c *cluster) {
	runTPCC(ctx, t, c, tpccOptions{
		Warehouses:     s.Warehouses,
		Duration:       s.Duration,
		ExtraSetupArgs: fmt.Sprintf("--interleaved=true"),
		SetupType:      usingInit,
	})
}

func registerTPCCInterleavedSpec(r *testRegistry, s tpccInterleavedSpec) {
	name := fmt.Sprintf("tpcc/interleaved/nodes=%d/cpu=%d/w=%d",
		s.Nodes, s.CPUs, s.Warehouses)
	r.Add(testSpec{
		Name:       name,
		Owner:      OwnerKV,
		Cluster:    makeClusterSpec(s.Nodes+1, cpu(s.CPUs)),
		Run:        s.run,
		MinVersion: "v20.1.0",
		Timeout:    3 * time.Hour,
	})
}

func registerTPCCInterleaved(r *testRegistry) {
	specs := []tpccInterleavedSpec{
		{
			Nodes: 3,
			CPUs:  16,
			// Currently, we do not support import on interleaved tables which
			//prohibits loading/importing a fixture. If/when this is supported the
			// number of warehouses should be increased as we would no longer
			// bottleneck on initialization which is significantly slower than import.
			Warehouses: 500,
			Duration:   time.Minute * 15,
		},
	}
	for _, s := range specs {
		registerTPCCInterleavedSpec(r, s)
	}
}
