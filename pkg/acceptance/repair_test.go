// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//

package acceptance

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// RepairTest kills and starts new nodes systematically to ensure we do
// indeed repair the cluster.
func TestRepair(t *testing.T) {
	t.Skip("TODO(bram): skip this test until failures are investigated - #6798, #6700, #6277, #6209, #5672")

	s := log.Scope(t)
	defer s.Close(t)

	runTestOnConfigs(t, testRepairInner)
}

func testRepairInner(ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	dc := newDynamicClient(c, stopper)
	stopper.AddCloser(stop.CloserFn(func() {
		dc.Close(ctx)
	}))

	// Add some loads.
	for i := 0; i < c.NumNodes()*2; i++ {
		ID := i
		stopper.RunWorker(ctx, func(ctx context.Context) {
			insertLoad(ctx, t, dc, ID)
		})
	}

	// TODO(bram): #5345 add repair mechanism.

	select {
	case <-stopper.ShouldStop():
	case <-time.After(cfg.Duration):
	}
}
