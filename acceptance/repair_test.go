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

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/util/stop"
)

// RepairTest kills and starts new nodes systematically to ensure we do
// indeed repair the cluster.
func TestRepair(t *testing.T) {
	runTestOnConfigs(t, testRepairInner)
}

func testRepairInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	testStopper := stop.NewStopper()
	dc := newDynamicClient(c, testStopper)
	testStopper.AddCloser(dc)
	defer testStopper.Stop()

	// Add some loads.
	for i := 0; i < c.NumNodes()*2; i++ {
		ID := i
		testStopper.RunWorker(func() {
			insertLoad(t, dc, ID)
		})
	}

	// TODO(bram): #5345 add repair mechanism.

	select {
	case <-stopper:
	case <-time.After(cfg.Duration):
	}
}
