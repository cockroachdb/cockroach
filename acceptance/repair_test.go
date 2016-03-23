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
)

// RepairTest kills and starts new nodes systematically to ensure we do
// indeed repair the cluster.
func TestRepair(t *testing.T) {
	runTestOnConfigs(t, testRepairInner)
}

func testRepairInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	finished := make(chan struct{})
	dc := newDynamicClient(t, c, finished)
	// close the dc after the finished channel has been closed to ensure that
	// no new connections are created.
	defer dc.close()
	defer close(finished)

	// Add some loads.
	for i := 0; i < c.NumNodes()*2; i++ {
		go insertLoad(t, dc, finished, i)
	}

	// TODO(bram): #5345 add repair mechanism.

	select {
	case <-stopper:
	case <-time.After(cfg.Duration):
	}
}
