// Copyright 2016 The Cockroach Authors.
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
// Author: Cuong Do <cdo@cockroachlabs.com>

package acceptance

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

func TestAdminLossOfQuorum(t *testing.T) {
	runTestOnConfigs(t, testAdminLossOfQuorumInner)
}

func testAdminLossOfQuorumInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	if c.NumNodes() < 2 {
		t.Logf("skipping test %s because given cluster has too few nodes", cfg.Name)
		return
	}

	// Get the ids for each node.
	idMap := make(map[int]string)
	for i := 0; i < c.NumNodes(); i++ {
		var detail details
		if err := getJSON(c.URL(i), "/_status/details/local", &detail); err != nil {
			t.Fatal(err)
		}
		idMap[i] = detail.NodeID.String()
	}

	// Leave only the first node alive.
	for i := 1; i < c.NumNodes(); i++ {
		if err := c.Kill(i); err != nil {
			t.Fatal(err)
		}
	}

	// Retrieve node statuses.
	var nodeStatuses interface{}
	if err := getJSON(c.URL(0), "/_status/nodes/", &nodeStatuses); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < c.NumNodes(); i++ {
		var nodeStatus interface{}
		url := fmt.Sprintf("/_status/nodes/%s", idMap[i])
		if err := getJSON(c.URL(0), url, &nodeStatus); err != nil {
			t.Fatal(err)
		}
	}

	// Retrieve time-series data.
	nowNanos := timeutil.Now().UnixNano()
	queryRequest := ts.TimeSeriesQueryRequest{
		StartNanos: nowNanos - 10*time.Second.Nanoseconds(),
		EndNanos:   nowNanos,
		Queries: []ts.Query{
			{Name: "doesnt_matter", Sources: []string{}},
		},
	}
	var queryResponse ts.TimeSeriesQueryResponse
	if err := postJSON(c.URL(0), "/ts/query", &queryRequest, &queryResponse); err != nil {
		t.Fatal(err)
	}

	// TODO(cdo): When we're able to issue SQL queries without a quorum, test all
	// admin endpoints that issue SQL queries here.
}
