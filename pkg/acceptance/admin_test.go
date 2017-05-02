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
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestAdminLossOfQuorum(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	runTestOnConfigs(t, testAdminLossOfQuorumInner)
}

func testAdminLossOfQuorumInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	if c.NumNodes() < 2 {
		t.Logf("skipping test %s because given cluster has too few nodes", cfg.Name)
		return
	}

	// Get the ids for each node.
	nodeIDs := make([]roachpb.NodeID, c.NumNodes())
	for i := 0; i < c.NumNodes(); i++ {
		var details serverpb.DetailsResponse
		if err := httputil.GetJSON(cluster.HTTPClient, c.URL(ctx, i)+"/_status/details/local", &details); err != nil {
			t.Fatalf("failed to get local details from node %d: %s", i, err)
		}
		nodeIDs[i] = details.NodeID
	}

	// Leave only the first node alive.
	for i := 1; i < c.NumNodes(); i++ {
		if err := c.Kill(ctx, i); err != nil {
			t.Fatal(err)
		}
	}

	// Retrieve node statuses.
	var nodes serverpb.NodesResponse
	if err := httputil.GetJSON(cluster.HTTPClient, c.URL(ctx, 0)+"/_status/nodes", &nodes); err != nil {
		t.Fatal(err)
	}

	for _, nodeID := range nodeIDs {
		var nodeStatus status.NodeStatus
		if err := httputil.GetJSON(cluster.HTTPClient, c.URL(ctx, 0)+"/_status/nodes/"+strconv.Itoa(int(nodeID)), &nodeStatus); err != nil {
			t.Fatal(err)
		}
	}

	// Retrieve time-series data.
	nowNanos := timeutil.Now().UnixNano()
	queryRequest := tspb.TimeSeriesQueryRequest{
		StartNanos: nowNanos - 10*time.Second.Nanoseconds(),
		EndNanos:   nowNanos,
		Queries: []tspb.Query{
			{Name: "doesn't_matter", Sources: []string{}},
		},
	}
	var queryResponse tspb.TimeSeriesQueryResponse
	if err := httputil.PostJSON(cluster.HTTPClient, c.URL(ctx, 0)+"/ts/query",
		&queryRequest, &queryResponse); err != nil {
		t.Fatal(err)
	}

	// TODO(cdo): When we're able to issue SQL queries without a quorum, test all
	// admin endpoints that issue SQL queries here.
}
