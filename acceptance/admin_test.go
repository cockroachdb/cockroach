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

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/ts/tspb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/pkg/errors"
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
	nodeIDs := make([]roachpb.NodeID, c.NumNodes())
	for i := 0; i < c.NumNodes(); i++ {
		var details serverpb.DetailsResponse
		if err := util.GetJSON(cluster.HTTPClient, c.URL(i)+"/_status/details/local", &details); err != nil {
			t.Fatal(err)
		}
		nodeIDs[i] = details.NodeID
	}

	// Leave only the first node alive.
	for i := 1; i < c.NumNodes(); i++ {
		if err := c.Kill(i); err != nil {
			t.Fatal(err)
		}
	}

	// Retrieve node statuses.
	var nodes serverpb.NodesResponse
	if err := util.GetJSON(cluster.HTTPClient, c.URL(0)+"/_status/nodes", &nodes); err != nil {
		t.Fatal(err)
	}

	for _, nodeID := range nodeIDs {
		var nodeStatus status.NodeStatus
		if err := util.GetJSON(cluster.HTTPClient, c.URL(0)+"/_status/nodes/"+strconv.Itoa(int(nodeID)), &nodeStatus); err != nil {
			t.Fatal(err)
		}
	}

	// Retrieve time-series data.
	nowNanos := timeutil.Now().UnixNano()
	queryRequest := tspb.TimeSeriesQueryRequest{
		StartNanos: nowNanos - 10*time.Second.Nanoseconds(),
		EndNanos:   nowNanos,
		Queries: []tspb.Query{
			{Name: "doesnt_matter", Sources: []string{}},
		},
	}
	var queryResponse tspb.TimeSeriesQueryResponse
	if err := util.PostJSON(cluster.HTTPClient, c.URL(0)+"/ts/query",
		&queryRequest, &queryResponse); err != nil {
		t.Fatal(err)
	}

	// TODO(cdo): When we're able to issue SQL queries without a quorum, test all
	// admin endpoints that issue SQL queries here.
}

func TestAdminTableStats(t *testing.T) {
	runTestOnConfigs(t, testAdminTableStatsInner)
}

func testAdminTableStatsInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	if c.NumNodes() < 3 {
		t.Logf("skipping test %s because given cluster has too few nodes", cfg.Name)
		return
	}

	// Make a single table and insert some data.
	db := makePGClient(t, c.PGUrl(0))
	defer db.Close()
	if _, err := db.Exec("CREATE DATABASE test"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE test.foo (
			id INT PRIMARY KEY,
			val STRING
		)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(`INSERT INTO test.foo VALUES(
			$1, $2
		)`, i, "test"); err != nil {
			t.Fatal(err)
		}
	}

	var tsResponse serverpb.TableStatsResponse
	url := c.URL(0) + "/_admin/v1/databases/test/tables/foo/stats"

	// The new SQL table may not yet have split into its own range yet. Wait
	// for this to occur, and for full replication.
	util.SucceedsSoon(t, func() error {
		if err := util.GetJSON(cluster.HTTPClient, url, &tsResponse); err != nil {
			return err
		}
		if tsResponse.RangeCount != 1 {
			return errors.Errorf("Table range not yet separated.")
		}
		if tsResponse.NodeCount != 3 {
			return errors.Errorf("Table range not yet replicated to %d nodes.", 3)
		}
		if a, e := tsResponse.ReplicaCount, int64(3); a != e {
			return errors.Errorf("Expected %d replicas, found %d", e, a)
		}
		return nil
	})

	// These two conditions *must* be true, given the above conditions.
	if a, e := tsResponse.MvccStats.KeyCount, int64(20); a < e {
		t.Fatalf("Expected at least 20 total keys, found %d", a)
	}
	if len(tsResponse.MissingNodes) > 0 {
		t.Fatalf("Expected no missing nodes, found %v", tsResponse.MissingNodes)
	}

	// Kill a node, ensure it shows up in MissingNodes and
	// that ReplicaCount is lower.
	if err := c.Kill(1); err != nil {
		t.Fatal(err)
	}

	client := cluster.HTTPClient
	client.Timeout = base.NetworkTimeout * 10
	if err := util.GetJSON(client, url, &tsResponse); err != nil {
		t.Fatal(err)
	}
	if a, e := tsResponse.NodeCount, int64(3); a != e {
		t.Errorf("Expected %d nodes, found %d", e, a)
	}
	if a, e := tsResponse.RangeCount, int64(1); a != e {
		t.Errorf("Expected %d ranges, found %d", e, a)
	}
	if a, e := tsResponse.ReplicaCount, int64(2); a != e {
		t.Errorf("Expected %d replicas, found %d", e, a)
	}
	if a, e := tsResponse.MvccStats.KeyCount, int64(10); a < e {
		t.Errorf("Expected at least 10 total keys, found %d", a)
	}
	if len(tsResponse.MissingNodes) != 1 {
		t.Errorf("Expected one missing node, found %v", tsResponse.MissingNodes)
	}

	if t.Failed() {
		t.FailNow()
	}
}
