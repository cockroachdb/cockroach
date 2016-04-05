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
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package acceptance

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
)

type details struct {
	NodeID roachpb.NodeID `json:"nodeID"`
}

var retryOptions = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxRetries:     4,
	Multiplier:     2,
}

// get performs an HTTPS GET to the specified path for a specific node.
func get(t *testing.T, base, rel string) []byte {
	// TODO(bram) #2059: Remove retry logic.
	url := fmt.Sprintf("%s/%s", base, rel)
	for r := retry.Start(retryOptions); r.Next(); {
		resp, err := cluster.HTTPClient.Get(url)
		if err != nil {
			log.Infof("could not GET %s - %s", url, err)
			continue
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Infof("could not read body for %s - %s", url, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Infof("could not GET %s - statuscode: %d - body: %s", url, resp.StatusCode, body)
			continue
		}
		if log.V(1) {
			log.Infof("OK response from %s", url)
		}
		return body
	}
	t.Fatalf("There was an error retrieving %s", url)
	return []byte("")
}

// checkNode checks all the endpoints of the status server hosted by node and
// requests info for the node with otherNodeID. That node could be the same
// other node, the same node or "local".
func checkNode(t *testing.T, c cluster.Cluster, i int, nodeID, otherNodeID, expectedNodeID string) {
	var detail details
	if err := getJSON(c.URL(i), "/_status/details/"+otherNodeID, &detail); err != nil {
		t.Fatal(util.ErrorfSkipFrames(1, "unable to parse details - %s", err))
	}
	if actualNodeID := detail.NodeID.String(); actualNodeID != expectedNodeID {
		t.Fatal(util.ErrorfSkipFrames(1, "%s calling %s: node ids don't match - expected %s, actual %s", nodeID, otherNodeID, expectedNodeID, actualNodeID))
	}

	get(t, c.URL(i), fmt.Sprintf("/_status/gossip/%s", otherNodeID))
	get(t, c.URL(i), fmt.Sprintf("/_status/logfiles/%s", otherNodeID))
	get(t, c.URL(i), fmt.Sprintf("/_status/logs/%s", otherNodeID))
	get(t, c.URL(i), fmt.Sprintf("/_status/stacks/%s", otherNodeID))
	get(t, c.URL(i), fmt.Sprintf("/_status/nodes/%s", otherNodeID))
}

// TestStatusServer starts up an N node cluster and tests the status server on
// each node.
func TestStatusServer(t *testing.T) {
	runTestOnConfigs(t, testStatusServerInner)
}

func testStatusServerInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	// Get the ids for each node.
	idMap := make(map[int]string)
	for i := 0; i < c.NumNodes(); i++ {
		var detail details
		if err := getJSON(c.URL(i), "/_status/details/local", &detail); err != nil {
			t.Fatal(err)
		}
		idMap[i] = detail.NodeID.String()
	}

	// Check local response for the every node.
	for i := 0; i < c.NumNodes(); i++ {
		checkNode(t, c, i, idMap[i], "local", idMap[i])
		get(t, c.URL(i), "/_status/nodes")
	}

	// TODO(tamird): remove this after #5530. The `if` here is present to trick
	// `go vet`.
	if true {
		return
	}

	// Proxy from the first node to the last node.
	firstNode := 0
	lastNode := c.NumNodes() - 1
	firstID := idMap[firstNode]
	lastID := idMap[lastNode]
	checkNode(t, c, firstNode, firstID, lastID, lastID)

	// And from the last node to the first node.
	checkNode(t, c, lastNode, lastID, firstID, firstID)

	// And from the last node to the last node.
	checkNode(t, c, lastNode, lastID, lastID, lastID)
}
