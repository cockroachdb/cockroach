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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
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
func get(t *testing.T, node *localcluster.Container, path string) []byte {
	url := fmt.Sprintf("https://%s%s", node.Addr(""), path)
	// TODO(bram) #2059: Remove retry logic.
	for r := retry.Start(retryOptions); r.Next(); {
		resp, err := localcluster.HTTPClient.Get(url)
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
		log.Infof("OK response from %s", url)
		return body
	}
	t.Fatalf("There was an error retrieving %s", url)
	return []byte("")
}

// checkNode checks all the endpoints of the status server hosted by node and
// requests info for the node with otherNodeID. That node could be the same
// other node, the same node or "local".
func checkNode(t *testing.T, node *localcluster.Container, nodeID, otherNodeID, expectedNodeID string) {
	body := get(t, node, "/_status/details/"+otherNodeID)
	var detail details
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatal(util.ErrorfSkipFrames(1, "unable to parse details - %s", err))
	}
	if actualNodeID := detail.NodeID.String(); actualNodeID != expectedNodeID {
		t.Fatal(util.ErrorfSkipFrames(1, "%s calling %s: node ids don't match - expected %s, actual %s", nodeID, otherNodeID, expectedNodeID, actualNodeID))
	}

	get(t, node, fmt.Sprintf("/_status/gossip/%s", otherNodeID))
	get(t, node, fmt.Sprintf("/_status/logfiles/%s", otherNodeID))
	get(t, node, fmt.Sprintf("/_status/logs/%s", otherNodeID))
	get(t, node, fmt.Sprintf("/_status/stacks/%s", otherNodeID))
	get(t, node, fmt.Sprintf("/_status/nodes/%s", otherNodeID))
}

// TestStatusServer starts up an N node cluster and tests the status server on
// each node.
func TestStatusServer(t *testing.T) {
	l := localcluster.Create(*numNodes, stopper) // intentionally using local cluster
	l.ForceLogging = true
	l.Start()
	defer l.AssertAndStop(t)

	checkRangeReplication(t, l, 20*time.Second)

	// Get the ids for each node.
	idMap := make(map[string]string)
	for _, node := range l.Nodes {
		body := get(t, node, "/_status/details/local")
		var detail details
		if err := json.Unmarshal(body, &detail); err != nil {
			t.Fatalf("unable to parse details - %s", err)
		}
		idMap[node.ID] = detail.NodeID.String()
	}

	// Check local response for the every node.
	for _, node := range l.Nodes {
		checkNode(t, node, idMap[node.ID], "local", idMap[node.ID])
		get(t, node, "/_status/nodes")
		get(t, node, "/_status/stores")
	}

	// Proxy from the first node to the last node.
	firstNode := l.Nodes[0]
	lastNode := l.Nodes[len(l.Nodes)-1]
	firstID := idMap[firstNode.ID]
	lastID := idMap[lastNode.ID]
	checkNode(t, firstNode, firstID, lastID, lastID)

	// And from the last node to the first node.
	checkNode(t, lastNode, lastID, firstID, firstID)

	// And from the last node to the last node.
	checkNode(t, lastNode, lastID, lastID, lastID)
}
