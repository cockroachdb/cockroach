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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

type details struct {
	NodeID proto.NodeID `json:"nodeID"`
}

// get performs an HTTPS GET to the specified path for a specific node.
func get(t *testing.T, client *http.Client, node *localcluster.Container, path string) []byte {
	url := fmt.Sprintf("https://%s%s", node.Addr(""), path)
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("could not GET %s - %s", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("could not GET %s - statuscode: %d", url, resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("could not open body for %s - %s", url, err)
	}
	log.Infof("OK response from %s", url)
	return body
}

// checkNode checks all the endpoints of the status server hosted by node and
// requests info for the node with otherNodeID. That node could be the same
// other node, the same node or "local".
func checkNode(t *testing.T, client *http.Client, node *localcluster.Container, nodeID, otherNodeID, expectedNodeID string) {
	body := get(t, client, node, "/_status/details/"+otherNodeID)
	var detail details
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatal(util.ErrorfSkipFrames(1, "unable to parse details - %s", err))
	}
	if actualNodeID := detail.NodeID.String(); actualNodeID != expectedNodeID {
		t.Fatal(util.ErrorfSkipFrames(1, "%s calling %s: node ids don't match - expected %s, actual %s", nodeID, otherNodeID, expectedNodeID, actualNodeID))
	}

	get(t, client, node, fmt.Sprintf("/_status/gossip/%s", otherNodeID))
	get(t, client, node, fmt.Sprintf("/_status/logfiles/%s", otherNodeID))
	get(t, client, node, fmt.Sprintf("/_status/logs/%s", otherNodeID))
	get(t, client, node, fmt.Sprintf("/_status/stacks/%s", otherNodeID))
	get(t, client, node, fmt.Sprintf("/_status/nodes/%s", otherNodeID))
}

// TestStatusServer starts up an N node cluster and tests the status server on
// each node.
func TestStatusServer(t *testing.T) {
	l := localcluster.Create(*numNodes, stopper)
	l.ForceLogging = true
	l.Start()
	defer l.Stop()
	checkRangeReplication(t, l, 20*time.Second)

	client := &http.Client{
		Timeout: 200 * time.Millisecond,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	// Get the ids for each node.
	idMap := make(map[string]string)
	for _, node := range l.Nodes {
		body := get(t, client, node, "/_status/details/local")
		var detail details
		if err := json.Unmarshal(body, &detail); err != nil {
			t.Fatalf("unable to parse details - %s", err)
		}
		idMap[node.ID] = detail.NodeID.String()
	}

	// Check local response for the every node.
	for _, node := range l.Nodes {
		checkNode(t, client, node, idMap[node.ID], "local", idMap[node.ID])
		get(t, client, node, "/_status/nodes")
		get(t, client, node, "/_status/stores")
	}

	// Proxy from the first node to the last node.
	firstNode := l.Nodes[0]
	lastNode := l.Nodes[len(l.Nodes)-1]
	firstID := idMap[firstNode.ID]
	lastID := idMap[lastNode.ID]
	checkNode(t, client, firstNode, firstID, lastID, lastID)

	// And from the last node to the first node.
	checkNode(t, client, lastNode, lastID, firstID, firstID)

	// And from the last node to the last node.
	checkNode(t, client, lastNode, lastID, lastID, lastID)
}
