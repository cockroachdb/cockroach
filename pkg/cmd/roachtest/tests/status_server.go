// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func runStatusServer(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx)

	// Get the ids for each node.
	idMap := make(map[int]roachpb.NodeID)
	urlMap := make(map[int]string)
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, c.All())
	if err != nil {
		t.Fatal(err)
	}
	for i, addr := range adminUIAddrs {
		var details serverpb.DetailsResponse
		url := `http://` + addr + `/_status/details/local`
		// Use a retry-loop when populating the maps because we might be trying to
		// talk to the servers before they are responding to status requests
		// (resulting in 404's).
		if err := retry.ForDuration(10*time.Second, func() error {
			return httputil.GetJSON(http.Client{}, url, &details)
		}); err != nil {
			t.Fatal(err)
		}
		idMap[i+1] = details.NodeID
		urlMap[i+1] = `http://` + addr
	}

	// The status endpoints below may take a while to produce their answer, maybe more
	// than the 3 second timeout of the default http client.
	httpClient := httputil.NewClientWithTimeout(15 * time.Second)

	// get performs an HTTP GET to the specified path for a specific node.
	get := func(base, rel string) []byte {
		url := base + rel
		resp, err := httpClient.Get(context.TODO(), url)
		if err != nil {
			t.Fatalf("could not GET %s - %s", url, err)
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("could not read body for %s - %s", url, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("could not GET %s - statuscode: %d - body: %s", url, resp.StatusCode, body)
		}
		t.L().Printf("OK response from %s\n", url)
		return body
	}

	// checkNode checks all the endpoints of the status server hosted by node and
	// requests info for the node with otherNodeID. That node could be the same
	// other node, the same node or "local".
	checkNode := func(url string, nodeID, otherNodeID, expectedNodeID roachpb.NodeID) {
		urlIDs := []string{otherNodeID.String()}
		if nodeID == otherNodeID {
			urlIDs = append(urlIDs, "local")
		}
		var details serverpb.DetailsResponse
		for _, urlID := range urlIDs {
			if err := httputil.GetJSON(http.Client{}, url+`/_status/details/`+urlID, &details); err != nil {
				t.Fatalf("unable to parse details - %s", err)
			}
			if details.NodeID != expectedNodeID {
				t.Fatalf("%d calling %s: node ids don't match - expected %d, actual %d",
					nodeID, urlID, expectedNodeID, details.NodeID)
			}

			get(url, fmt.Sprintf("/_status/gossip/%s", urlID))
			get(url, fmt.Sprintf("/_status/nodes/%s", urlID))
			get(url, fmt.Sprintf("/_status/logfiles/%s", urlID))
			get(url, fmt.Sprintf("/_status/logs/%s", urlID))
			get(url, fmt.Sprintf("/_status/stacks/%s", urlID))
		}

		get(url, "/_status/vars")
	}

	// Check local response for the every node.
	for i := 1; i <= c.Spec().NodeCount; i++ {
		id := idMap[i]
		checkNode(urlMap[i], id, id, id)
		get(urlMap[i], "/_status/nodes")
	}

	// Proxy from the first node to the last node.
	firstNode := 1
	lastNode := c.Spec().NodeCount
	firstID := idMap[firstNode]
	lastID := idMap[lastNode]
	checkNode(urlMap[firstNode], firstID, lastID, lastID)

	// And from the last node to the first node.
	checkNode(urlMap[lastNode], lastID, firstID, firstID)

	// And from the last node to the last node.
	checkNode(urlMap[lastNode], lastID, lastID, lastID)
}
