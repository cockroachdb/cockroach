// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func runStatusServer(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	// The status endpoints below may take a while to produce their answer, maybe more
	// than the 3 second timeout of the default http client.
	client := roachtestutil.DefaultHTTPClient(c, t.L(), roachtestutil.HTTPTimeout(15*time.Second))

	// Get the ids for each node.
	idMap := make(map[int]roachpb.NodeID)
	urlMap := make(map[int]string)
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.All())
	if err != nil {
		t.Fatal(err)
	}
	for i, addr := range adminUIAddrs {
		var details serverpb.DetailsResponse
		url := `https://` + addr + `/_status/details/local`
		// Use a retry-loop when populating the maps because we might be trying to
		// talk to the servers before they are responding to status requests
		// (resulting in 404's).
		if err := retry.ForDuration(10*time.Second, func() error {
			return client.GetJSON(ctx, url, &details)
		}); err != nil {
			t.Fatal(err)
		}
		idMap[i+1] = details.NodeID
		urlMap[i+1] = `https://` + addr
	}

	// get performs an HTTP GET to the specified path for a specific node.
	get := func(path string, httpClient *roachtestutil.RoachtestHTTPClient) []byte {
		resp, err := httpClient.Get(ctx, path)
		if err != nil {
			t.Fatalf("could not GET %s - %s", path, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("could not read body for %s - %s", path, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("could not GET %s - statuscode: %d - body: %s", path, resp.StatusCode, body)
		}
		t.L().Printf("OK response from %s\n", path)
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
			if err := client.GetJSON(ctx, url+`/_status/details/`+urlID, &details); err != nil {
				t.Fatalf("unable to parse details - %s", err)
			}
			if details.NodeID != expectedNodeID {
				t.Fatalf("%d calling %s: node ids don't match - expected %d, actual %d",
					nodeID, urlID, expectedNodeID, details.NodeID)
			}
			endpoints := []string{
				fmt.Sprintf("%s/_status/gossip/%s", url, urlID),
				fmt.Sprintf("%s/_status/nodes/%s", url, urlID),
				fmt.Sprintf("%s/_status/logfiles/%s", url, urlID),
				fmt.Sprintf("%s/_status/logs/%s", url, urlID),
				fmt.Sprintf("%s/_status/stacks/%s", url, urlID),
			}
			for _, endpoint := range endpoints {
				get(endpoint, client)
			}
		}
		get(url+"/_status/vars", client)
	}

	// Check local response for the every node.
	for i := 1; i <= c.Spec().NodeCount; i++ {
		id := idMap[i]
		checkNode(urlMap[i], id, id, id)
		get(urlMap[i]+"/_status/nodes", client)
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
