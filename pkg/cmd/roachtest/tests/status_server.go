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
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func runStatusServer(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	// Get the ids for each node.
	idMap := make(map[int]roachpb.NodeID)
	urlMap := make(map[int]string)
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.All())
	if err != nil {
		t.Fatal(err)
	}
	for i, addr := range adminUIAddrs {
		var details serverpb.DetailsResponse
		url := `http://` + addr + `/_status/details/local`
		// Use a retry-loop when populating the maps because we might be trying to
		// talk to the servers before they are responding to status requests
		// (resulting in 404's).
		client, err := roachtestutil.DefaultHttpClientWithSessionCookie(ctx, c, t.L(), c.Node(1), url)
		if err != nil {
			t.Fatal(err)
		}
		if err := retry.ForDuration(10*time.Second, func() error {
			return httputil.GetJSON(client, url, &details)
		}); err != nil {
			t.Fatal(err)
		}
		idMap[i+1] = details.NodeID
		urlMap[i+1] = `http://` + addr
	}

	// get performs an HTTP GET to the specified path for a specific node.
	get := func(path string, httpClient *http.Client) []byte {
		url, err := url.Parse(path)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := httpClient.Get(url.String())
		if err != nil {
			t.Fatalf("could not GET %s - %s", url, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
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
			endpoints := []string{
				fmt.Sprintf("%s/_status/details/%s", url, urlID),
				fmt.Sprintf("%s/_status/gossip/%s", url, urlID),
				fmt.Sprintf("%s/_status/nodes/%s", url, urlID),
				fmt.Sprintf("%s/_status/logfiles/%s", url, urlID),
				fmt.Sprintf("%s/_status/logs/%s", url, urlID),
				fmt.Sprintf("%s/_status/stacks/%s", url, urlID),
			}
			client, err := roachtestutil.DefaultHttpClientWithSessionCookie(ctx, c, t.L(), c.Node(1), endpoints...)
			if err != nil {
				t.Fatal(err)
			}
			// The status endpoints below may take a while to produce their answer, maybe more
			// than the 3 second timeout of the default http client.
			client.Timeout = 15 * time.Second
			if err := httputil.GetJSON(client, url+`/_status/details/`+urlID, &details); err != nil {
				t.Fatalf("unable to parse details - %s", err)
			}
			if details.NodeID != expectedNodeID {
				t.Fatalf("%d calling %s: node ids don't match - expected %d, actual %d",
					nodeID, urlID, expectedNodeID, details.NodeID)
			}

			// Skip the first endpoint as that was used above
			for _, endpoint := range endpoints[1:] {
				get(endpoint, &client)
			}
		}

		client, err := roachtestutil.DefaultHttpClientWithSessionCookie(ctx, c, t.L(), c.Node(1), url+"/_status/vars")
		if err != nil {
			t.Fatal(err)
		}
		client.Timeout = 15 * time.Second
		get(url+"/_status/vars", &client)
	}

	// Check local response for the every node.
	for i := 1; i <= c.Spec().NodeCount; i++ {
		id := idMap[i]
		checkNode(urlMap[i], id, id, id)
		client, err := roachtestutil.DefaultHttpClientWithSessionCookie(ctx, c, t.L(), c.Node(1), urlMap[i]+"/_status/nodes")
		if err != nil {
			t.Fatal(err)
		}
		client.Timeout = 15 * time.Second
		get(urlMap[i]+"/_status/nodes", &client)
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
