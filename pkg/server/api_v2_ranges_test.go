// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/rangetestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestHotRangesV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts := rangetestutils.StartServer(t)
	defer ts.Stopper().Stop(context.Background())

	var hotRangesResp hotRangesResponse
	client, err := ts.GetAdminHTTPClient()
	require.NoError(t, err)

	req, err := http.NewRequest("GET", ts.AdminURL().WithPath(apiconstants.APIV2Path+"ranges/hot/").String(), nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&hotRangesResp))
	require.NoError(t, resp.Body.Close())

	if len(hotRangesResp.Ranges) == 0 {
		t.Fatalf("didn't get hot range responses from any nodes")
	}
	if len(hotRangesResp.Errors) > 0 {
		t.Errorf("got an error in hot range response from n%d: %v",
			hotRangesResp.Errors[0].NodeID, hotRangesResp.Errors[0].ErrorMessage)
	}
	for _, r := range hotRangesResp.Ranges {
		if r.RangeID == 0 || r.NodeID == 0 {
			t.Errorf("unexpected empty/unpopulated range descriptor: %+v", r)
		}
	}
}

func TestNodeRangesV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := rangetestutils.StartServer(t)
	defer ts.Stopper().Stop(context.Background())

	// Perform a scan to ensure that all the raft groups are initialized.
	if _, err := ts.DB().Scan(context.Background(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
		t.Fatal(err)
	}

	var nodeRangesResp nodeRangesResponse
	client, err := ts.GetAdminHTTPClient()
	require.NoError(t, err)

	req, err := http.NewRequest("GET", ts.AdminURL().WithPath(apiconstants.APIV2Path+"nodes/local/ranges/").String(), nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&nodeRangesResp))
	require.NoError(t, resp.Body.Close())

	if len(nodeRangesResp.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}
	for _, ri := range nodeRangesResp.Ranges {
		require.Equal(t, int32(1), ri.SourceNodeID)
		require.Equal(t, int32(1), ri.SourceStoreID)
		require.GreaterOrEqual(t, len(ri.LeaseHistory), 1)
		require.NotEmpty(t, ri.Span.StartKey)
		require.NotEmpty(t, ri.Span.EndKey)
	}

	// Take the first range ID, and call the ranges/ endpoint with it.
	rangeID := nodeRangesResp.Ranges[0].Desc.RangeID
	req, err = http.NewRequest(
		"GET", ts.AdminURL().WithPath(fmt.Sprintf("%sranges/%d/", apiconstants.APIV2Path, rangeID)).String(), nil,
	)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	var rangeResp rangeResponse
	require.Equal(t, 200, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rangeResp))
	require.NoError(t, resp.Body.Close())

	require.Greater(t, len(rangeResp.Responses), 0)
	nodeRangeResp := rangeResp.Responses[roachpb.NodeID(1).String()]
	require.NotZero(t, nodeRangeResp)
	// The below comparison is from the response returned in the previous API call
	// ("nodeRangesResp") vs the current one ("nodeRangeResp").
	require.Equal(t, nodeRangesResp.Ranges[0].Desc, nodeRangeResp.RangeInfo.Desc)
	require.Empty(t, nodeRangeResp.Error)
}

func TestNodesV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				base.DefaultTestStoreSpec,
				base.DefaultTestStoreSpec,
				base.DefaultTestStoreSpec,
			},
		},
	})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	ts1 := testCluster.Server(0)

	var nodesResp nodesResponse
	client, err := ts1.GetAdminHTTPClient()
	require.NoError(t, err)

	req, err := http.NewRequest("GET", ts1.AdminURL().WithPath(apiconstants.APIV2Path+"nodes/").String(), nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&nodesResp))
	require.NoError(t, resp.Body.Close())

	require.Equal(t, 3, len(nodesResp.Nodes))
	for _, n := range nodesResp.Nodes {
		require.Greater(t, int(n.NodeID), 0)
		require.Less(t, int(n.NodeID), 4)
		require.Equal(t, len(n.StoreMetrics), 3)
	}
}
