// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestEnqueueRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},

		ReplicationMode: base.ReplicationManual,
	})
	defer testCluster.Stopper().Stop(context.Background())
	s0 := testCluster.Server(0).SystemLayer()

	// Up-replicate r1 to all 3 nodes. We use manual replication to avoid lease
	// transfers causing temporary conditions in which no store is the
	// leaseholder, which can break the tests below.
	_, err := testCluster.AddVoters(roachpb.KeyMin, testCluster.Target(1), testCluster.Target(2))
	if err != nil {
		t.Fatal(err)
	}

	// RangeID being queued
	const realRangeID = 1
	const fakeRangeID = 999

	// Who we expect responses from.
	const none = 0
	const leaseholder = 1
	const allReplicas = 3

	testCases := []struct {
		nodeID            roachpb.NodeID
		queue             string
		rangeID           roachpb.RangeID
		expectedDetails   int
		expectedNonErrors int
	}{
		// Success cases
		{0, "mvccGC", realRangeID, allReplicas, leaseholder},
		{0, "split", realRangeID, allReplicas, leaseholder},
		{0, "replicaGC", realRangeID, allReplicas, allReplicas},
		{0, "RaFtLoG", realRangeID, allReplicas, allReplicas},
		{0, "RAFTSNAPSHOT", realRangeID, allReplicas, allReplicas},
		{0, "consistencyChecker", realRangeID, allReplicas, leaseholder},
		{0, "TIMESERIESmaintenance", realRangeID, allReplicas, leaseholder},
		{1, "raftlog", realRangeID, leaseholder, leaseholder},
		{2, "raftlog", realRangeID, leaseholder, 1},
		{3, "raftlog", realRangeID, leaseholder, 1},
		// Compatibility cases.
		// TODO(nvanbenschoten): remove this in v23.1.
		{0, "gc", realRangeID, allReplicas, leaseholder},
		{0, "GC", realRangeID, allReplicas, leaseholder},
		// Error cases
		{0, "gv", realRangeID, allReplicas, none},
		{0, "GC", fakeRangeID, allReplicas, none},
	}

	for _, tc := range testCases {
		t.Run(tc.queue, func(t *testing.T) {
			req := &serverpb.EnqueueRangeRequest{
				NodeID:  tc.nodeID,
				Queue:   tc.queue,
				RangeID: tc.rangeID,
			}
			var resp serverpb.EnqueueRangeResponse
			if err := srvtestutils.PostAdminJSONProto(s0, "enqueue_range", req, &resp); err != nil {
				t.Fatal(err)
			}
			if e, a := tc.expectedDetails, len(resp.Details); e != a {
				t.Errorf("expected %d details; got %d: %+v", e, a, resp)
			}
			var numNonErrors int
			for _, details := range resp.Details {
				if len(details.Events) > 0 && details.Error == "" {
					numNonErrors++
				}
			}
			if tc.expectedNonErrors != numNonErrors {
				t.Errorf("expected %d non-error details; got %d: %+v", tc.expectedNonErrors, numNonErrors, resp)
			}
		})
	}

	// Finally, test a few more basic error cases.
	reqs := []*serverpb.EnqueueRangeRequest{
		{NodeID: -1, Queue: "mvccGC"},
		{Queue: ""},
		{RangeID: -1, Queue: "mvccGC"},
	}
	for _, req := range reqs {
		t.Run(fmt.Sprint(req), func(t *testing.T) {
			var resp serverpb.EnqueueRangeResponse
			err := srvtestutils.PostAdminJSONProto(testCluster.Server(0), "enqueue_range", req, &resp)
			if err == nil {
				t.Fatalf("unexpected success: %+v", resp)
			}
			if !testutils.IsError(err, "400 Bad Request") {
				t.Fatalf("unexpected error type: %+v", err)
			}
		})
	}
}
