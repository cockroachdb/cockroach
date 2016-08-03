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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package acceptance

import (
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

func TestFreezeCluster(t *testing.T) {
	t.Skip("#7957")
	runTestOnConfigs(t, testFreezeClusterInner)
}

func postFreeze(c cluster.Cluster, freeze bool, timeout time.Duration) (serverpb.ClusterFreezeResponse, error) {
	httpClient := cluster.HTTPClient
	httpClient.Timeout = timeout

	var resp serverpb.ClusterFreezeResponse
	log.Infof(context.Background(), "requesting: freeze=%t, timeout=%s", freeze, timeout)
	cb := func(v proto.Message) {
		oldNum := resp.RangesAffected
		resp = *v.(*serverpb.ClusterFreezeResponse)
		if oldNum > resp.RangesAffected {
			resp.RangesAffected = oldNum
		}
		if (resp != serverpb.ClusterFreezeResponse{}) {
			log.Infof(context.Background(), "%+v", &resp)
		}
	}
	err := util.StreamJSON(
		httpClient,
		c.URL(0)+"/_admin/v1/cluster/freeze",
		&serverpb.ClusterFreezeRequest{Freeze: freeze},
		&serverpb.ClusterFreezeResponse{},
		cb,
	)
	return resp, err
}

func testFreezeClusterInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	minAffected := int64(server.ExpectedInitialRangeCount())

	const long = time.Minute
	const short = 10 * time.Second

	mustPost := func(freeze bool) serverpb.ClusterFreezeResponse {
		reply, err := postFreeze(c, freeze, long)
		if err != nil {
			t.Fatal(errors.Errorf("%v", err))
		}
		return reply
	}

	if reply := mustPost(false); reply.RangesAffected != 0 {
		t.Fatalf("expected initial unfreeze to affect no ranges, got %d", reply.RangesAffected)
	}

	if reply := mustPost(true); reply.RangesAffected < minAffected {
		t.Fatalf("expected >=%d frozen ranges, got %d", minAffected, reply.RangesAffected)
	}

	if reply := mustPost(true); reply.RangesAffected != 0 {
		t.Fatalf("expected second freeze to affect no ranges, got %d", reply.RangesAffected)
	}

	if reply := mustPost(false); reply.RangesAffected < minAffected {
		t.Fatalf("expected >=%d thawed ranges, got %d", minAffected, reply.RangesAffected)
	}

	num := c.NumNodes()
	if num < 3 {
		t.Skip("skipping remainder of test; needs at least 3 nodes")
	}

	// Kill the last node.
	if err := c.Kill(num - 1); err != nil {
		t.Fatal(err)
	}

	// Attempt to freeze should get stuck (since it does not get confirmation
	// of the last node receiving the freeze command).
	// Note that this is the freeze trigger stalling on the Replica, not the
	// Store-polling mechanism.
	acceptErrs := strings.Join([]string{
		"timed out waiting for Range",
		"Timeout exceeded while",
		"connection is closing",
		"deadline",
		// error returned via JSON when the server-side gRPC stream times out (due to
		// lack of new input). Unmarshaling that JSON fails with a message referencing
		// unknown fields, unfortunately in map order.
		"unknown field .*",
	}, "|")
	if reply, err := postFreeze(c, true, short); !testutils.IsError(err, acceptErrs) {
		t.Fatalf("expected timeout, got %v: %v", err, reply)
	}

	// Shut down the remaining nodes and restart them.
	for i := 0; i < num-1; i++ {
		if err := c.Kill(i); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < num; i++ {
		if err := c.Restart(i); err != nil {
			t.Fatal(err)
		}
	}

	// The cluster should now be fully operational (at least after waiting
	// a little bit) since each node tries to unfreeze everything when it
	// starts.
	if err := util.RetryForDuration(time.Minute, func() error {
		if _, err := postFreeze(c, false, short); err != nil {
			if testutils.IsError(err, "404 Not Found") {
				// It can take a bit until the endpoint is available.
				return err
			}
			t.Fatal(err)
		}

		// TODO(tschottdorf): moving the client creation outside of the retry
		// loop will break the test with the following message:
		//
		//   client/rpc_sender.go:61: roachpb.Batch RPC failed as client
		//   connection was closed
		//
		// Perhaps the cluster updates the address too late after restarting
		// the node.
		db, dbStopper := c.NewClient(t, 0)
		defer dbStopper.Stop()

		if _, err := db.Scan(keys.LocalMax, roachpb.KeyMax, 0); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Unfreezing again should be a no-op.
	if reply, err := postFreeze(c, false, long); err != nil {
		t.Fatal(err)
	} else if reply.RangesAffected > 0 {
		t.Fatalf("still %d frozen ranges", reply.RangesAffected)
	}
}
