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
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
)

// TODO(tschottdorf): name and location of this test. Should also test an actual
// migration, cf. the reference test.
func TestRaftUpdate(t *testing.T) {
	runTestOnConfigs(t, testRaftUpdateInner)
}

func post(c cluster.Cluster, freeze bool) (server.ClusterFreezeResponse, error) {
	var reply server.ClusterFreezeResponse

	httpClient := cluster.HTTPClient()
	httpClient.Timeout = 10 * time.Second

	body, err := json.Marshal(server.ClusterFreezeRequest{
		Freeze: freeze,
	})
	if err != nil {
		return reply, err
	}
	resp, err := httpClient.Post(c.URL(0)+"/_admin/v1/cluster/freeze",
		"application/json", bytes.NewReader(body))
	if err != nil {
		return reply, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return reply, err
	}
	if err := json.Unmarshal(b, &reply); err != nil {
		return reply, util.Errorf("could not unmarshal: %s: %s", err, b)
	}
	return reply, nil
}

func testRaftUpdateInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	const minAffected = 3 // actually more

	mustPost := func(freeze bool) server.ClusterFreezeResponse {
		reply, err := post(c, freeze)
		if err != nil {
			t.Fatal(util.ErrorfSkipFrames(1, "%v", err))
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
	if reply, err := post(c, true); !testutils.IsError(err, "Timeout exceeded while") {
		t.Logf("expected timeout, got %v: %v", err, reply)
	}

	// Shut down the remaining nodes...
	for i := 0; i < num-1; i++ {
		if err := c.Kill(i); err != nil {
			t.Fatal(err)
		}
	}
	// ... and restart them.
	for i := 0; i < num; i++ {
		if err := c.Restart(i); err != nil {
			t.Fatal(err)
		}
	}

	// The cluster should now be fully operational (at least after waiting
	// a little bit) since each node tries to unfreeze everything when it
	// starts.
	t.Skip("TODO(tschottdorf): bad things are happening")
}
