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

package acceptance

import (
	"regexp"
	"strconv"
	"testing"

	"golang.org/x/net/context"

	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestDecommission starts up an >3 node cluster and decomissions and
// recommissions nodes in various ways.
func TestDecommission(t *testing.T) {
	t.Skip("#17477")
	s := log.Scope(t)
	defer s.Close(t)

	runTestOnConfigs(t, testDecommissionInner)
}

func decommission(
	ctx context.Context, c cluster.Cluster, runNode int, targetNode roachpb.NodeID, verbs ...string,
) (string, error) {
	args := []string{
		cluster.CockroachBinaryInContainer,
		"node", verbs[0], "--host", c.Hostname(runNode), "--certs-dir=/certs",
	}
	if targetNode > 0 {
		args = append(args, strconv.Itoa(int(targetNode)))
	}
	args = append(args, verbs[1:]...)
	o, _, err := c.ExecRoot(ctx, runNode, args)
	return o, err
}

func testDecommissionInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	SkipUnlessLocal(t) // hard-coded name of cockroach binary below

	if c.NumNodes() < 4 {
		// TODO(tschottdorf): or we invent a way to change the ZoneConfig in
		// this test and test less ambitiously (or split up the tests).
		t.Skip("need at least four nodes")
	}

	withDB := func(n int, stmt string) {
		db, err := gosql.Open("postgres", c.PGUrl(ctx, n))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	withDB(1, "SET CLUSTER SETTING server.remote_debugging.mode = 'any'")

	// Get the ids for each node.
	idMap := make(map[int]roachpb.NodeID)
	for i := 0; i < c.NumNodes(); i++ {
		var details serverpb.DetailsResponse
		if err := httputil.GetJSON(cluster.HTTPClient, c.URL(ctx, i)+"/_status/details/local", &details); err != nil {
			t.Fatal(err)
		}
		idMap[i] = details.NodeID
	}

	log.Info(ctx, "decommissioning first node from the second, polling the status manually")
	for {
		o, err := decommission(ctx, c, 1, idMap[0], "decommission", "--wait", "none")
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)
		// Matches:
		// 1	true	0	true	true
		if ok, err := regexp.MatchString(strconv.Itoa(int(idMap[0]))+`\s+true\s+0\s+true\s+true`, o); ok && err == nil {
			break
		}
	}

	log.Info(ctx, "recommissioning first node (from third node)")
	{
		o, err := decommission(ctx, c, 2, idMap[0], "recommission")
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)
	}

	log.Info(ctx, "restarting first node so that it can accept replicas again")
	if err := c.Restart(ctx, 0); err != nil {
		t.Fatal(err)
	}

	log.Info(ctx, "decommissioning second node from third, using --wait=all")
	{
		target := idMap[1]
		o, err := decommission(ctx, c, 2, target, "decommission", "--wait", "all")
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)
		if ok, err := regexp.MatchString(strconv.Itoa(int(target))+`\s+true\s+0\s+true\s+true`, o); !ok || err != nil {
			t.Fatalf("node did not decommission: ok=%t, err=%v, output:\n%s", ok, err, o)
		}
	}

	log.Info(ctx, "recommissioning second node from itself and restarting")
	{
		o, err := decommission(ctx, c, 1, idMap[1], "recommission")
		if err != nil {
			t.Fatalf("could no recommission: %s\n%s", err, o)
		}
		log.Infof(ctx, o)
		if err := c.Restart(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}

	log.Info(ctx, "decommissioning third node via `quit --decommission`")
	{
		o, err := func() (string, error) {
			args := []string{
				cluster.CockroachBinaryInContainer,
				"quit", "--decommission", "--host", c.Hostname(2), "--certs-dir=/certs",
			}
			o, _, err := c.ExecRoot(ctx, 2, args)
			return o, err
		}()
		if err != nil {
			t.Logf("ignoring error on quit --decommission: %s", err)
		} else {
			log.Infof(ctx, o)
		}

		// Kill the node to generate the expected event (Kill is idempotent, so this works).
		if err := c.Kill(ctx, 2); err != nil {
			log.Warning(ctx, err)
		}
	}

	// Now that the third node is down and decommissioned, decommissioning it
	// again should be a no-op. We do it from node one but as always it doesn't
	// matter.
	log.Info(ctx, "checking that other nodes see node three as successfully decommissioned")
	{
		target := idMap[2]
		o, err := decommission(ctx, c, 1, target, "decommission") // wait=all is implied
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)
		if ok, err := regexp.MatchString(strconv.Itoa(int(target))+`\s+true\s+0\s+true\s+true`, o); !ok || err != nil {
			t.Fatalf("node not decommissioned: ok=%t, err=%v", ok, err)
		}

		// Recommission. Welcome back!
		o, err = decommission(ctx, c, 1, target, "recommission")
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)
		if err := c.Restart(ctx, 2); err != nil {
			t.Fatal(err)
		}
	}

	// Kill the first node and verify that we can decommission it while it's down,
	// bringing it back up to verify that its replicas still get removed.
	log.Info(ctx, "intentionally killing first node")
	if err := c.Kill(ctx, 0); err != nil {
		t.Fatal(err)
	}
	log.Info(ctx, "decommission first node, starting with it down but restarting it for verification")
	{
		target := idMap[0]
		o, err := decommission(ctx, c, 2, target, "decommission", "--wait", "live")
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)
		if err := c.Restart(ctx, 0); err != nil {
			t.Fatal(err)
		}
		// Run a second time to wait until the replicas have all been GC'ed.
		// Note that we specify "all" because even though the first node is
		// now running, it may not be live by the time the command runs.
		o, err = decommission(ctx, c, 2, target, "decommission", "--wait", "all")
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := regexp.MatchString(strconv.Itoa(int(target))+`\s+true\s+0\s+true\s+true`, o); !ok || err != nil {
			t.Fatalf("node did not decommission: ok=%t, err=%v, output:\n%s", ok, err, o)
		}
	}

	// Now we want to test decommissioning a truly dead node. Make sure we don't
	// waste too much time waiting for the node to be recognized as dead. Note that
	// we don't want to set this number too low or everything will seem dead to the
	// allocator at all times, so nothing will ever happen.
	withDB(1, "SET CLUSTER SETTING server.time_until_store_dead = '15s'")

	log.Info(ctx, "intentionally killing first node")
	if err := c.Kill(ctx, 0); err != nil {
		t.Fatal(err)
	}
	// It is being decommissioned in absentia, meaning that its replicas are
	// being removed due to deadness. We can't see that reflected in the output
	// since the current mechanism gets its replica counts from what the node
	// reports about itself, so our assertion here is somewhat weak.
	log.Info(ctx, "decommission first node in absentia using --wait=live")
	{
		target := idMap[0]
		o, err := decommission(ctx, c, 2, target, "decommission", "--wait", "live")
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)
		// Note we don't check precisely zero replicas or that draining=true
		// (which the node would write itself, but it's dead). We do check that
		// the node isn't live, though, which is essentially what `--wait=live`
		// waits for.
		// Note that the target node may still be "live" when it's marked as
		// decommissioned, as its replica count may drop to zero faster than
		// liveness times out.
		if ok, err := regexp.MatchString(strconv.Itoa(int(target))+`\s+true|false\s+\d+\s+true\s+.*`, o); !ok || err != nil {
			t.Fatalf("node did not decommission: ok=%t, err=%v, output:\n%s", ok, err, o)
		}
	}
}
