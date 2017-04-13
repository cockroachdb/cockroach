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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package acceptance

import (
	"math/rand"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const longWaitTime = 2 * time.Minute
const shortWaitTime = 20 * time.Second

func TestGossipPeerings(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	runTestOnConfigs(t, testGossipPeeringsInner)
}

func testGossipPeeringsInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	num := c.NumNodes()

	deadline := timeutil.Now().Add(cfg.Duration)

	waitTime := longWaitTime
	if cfg.Duration < waitTime {
		waitTime = shortWaitTime
	}

	for timeutil.Now().Before(deadline) {
		CheckGossip(ctx, t, c, waitTime, HasPeers(num))

		// Restart the first node.
		log.Infof(ctx, "restarting node 0")
		if err := c.Restart(ctx, 0); err != nil {
			t.Fatal(err)
		}
		CheckGossip(ctx, t, c, waitTime, HasPeers(num))

		// Restart another node (if there is one).
		var pickedNode int
		if num > 1 {
			pickedNode = rand.Intn(num-1) + 1
		}
		log.Infof(ctx, "restarting node %d", pickedNode)
		if err := c.Restart(ctx, pickedNode); err != nil {
			t.Fatal(err)
		}
		CheckGossip(ctx, t, c, waitTime, HasPeers(num))
	}
}

// TestGossipRestart verifies that the gossip network can be
// re-bootstrapped after a time when all nodes were down
// simultaneously.
func TestGossipRestart(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	// TODO(bram): #4559 Limit this test to only the relevant cases. No chaos
	// agents should be required.
	runTestOnConfigs(t, testGossipRestartInner)
}

func testGossipRestartInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	// This already replicates the first range (in the local setup).
	// The replication of the first range is important: as long as the
	// first range only exists on one node, that node can trivially
	// acquire the range lease. Once the range is replicated, however,
	// nodes must be able to discover each other over gossip before the
	// lease can be acquired.
	num := c.NumNodes()

	deadline := timeutil.Now().Add(cfg.Duration)

	waitTime := longWaitTime
	if cfg.Duration < waitTime {
		waitTime = shortWaitTime
	}

	for timeutil.Now().Before(deadline) {
		log.Infof(ctx, "waiting for initial gossip connections")
		CheckGossip(ctx, t, c, waitTime, HasPeers(num))
		CheckGossip(ctx, t, c, waitTime, hasClusterID)
		CheckGossip(ctx, t, c, waitTime, hasSentinel)

		log.Infof(ctx, "killing all nodes")
		for i := 0; i < num; i++ {
			if err := c.Kill(ctx, i); err != nil {
				t.Fatal(err)
			}
		}

		log.Infof(ctx, "restarting all nodes")
		for i := 0; i < num; i++ {
			if err := c.Restart(ctx, i); err != nil {
				t.Fatal(err)
			}
		}

		log.Infof(ctx, "waiting for gossip to be connected")
		CheckGossip(ctx, t, c, waitTime, HasPeers(num))
		CheckGossip(ctx, t, c, waitTime, hasClusterID)
		CheckGossip(ctx, t, c, waitTime, hasSentinel)

		for i := 0; i < num; i++ {
			db, err := c.NewClient(ctx, i)
			if err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				if err := db.Del(ctx, "count"); err != nil {
					t.Fatal(err)
				}
			}
			var kv client.KeyValue
			if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				var err error
				kv, err = txn.Inc(ctx, "count", 1)
				return err
			}); err != nil {
				t.Fatal(err)
			} else if v := kv.ValueInt(); v != int64(i+1) {
				t.Fatalf("unexpected value %d for write #%d (expected %d)", v, i, i+1)
			}
		}
	}
}
