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
	"context"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const waitTime = 2 * time.Minute

func TestGossipPeerings(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	RunLocal(t, func(t *testing.T) {
		runTestWithCluster(t, testGossipPeeringsInner)
	})
}

func testGossipPeeringsInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	num := c.NumNodes()

	deadline := timeutil.Now().Add(cfg.Duration)

	for timeutil.Now().Before(deadline) {
		if err := CheckGossip(ctx, c, waitTime, HasPeers(num)); err != nil {
			t.Fatal(err)
		}

		// Restart the first node.
		log.Infof(ctx, "restarting node 0")
		if err := c.Restart(ctx, 0); err != nil {
			t.Fatal(err)
		}
		if err := CheckGossip(ctx, c, waitTime, HasPeers(num)); err != nil {
			t.Fatal(err)
		}

		// Restart another node (if there is one).
		var pickedNode int
		if num > 1 {
			pickedNode = rand.Intn(num-1) + 1
		}
		log.Infof(ctx, "restarting node %d", pickedNode)
		if err := c.Restart(ctx, pickedNode); err != nil {
			t.Fatal(err)
		}
		if err := CheckGossip(ctx, c, waitTime, HasPeers(num)); err != nil {
			t.Fatal(err)
		}
	}
}

// TestGossipRestart verifies that the gossip network can be
// re-bootstrapped after a time when all nodes were down
// simultaneously.
func TestGossipRestart(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	cfg := readConfigFromFlags()
	RunLocal(t, func(t *testing.T) {
		c := StartCluster(ctx, t, cfg)
		defer c.AssertAndStop(ctx, t)

		testGossipRestartInner(ctx, t, c, cfg)
	})
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

	for timeutil.Now().Before(deadline) {
		log.Infof(ctx, "waiting for initial gossip connections")
		if err := CheckGossip(ctx, c, waitTime, HasPeers(num)); err != nil {
			t.Fatal(err)
		}
		if err := CheckGossip(ctx, c, waitTime, hasClusterID); err != nil {
			t.Fatal(err)
		}
		if err := CheckGossip(ctx, c, waitTime, hasSentinel); err != nil {
			t.Fatal(err)
		}

		log.Infof(ctx, "killing all nodes")
		for i := 0; i < num; i++ {
			if err := c.Kill(ctx, i); err != nil {
				t.Fatal(err)
			}
		}

		log.Infof(ctx, "restarting all nodes")
		var chs []<-chan error
		for i := 0; i < num; i++ {
			// We need to restart asynchronously because the local cluster nodes
			// like to wait until they are ready to serve.
			chs = append(chs, c.(*localcluster.LocalCluster).RestartAsync(ctx, i))
		}
		for i, ch := range chs {
			if err := <-ch; err != nil {
				t.Errorf("error restarting node %d: %s", i, err)
			}
		}
		if t.Failed() {
			t.FailNow()
		}

		testClusterConnectedAndFunctional(ctx, t, c)
	}
}

func testClusterConnectedAndFunctional(ctx context.Context, t *testing.T, c cluster.Cluster) {
	num := c.NumNodes()

	log.Infof(ctx, "waiting for gossip to be connected")
	if err := CheckGossip(ctx, c, waitTime, HasPeers(num)); err != nil {
		t.Fatal(err)
	}
	if err := CheckGossip(ctx, c, waitTime, hasClusterID); err != nil {
		t.Fatal(err)
	}
	if err := CheckGossip(ctx, c, waitTime, hasSentinel); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < num; i++ {
		db, err := c.NewDB(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if _, err := db.Exec("CREATE DATABASE test"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE test.kv (k INT PRIMARY KEY, v INT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO test.kv (k, v) VALUES (1, 0)`); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := db.Query(`UPDATE test.kv SET v=v+1 WHERE k=1 RETURNING v`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var count int
		if rows.Next() {
			if err := rows.Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != (i + 1) {
				t.Fatalf("unexpected value %d for write #%d (expected %d)", count, i, i+1)
			}
		} else {
			t.Fatalf("no results found from update")
		}
	}
}

// This test verifies that when the first node is restarted and has no valid
// join flags or persisted Gossip bootstrap records (achieved through restarting
// the other nodes and thus randomizing their ports), it will still be able to
// accept incoming Gossip connections and manages to bootstrap that way.
//
// See https://github.com/cockroachdb/cockroach/issues/18027.
func TestGossipRestartFirstNodeNeedsIncoming(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	cfg := readConfigFromFlags()
	RunLocal(t, func(t *testing.T) {
		c := StartCluster(ctx, t, cfg)
		defer c.AssertAndStop(ctx, t)

		testGossipRestartFirstNodeNeedsIncomingInner(ctx, t, c, cfg)
	})
}

func testGossipRestartFirstNodeNeedsIncomingInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	num := c.NumNodes()

	if err := c.Kill(ctx, 0); err != nil {
		t.Fatal(err)
	}

	// Configure the first node to repel all replicas.

	lc := c.(*localcluster.LocalCluster)
	lc.Nodes[0].Cfg.ExtraArgs = append(lc.Nodes[0].Cfg.ExtraArgs, "--attrs", "empty")

	if err := c.Restart(ctx, 0); err != nil {
		t.Fatal(err)
	}

	const zoneCfg = "constraints: [-empty]"
	zoneFile := filepath.Join(lc.Nodes[0].Cfg.DataDir, ".customzone")

	if err := ioutil.WriteFile(zoneFile, []byte(zoneCfg), 0644); err != nil {
		t.Fatal(err)
	}

	for _, zone := range []string{".default", ".meta", ".liveness"} {
		if _, _, err := c.ExecCLI(ctx, 0, []string{"zone", "set", zone, "-f", zoneFile}); err != nil {
			t.Fatal(err)
		}
	}

	db := makePGClient(t, c.PGUrl(ctx, 0))
	defer db.Close()

	// NB: This was flaky with `SucceedsSoon`.
	if err := retry.ForDuration(2*time.Minute, func() error {
		const query = "SELECT COUNT(replicas) FROM crdb_internal.ranges WHERE ARRAY_POSITION(replicas, 1) IS NOT NULL"
		var count int
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			err := errors.Errorf("first node still has %d replicas", count)
			log.Info(ctx, err)
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	log.Infof(ctx, "killing all nodes")
	for i := 0; i < num; i++ {
		if err := c.Kill(ctx, i); err != nil {
			t.Fatal(err)
		}
	}

	log.Infof(ctx, "restarting only first node so that it writes its advertised address")

	// Note that at this point, the first node has no join flags and no useful
	// persisted Gossip info (since all other nodes are down and they'll get new
	// ports when they respawn).
	chs := []<-chan error{lc.RestartAsync(ctx, 0)}

	testutils.SucceedsSoon(t, func() error {
		if lc.Nodes[0].AdvertiseAddr() != "" {
			return nil
		}
		return errors.New("no advertised addr yet")
	})

	// The other nodes will be joined to the first node, so they can
	// reach out to it (and in fact they will, since that's the only
	// reachable peer they have -- their persisted info is outdated, too!)
	log.Infof(ctx, "restarting the other nodes")
	for i := 1; i < num; i++ {
		chs = append(chs, lc.RestartAsync(ctx, i))
	}

	log.Infof(ctx, "waiting for healthy cluster")
	for i, ch := range chs {
		if err := <-ch; err != nil {
			t.Errorf("error restarting node %d: %s", i, err)
		}
	}

	testClusterConnectedAndFunctional(ctx, t, c)
}
