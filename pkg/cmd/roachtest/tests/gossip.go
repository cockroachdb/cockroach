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
	gosql "database/sql"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func registerGossip(r registry.Registry) {
	runGossipChaos := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--vmodule=*=1")
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.All())
		err := WaitFor3XReplication(ctx, t, c.Conn(ctx, t.L(), 1))
		require.NoError(t, err)

		gossipNetworkAccordingTo := func(node int) (nodes []int) {
			// Expiration timestamp format: <seconds>.<nanos>,<logical>. We'll
			// capture just the <seconds> portion.
			const query = `
SELECT node_id
  FROM (SELECT node_id, to_timestamp(split_part(split_part(expiration, ',', 1), '.', 1)::FLOAT8) AS expiration
		  FROM crdb_internal.gossip_liveness)
 WHERE expiration > now();
`

			db := c.Conn(ctx, t.L(), node)
			defer db.Close()

			rows, err := db.Query(query)
			if err != nil {
				t.Fatal(err)
			}

			for rows.Next() {
				var nodeID int
				require.NoError(t, rows.Scan(&nodeID))
				require.NotZero(t, nodeID)
				nodes = append(nodes, nodeID)
			}
			sort.Ints(nodes)
			return nodes
		}

		gossipOK := func(start time.Time, deadNode int) bool {
			var expLiveNodes []int

			for i := 1; i <= c.Spec().NodeCount; i++ {
				if elapsed := timeutil.Since(start); elapsed >= 20*time.Second {
					t.Fatalf("gossip did not stabilize (dead node %d) in %.1fs", deadNode, elapsed.Seconds())
				}

				if i == deadNode {
					continue
				}

				t.L().Printf("%d: checking gossip\n", i)
				liveNodes := gossipNetworkAccordingTo(i)
				for _, id := range liveNodes {
					if id == deadNode {
						t.L().Printf("%d: gossip not ok (dead node %d present) (%.0fs)\n",
							i, deadNode, timeutil.Since(start).Seconds())
						return false
					}
				}

				if len(expLiveNodes) == 0 {
					expLiveNodes = liveNodes
					continue
				}

				if len(liveNodes) != len(expLiveNodes) {
					t.L().Printf("%d: gossip not ok (mismatched size of network); expected %d, got %d (%.0fs)\n",
						i, len(expLiveNodes), len(liveNodes), timeutil.Since(start).Seconds())
					return false
				}

				for i := range liveNodes {
					if liveNodes[i] != expLiveNodes[i] {
						t.L().Printf("%d: gossip not ok (mismatched view of live nodes); expected %s, got %s (%.0fs)\n",
							i, expLiveNodes, liveNodes, timeutil.Since(start).Seconds())
						return false
					}
				}
			}
			t.L().Printf("gossip ok (size: %d) (%0.0fs)\n", len(expLiveNodes), timeutil.Since(start).Seconds())
			return true
		}

		waitForGossip := func(deadNode int) {
			t.Status("waiting for gossip to exclude dead node")
			start := timeutil.Now()
			for {
				if gossipOK(start, deadNode) {
					return
				}
				time.Sleep(time.Second)
			}
		}

		waitForGossip(0)
		nodes := c.All()
		for j := 0; j < 10; j++ {
			deadNode := nodes.RandNode()[0]
			c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(deadNode))
			waitForGossip(deadNode)
			c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(deadNode))
		}
	}

	r.Add(registry.TestSpec{
		Name:    "gossip/chaos/nodes=9",
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(9),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runGossipChaos(ctx, t, c)
		},
	})
}

type gossipUtil struct {
	waitTime time.Duration
	urlMap   map[int]string
	conn     func(ctx context.Context, l *logger.Logger, i int, opts ...func(*option.ConnOption)) *gosql.DB
}

func newGossipUtil(ctx context.Context, t test.Test, c cluster.Cluster) *gossipUtil {
	urlMap := make(map[int]string)
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.All())
	if err != nil {
		t.Fatal(err)
	}
	for i, addr := range adminUIAddrs {
		urlMap[i+1] = `http://` + addr
	}
	return &gossipUtil{
		waitTime: 30 * time.Second,
		urlMap:   urlMap,
		conn:     c.Conn,
	}
}

type checkGossipFunc func(map[string]gossip.Info) error

// checkGossip fetches the gossip infoStore from each node and invokes the
// given function. The test passes if the function returns 0 for every node,
// retrying for up to the given duration.
func (g *gossipUtil) check(ctx context.Context, c cluster.Cluster, f checkGossipFunc) error {
	return retry.ForDuration(g.waitTime, func() error {
		var infoStatus gossip.InfoStatus
		for i := 1; i <= c.Spec().NodeCount; i++ {
			url := g.urlMap[i] + `/_status/gossip/local`
			if err := httputil.GetJSON(http.Client{}, url, &infoStatus); err != nil {
				return errors.Wrapf(err, "failed to get gossip status from node %d", i)
			}
			if err := f(infoStatus.Infos); err != nil {
				return errors.Wrapf(err, "node %d", i)
			}
		}

		return nil
	})
}

// hasPeers returns a checkGossipFunc that passes when the given number of
// peers are connected via gossip.
func (gossipUtil) hasPeers(expected int) checkGossipFunc {
	return func(infos map[string]gossip.Info) error {
		count := 0
		for k := range infos {
			if strings.HasPrefix(k, gossip.KeyNodeDescPrefix) {
				count++
			}
		}
		if count != expected {
			return errors.Errorf("expected %d peers, found %d", expected, count)
		}
		return nil
	}
}

// hasSentinel is a checkGossipFunc that passes when the sentinel gossip is present.
func (gossipUtil) hasSentinel(infos map[string]gossip.Info) error {
	if _, ok := infos[gossip.KeySentinel]; !ok {
		return errors.Errorf("sentinel not found")
	}
	return nil
}

// hasClusterID is a checkGossipFunc that passes when the cluster ID gossip is present.
func (gossipUtil) hasClusterID(infos map[string]gossip.Info) error {
	if _, ok := infos[gossip.KeyClusterID]; !ok {
		return errors.Errorf("cluster ID not found")
	}
	return nil
}

func (g *gossipUtil) checkConnectedAndFunctional(
	ctx context.Context, t test.Test, c cluster.Cluster,
) {
	t.L().Printf("waiting for gossip to be connected\n")
	if err := g.check(ctx, c, g.hasPeers(c.Spec().NodeCount)); err != nil {
		t.Fatal(err)
	}
	if err := g.check(ctx, c, g.hasClusterID); err != nil {
		t.Fatal(err)
	}
	if err := g.check(ctx, c, g.hasSentinel); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= c.Spec().NodeCount; i++ {
		db := g.conn(ctx, t.L(), i)
		defer db.Close()
		if i == 1 {
			if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS test"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE IF NOT EXISTS test.kv (k INT PRIMARY KEY, v INT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPSERT INTO test.kv (k, v) VALUES (1, 0)`); err != nil {
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
			if count != i {
				t.Fatalf("unexpected value %d for write #%d (expected %d)", count, i, i)
			}
		} else {
			t.Fatalf("no results found from update")
		}
	}
}

func runGossipPeerings(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	// Repeatedly restart a random node and verify that all of the nodes are
	// seeing the gossiped values.

	g := newGossipUtil(ctx, t, c)
	deadline := timeutil.Now().Add(time.Minute)

	for i := 1; timeutil.Now().Before(deadline); i++ {
		if err := g.check(ctx, c, g.hasPeers(c.Spec().NodeCount)); err != nil {
			t.Fatal(err)
		}
		if err := g.check(ctx, c, g.hasClusterID); err != nil {
			t.Fatal(err)
		}
		if err := g.check(ctx, c, g.hasSentinel); err != nil {
			t.Fatal(err)
		}
		t.L().Printf("%d: OK\n", i)

		// Restart a random node.
		node := c.All().RandNode()
		t.L().Printf("%d: restarting node %d\n", i, node[0])
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), node)
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), node)
		// Sleep a bit to avoid hitting:
		// https://github.com/cockroachdb/cockroach/issues/48005
		time.Sleep(3 * time.Second)
	}
}

func runGossipRestart(ctx context.Context, t test.Test, c cluster.Cluster) {
	t.Skip("skipping flaky acceptance/gossip/restart", "https://github.com/cockroachdb/cockroach/issues/48423")

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	// Repeatedly stop and restart a cluster and verify that we can perform basic
	// operations. This is stressing the gossiping of the first range descriptor
	// which is required for any node to be able do even the most basic
	// operations on a cluster.

	g := newGossipUtil(ctx, t, c)
	deadline := timeutil.Now().Add(time.Minute)

	for i := 1; timeutil.Now().Before(deadline); i++ {
		g.checkConnectedAndFunctional(ctx, t, c)
		t.L().Printf("%d: OK\n", i)

		t.L().Printf("%d: killing all nodes\n", i)
		c.Stop(ctx, t.L(), option.DefaultStopOpts())

		t.L().Printf("%d: restarting all nodes\n", i)
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	}
}

func runGossipRestartNodeOne(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	// Reduce the scan max idle time to speed up evacuation of node 1.
	settings := install.MakeClusterSettings(install.NumRacksOption(c.Spec().NodeCount))
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=5ms")

	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings)

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	run := func(stmtStr string) {
		stmt := fmt.Sprintf(stmtStr, "", "=")
		t.L().Printf("%s\n", stmt)
		_, err := db.ExecContext(ctx, stmt)
		if err != nil && strings.Contains(err.Error(), "syntax error") {
			// Pre-2.1 was EXPERIMENTAL.
			// TODO(knz): Remove this in 2.2.
			stmt = fmt.Sprintf(stmtStr, "EXPERIMENTAL", "")
			t.L().Printf("%s\n", stmt)
			_, err = db.ExecContext(ctx, stmt)
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for gossip to propagate - otherwise attempting to set zone
	// constraints can fail with an error about how the constraint doesn't match
	// any nodes in the cluster (#30220).
	var lastNodeCount int
	if err := retry.ForDuration(30*time.Second, func() error {
		const query = `SELECT count(*) FROM crdb_internal.gossip_nodes`
		var count int
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count <= 1 {
			err := errors.Errorf("node 1 still only knows about %d node%s",
				count, util.Pluralize(int64(count)))
			if count != lastNodeCount {
				lastNodeCount = count
				t.L().Printf("%s\n", err)
			}
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Evacuate all of the ranges off node 1 with zone config constraints. See
	// the racks setting specified when the cluster was started.
	run(`ALTER RANGE default %[1]s CONFIGURE ZONE %[2]s 'constraints: {"-rack=0"}'`)
	run(`ALTER RANGE system %[1]s CONFIGURE ZONE %[2]s 'constraints: {"-rack=0"}'`)
	run(`ALTER DATABASE system %[1]s CONFIGURE ZONE %[2]s 'constraints: {"-rack=0"}'`)
	run(`ALTER RANGE meta %[1]s CONFIGURE ZONE %[2]s 'constraints: {"-rack=0"}'`)
	run(`ALTER RANGE liveness %[1]s CONFIGURE ZONE %[2]s 'constraints: {"-rack=0"}'`)
	if t.IsBuildVersion("v19.2.0") {
		run(`ALTER TABLE system.replication_stats %[1]s CONFIGURE ZONE %[2]s 'constraints: {"-rack=0"}'`)
		run(`ALTER TABLE system.replication_constraint_stats %[1]s CONFIGURE ZONE %[2]s 'constraints: {"-rack=0"}'`)
	}
	if t.IsBuildVersion("v21.2.0") {
		run(`ALTER TABLE system.tenant_usage %[1]s CONFIGURE ZONE %[2]s 'constraints: {"-rack=0"}'`)
	}

	var lastReplCount int
	if err := retry.ForDuration(2*time.Minute, func() error {
		const query = `
SELECT count(replicas)
  FROM crdb_internal.ranges
 WHERE array_position(replicas, 1) IS NOT NULL
`
		var count int
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			err := errors.Errorf("node 1 still has %d replicas", count)
			if count != lastReplCount {
				lastReplCount = count
				t.L().Printf("%s\n", err)
			}
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	t.L().Printf("killing all nodes\n")
	c.Stop(ctx, t.L(), option.DefaultStopOpts())

	// Restart node 1, but have it listen on a different port for internal
	// connections. This will require node 1 to reach out to the other nodes in
	// the cluster for gossip info.
	err := c.RunE(ctx, c.Node(1),
		`./cockroach start --insecure --background --store={store-dir} `+
			`--log-dir={log-dir} --cache=10% --max-sql-memory=10% `+
			`--listen-addr=:$[{pgport:1}+10000] --http-port=$[{pgport:1}+1] `+
			`--join={pghost:1}:{pgport:1}`+
			`> {log-dir}/cockroach.stdout 2> {log-dir}/cockroach.stderr`)
	if err != nil {
		t.Fatal(err)
	}

	// Restart the other nodes. These nodes won't be able to talk to node 1 until
	// node 1 talks to it (they have out of date address info). Node 1 needs
	// incoming gossip info in order to determine where range 1 is.
	settings = install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Range(2, c.Spec().NodeCount))

	// We need to override DB connection creation to use the correct port for
	// node 1. This is more complicated than it should be and a limitation of the
	// current infrastructure which doesn't know about cockroach nodes started on
	// non-standard ports.
	g := newGossipUtil(ctx, t, c)
	g.conn = func(ctx context.Context, l *logger.Logger, i int, opts ...func(*option.ConnOption)) *gosql.DB {
		if i != 1 {
			return c.Conn(ctx, l, i)
		}
		urls, err := c.ExternalPGUrl(ctx, l, c.Node(1), "")
		if err != nil {
			t.Fatal(err)
		}
		url, err := url.Parse(urls[0])
		if err != nil {
			t.Fatal(err)
		}
		host, port, err := net.SplitHostPort(url.Host)
		if err != nil {
			t.Fatal(err)
		}
		v, err := strconv.Atoi(port)
		if err != nil {
			t.Fatal(err)
		}
		url.Host = fmt.Sprintf("%s:%d", host, v+10000)
		db, err := gosql.Open("postgres", url.String())
		if err != nil {
			t.Fatal(err)
		}
		return db
	}

	g.checkConnectedAndFunctional(ctx, t, c)

	// Stop our special snowflake process which won't be recognized by the test
	// harness, and start it again on the regular.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(1))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1))
}

func runCheckLocalityIPAddress(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")

	externalIP, err := c.ExternalIP(ctx, t.L(), c.Range(1, c.Spec().NodeCount))
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= c.Spec().NodeCount; i++ {
		if c.IsLocal() {
			externalIP[i-1] = "localhost"
		}
		extAddr := externalIP[i-1]

		settings := install.MakeClusterSettings(install.NumRacksOption(1))
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--locality-advertise-addr=rack=0@%s", extAddr))
		c.Start(ctx, t.L(), startOpts, settings, c.Node(i))
	}

	rowCount := 0

	for i := 1; i <= c.Spec().NodeCount; i++ {
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()

		rows, err := db.Query(
			`SELECT node_id, advertise_address FROM crdb_internal.gossip_nodes`,
		)
		if err != nil {
			t.Fatal(err)
		}

		for rows.Next() {
			rowCount++
			var nodeID int
			var advertiseAddress string
			if err := rows.Scan(&nodeID, &advertiseAddress); err != nil {
				t.Fatal(err)
			}

			if c.IsLocal() {
				if !strings.Contains(advertiseAddress, "localhost") {
					t.Fatal("Expected connect address to contain localhost")
				}
			} else {
				exps, err := c.ExternalAddr(ctx, t.L(), c.Node(nodeID))
				if err != nil {
					t.Fatal(err)
				}
				if exps[0] != advertiseAddress {
					t.Fatalf("Connection address is %s but expected %s", advertiseAddress, exps[0])
				}
			}
		}
	}
	if rowCount <= 0 {
		t.Fatal("No results for " +
			"SELECT node_id, advertise_address FROM crdb_internal.gossip_nodes")
	}
}
