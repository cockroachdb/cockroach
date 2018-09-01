// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

func registerGossip(r *registry) {
	runGossipChaos := func(ctx context.Context, t *test, c *cluster) {
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Start(ctx, c.All(), startArgs("--sequential"))

		gossipNetwork := func(node int) string {
			const query = `
SELECT string_agg(source_id::TEXT || ':' || target_id::TEXT, ',')
  FROM (SELECT * FROM crdb_internal.gossip_network ORDER BY source_id, target_id)
`

			db := c.Conn(ctx, node)
			defer db.Close()
			var s gosql.NullString
			if err := db.QueryRow(query).Scan(&s); err != nil {
				t.Fatal(err)
			}
			if s.Valid {
				return s.String
			}
			return ""
		}

		var deadNode int
		gossipOK := func(start time.Time) bool {
			var expected string
			var initialized bool
			for i := 1; i <= c.nodes; i++ {
				if elapsed := timeutil.Since(start); elapsed >= 20*time.Second {
					t.Fatalf("gossip did not stabilize in %.1fs", elapsed.Seconds())
				}

				if i == deadNode {
					continue
				}
				s := gossipNetwork(i)
				if !initialized {
					deadNodeStr := fmt.Sprint(deadNode)
					split := func(c rune) bool {
						return !unicode.IsNumber(c)
					}
					for _, id := range strings.FieldsFunc(s, split) {
						if id == deadNodeStr {
							fmt.Printf("%d: gossip not ok (dead node %d present): %s (%.0fs)\n",
								i, deadNode, s, timeutil.Since(start).Seconds())
							return false
						}
					}
					initialized = true
					expected = s
					continue
				}
				if expected != s {
					fmt.Printf("%d: gossip not ok: %s != %s (%.0fs)\n",
						i, expected, s, timeutil.Since(start).Seconds())
					return false
				}
			}
			fmt.Printf("gossip ok: %s (%0.0fs)\n", expected, timeutil.Since(start).Seconds())
			return true
		}

		waitForGossip := func() {
			t.Status("waiting for gossip to stabilize")
			start := timeutil.Now()
			for {
				if gossipOK(start) {
					return
				}
				time.Sleep(time.Second)
			}
		}

		waitForGossip()
		nodes := c.All()
		for j := 0; j < 100; j++ {
			deadNode = nodes.randNode()[0]
			c.Stop(ctx, c.Node(deadNode))
			waitForGossip()
			c.Start(ctx, c.Node(deadNode))
		}
	}

	r.Add(testSpec{
		Name:  fmt.Sprintf("gossip/chaos/nodes=9"),
		Nodes: nodes(9),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runGossipChaos(ctx, t, c)
		},
	})
}

type gossipUtil struct {
	waitTime time.Duration
	urlMap   map[int]string
	conn     func(ctx context.Context, i int) *gosql.DB
}

func newGossipUtil(ctx context.Context, c *cluster) *gossipUtil {
	urlMap := make(map[int]string)
	for i, addr := range c.ExternalAdminUIAddr(ctx, c.All()) {
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
func (g *gossipUtil) check(ctx context.Context, c *cluster, f checkGossipFunc) error {
	return retry.ForDuration(g.waitTime, func() error {
		var infoStatus gossip.InfoStatus
		for i := 1; i <= c.nodes; i++ {
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
			if strings.HasPrefix(k, gossip.KeyNodeIDPrefix) {
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

func (g *gossipUtil) checkConnectedAndFunctional(ctx context.Context, t *test, c *cluster) {
	c.l.Printf("waiting for gossip to be connected\n")
	if err := g.check(ctx, c, g.hasPeers(c.nodes)); err != nil {
		t.Fatal(err)
	}
	if err := g.check(ctx, c, g.hasClusterID); err != nil {
		t.Fatal(err)
	}
	if err := g.check(ctx, c, g.hasSentinel); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= c.nodes; i++ {
		db := g.conn(ctx, i)
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

func runGossipPeerings(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx)

	// Repeatedly restart a random node and verify that all of the nodes are
	// seeing the gossiped values.

	g := newGossipUtil(ctx, c)
	deadline := timeutil.Now().Add(time.Minute)

	for i := 1; timeutil.Now().Before(deadline); i++ {
		if err := g.check(ctx, c, g.hasPeers(c.nodes)); err != nil {
			t.Fatal(err)
		}
		if err := g.check(ctx, c, g.hasClusterID); err != nil {
			t.Fatal(err)
		}
		if err := g.check(ctx, c, g.hasSentinel); err != nil {
			t.Fatal(err)
		}
		c.l.Printf("%d: OK\n", i)

		// Restart a random node.
		node := c.All().randNode()
		c.l.Printf("%d: restarting node %d\n", i, node[0])
		c.Stop(ctx, node)
		c.Start(ctx, node)
	}
}

func runGossipRestart(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx)

	// Repeatedly stop and restart a cluster and verify that we can perform basic
	// operations. This is stressing the gossiping of the first range descriptor
	// which is required for any node to be able do even the most basic
	// operations on a cluster.

	g := newGossipUtil(ctx, c)
	deadline := timeutil.Now().Add(time.Minute)

	for i := 1; timeutil.Now().Before(deadline); i++ {
		g.checkConnectedAndFunctional(ctx, t, c)
		c.l.Printf("%d: OK\n", i)

		c.l.Printf("%d: killing all nodes\n", i)
		c.Stop(ctx)

		c.l.Printf("%d: restarting all nodes\n", i)
		c.Start(ctx)
	}
}

func runGossipRestartNodeOne(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, racks(c.nodes))

	db := c.Conn(ctx, 1)
	defer db.Close()

	run := func(stmt string) {
		c.l.Printf("%s\n", stmt)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	// Evacuate all of the ranges off node 1 with zone config constraints. See
	// the racks setting specified when the cluster was started.
	run(`ALTER RANGE default EXPERIMENTAL CONFIGURE ZONE 'constraints: {"-rack=0"}'`)
	run(`ALTER RANGE meta EXPERIMENTAL CONFIGURE ZONE 'constraints: {"-rack=0"}'`)
	run(`ALTER RANGE liveness EXPERIMENTAL CONFIGURE ZONE 'constraints: {"-rack=0"}'`)

	var lastCount int
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
			if count != lastCount {
				lastCount = count
				c.l.Printf("%s\n", err)
			}
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	c.l.Printf("killing all nodes\n")
	c.Stop(ctx)

	// Restart node 1, but have it listen on a different port for internal
	// connections. This will require node 1 to reach out to the other nodes in
	// the cluster for gossip info.
	err := c.RunE(ctx, c.Node(1),
		`./cockroach start --insecure --background --store={store-dir} `+
			`--log-dir={log-dir} --cache=10% --max-sql-memory=10% `+
			`--listen-addr=:$[{pgport:1}+10000] --http-port=$[{pgport:1}+1] `+
			`> {log-dir}/cockroach.stdout 2> {log-dir}/cockroach.stderr`)
	if err != nil {
		t.Fatal(err)
	}

	// Restart the other nodes. These nodes won't be able to talk to node 1 until
	// node 1 talks to it (they have out of date address info). Node 1 needs
	// incoming gossip info in order to determine where range 1 is.
	c.Start(ctx, c.Range(2, c.nodes))

	// We need to override DB connection creation to use the correct port for
	// node 1. This is more complicated than it should be and a limitation of the
	// current infrastructure which doesn't know about cockroach nodes started on
	// non-standard ports.
	g := newGossipUtil(ctx, c)
	g.conn = func(ctx context.Context, i int) *gosql.DB {
		if i != 1 {
			return c.Conn(ctx, i)
		}
		url, err := url.Parse(c.ExternalPGUrl(ctx, c.Node(1))[0])
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
}
