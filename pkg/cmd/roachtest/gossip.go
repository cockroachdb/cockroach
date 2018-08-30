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
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
