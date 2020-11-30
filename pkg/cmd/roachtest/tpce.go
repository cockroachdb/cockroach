// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

func registerTPCE(r *testRegistry) {
	const customers = 5000
	const nodes = 3
	const duration = 1 * time.Hour
	r.Add(testSpec{
		Name:    fmt.Sprintf("tpce/c=%d/nodes=%d", customers, nodes),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(nodes+1, cpu(4)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			roachNodes := c.Range(1, nodes)
			loadNode := c.Node(nodes + 1)
			racks := nodes

			t.Status("installing cockroach")
			c.Put(ctx, cockroach, "./cockroach", roachNodes)
			c.Start(ctx, t, roachNodes, startArgs(fmt.Sprintf("--racks=%d", racks)))

			t.Status("installing docker")
			if err := c.Install(ctx, t.l, loadNode, "docker"); err != nil {
				t.Fatal(err)
			}

			m := newMonitor(ctx, c, roachNodes)
			m.Go(func(ctx context.Context) error {
				// TODO(nvanbenschoten): switch this to `cockroachdb/tpce` once
				// I get permissions to push the docker image there.
				const dockerRun = `sudo docker run nvanbenschoten/tpce:latest`

				roachNodeIPs := c.InternalIP(ctx, roachNodes)
				roachNodeIPFlags := make([]string, len(roachNodeIPs))
				for i, ip := range roachNodeIPs {
					roachNodeIPFlags[i] = fmt.Sprintf("--hosts=%s", ip)
				}

				t.Status("preparing workload")
				c.Run(ctx, loadNode, fmt.Sprintf("%s --customers=%d --racks=%d --init %s",
					dockerRun, customers, racks, roachNodeIPFlags[0]))

				t.Status("running workload")
				out, err := c.RunWithBuffer(ctx, t.l, loadNode,
					fmt.Sprintf("%s --customers=%d --racks=%d --duration=%s %s",
						dockerRun, customers, racks, duration, strings.Join(roachNodeIPFlags, " ")))
				if err != nil {
					t.Fatalf("%v\n%s", err, out)
				}
				outStr := string(out)
				t.l.Printf("workload output:\n%s\n", outStr)
				if strings.Contains(outStr, "Reported tpsE :    --   (not between 80% and 100%)") {
					return errors.New("invalid tpsE fraction")
				}
				return nil
			})
			m.Wait()
		},
	})
}
