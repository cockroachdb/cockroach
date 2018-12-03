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
	"fmt"
)

func registerInterleaved(r *registry) {
	type config struct {
		eastName        string
		westName        string
		centralName     string
		initSessions    int
		insertPercent   int
		retrievePercent int
		updatePercent   int
		localPercent    int
		rowsPerDelete   int
	}

	runInterleaved := func(
		ctx context.Context,
		t *test,
		c *cluster,
		config config,
	) {
		cockroachWest := c.Range(1, 3)
		workloadWest := c.Node(4)
		cockroachEast := c.Range(5, 7)
		workloadEast := c.Node(8)
		cockroachCentral := c.Range(9, 11)
		workloadCentral := c.Node(12)

		cockroachNodes := cockroachWest.merge(cockroachEast.merge(cockroachCentral))
		workloadNodes := workloadWest.merge(workloadEast.merge(workloadCentral))

		c.l.Printf("cockroach nodes: %s", cockroachNodes.String()[1:])
		c.l.Printf("workload nodes: %s", workloadNodes.String()[1:])

		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Put(ctx, workload, "./workload", c.All())
		c.Start(ctx, t, cockroachNodes)

		zones := fmt.Sprintf("--east-zone-name %s --west-zone-name %s --central-zone-name %s",
			config.eastName, config.westName, config.centralName)

		cmdInit := fmt.Sprintf("./workload init interleavedpartitioned %s --drop "+
			"--locality east --init-sessions %d",
			zones,
			config.initSessions,
		)

		t.Status("initializing workload")

		// Always init on an east node.
		c.Run(ctx, cockroachEast.randNode(), cmdInit)

		duration := " --duration " + ifLocal("10s", "10m")
		histograms := " --histograms logs/stats.json"

		createCmd := func(locality string, cockroachNodes nodeListOption) string {
			return fmt.Sprintf(
				"./workload run interleavedpartitioned %s --locality %s "+
					"--insert-percent %d --insert-local-percent %d "+
					"--retrieve-percent %d --retrieve-local-percent %d "+
					"--update-percent %d --update-local-percent %d "+
					"%s %s {pgurl%s}",
				zones,
				locality,
				config.insertPercent,
				config.localPercent,
				config.retrievePercent,
				config.localPercent,
				config.updatePercent,
				config.localPercent,
				duration,
				histograms,
				cockroachNodes,
			)
		}

		cmdCentral := fmt.Sprintf(
			"./workload run interleavedpartitioned %s "+
				"--locality central --rows-per-delete %d "+
				"%s %s {pgurl%s}",
			zones,
			config.rowsPerDelete,
			duration,
			histograms,
			cockroachCentral,
		)

		t.Status("running workload")
		m := newMonitor(ctx, c, cockroachNodes)

		runLocality := func(name string, node nodeListOption, cmd string) {
			m.Go(func(ctx context.Context) error {
				l, err := t.l.ChildLogger(name)
				if err != nil {
					t.Fatal(err)
				}
				defer l.close()
				return c.RunL(ctx, l, node, cmd)
			})
		}

		runLocality("west", workloadWest, createCmd("west", cockroachWest))
		runLocality("east", workloadEast, createCmd("east", cockroachEast))
		runLocality("central", workloadCentral, cmdCentral)

		m.Wait()
	}

	r.Add(testSpec{
		Name:  "interleavedpartitioned",
		Nodes: nodes(12, geo(), zones("us-west1-b,us-east4-b,us-central1-a")),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runInterleaved(ctx, t, c,
				config{
					eastName:        `us-east4-b`,
					westName:        `us-west1-b`,
					centralName:     `us-central1-a`,
					initSessions:    1000,
					insertPercent:   80,
					retrievePercent: 10,
					updatePercent:   10,
					rowsPerDelete:   20,
				},
			)
		},
	})
}
