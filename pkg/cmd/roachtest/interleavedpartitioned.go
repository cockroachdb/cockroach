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
		eastName             string
		westName             string
		centralName          string
		sessions             int
		customersPerSession  int
		devicesPerSession    int
		variantsPerSession   int
		parametersPerSession int
		queriesPerSession    int
		insertPercent        int
		insertLocalPercent   int
		retrievePercent      int
		retrieveLocalPercent int
		updatePercent        int
		updateLocalPercent   int
		rowsPerDelete        int
	}

	runInterleaved := func(
		ctx context.Context,
		t *test,
		c *cluster,
		config config,
	) {
		nodes := c.nodes
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Range(1, nodes))
		c.Start(ctx, t, c.Range(1, nodes))

		zones := fmt.Sprintf(" --east-zone-name %s --west-zone-name %s --central-zone-name %s ",
			config.eastName, config.westName, config.centralName)
		cmdInit := "./workload init interleavedpartitioned" + zones +
			"--local=false --drop --locality east --sessions 0"

		t.Status("initializing workload")
		c.Run(ctx, c.Node(1), cmdInit)

		duration := " --duration " + ifLocal("10s", "10m")
		histograms := " --histograms logs/stats.json"

		cmdEast := fmt.Sprintf(
			"./workload run interleavedpartitioned"+zones+
				"--local=false --customers-per-session %d --devices-per-session %d "+
				"--variants-per-session %d --parameters-per-session %d --queries-per-session %d "+
				"--insert-percent %d --insert-local-percent %d --retrieve-percent %d "+
				"--retrieve-local-percent %d --update-percent %d --update-local-percent %d"+
				duration+histograms+" {pgurl:4-6}",
			config.customersPerSession,
			config.devicesPerSession,
			config.variantsPerSession,
			config.parametersPerSession,
			config.queriesPerSession,
			config.insertPercent,
			config.insertLocalPercent,
			config.retrievePercent,
			config.retrieveLocalPercent,
			config.updatePercent,
			config.updateLocalPercent,
		)

		cmdWest := fmt.Sprintf(
			"./workload run interleavedpartitioned"+zones+
				"--local=false --customers-per-session %d --devices-per-session %d "+
				"--variants-per-session %d --parameters-per-session %d --queries-per-session %d "+
				"--insert-percent %d --insert-local-percent %d --retrieve-percent %d "+
				"--retrieve-local-percent %d --update-percent %d --update-local-percent %d"+
				duration+histograms+" {pgurl:1-3}",
			config.customersPerSession,
			config.devicesPerSession,
			config.variantsPerSession,
			config.parametersPerSession,
			config.queriesPerSession,
			config.insertPercent,
			config.insertLocalPercent,
			config.retrievePercent,
			config.retrieveLocalPercent,
			config.updatePercent,
			config.updateLocalPercent,
		)

		cmdCentral := fmt.Sprintf(
			"./workload run interleavedpartitioned"+zones+
				"--local=false --deletes --rows-per-delete %d"+
				duration+histograms+" {pgurl:8}",
			config.rowsPerDelete,
		)

		t.Status("running workload")
		m := newMonitor(ctx, c)

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

		runLocality("west", c.Node(1), cmdWest)
		runLocality("east", c.Node(4), cmdEast)
		runLocality("central", c.Node(7), cmdCentral)

		m.Wait()
	}

	r.Add(testSpec{
		Name:   "interleavedpartitioned",
		Nodes:  nodes(9, geo(), zones("us-west1-b,us-east4-b,us-central1-a")),
		Stable: false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runInterleaved(ctx, t, c,
				config{
					eastName:             `us-east4-b`,
					westName:             `us-west1-b`,
					centralName:          `us-central1-a`,
					sessions:             10000,
					customersPerSession:  2,
					devicesPerSession:    2,
					variantsPerSession:   5,
					parametersPerSession: 1,
					queriesPerSession:    1,
					insertPercent:        80,
					insertLocalPercent:   100,
					retrievePercent:      10,
					retrieveLocalPercent: 100,
					updatePercent:        10,
					updateLocalPercent:   100,
					rowsPerDelete:        20,
				},
			)
		},
	})
}
