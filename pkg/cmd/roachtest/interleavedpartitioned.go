// Copyright 2018 The Cockroach Authors.
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
)

func registerInterleaved(r *testRegistry) {
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
		numZones, numRoachNodes, numLoadNodes := 3, 9, 3
		loadGroups := makeLoadGroups(c, numZones, numRoachNodes, numLoadNodes)
		cockroachWest := loadGroups[0].roachNodes
		workloadWest := loadGroups[0].loadNodes
		cockroachEast := loadGroups[1].roachNodes
		workloadEast := loadGroups[1].loadNodes
		cockroachCentral := loadGroups[2].roachNodes
		workloadCentral := loadGroups[2].loadNodes
		cockroachNodes := loadGroups.roachNodes()
		workloadNodes := loadGroups.loadNodes()

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
		histograms := " --histograms=" + perfArtifactsDir + "/stats.json"

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

		runLocality := func(node nodeListOption, cmd string) {
			m.Go(func(ctx context.Context) error {
				return c.RunE(ctx, node, cmd)
			})
		}

		runLocality(workloadWest, createCmd("west", cockroachWest))
		runLocality(workloadEast, createCmd("east", cockroachEast))
		runLocality(workloadCentral, cmdCentral)

		m.Wait()
	}

	r.Add(testSpec{
		Name:    "interleavedpartitioned",
		Owner:   OwnerPartitioning,
		Cluster: makeClusterSpec(12, geo(), zones("us-west1-b,us-east4-b,us-central1-a")),
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
