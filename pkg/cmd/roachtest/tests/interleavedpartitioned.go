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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerInterleaved(r registry.Registry) {
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
		t test.Test,
		c cluster.Cluster,
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

		t.L().Printf("cockroach nodes: %s", cockroachNodes.String()[1:])
		t.L().Printf("workload nodes: %s", workloadNodes.String()[1:])

		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.All())
		c.Start(ctx, cockroachNodes)

		zones := fmt.Sprintf("--east-zone-name %s --west-zone-name %s --central-zone-name %s",
			config.eastName, config.westName, config.centralName)

		cmdInit := fmt.Sprintf("./workload init interleavedpartitioned %s --drop "+
			"--locality east --init-sessions %d",
			zones,
			config.initSessions,
		)

		t.Status("initializing workload")

		// Always init on an east node.
		c.Run(ctx, cockroachEast.RandNode(), cmdInit)

		duration := " --duration " + ifLocal(c, "10s", "10m")
		histograms := " --histograms=" + t.PerfArtifactsDir() + "/stats.json"

		createCmd := func(locality string, cockroachNodes option.NodeListOption) string {
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
		m := c.NewMonitor(ctx, cockroachNodes)

		runLocality := func(node option.NodeListOption, cmd string) {
			m.Go(func(ctx context.Context) error {
				return c.RunE(ctx, node, cmd)
			})
		}

		runLocality(workloadWest, createCmd("west", cockroachWest))
		runLocality(workloadEast, createCmd("east", cockroachEast))
		runLocality(workloadCentral, cmdCentral)

		m.Wait()
	}

	r.Add(registry.TestSpec{
		Name:    "interleavedpartitioned",
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(12, spec.Geo(), spec.Zones("us-east1-b,us-west1-b,europe-west2-b")),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runInterleaved(ctx, t, c,
				config{
					eastName:        `europe-west2-b`,
					westName:        `us-west1-b`,
					centralName:     `us-east1-b`, // us-east is central between us-west and eu-west
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
