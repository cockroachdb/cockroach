// Copyright 2021 The Cockroach Authors.
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

	"github.com/stretchr/testify/require"
)

const (
	regionUsEast = "us-east1-b"
	regionUsWest = "us-west1-b"
	regionEuWest = "europe-west2-b"
)

func runConnectionLatencyTest(
	ctx context.Context, t *test, c *cluster, numNodes int, numZones int,
) {
	err := c.PutE(ctx, t.l, cockroach, "./cockroach")
	require.NoError(t, err)

	err = c.PutE(ctx, t.l, workload, "./workload")
	require.NoError(t, err)

	err = c.StartE(ctx, startArgs("--secure"))
	require.NoError(t, err)

	certsDir := "certs"
	urlTemplate := "postgres://%s@%s:%s?sslcert=%s/client.%s.crt&sslkey=%s/client.%s.key&sslrootcert=%s/ca.crt&sslmode=require"
	var urls []string
	externalIps := c.ExternalIP(ctx, c.All())
	for _, u := range externalIps {
		url := fmt.Sprintf(urlTemplate, "testuser", u, "26257", certsDir, "testuser", certsDir, "testuser", certsDir)
		urls = append(urls, fmt.Sprintf("'%s'", url))
	}

	urlString := strings.Join(urls, " ")

	// Only create the user once.
	err = c.RunE(ctx, c.Node(1), `./cockroach sql --certs-dir certs -e "CREATE USER testuser CREATEDB"`)
	require.NoError(t, err)

	err = c.RunE(ctx, c.All(),
		fmt.Sprintf(`./cockroach cert create-client testuser --certs-dir %s --ca-key=%s/ca.key`,
			certsDir, certsDir))
	require.NoError(t, err)

	err = c.RunE(ctx, c.Node(1), "./workload init connectionlatency --user testuser --secure")
	require.NoError(t, err)

	runWorkload := func(loadNode nodeListOption, locality string) {
		workloadCmd := fmt.Sprintf(
			`./workload run connectionlatency %s --user testuser --secure --duration 30s --histograms=%s/stats.json --locality %s`,
			urlString,
			perfArtifactsDir,
			locality,
		)
		err = c.RunE(ctx, loadNode, workloadCmd)
		require.NoError(t, err)
	}

	if numZones > 1 {
		numLoadNodes := numZones
		loadGroups := makeLoadGroups(c, numZones, numNodes, numLoadNodes)
		cockroachUsEast := loadGroups[0].loadNodes
		cockroachUsWest := loadGroups[1].loadNodes
		cockroachEuWest := loadGroups[2].loadNodes

		runWorkload(cockroachUsEast, regionUsEast)
		runWorkload(cockroachUsWest, regionUsWest)
		runWorkload(cockroachEuWest, regionEuWest)
	} else {
		// Run only on the load node.
		runWorkload(c.Node(numNodes+1), regionUsEast)
	}
}

func registerConnectionLatencyTest(r *testRegistry) {
	// Single region test.
	numNodes := 3
	r.Add(testSpec{
		MinVersion: "v20.1.0",
		Name:       fmt.Sprintf("connection_latency/nodes=%d", numNodes),
		Owner:      OwnerSQLExperience,
		Cluster:    makeClusterSpec(numNodes + 1), // Add one for load node.
		Run: func(ctx context.Context, t *test, c *cluster) {
			runConnectionLatencyTest(ctx, t, c, numNodes, 1)
		},
	})

	geoZones := []string{regionUsEast, regionUsWest, regionEuWest}
	geoZonesStr := strings.Join(geoZones, ",")
	numMultiRegionNodes := 9
	numZones := len(geoZones)
	loadNodes := numZones
	r.Add(testSpec{
		MinVersion: "v20.1.0",
		Name:       fmt.Sprintf("connection_latency/nodes=%d/multiregion", numMultiRegionNodes),
		Owner:      OwnerSQLExperience,
		Cluster:    makeClusterSpec(numMultiRegionNodes+loadNodes, geo(), zones(geoZonesStr)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runConnectionLatencyTest(ctx, t, c, numMultiRegionNodes, numZones)
		},
	})
}
