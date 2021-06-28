// Copyright 2021 The Cockroach Authors.
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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/stretchr/testify/require"
)

const (
	regionUsEast = "us-east1-b"
	regionUsWest = "us-west1-b"
	regionEuWest = "europe-west2-b"
)

func runConnectionLatencyTest(
	ctx context.Context, t test.Test, c cluster.Cluster, numNodes int, numZones int, password bool,
) {
	err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach")
	require.NoError(t, err)

	err = c.PutE(ctx, t.L(), t.DeprecatedWorkload(), "./workload")
	require.NoError(t, err)

	err = c.StartE(ctx, option.StartArgs("--secure"))
	require.NoError(t, err)

	certsDir := "certs"
	var urlString string
	var passwordFlag string
	externalIps, err := c.ExternalIP(ctx, c.All())
	require.NoError(t, err)

	// Only create the user once.
	if password {
		urlTemplate := "postgres://%s:%s@%s:%s?sslmode=require&sslrootcert=%s/ca.crt"
		var urls []string
		for _, u := range externalIps {
			url := fmt.Sprintf(urlTemplate, "testuser", "123", u, "26257", certsDir)
			urls = append(urls, fmt.Sprintf("'%s'", url))
		}
		urlString = strings.Join(urls, " ")

		err = c.RunE(ctx, c.Node(1), `./cockroach sql --certs-dir certs -e "CREATE USER testuser WITH PASSWORD '123' CREATEDB"`)
		require.NoError(t, err)
		err = c.RunE(ctx, c.Node(1), "./workload init connectionlatency --user testuser --password '123' --secure")
		require.NoError(t, err)
		passwordFlag = "--password 123 "
	} else {
		urlTemplate := "postgres://%s@%s:%s?sslcert=%s/client.%s.crt&sslkey=%s/client.%s.key&sslrootcert=%s/ca.crt&sslmode=require"
		var urls []string
		for _, u := range externalIps {
			url := fmt.Sprintf(urlTemplate, "testuser", u, "26257", certsDir, "testuser", certsDir, "testuser", certsDir)
			urls = append(urls, fmt.Sprintf("'%s'", url))
		}
		urlString = strings.Join(urls, " ")

		err = c.RunE(ctx, c.Node(1), `./cockroach sql --certs-dir certs -e "CREATE USER testuser CREATEDB"`)
		require.NoError(t, err)
		err = c.RunE(ctx, c.All(),
			fmt.Sprintf(`./cockroach cert create-client testuser --certs-dir %s --ca-key=%s/ca.key`,
				certsDir, certsDir))
		require.NoError(t, err)
		err = c.RunE(ctx, c.Node(1), "./workload init connectionlatency --user testuser --secure")
		require.NoError(t, err)
	}

	runWorkload := func(loadNode option.NodeListOption, locality string) {
		workloadCmd := fmt.Sprintf(
			`./workload run connectionlatency %s --user testuser --secure %s --duration 30s --histograms=%s/stats.json --locality %s`,
			urlString,
			passwordFlag,
			t.PerfArtifactsDir(),
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

func registerConnectionLatencyTest(r registry.Registry) {
	// Single region test.
	numNodes := 3
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("connection_latency/nodes=%d/certs", numNodes),
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(numNodes + 1), // Add one for load node.
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numNodes, 1, false /*password*/)
		},
	})

	geoZones := []string{regionUsEast, regionUsWest, regionEuWest}
	geoZonesStr := strings.Join(geoZones, ",")
	numMultiRegionNodes := 9
	numZones := len(geoZones)
	loadNodes := numZones

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("connection_latency/nodes=%d/multiregion/certs", numMultiRegionNodes),
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(numMultiRegionNodes+loadNodes, spec.Geo(), spec.Zones(geoZonesStr)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numMultiRegionNodes, numZones, false /*password*/)
		},
	})

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("connection_latency/nodes=%d/multiregion/password", numMultiRegionNodes),
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(numMultiRegionNodes+loadNodes, spec.Geo(), spec.Zones(geoZonesStr)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numMultiRegionNodes, numZones, true /*password*/)
		},
	})
}
