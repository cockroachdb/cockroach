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
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

const (
	regionUsEast    = "us-east1-b"
	regionUsCentral = "us-central1-b"
	regionUsWest    = "us-west1-b"
	regionEuWest    = "europe-west2-b"
)

func runConnectionLatencyTest(
	ctx context.Context, t test.Test, c cluster.Cluster, numNodes int, numZones int, password bool,
) {
	err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach")
	require.NoError(t, err)

	err = c.PutE(ctx, t.L(), t.DeprecatedWorkload(), "./workload")
	require.NoError(t, err)

	settings := install.MakeClusterSettings(install.SecureOption(true))
	// Don't start a backup schedule as this roachtest reports roachperf results.
	err = c.StartE(ctx, t.L(), option.DefaultStartOptsNoBackups(), settings)
	require.NoError(t, err)

	var passwordFlag string
	// Only create the user once.
	t.L().Printf("creating testuser")
	if password {
		err = c.RunE(ctx, c.Node(1), `./cockroach sql --certs-dir certs -e "CREATE USER testuser WITH PASSWORD '123' CREATEDB"`)
		require.NoError(t, err)
		err = c.RunE(ctx, c.Node(1), "./workload init connectionlatency --user testuser --password '123' --secure")
		require.NoError(t, err)
		passwordFlag = "--password 123 "
	} else {
		// NB: certs for `testuser` are created by `roachprod start --secure`.
		err = c.RunE(ctx, c.Node(1), `./cockroach sql --certs-dir certs -e "CREATE USER testuser CREATEDB"`)
		require.NoError(t, err)
		require.NoError(t, err)
		err = c.RunE(ctx, c.Node(1), "./workload init connectionlatency --user testuser --secure")
		require.NoError(t, err)
	}

	runWorkload := func(roachNodes, loadNode option.NodeListOption, locality string) {
		var urlString string
		var urls []string
		externalIps, err := c.ExternalIP(ctx, t.L(), roachNodes)
		require.NoError(t, err)

		if password {
			urlTemplate := "postgres://testuser:123@%s:26257?sslmode=require&sslrootcert=certs/ca.crt"
			for _, u := range externalIps {
				url := fmt.Sprintf(urlTemplate, u)
				urls = append(urls, fmt.Sprintf("'%s'", url))
			}
			urlString = strings.Join(urls, " ")
		} else {
			urlTemplate := "postgres://testuser@%s:26257?sslcert=certs/client.testuser.crt&sslkey=certs/client.testuser.key&sslrootcert=certs/ca.crt&sslmode=require"
			for _, u := range externalIps {
				url := fmt.Sprintf(urlTemplate, u)
				urls = append(urls, fmt.Sprintf("'%s'", url))
			}
			urlString = strings.Join(urls, " ")
		}

		t.L().Printf("running workload in %q against urls:\n%s", locality, strings.Join(urls, "\n"))

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

		runWorkload(loadGroups[0].roachNodes, cockroachUsEast, regionUsEast)
		runWorkload(loadGroups[1].roachNodes, cockroachUsWest, regionUsWest)
		runWorkload(loadGroups[2].roachNodes, cockroachEuWest, regionEuWest)
	} else {
		// Run only on the load node.
		runWorkload(c.Range(1, numNodes), c.Node(numNodes+1), regionUsCentral)
	}
}

func registerConnectionLatencyTest(r registry.Registry) {
	// Single region test.
	numNodes := 3
	r.Add(registry.TestSpec{
		Name:  fmt.Sprintf("connection_latency/nodes=%d/certs", numNodes),
		Owner: registry.OwnerSQLFoundations,
		// Add one more node for load node.
		Cluster: r.MakeClusterSpec(numNodes+1, spec.Zones(regionUsCentral)),
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
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(numMultiRegionNodes+loadNodes, spec.Geo(), spec.Zones(geoZonesStr)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numMultiRegionNodes, numZones, false /*password*/)
		},
	})

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("connection_latency/nodes=%d/multiregion/password", numMultiRegionNodes),
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(numMultiRegionNodes+loadNodes, spec.Geo(), spec.Zones(geoZonesStr)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numMultiRegionNodes, numZones, true /*password*/)
		},
	})
}
