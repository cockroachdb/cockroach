// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccltestutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

// TestingCreateMultiRegionCluster creates a test cluster with numServers number
// of nodes and the provided testing knobs applied to each of the nodes. Every
// node is placed in its own locality, named "us-east1", "us-east2", and so on.
func TestingCreateMultiRegionCluster(
	t *testing.T, numServers int, knobs base.TestingKnobs,
) (serverutils.TestClusterInterface, *gosql.DB, func()) {
	serverArgs := make(map[int]base.TestServerArgs)
	regionNames := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		// "us-east1", "us-east2"...
		regionNames[i] = fmt.Sprintf("us-east%d", i+1)
	}

	for i := 0; i < numServers; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: knobs,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: regionNames[i]}},
			},
		}
	}

	tc := serverutils.StartNewTestCluster(t, numServers, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})

	ctx := context.Background()
	cleanup := func() {
		tc.Stopper().Stop(ctx)
	}

	sqlDB := tc.ServerConn(0)

	return tc, sqlDB, cleanup
}
