// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package migration_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestHelperIterateRangeDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)

	cv := clusterversion.ClusterVersion{}
	ctx := context.Background()
	const numNodes = 1

	params, _ := tests.CreateTestServerParams()
	server, _, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	var numRanges int
	if err := server.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
		numRanges = s.ReplicaCount()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	c := migration.TestingNewCluster(numNodes, kvDB, server.InternalExecutor().(sqlutil.InternalExecutor))
	h := migration.TestingNewHelper(c, cv)

	for _, blockSize := range []int{1, 5, 10, 50} {
		var numDescs int
		init := func() { numDescs = 0 }
		if err := h.IterateRangeDescriptors(ctx, blockSize, init, func(descriptors ...roachpb.RangeDescriptor) error {
			numDescs += len(descriptors)
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// TODO(irfansharif): We always seem to include a second copy of r1's
		// desc. Unsure why.
		if numDescs != numRanges+1 {
			t.Fatalf("expected to find %d ranges, found %d", numRanges+1, numDescs)
		}
	}
}
