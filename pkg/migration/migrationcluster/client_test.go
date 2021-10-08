// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrationcluster_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/migration/migrationcluster"
	"github.com/cockroachdb/cockroach/pkg/migration/nodelivenesstest"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestClusterIterateRangeDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	const numNodes = 1

	for _, splits := range [][]roachpb.Key{
		{},                                    // no splits
		{keys.Meta2Prefix},                    // split between meta1 and meta2
		{keys.SystemPrefix},                   // split after the meta range
		{keys.Meta2Prefix, keys.SystemPrefix}, // split before and after meta2
		{keys.RangeMetaKey(roachpb.RKey("middle")).AsRawKey()},                   // split within meta2
		{keys.Meta2Prefix, keys.RangeMetaKey(roachpb.RKey("middle")).AsRawKey()}, // split at start of and within meta2
	} {
		t.Run(fmt.Sprintf("with-splits-at=%s", splits), func(t *testing.T) {
			params, _ := tests.CreateTestServerParams()
			server, _, kvDB := serverutils.StartServer(t, params)
			defer server.Stopper().Stop(context.Background())

			for _, split := range splits {
				if _, _, err := server.SplitRange(split); err != nil {
					t.Fatal(err)
				}
			}

			var numRanges int
			if err := server.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				numRanges = s.ReplicaCount()
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			c := nodelivenesstest.New(numNodes)
			h := migrationcluster.New(migrationcluster.ClusterConfig{
				NodeLiveness: c,
				Dialer:       migrationcluster.NoopDialer{},
				DB:           kvDB,
			})

			for _, blockSize := range []int{1, 5, 10, 50} {
				var numDescs int
				init := func() { numDescs = 0 }
				if err := h.IterateRangeDescriptors(ctx, blockSize, init, func(descriptors ...roachpb.RangeDescriptor) error {
					numDescs += len(descriptors)
					return nil
				}); err != nil {
					t.Fatal(err)
				}

				if numDescs != numRanges {
					t.Fatalf("expected to find %d ranges, found %d", numRanges, numDescs)
				}
			}
		})
	}
}
