// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

func TestTestServerArgsForTransientCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyEnginesRegistry := server.NewStickyInMemEnginesRegistry()

	testCases := []struct {
		nodeID            roachpb.NodeID
		joinAddr          string
		sqlPoolMemorySize int64
		cacheSize         int64

		expected base.TestServerArgs
	}{
		{
			nodeID:            roachpb.NodeID(1),
			joinAddr:          "127.0.0.1",
			sqlPoolMemorySize: 2 << 10,
			cacheSize:         1 << 10,
			expected: base.TestServerArgs{
				PartOfCluster:           true,
				JoinAddr:                "127.0.0.1",
				DisableTLSForHTTP:       true,
				SQLAddr:                 ":1234",
				HTTPAddr:                ":4567",
				SQLMemoryPoolSize:       2 << 10,
				CacheSize:               1 << 10,
				NoAutoInitializeCluster: true,
				TenantAddr:              new(string),
				EnableDemoLoginEndpoint: true,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyEngineRegistry: stickyEnginesRegistry,
					},
				},
			},
		},
		{
			nodeID:            roachpb.NodeID(3),
			joinAddr:          "127.0.0.1",
			sqlPoolMemorySize: 4 << 10,
			cacheSize:         4 << 10,
			expected: base.TestServerArgs{
				PartOfCluster:           true,
				JoinAddr:                "127.0.0.1",
				SQLAddr:                 ":1236",
				HTTPAddr:                ":4569",
				DisableTLSForHTTP:       true,
				SQLMemoryPoolSize:       4 << 10,
				CacheSize:               4 << 10,
				NoAutoInitializeCluster: true,
				TenantAddr:              new(string),
				EnableDemoLoginEndpoint: true,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyEngineRegistry: stickyEnginesRegistry,
					},
				},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			demoCtxTemp := demoCtx
			demoCtx.sqlPoolMemorySize = tc.sqlPoolMemorySize
			demoCtx.cacheSize = tc.cacheSize

			actual := testServerArgsForTransientCluster(unixSocketDetails{}, tc.nodeID, tc.joinAddr, "", 1234, 4567, stickyEnginesRegistry)
			stopper := actual.Stopper
			defer stopper.Stop(context.Background())

			assert.Len(t, actual.StoreSpecs, 1)
			assert.Equal(
				t,
				fmt.Sprintf("demo-node%d", tc.nodeID),
				actual.StoreSpecs[0].StickyInMemoryEngineID,
			)

			// We cannot compare these.
			actual.Stopper = nil
			actual.StoreSpecs = nil

			assert.Equal(t, tc.expected, actual)

			// Restore demoCtx state after each test.
			demoCtx = demoCtxTemp
		})
	}
}
