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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestTestServerArgsForTransientCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
				PartOfCluster:     true,
				JoinAddr:          "127.0.0.1",
				DisableTLSForHTTP: true,
				SQLMemoryPoolSize: 2 << 10,
				CacheSize:         1 << 10,
			},
		},
		{
			nodeID:            roachpb.NodeID(3),
			joinAddr:          "127.0.0.1",
			sqlPoolMemorySize: 4 << 10,
			cacheSize:         4 << 10,
			expected: base.TestServerArgs{
				PartOfCluster:     true,
				JoinAddr:          "127.0.0.1",
				DisableTLSForHTTP: true,
				SQLMemoryPoolSize: 4 << 10,
				CacheSize:         4 << 10,
			},
		},
	}

	for _, tc := range testCases {
		demoCtxTemp := demoCtx
		demoCtx.sqlPoolMemorySize = tc.sqlPoolMemorySize
		demoCtx.cacheSize = tc.cacheSize

		actual := testServerArgsForTransientCluster(unixSocketDetails{}, tc.nodeID, tc.joinAddr, "")

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
	}
}
