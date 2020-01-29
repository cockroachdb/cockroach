package cli

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/assert"
)

func TestTestServerArgsForTransientCluster(t *testing.T) {
	memSize := serverCfg.SQLMemoryPoolSize
	cacheSize := serverCfg.CacheSize

	testCases := []struct {
		nodeID       roachpb.NodeID
		joinAddr     string
		demoCtxNodes int

		expected base.TestServerArgs
	}{
		{
			nodeID:       roachpb.NodeID(1),
			joinAddr:     "127.0.0.1",
			demoCtxNodes: 1,
			expected: base.TestServerArgs{
				PartOfCluster:     true,
				Insecure:          true,
				JoinAddr:          "127.0.0.1",
				SQLMemoryPoolSize: memSize,
				CacheSize:         cacheSize,
			},
		},
		{
			nodeID:       roachpb.NodeID(3),
			joinAddr:     "127.0.0.1",
			demoCtxNodes: 4,
			expected: base.TestServerArgs{
				PartOfCluster:     true,
				Insecure:          true,
				JoinAddr:          "127.0.0.1",
				SQLMemoryPoolSize: memSize / 4,
				CacheSize:         cacheSize / 4,
			},
		},
	}

	for _, tc := range testCases {
		demoCtxTemp := demoCtx

		demoCtx.nodes = tc.demoCtxNodes

		actual := testServerArgsForTransientCluster(tc.nodeID, tc.joinAddr)

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
