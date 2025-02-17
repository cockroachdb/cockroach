// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/stretchr/testify/require"
)

func newTestCluster(t *testing.T) *SyncedCluster {
	testClusterFile := datapathutils.TestDataPath(t, "test_cluster.txt")
	data, err := os.ReadFile(testClusterFile)
	if err != nil {
		t.Fatalf("could not read test cluster file: %s", err)
	}
	metadata := &cloud.Cluster{}
	if err := json.Unmarshal(data, metadata); err != nil {
		t.Fatalf("could not unmarshal test cluster file: %s", err)
	}
	c, err := NewSyncedCluster(metadata, "all", MakeClusterSettings())
	if err != nil {
		t.Fatalf("could not create synced cluster: %s", err)
	}
	return c
}

func TestIPExpander(t *testing.T) {
	ctx := context.Background()
	l := &logger.Logger{}

	c := newTestCluster(t)
	e := &expander{
		node: c.Nodes[0],
	}
	cfg := ExpanderConfig{}

	for _, tc := range []struct {
		name     string
		command  string
		expected string
	}{
		{
			name:     "same node",
			command:  "{ip:1}",
			expected: "10.142.1.1",
		},
		{
			name:     "different node",
			command:  "{ip:2}",
			expected: "10.142.1.2",
		},
		{
			name:     "range of nodes",
			command:  "{ip:1-3}",
			expected: "10.142.1.1 10.142.1.2 10.142.1.3",
		},
		{
			name:     "non consecutive nodes",
			command:  "{ip:2,4}",
			expected: "10.142.1.2 10.142.1.4",
		},
		{
			name:     "public ip",
			command:  "{ip:1-4:public}",
			expected: "35.196.120.1 35.227.25.2 34.75.95.3 34.139.54.4",
		},
		{
			name:     "private ip",
			command:  "{ip:1-4:private}",
			expected: "10.142.1.1 10.142.1.2 10.142.1.3 10.142.1.4",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := e.expand(ctx, l, c, cfg, tc.command)
			require.NoError(t, err)
			require.Equal(t, tc.expected, res)
		})
	}
}
