// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterindexusagestats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/indexusagestats"
)

// ClusterDataLoader provides interfaces to load the fresh cluster-wide index usage
// stats.
type ClusterDataLoader interface {
	// LoadData loads index usage stats data from the cluster.
	LoadData(ctx context.Context) error
}

// Provider provides the interface for fetching cluster-wide index usage stats.
type Provider interface {
	indexusagestats.Reader
	indexusagestats.Sink
	ClusterDataLoader

	// Clear clears the stored stats from the current Provider.
	Clear()
}

type clusterIndexUsageStats struct {
	indexusagestats.Storage
	statusServer serverpb.SQLStatusServer
}

var _ Provider = &clusterIndexUsageStats{}

// New returns a new instance of clusterIndexUsageStats.
func New(storage indexusagestats.Storage, statusServer serverpb.SQLStatusServer) Provider {
	return &clusterIndexUsageStats{
		Storage:      storage,
		statusServer: statusServer,
	}
}

// LoadData implements the Provider interface.
func (c *clusterIndexUsageStats) LoadData(ctx context.Context) error {
	_, err := c.statusServer.IndexUsageStatistics(ctx, &serverpb.IndexUsageStatisticsRequest{
		Max: &serverpb.IndexUsageStatisticsRequest_MaxLimit{MaxLimit: 0},
	})

	if err != nil {
		return err
	}

	return nil
}
