// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxusage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// ClusterIndexUsageStats implements the Reader interface and is able to returns
// cluster-wide index usage statistics. ClusterIndexUsageStats is not
// responsible for collecting index usage statistics in the local node.
type ClusterIndexUsageStats struct {
	// storage is used as the temporary in-memory store for index usage statistics
	// for the ClusterIndexUsageStats. It is not responsible for collecting index
	// usage stats, therefore, we do not call LocalIndexUsageStats.Start() method.
	storage      *LocalIndexUsageStats
	statusServer serverpb.SQLStatusServer
}

var _ Reader = &ClusterIndexUsageStats{}

// NewClusterIndexUsageStats returns a new instance of ClusterIndexUsageStats.
func NewClusterIndexUsageStats(
	localIndexUsageStats *LocalIndexUsageStats, statusServer serverpb.SQLStatusServer,
) *ClusterIndexUsageStats {
	return &ClusterIndexUsageStats{
		storage:      localIndexUsageStats,
		statusServer: statusServer,
	}
}

// Clear clears all the currently stored data inside ClusterIndexUsageStats.
func (c *ClusterIndexUsageStats) Clear() {
	c.storage.clear()
}

// LoadData is called to fetch and refresh the cluster-wide index usage
// statistics before calling the Reader interface.
func (c *ClusterIndexUsageStats) LoadData(ctx context.Context) error {
	_, err := c.statusServer.IndexUsageStatistics(ctx, &serverpb.IndexUsageStatisticsRequest{
		Max: &serverpb.IndexUsageStatisticsRequest_MaxLimit{MaxLimit: 0},
	})

	if err != nil {
		return err
	}

	return nil
}

// BatchInsert insert all the roachpb.CollectedIndexUsageStatistics in the
// otherStats into the local in-memory storage of the ClusterIndexUsageStats.
func (c *ClusterIndexUsageStats) BatchInsert(otherStats []roachpb.CollectedIndexUsageStatistics) {
	c.storage.batchInsert(otherStats)
}

// GetIndexUsageStats implements the idxusage.Reader interface.
func (c *ClusterIndexUsageStats) GetIndexUsageStats(
	key roachpb.IndexUsageKey,
) roachpb.IndexUsageStatistics {
	return c.storage.GetIndexUsageStats(key)
}

// IterateIndexUsageStats the idxusage.Reader interface.
func (c *ClusterIndexUsageStats) IterateIndexUsageStats(
	options IteratorOptions, visitor StatsVisitor,
) error {
	return c.storage.IterateIndexUsageStats(options, visitor)
}
