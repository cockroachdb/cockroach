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

// BatchInsert insert all the roachpb.CollectedIndexUsageStatistics in the
// otherStats into the local in-memory storage of the ClusterIndexUsageStats.
func (c *ClusterIndexUsageStats) BatchInsert(otherStats []roachpb.CollectedIndexUsageStatistics) {
	c.storage.batchInsert(otherStats)
}

// Get implements the idxusage.Reader interface.
func (c *ClusterIndexUsageStats) Get(key roachpb.IndexUsageKey) roachpb.IndexUsageStatistics {
	return c.storage.Get(key)
}

// ForEach the idxusage.Reader interface.
func (c *ClusterIndexUsageStats) ForEach(options IteratorOptions, visitor StatsVisitor) error {
	return c.storage.ForEach(options, visitor)
}
