// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func checkDependencies(ctx context.Context, c cluster.Cluster, l *logger.Logger, spec *registry.OperationSpec) (ok bool, err error) {
	for _, dep := range spec.Dependencies {
		switch dep {
		case registry.OperationRequiresNodes:
			if len(c.Nodes()) == 0 {
				return false, nil
			}
		case registry.OperationRequiresPopulatedDatabase:
			conn := c.Conn(ctx, l, 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
			defer conn.Close()

			dbsCount, err := conn.QueryContext(ctx, "SELECT COUNT(database_name) FROM [SHOW DATABASES] WHERE database_name NOT IN ('postgres', 'system')")
			if err != nil {
				return false, err
			}
			dbsCount.Next()
			var count int
			if err := dbsCount.Scan(&count); err != nil {
				return false, err
			}
			if count == 0 {
				return false, nil
			}
		case registry.OperationRequiresZeroUnavailableRanges:
			conn := c.Conn(ctx, l, 1, option.VirtualClusterName("system"))
			defer conn.Close()

			rangesCur, err := conn.QueryContext(ctx, "SELECT SUM(unavailable_ranges) FROM system.replication_stats")
			if err != nil {
				return false, err
			}
			rangesCur.Next()
			var count int
			if err := rangesCur.Scan(&count); err != nil {
				return false, err
			}
			if count != 0 {
				return false, nil
			}
		case registry.OperationRequiresZeroUnderreplicatedRanges:
			conn := c.Conn(ctx, l, 1, option.VirtualClusterName("system"))
			defer conn.Close()

			rangesCur, err := conn.QueryContext(ctx, "SELECT SUM(under_replicated_ranges) FROM system.replication_stats")
			if err != nil {
				return false, err
			}
			rangesCur.Next()
			var count int
			if err := rangesCur.Scan(&count); err != nil {
				return false, err
			}
			if count != 0 {
				return false, nil
			}
		default:
			panic(fmt.Sprintf("unknown operation dependency %d", dep))
		}
	}
	return true, nil
}
