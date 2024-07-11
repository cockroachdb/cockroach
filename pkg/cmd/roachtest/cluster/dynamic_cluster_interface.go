// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cluster

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// DynamicCluster is a cluster that can be resized. This is only for use by
// operations that mutate the underlying cluster topology.
type DynamicCluster interface {
	Cluster
	Grow(ctx context.Context, l *logger.Logger, nodeCount int) error
	Shrink(ctx context.Context, l *logger.Logger, nodeCount int) error
}
