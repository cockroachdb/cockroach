// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
