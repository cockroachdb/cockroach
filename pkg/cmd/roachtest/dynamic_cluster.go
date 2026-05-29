// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type dynamicClusterImpl struct {
	*roachprodCluster
}

// Grow adds nodes to the cluster.
func (c *roachprodCluster) Grow(ctx context.Context, l *logger.Logger, nodeCount int) error {
	err := roachprod.Grow(ctx, l, c.name, install.SimpleSecureOption(c.IsSecure()), nodeCount)
	if err != nil {
		return err
	}
	c.spec.NodeCount += nodeCount
	return nil
}

// Shrink removes nodes from the cluster.
func (c *roachprodCluster) Shrink(ctx context.Context, l *logger.Logger, nodeCount int) error {
	err := roachprod.Shrink(ctx, l, c.name, nodeCount)
	if err != nil {
		return err
	}
	c.spec.NodeCount -= nodeCount
	return nil
}
