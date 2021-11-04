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

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// Controller implements the index usage stats subsystem control plane. This exposes
// administrative interfaces that can be consumed by other parts of the database
// (e.g. status server, builtins) to control the behavior of index usage stas
// subsystem.
type Controller struct {
	statusServer serverpb.SQLStatusServer
}

// NewController returns a new instance of idxusage.Controller.
func NewController(status serverpb.SQLStatusServer) *Controller {
	return &Controller{
		statusServer: status,
	}
}

// ResetIndexUsageStats implements the tree.IndexUsageStatsController interface.
func (s *Controller) ResetIndexUsageStats(ctx context.Context) error {
	req := &serverpb.ResetIndexUsageStatsRequest{}
	_, err := s.statusServer.ResetIndexUsageStats(ctx, req)
	if err != nil {
		return err
	}
	return nil
}
