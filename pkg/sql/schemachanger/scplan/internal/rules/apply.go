// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rules

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/current"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/release_22_2"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func ApplyOpRules(
	ctx context.Context, activeVersion clusterversion.ClusterVersion, g *scgraph.Graph,
) (*scgraph.Graph, error) {
	if activeVersion.IsActive(clusterversion.V23_1) {
		return current.ApplyOpRules(ctx, g)
	} else if activeVersion.IsActive(clusterversion.V22_2) {
		return release_22_2.ApplyOpRules(ctx, g)
	} else {
		log.Warningf(ctx, "Falling back to the oldest supported version 22.2")
		return release_22_2.ApplyOpRules(ctx, g)
	}
}

func ApplyDepRules(
	ctx context.Context, activeVersion clusterversion.ClusterVersion, g *scgraph.Graph,
) error {
	if activeVersion.IsActive(clusterversion.V23_1) {
		return current.ApplyDepRules(ctx, g)
	} else if activeVersion.IsActive(clusterversion.V22_2) {
		return release_22_2.ApplyDepRules(ctx, g)
	} else {
		log.Warningf(ctx, "Falling back to the oldest supported version 22.2")
		return release_22_2.ApplyDepRules(ctx, g)
	}
}
