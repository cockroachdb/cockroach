// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package reports

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// computeConstraintConformanceReport iterates through all the ranges and
// generates the constraint conformance report.
func computeConstraintConformanceReport(
	ctx context.Context,
	rangeStore RangeIterator,
	cfg *config.SystemConfig,
	storeResolver StoreResolver,
) (ConstraintReport, error) {
	v := makeConstraintConformanceVisitor(ctx, cfg, storeResolver)
	err := visitRanges(ctx, rangeStore, cfg, &v)
	return v.Report(), err
}

// computeReplicationStatsReport iterates through all the ranges and generates
// the replication stats report.
func computeReplicationStatsReport(
	ctx context.Context, rangeStore RangeIterator, checker nodeChecker, cfg *config.SystemConfig,
) (RangeReport, error) {
	v := makeReplicationStatsVisitor(ctx, cfg, checker)
	err := visitRanges(ctx, rangeStore, cfg, &v)
	return v.Report(), err
}

// computeCriticalLocalitiesReport iterates through all the ranges and generates
// the critical localities report.
func computeCriticalLocalitiesReport(
	ctx context.Context,
	nodeLocalities map[roachpb.NodeID]roachpb.Locality,
	rangeStore RangeIterator,
	checker nodeChecker,
	cfg *config.SystemConfig,
	storeResolver StoreResolver,
) (LocalityReport, error) {
	v := makeCriticalLocalitiesVisitor(ctx, nodeLocalities, cfg, storeResolver, checker)
	err := visitRanges(ctx, rangeStore, cfg, &v)
	return v.Report(), err
}
