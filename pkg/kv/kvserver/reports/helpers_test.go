// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reports

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
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
