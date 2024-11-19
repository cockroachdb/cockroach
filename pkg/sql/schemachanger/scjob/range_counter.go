// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/backfiller"
)

// rangeCounter implements scdeps.RangeCounter
type rangeCounter struct {
	db  *kv.DB
	dsp *sql.DistSQLPlanner
}

// NewRangeCounter constructs a new RangeCounter.
func NewRangeCounter(db *kv.DB, dsp *sql.DistSQLPlanner) backfiller.RangeCounter {
	return &rangeCounter{
		db:  db,
		dsp: dsp,
	}
}

var _ backfiller.RangeCounter = (*rangeCounter)(nil)

func (r rangeCounter) NumRangesInSpanContainedBy(
	ctx context.Context, span roachpb.Span, containedBy []roachpb.Span,
) (total, inContainedBy int, _ error) {
	return sql.NumRangesInSpanContainedBy(ctx, r.db, r.dsp, span, containedBy)
}
