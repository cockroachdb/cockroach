// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// frontierResolvedSpanFrontier wraps a schemaChangeFrontier with additional
// checks specific to how change frontiers processes schema changes.
type frontierResolvedSpanFrontier struct {
	schemaChangeFrontier

	// backfills is a sorted list of timestamps for ongoing backfills.
	// Usually there will only be one, but since aggregators run
	// backfills in parallel without synchronization, there may be
	// multiple backfills happening at one time.
	backfills []hlc.Timestamp
}

// newFrontierResolvedSpanFrontier returns a new frontierResolvedSpanFrontier.
func newFrontierResolvedSpanFrontier(
	initialHighWater hlc.Timestamp, spans ...roachpb.Span,
) (*frontierResolvedSpanFrontier, error) {
	scf, err := makeSchemaChangeFrontier(initialHighWater, spans...)
	if err != nil {
		return nil, err
	}
	return &frontierResolvedSpanFrontier{
		schemaChangeFrontier: *scf,
	}, nil
}

// ForwardResolvedSpan forwards the progress of a resolved span and also does
// some boundary validation.
func (f *frontierResolvedSpanFrontier) ForwardResolvedSpan(
	ctx context.Context, r jobspb.ResolvedSpan,
) (bool, error) {
	switch boundaryType := r.BoundaryType; boundaryType {
	case jobspb.ResolvedSpan_NONE:
	case jobspb.ResolvedSpan_BACKFILL:
		// The change frontier collects resolved spans from all the change
		// aggregators. Since a BACKFILL schema change does not cause an
		// aggregator to shut down, an aggregator may encounter a second
		// schema change (and send resolved spans for that second schema
		// change) before the frontier has received resolved spans for the
		// first BACKFILL schema change from all aggregators. Thus, as long as
		// it is a BACKFILL we have already seen, then it is fine for it to be
		// an earlier timestamp than the latest boundary.
		boundaryTS := r.Timestamp
		_, ok := slices.BinarySearchFunc(f.backfills, boundaryTS, func(elem hlc.Timestamp, ts hlc.Timestamp) int {
			return elem.Compare(ts)
		})
		if ok {
			break
		}
		if err := f.assertBoundaryNotEarlier(ctx, r); err != nil {
			return false, err
		}
		f.backfills = append(f.backfills, boundaryTS)
		f.boundaryTime = r.Timestamp
		f.boundaryType = r.BoundaryType
	case jobspb.ResolvedSpan_EXIT, jobspb.ResolvedSpan_RESTART:
		// EXIT and RESTART are final boundaries that cause the changefeed
		// processors to all move to draining and so should not be followed
		// by any other boundaries.
		if err := f.assertBoundaryNotEarlier(ctx, r); err != nil {
			return false, err
		}
		f.boundaryTime = r.Timestamp
		f.boundaryType = r.BoundaryType
	default:
		return false, errors.AssertionFailedf("unknown boundary type: %v", boundaryType)
	}
	f.latestTs.Forward(r.Timestamp)
	frontierChanged, err := f.Forward(r.Span, r.Timestamp)
	if err != nil {
		return false, err
	}
	// If the frontier changed, we check if the frontier has advanced past any known backfills.
	if frontierChanged {
		frontier := f.Frontier()
		i, _ := slices.BinarySearchFunc(f.backfills, frontier, func(elem hlc.Timestamp, ts hlc.Timestamp) int {
			return elem.Compare(ts)
		})
		f.backfills = f.backfills[i:]
	}
	return frontierChanged, nil
}

// InBackfill returns whether a resolved span is part of an ongoing backfill
// (either an initial scan backfill or a schema change backfill).
// NB: Since the frontierResolvedSpanFrontier consolidates the frontiers of
// multiple change aggregators, there may be more than one concurrent backfill
// happening at different timestamps.
func (f *frontierResolvedSpanFrontier) InBackfill(r jobspb.ResolvedSpan) bool {
	boundaryTS := r.Timestamp
	_, ok := slices.BinarySearchFunc(f.backfills, boundaryTS, func(elem hlc.Timestamp, ts hlc.Timestamp) int {
		return elem.Compare(ts)
	})
	if ok {
		return true
	}

	return f.schemaChangeFrontier.InBackfill(r)
}
