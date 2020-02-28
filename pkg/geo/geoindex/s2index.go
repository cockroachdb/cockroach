// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoindex

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geofn"
	"github.com/golang/geo/s2"
)

// S2Index is an Index implemented using the s2 geometry library's cell
// decomposition.
//
// WIP Explain how this works.
type S2Index struct {
	rc *s2.RegionCoverer
}

var _ Index = (*S2Index)(nil)

// NewS2Index returns an S2Index with the given configuration. All reads and
// writes on this index must use the same config.
func NewS2Index(cfg S2Config) *S2Index {
	// TODO(dan): Sanity check cfg.
	return &S2Index{
		rc: &s2.RegionCoverer{
			MinLevel: int(cfg.MinLevel),
			MaxLevel: int(cfg.MaxLevel),
			LevelMod: int(cfg.LevelMod),
			MaxCells: int(cfg.MaxCells),
		},
	}
}

// Contains implements the Index interface.
func (i *S2Index) Contains(ctx context.Context, r geo.Region) (KeySpanQuery, error) {
	spans, selectivity := i.containsQuerySpans(ctx, r)
	q := KeySpanQuery{
		Union:                spans,
		Filter:               geofn.Contains,
		EstimatedSelectivity: selectivity,
	}
	return q, nil
}

// AtOperator implements the Index interface.
func (i *S2Index) AtOperator(ctx context.Context, r geo.Region) (KeySpanQuery, error) {
	spans, _ := i.containsQuerySpans(ctx, r)
	q := KeySpanQuery{
		Union: spans,
		// No filter means we keep everything.
		EstimatedSelectivity: 1.0,
	}
	return q, nil
}

func (i *S2Index) containsQuerySpans(ctx context.Context, r geo.Region) ([]KeySpan, float64) {
	queryCovering := i.rc.Covering(r.Region)
	querySpans := make([]KeySpan, len(queryCovering))
	for idx, queryCellID := range queryCovering {
		querySpans[idx].Start = Key(queryCellID.RangeMin())
		querySpans[idx].End = Key(queryCellID.RangeMax())
	}
	regionArea := geofn.Area(r)
	if regionArea == 0.0 {
		// A point has an area of 0, but this leads to an estimated selectivity of
		// 0, which is probably undesirable because an indential point will match.
		// Use some very small number as the size (this happens to be the average
		// size of the smallest level of cells).
		regionArea = s2.AvgAreaMetric.Value(30)
	}
	queryArea := queryCovering.ApproxArea()
	// Assume that things in the index are evenly distributed, even though this is
	// never true for real geographic data. Is there something better we can do
	// here with optimizer histograms?
	selectivity := regionArea / queryArea
	return querySpans, selectivity
}

// InvertedIndexKeys implements the Index interface.
func (i *S2Index) InvertedIndexKeys(ctx context.Context, r geo.Region) ([]Key, error) {
	covering := i.rc.Covering(r.Region)
	keys := make([]Key, len(covering))
	for idx, cellID := range covering {
		keys[idx] = Key(cellID)
	}
	return keys, nil
}
