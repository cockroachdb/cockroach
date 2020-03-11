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
	"github.com/golang/geo/s2"
)

// S2Index is an Index implemented using the s2 geometry library's cell
// decomposition.
//
// TODO(dan): Explain how this works.
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
func (i *S2Index) Contains(ctx context.Context, g *geo.Geography) (KeySpanQuery, error) {
	queryCovering := i.covering(g.Figure)
	querySpans := make([]KeySpan, len(queryCovering))
	for idx, queryCellID := range queryCovering {
		querySpans[idx].Start = Key(queryCellID.RangeMin())
		querySpans[idx].End = Key(queryCellID.RangeMax())
	}
	regionArea, err := geo.STAreaGeography(g)
	if err != nil {
		return KeySpanQuery{}, err
	}
	if regionArea == 0.0 {
		// A point has an area of 0, but this leads to an estimated selectivity of
		// 0, which is probably undesirable because an indentical point would match.
		// Use some very small number as the size (this happens to be the average
		// size of the smallest level of s2 cells).
		regionArea = s2.AvgAreaMetric.Value(30)
	}
	queryArea := queryCovering.ApproxArea()
	// Assume that things in the index are evenly distributed, even though this is
	// never true for real geographic data. Is there something better we can do
	// here with optimizer histograms?
	selectivity := regionArea / queryArea

	q := KeySpanQuery{
		Union:                querySpans,
		Filter:               geo.STContainsGeography,
		EstimatedSelectivity: selectivity,
	}
	return q, nil
}

// TildeOperator implements the Index interface.
func (i *S2Index) TildeOperator(ctx context.Context, g *geo.Geography) (KeySpanQuery, error) {
	q, err := i.Contains(ctx, g)
	if err != nil {
		return q, err
	}
	// TildeOperator is a bounding-box only Contains, which is the same as an
	// index retrival with no filter.
	//
	// TODO(dan): Since the boxes we index are coverings of s2 cells but PostGIS
	// uses a single rect, we'll get different answers for the ~ operator. Do we
	// care?
	q.Filter = nil
	// No filter means we keep everything.
	q.EstimatedSelectivity = 1.0
	return q, nil
}

// InvertedIndexKeys implements the Index interface.
func (i *S2Index) InvertedIndexKeys(ctx context.Context, g *geo.Geography) ([]Key, error) {
	covering := i.covering(g.Figure)
	keys := make([]Key, len(covering))
	for idx, cellID := range covering {
		keys[idx] = Key(cellID)
	}
	return keys, nil
}

func (i *S2Index) covering(figure []s2.Region) s2.CellUnion {
	// TODO(dan): This doesn't respect things like the maximum cell bound.
	var u s2.CellUnion
	for _, sf := range figure {
		u = append(u, i.rc.Covering(sf)...)
	}
	// Ensure the cells are non-overlapping.
	u.Normalize()
	return u
}
