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
	"github.com/cockroachdb/cockroach/pkg/geo/geogfn"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// s2GeographyIndex is an implementation of GeographyIndex that uses the S2 geometry
// library.
type s2GeographyIndex struct {
	rc *s2.RegionCoverer
}

var _ GeographyIndex = (*s2GeographyIndex)(nil)

// NewS2GeographyIndex returns an index with the given configuration. The
// configuration of an index cannot be changed without rewriting the index
// since deletes could miss some index entries. Currently, reads could use a
// different configuration, but that is subject to change if we manage to
// strengthen the covering invariants (see the todo in covers() in index.go).
func NewS2GeographyIndex(cfg S2GeographyConfig) GeographyIndex {
	// TODO(sumeer): Sanity check cfg.
	return &s2GeographyIndex{
		rc: &s2.RegionCoverer{
			MinLevel: int(cfg.S2Config.MinLevel),
			MaxLevel: int(cfg.S2Config.MaxLevel),
			LevelMod: int(cfg.S2Config.LevelMod),
			MaxCells: int(cfg.S2Config.MaxCells),
		},
	}
}

// DefaultGeographyIndexConfig returns a default config for a geography index.
func DefaultGeographyIndexConfig() *Config {
	return &Config{
		S2Geography: &S2GeographyConfig{S2Config: defaultS2Config()},
	}
}

// InvertedIndexKeys implements the GeographyIndex interface.
func (i *s2GeographyIndex) InvertedIndexKeys(c context.Context, g *geo.Geography) ([]Key, error) {
	r, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return nil, err
	}
	return invertedIndexKeys(c, i.rc, r), nil
}

// Covers implements the GeographyIndex interface.
func (i *s2GeographyIndex) Covers(c context.Context, g *geo.Geography) (UnionKeySpans, error) {
	r, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return nil, err
	}
	return covers(c, i.rc, r), nil
}

// CoveredBy implements the GeographyIndex interface.
func (i *s2GeographyIndex) CoveredBy(c context.Context, g *geo.Geography) (RPKeyExpr, error) {
	r, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return nil, err
	}
	return coveredBy(c, i.rc, r), nil
}

// Intersects implements the GeographyIndex interface.
func (i *s2GeographyIndex) Intersects(c context.Context, g *geo.Geography) (UnionKeySpans, error) {
	r, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return nil, err
	}
	return intersects(c, i.rc, r), nil
}

func (i *s2GeographyIndex) DWithin(
	_ context.Context,
	g *geo.Geography,
	distanceMeters float64,
	useSphereOrSpheroid geogfn.UseSphereOrSpheroid,
) (UnionKeySpans, error) {
	projInfo, ok := geoprojbase.Projection(g.SRID())
	if !ok {
		return nil, errors.Errorf("projection not found for SRID: %d", g.SRID())
	}
	if projInfo.Spheroid == nil {
		return nil, errors.Errorf("projection %d does not have spheroid", g.SRID())
	}
	r, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return nil, err
	}
	// The following approach of constructing the covering and then expanding by
	// an angle is worse than first expanding the original shape and then
	// constructing a covering. However the s2 golang library lacks the c++
	// S2ShapeIndexBufferedRegion, whose GetCellUnionBound() method is what we
	// desire.
	//
	// Construct the cell covering for the shape.
	gCovering := covering(i.rc, r)
	// Convert the distanceMeters to an angle, in order to expand the cell covering
	// on the sphere by the angle.
	multiplier := 1.0
	if useSphereOrSpheroid == geogfn.UseSpheroid {
		// We are using a sphere to calculate an angle on a spheroid, so adjust by the
		// error.
		multiplier += geogfn.SpheroidErrorFraction
	}
	angle := s1.Angle(multiplier * distanceMeters / projInfo.Spheroid.SphereRadius)
	// maxLevelDiff puts a bound on the number of cells used after the expansion.
	// For example, we do not want expanding a large country by 1km to generate too
	// many cells.
	const maxLevelDiff = 2
	gCovering.ExpandByRadius(angle, maxLevelDiff)
	// Finally, make the expanded covering obey the configuration of the index, which
	// is used in the RegionCoverer.
	var covering s2.CellUnion
	for _, c := range gCovering {
		if c.Level() > i.rc.MaxLevel {
			c = c.Parent(i.rc.MaxLevel)
		}
		covering = append(covering, c)
	}
	covering.Normalize()
	return intersectsUsingCovering(covering), nil
}

func (i *s2GeographyIndex) TestingInnerCovering(g *geo.Geography) s2.CellUnion {
	r, _ := g.AsS2(geo.EmptyBehaviorOmit)
	if r == nil {
		return nil
	}
	return innerCovering(i.rc, r)
}
