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
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
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
		S2Geography: &S2GeographyConfig{S2Config: DefaultS2Config()},
	}
}

// geogCovererWithBBoxFallback first computes the covering for the provided
// regions (which were computed using g), and if the covering is too broad
// (contains top-level cells from all faces), falls back to using the bounding
// box of g to compute the covering.
type geogCovererWithBBoxFallback struct {
	rc *s2.RegionCoverer
	g  geo.Geography
}

var _ covererInterface = geogCovererWithBBoxFallback{}

func toDeg(radians float64) float64 {
	return s1.Angle(radians).Degrees()
}

func (rc geogCovererWithBBoxFallback) covering(regions []s2.Region) s2.CellUnion {
	cu := simpleCovererImpl{rc: rc.rc}.covering(regions)
	if isBadGeogCovering(cu) {
		bbox := rc.g.SpatialObject().BoundingBox
		if bbox == nil {
			return cu
		}
		flatCoords := []float64{
			toDeg(bbox.LoX), toDeg(bbox.LoY), toDeg(bbox.HiX), toDeg(bbox.LoY),
			toDeg(bbox.HiX), toDeg(bbox.HiY), toDeg(bbox.LoX), toDeg(bbox.HiY),
			toDeg(bbox.LoX), toDeg(bbox.LoY)}
		bboxT := geom.NewPolygonFlat(geom.XY, flatCoords, []int{len(flatCoords)})
		bboxRegions, err := geo.S2RegionsFromGeomT(bboxT, geo.EmptyBehaviorOmit)
		if err != nil {
			return cu
		}
		bboxCU := simpleCovererImpl{rc: rc.rc}.covering(bboxRegions)
		if !isBadGeogCovering(bboxCU) {
			cu = bboxCU
		}
	}
	return cu
}

func isBadGeogCovering(cu s2.CellUnion) bool {
	const numFaces = 6
	if len(cu) != numFaces {
		return false
	}
	numFaceCells := 0
	for _, c := range cu {
		if c.Level() == 0 {
			numFaceCells++
		}
	}
	return numFaces == numFaceCells
}

// InvertedIndexKeys implements the GeographyIndex interface.
func (i *s2GeographyIndex) InvertedIndexKeys(
	c context.Context, g geo.Geography,
) ([]Key, geopb.BoundingBox, error) {
	r, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return nil, geopb.BoundingBox{}, err
	}
	rect := g.BoundingRect()
	bbox := geopb.BoundingBox{
		LoX: rect.Lng.Lo,
		HiX: rect.Lng.Hi,
		LoY: rect.Lat.Lo,
		HiY: rect.Lat.Hi,
	}
	return invertedIndexKeys(c, geogCovererWithBBoxFallback{rc: i.rc, g: g}, r), bbox, nil
}

// Covers implements the GeographyIndex interface.
func (i *s2GeographyIndex) Covers(c context.Context, g geo.Geography) (UnionKeySpans, error) {
	r, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return nil, err
	}
	return covers(c, geogCovererWithBBoxFallback{rc: i.rc, g: g}, r), nil
}

// CoveredBy implements the GeographyIndex interface.
func (i *s2GeographyIndex) CoveredBy(c context.Context, g geo.Geography) (RPKeyExpr, error) {
	r, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return nil, err
	}
	return coveredBy(c, i.rc, r), nil
}

// Intersects implements the GeographyIndex interface.
func (i *s2GeographyIndex) Intersects(c context.Context, g geo.Geography) (UnionKeySpans, error) {
	r, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return nil, err
	}
	return intersects(c, geogCovererWithBBoxFallback{rc: i.rc, g: g}, r), nil
}

func (i *s2GeographyIndex) DWithin(
	_ context.Context,
	g geo.Geography,
	distanceMeters float64,
	useSphereOrSpheroid geogfn.UseSphereOrSpheroid,
) (UnionKeySpans, error) {
	projInfo, err := geoprojbase.Projection(g.SRID())
	if err != nil {
		return nil, err
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
	gCovering := geogCovererWithBBoxFallback{rc: i.rc, g: g}.covering(r)
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

func (i *s2GeographyIndex) TestingInnerCovering(g geo.Geography) s2.CellUnion {
	r, _ := g.AsS2(geo.EmptyBehaviorOmit)
	if r == nil {
		return nil
	}
	return innerCovering(i.rc, r)
}

func (i *s2GeographyIndex) CoveringGeography(
	c context.Context, g geo.Geography,
) (geo.Geography, error) {
	keys, _, err := i.InvertedIndexKeys(c, g)
	if err != nil {
		return geo.Geography{}, err
	}
	t, err := makeGeomTFromKeys(keys, g.SRID(), func(p s2.Point) (float64, float64) {
		latlng := s2.LatLngFromPoint(p)
		return latlng.Lng.Degrees(), latlng.Lat.Degrees()
	})
	if err != nil {
		return geo.Geography{}, err
	}
	return geo.MakeGeographyFromGeomT(t)
}
