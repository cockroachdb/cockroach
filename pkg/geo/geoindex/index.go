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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/golang/geo/s2"
)

// Interfaces for accelerating certain spatial operations, by allowing the
// caller to use an externally stored index.
//
// An index interface has methods that specify what to write or read from the
// stored index. It is the caller's responsibility to do the actual writes and
// reads. Index keys are represented using uint64 which implies that the index
// is transforming the 2D space (spherical or planar) to 1D space, say using a
// space-filling curve. The index can return false positives so query
// execution must use an exact filtering step after using the index. The
// current implementations use the S2 geometry library.
//
// The externally stored index must support key-value semantics and range
// queries over keys.

// GeographyIndex is an index over the unit sphere.
type GeographyIndex interface {
	// InvertedIndexKeys returns the keys to store this object under when adding
	// it to the index.
	InvertedIndexKeys(c context.Context, g *geo.Geography) ([]Key, error)

	// Acceleration for topological relationships (see
	// https://postgis.net/docs/reference.html#Spatial_Relationships). Distance
	// relationships can be accelerated by adjusting g before calling these
	// functions. Bounding box operators are not accelerated since we do not
	// index the bounding box -- bounding box queries are an implementation
	// detail of a particular indexing approach in PostGIS and are not part of
	// the OGC or SQL/MM specs.

	// Covers returns the index spans to read and union for the relationship
	// ST_Covers(g, x), where x are the indexed geometries.
	Covers(c context.Context, g *geo.Geography) (UnionKeySpans, error)

	// CoveredBy returns the index entries to read and the expression to compute
	// for ST_CoveredBy(g, x), where x are the indexed geometries.
	CoveredBy(c context.Context, g *geo.Geography) (RPKeyExpr, error)

	// Intersects returns the index spans to read and union for the relationship
	// ST_Intersects(g, x), where x are the indexed geometries.
	Intersects(c context.Context, g *geo.Geography) (UnionKeySpans, error)
}

// GeometryIndex is an index over 2D cartesian coordinates.
type GeometryIndex interface {
	// InvertedIndexKeys returns the keys to store this object under when adding
	// it to the index.
	InvertedIndexKeys(c context.Context, g *geo.Geometry) ([]Key, error)

	// Acceleration for topological relationships (see
	// https://postgis.net/docs/reference.html#Spatial_Relationships). Distance
	// relationships can be accelerated by adjusting g before calling these
	// functions. Bounding box operators are not accelerated since we do not
	// index the bounding box -- bounding box queries are an implementation
	// detail of a particular indexing approach in PostGIS and are not part of
	// the OGC or SQL/MM specs.

	// Covers returns the index spans to read and union for the relationship
	// ST_Covers(g, x), where x are the indexed geometries.
	Covers(c context.Context, g *geo.Geometry) (UnionKeySpans, error)

	// CoveredBy returns the index entries to read and the expression to compute
	// for ST_CoveredBy(g, x), where x are the indexed geometries.
	CoveredBy(c context.Context, g *geo.Geometry) (RPKeyExpr, error)

	// Intersects returns the index spans to read and union for the relationship
	// ST_Intersects(g, x), where x are the indexed geometries.
	Intersects(c context.Context, g *geo.Geometry) (UnionKeySpans, error)
}

// Key is one entry under which a geospatial shape is stored on behalf of an
// Index. The index is of the form (Key, Primary Key).
type Key uint64

func (k Key) rpExprElement() {}

// KeySpan represents a range of Keys.
type KeySpan struct {
	// NB: In contrast to most of our Spans, End is inclusive.
	Start, End Key
}

// UnionKeySpans is the set of indexed spans to retrieve and combine via set
// union. The spans are guaranteed to be non-overlapping. Duplicate primary
// keys will not be retrieved by any individual key, but they may be present
// if more than one key is retrieved (including duplicates in a single span
// where End - Start > 1).
type UnionKeySpans []KeySpan

func (s UnionKeySpans) String() string {
	var spans []string
	for _, span := range s {
		if span.Start == span.End {
			spans = append(spans, fmt.Sprintf("%s", s2.CellID(span.Start).ToToken()))
		} else {
			spans = append(spans, fmt.Sprintf("[%s, %s]",
				s2.CellID(span.Start).ToToken(), s2.CellID(span.End).ToToken()))
		}
	}
	return strings.Join(spans, ",")
}

// RPExprElement is an element in the Reverse Polish notation expression.
type RPExprElement interface {
	rpExprElement()
}

// RPSetOperator is a set operator in the Reverse Polish notation expression.
type RPSetOperator int

const (
	RPSetUnion        RPSetOperator = 1
	RPSetIntersection RPSetOperator = 2
)

func (o RPSetOperator) rpExprElement() {}

// RPKeyExpr is an expression to evaluate over primary keys retrieved for
// index keys. If we view each index key as a posting list of primary keys,
// the expression involves union and intersection over the sets represented by
// each posting list. For S2, this expression represents an intersection of
// ancestors of different keys (cell ids) and is likely to contain many common
// keys. This special structure allows us to efficiently and easily eliminate
// common sub-expressions, hence the interface presents the factored
// expression. The expression is represented in Reverse Polish notation.
type RPKeyExpr []RPExprElement

func (x RPKeyExpr) String() string {
	var elements []string
	for _, e := range x {
		switch elem := e.(type) {
		case Key:
			elements = append(elements, fmt.Sprintf("%s", s2.CellID(elem).ToToken()))
		case RPSetOperator:
			switch elem {
			case RPSetUnion:
				elements = append(elements, "union")
			case RPSetIntersection:
				elements = append(elements, "intersection")
			}
		}
	}
	return strings.Join(elements, " ")
}

// Helper functions for index implementations that use the S2 geometry
// library.

func covering(rc *s2.RegionCoverer, regions []s2.Region) s2.CellUnion {
	// TODO(sumeer): Add a max cells constraint for the whole covering,
	// to respect the index configuration.
	var u s2.CellUnion
	for _, r := range regions {
		u = append(u, rc.Covering(r)...)
	}
	// Ensure the cells are non-overlapping.
	u.Normalize()
	return u
}

// The inner covering, for the shape represented by regions, is used to find
// shapes that contain this shape. Note that this does not have the same
// semantics as RegionCoverer.InteriorCovering() which will be empty for
// points and lines. We can use the lowest level cells that intersect with the
// points and lines since anything containing this shape will need to have a
// covering that includes those cells or their ancestor cells.
func innerCovering(rc *s2.RegionCoverer, regions []s2.Region) s2.CellUnion {
	var u s2.CellUnion
	for _, r := range regions {
		switch region := r.(type) {
		case s2.Point:
			cellID := s2.CellFromPoint(region).ID()
			if !cellID.IsLeaf() {
				panic("bug in S2")
			}
			u = append(u, cellID)
		case *s2.Polyline:
			// TODO(sumeer): for long lines could also pick some intermediate
			// points along the line. Decide based on experiments.
			for _, p := range *region {
				cellID := s2.CellFromPoint(p).ID()
				u = append(u, cellID)
			}
		case *s2.Polygon:
			// Iterate over all exterior points
			if region.NumLoops() > 0 {
				loop := region.Loop(0)
				for _, p := range loop.Vertices() {
					cellID := s2.CellFromPoint(p).ID()
					u = append(u, cellID)
				}
				// Arbitrary threshold value. This is to avoid computing an expensive
				// region covering for regions with small area.
				// TODO(sumeer): for large area regions, put an upper bound on the
				// level used for cells. Decide based on experiments.
				const smallPolygonLevelThreshold = 25
				if region.Area() > s2.AvgAreaMetric.Value(smallPolygonLevelThreshold) {
					u = append(u, rc.InteriorCovering(region)...)
				}
			}
		default:
			panic("bug: code should not be producing unhandled Region type")
		}
	}
	// Ensure the cells are non-overlapping.
	u.Normalize()

	// TODO(sumeer): if the number of cells is too many, make the list sparser.
	// u[len(u)-1] - u[0] / len(u) is the mean distance between cells. Pick a
	// target distance based on the goal to reduce to k cells: target_distance
	// := mean_distance * k / len(u) Then iterate over u and for every sequence
	// of cells that are within target_distance, replace by median cell or by
	// largest cell. Decide based on experiments.

	return u
}

// ancestorCells returns the set of cells containing these cells, not
// including the given cells.
func ancestorCells(cells ...s2.CellID) []s2.CellID {
	var ancestors []s2.CellID
	var seen map[s2.CellID]struct{}
	if len(cells) > 1 {
		seen = make(map[s2.CellID]struct{})
	}
	for _, c := range cells {
		for l := c.Level() - 1; l >= 0; l-- {
			p := c.Parent(l)
			if seen != nil {
				if _, ok := seen[p]; ok {
					break
				}
			}
			ancestors = append(ancestors, p)
			seen[p] = struct{}{}
		}
	}
	return ancestors
}

// Helper for InvertedIndexKeys.
func invertedIndexKeys(_ context.Context, rc *s2.RegionCoverer, r []s2.Region) []Key {
	covering := covering(rc, r)
	keys := make([]Key, len(covering))
	for i, cid := range covering {
		keys[i] = Key(cid)
	}
	return keys
}

// TODO(sumeer): examine RegionCoverer carefully to see if we can strengthen
// the covering invariant, which would increase the efficiency of covers() and
// remove the need for innerCovering().
//
// Helper for Covers.
func covers(c context.Context, rc *s2.RegionCoverer, r []s2.Region) UnionKeySpans {
	// We use intersects since geometries covered by r may have been indexed
	// using cells that are ancestors of the covering of r. We could avoid
	// reading ancestors if we had a stronger covering invariant, such as by
	// indexing inner coverings.
	return intersects(c, rc, r)
}

// Helper for Intersects.
func intersects(_ context.Context, rc *s2.RegionCoverer, r []s2.Region) UnionKeySpans {
	covering := covering(rc, r)
	querySpans := make([]KeySpan, len(covering))
	for i, cid := range covering {
		querySpans[i].Start = Key(cid.RangeMin())
		querySpans[i].End = Key(cid.RangeMax())
	}
	for _, cid := range ancestorCells(covering...) {
		querySpans = append(querySpans, KeySpan{Start: Key(cid), End: Key(cid)})
	}
	return querySpans
}

// Helper for CoveredBy.
func coveredBy(_ context.Context, rc *s2.RegionCoverer, r []s2.Region) RPKeyExpr {
	covering := innerCovering(rc, r)

	// The covering is normalized so no 2 cells are such that one is an ancestor
	// of another. Arrange these cells and their ancestors in a tree. Any cell
	// with more than one child needs to be unioned with the intersection of the
	// expressions corresponding to each child.

	// It is sufficient to represent the tree using presentCells since the ids
	// of all possible 4 children of a cell can be computed and checked for
	// presence in the map.
	presentCells := make(map[s2.CellID]struct{})
	for _, c := range covering {
		presentCells[c] = struct{}{}
	}
	ancestors := ancestorCells(covering...)
	for _, c := range ancestors {
		presentCells[c] = struct{}{}
	}

	// Construct the reverse polish expression. Note that there are up to 6
	// trees corresponding to the 6 faces in S2.
	expr := make([]RPExprElement, 0, len(presentCells)*2)
	numFaces := 0
	for face := 0; face < 6; face++ {
		rootID := s2.CellIDFromFace(face)
		if _, ok := presentCells[rootID]; !ok {
			continue
		}
		expr = append(expr, generateRPExprForTree(rootID, presentCells)...)
		numFaces++
		if numFaces > 1 {
			expr = append(expr, RPSetIntersection)
		}
	}
	return expr
}

func generateRPExprForTree(rootID s2.CellID, presentCells map[s2.CellID]struct{}) []RPExprElement {
	expr := []RPExprElement{Key(rootID)}
	if rootID.IsLeaf() {
		return expr
	}
	numChildren := 0
	for _, childCellID := range rootID.Children() {
		if _, ok := presentCells[childCellID]; !ok {
			continue
		}
		expr = append(expr, generateRPExprForTree(childCellID, presentCells)...)
		numChildren++
		if numChildren > 1 {
			expr = append(expr, RPSetIntersection)
		}
	}
	if numChildren > 0 {
		expr = append(expr, RPSetUnion)
	}
	return expr
}
