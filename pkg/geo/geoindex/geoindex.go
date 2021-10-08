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
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geogfn"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// RelationshipMap contains all the geospatial functions that can be index-
// accelerated. Each function implies a certain type of geospatial relationship,
// which affects how the index is queried as part of a constrained scan or
// geospatial lookup join. RelationshipMap maps the function name to its
// corresponding relationship (Covers, CoveredBy, DFullyWithin, DWithin or Intersects).
//
// Note that for all of these functions, a geospatial lookup join or constrained
// index scan may produce false positives. Therefore, the original function must
// be called on the output of the index operation to filter the results.
var RelationshipMap = map[string]RelationshipType{
	"st_covers":                Covers,
	"st_coveredby":             CoveredBy,
	"st_contains":              Covers,
	"st_containsproperly":      Covers,
	"st_crosses":               Intersects,
	"st_dwithin":               DWithin,
	"st_dfullywithin":          DFullyWithin,
	"st_equals":                Intersects,
	"st_intersects":            Intersects,
	"st_overlaps":              Intersects,
	"st_touches":               Intersects,
	"st_within":                CoveredBy,
	"st_dwithinexclusive":      DWithin,
	"st_dfullywithinexclusive": DFullyWithin,
}

// RelationshipReverseMap contains a default function for each of the
// possible geospatial relationships.
var RelationshipReverseMap = map[RelationshipType]string{
	Covers:       "st_covers",
	CoveredBy:    "st_coveredby",
	DWithin:      "st_dwithin",
	DFullyWithin: "st_dfullywithin",
	Intersects:   "st_intersects",
}

// CommuteRelationshipMap is used to determine how the geospatial
// relationship changes if the arguments to the index-accelerated function are
// commuted.
//
// The relationships in the RelationshipMap map above only apply when the
// second argument to the function is the indexed column. If the arguments are
// commuted so that the first argument is the indexed column, the relationship
// may change.
var CommuteRelationshipMap = map[RelationshipType]RelationshipType{
	Covers:       CoveredBy,
	CoveredBy:    Covers,
	DWithin:      DWithin,
	DFullyWithin: DFullyWithin,
	Intersects:   Intersects,
}

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
// queries over keys. The return values from Covers/CoveredBy/Intersects are
// specialized to the computation that needs to be performed:
// - Intersects needs to union (a) all the ranges corresponding to subtrees
//   rooted at the covering of g, (b) all the parent nodes of the covering
//   of g. The individual entries in (b) are representable as ranges of
//   length 1. All of this is represented as UnionKeySpans. Covers, which
//   is the shape that g covers, currently delegates to Interects so
//   returns the same.
//
// - CoveredBy, which are the shapes that g is covered-by, needs to compute
//   on the paths from each covering node to the root. For example, consider
//   a quad-tree approach to dividing the space, where the nodes/cells
//   covering g are c53, c61, c64. The corresponding ancestor path sets are
//   {c53, c13, c3, c0}, {c61, c15, c3, c0}, {c64, c15, c3, c0}. Let I(c53)
//   represent the index entries for c53. Then,
//   I(c53) \union I(c13) \union I(c3) \union I(c0)
//   represents all the shapes that cover cell c53. Similar union expressions
//   can be constructed for all these paths. The computation needs to
//   intersect these unions since all these cells need to be covered by a
//   shape that covers g. One can extract the common sub-expressions to give
//   I(c0) \union I(c3) \union
//    ((I(c13) \union I(c53)) \intersection
//     (I(c15) \union (I(c61) \intersection I(c64)))
//   CoveredBy returns this factored expression in Reverse Polish notation.

// GeographyIndex is an index over the unit sphere.
type GeographyIndex interface {
	// InvertedIndexKeys returns the keys to store this object under when adding
	// it to the index. Additionally returns a bounding box, which is non-empty
	// iff the key slice is non-empty.
	InvertedIndexKeys(c context.Context, g geo.Geography) ([]Key, geopb.BoundingBox, error)

	// Acceleration for topological relationships (see
	// https://postgis.net/docs/reference.html#Spatial_Relationships). Distance
	// relationships can be accelerated by adjusting g before calling these
	// functions. Bounding box operators are not accelerated since we do not
	// index the bounding box -- bounding box queries are an implementation
	// detail of a particular indexing approach in PostGIS and are not part of
	// the OGC or SQL/MM specs.

	// Covers returns the index spans to read and union for the relationship
	// ST_Covers(g, x), where x are the indexed geometries.
	Covers(c context.Context, g geo.Geography) (UnionKeySpans, error)

	// CoveredBy returns the index entries to read and the expression to compute
	// for ST_CoveredBy(g, x), where x are the indexed geometries.
	CoveredBy(c context.Context, g geo.Geography) (RPKeyExpr, error)

	// Intersects returns the index spans to read and union for the relationship
	// ST_Intersects(g, x), where x are the indexed geometries.
	Intersects(c context.Context, g geo.Geography) (UnionKeySpans, error)

	// DWithin returns the index spans to read and union for the relationship
	// ST_DWithin(g, x, distanceMeters). That is, there exists a part of
	// geometry g that is within distanceMeters of x, where x is an indexed
	// geometry. This function assumes a sphere.
	DWithin(
		c context.Context, g geo.Geography, distanceMeters float64,
		useSphereOrSpheroid geogfn.UseSphereOrSpheroid,
	) (UnionKeySpans, error)

	// TestingInnerCovering returns an inner covering of g.
	TestingInnerCovering(g geo.Geography) s2.CellUnion
	// CoveringGeography returns a Geography which represents the covering of g
	// using the index configuration.
	CoveringGeography(c context.Context, g geo.Geography) (geo.Geography, error)
}

// GeometryIndex is an index over 2D cartesian coordinates.
type GeometryIndex interface {
	// InvertedIndexKeys returns the keys to store this object under when adding
	// it to the index. Additionally returns a bounding box, which is non-empty
	// iff the key slice is non-empty.
	InvertedIndexKeys(c context.Context, g geo.Geometry) ([]Key, geopb.BoundingBox, error)

	// Acceleration for topological relationships (see
	// https://postgis.net/docs/reference.html#Spatial_Relationships). Distance
	// relationships can be accelerated by adjusting g before calling these
	// functions. Bounding box operators are not accelerated since we do not
	// index the bounding box -- bounding box queries are an implementation
	// detail of a particular indexing approach in PostGIS and are not part of
	// the OGC or SQL/MM specs.

	// Covers returns the index spans to read and union for the relationship
	// ST_Covers(g, x), where x are the indexed geometries.
	Covers(c context.Context, g geo.Geometry) (UnionKeySpans, error)

	// CoveredBy returns the index entries to read and the expression to compute
	// for ST_CoveredBy(g, x), where x are the indexed geometries.
	CoveredBy(c context.Context, g geo.Geometry) (RPKeyExpr, error)

	// Intersects returns the index spans to read and union for the relationship
	// ST_Intersects(g, x), where x are the indexed geometries.
	Intersects(c context.Context, g geo.Geometry) (UnionKeySpans, error)

	// DWithin returns the index spans to read and union for the relationship
	// ST_DWithin(g, x, distance). That is, there exists a part of geometry g
	// that is within distance units of x, where x is an indexed geometry.
	DWithin(c context.Context, g geo.Geometry, distance float64) (UnionKeySpans, error)

	// DFullyWithin returns the index spans to read and union for the
	// relationship ST_DFullyWithin(g, x, distance). That is, the maximum distance
	// across every pair of points comprising geometries g and x is within distance
	// units, where x is an indexed geometry.
	DFullyWithin(c context.Context, g geo.Geometry, distance float64) (UnionKeySpans, error)

	// TestingInnerCovering returns an inner covering of g.
	TestingInnerCovering(g geo.Geometry) s2.CellUnion
	// CoveringGeometry returns a Geometry which represents the covering of g
	// using the index configuration.
	CoveringGeometry(c context.Context, g geo.Geometry) (geo.Geometry, error)
}

// RelationshipType stores a type of geospatial relationship query that can
// be accelerated using an index.
type RelationshipType uint8

const (
	// Covers corresponds to the relationship in which one geospatial object
	// covers another geospatial object.
	Covers RelationshipType = (1 << iota)

	// CoveredBy corresponds to the relationship in which one geospatial object
	// is covered by another geospatial object.
	CoveredBy

	// Intersects corresponds to the relationship in which one geospatial object
	// intersects another geospatial object.
	Intersects

	// DWithin corresponds to a relationship where there exists a part of one
	// geometry within d distance units of the other geometry.
	DWithin

	// DFullyWithin corresponds to a relationship where every pair of points in
	// two geometries are within d distance units.
	DFullyWithin
)

var geoRelationshipTypeStr = map[RelationshipType]string{
	Covers:       "covers",
	CoveredBy:    "covered by",
	Intersects:   "intersects",
	DWithin:      "dwithin",
	DFullyWithin: "dfullywithin",
}

func (gr RelationshipType) String() string {
	return geoRelationshipTypeStr[gr]
}

// IsEmptyConfig returns whether the given config contains a geospatial index
// configuration.
func IsEmptyConfig(cfg *Config) bool {
	if cfg == nil {
		return true
	}
	return cfg.S2Geography == nil && cfg.S2Geometry == nil
}

// IsGeographyConfig returns whether the config is a geography geospatial
// index configuration.
func IsGeographyConfig(cfg *Config) bool {
	if cfg == nil {
		return false
	}
	return cfg.S2Geography != nil
}

// IsGeometryConfig returns whether the config is a geometry geospatial
// index configuration.
func IsGeometryConfig(cfg *Config) bool {
	if cfg == nil {
		return false
	}
	return cfg.S2Geometry != nil
}

// Key is one entry under which a geospatial shape is stored on behalf of an
// Index. The index is of the form (Key, Primary Key).
type Key uint64

// rpExprElement implements the RPExprElement interface.
func (k Key) rpExprElement() {}

func (k Key) String() string {
	c := s2.CellID(k)
	if !c.IsValid() {
		return "spilled"
	}
	var b strings.Builder
	b.WriteByte('F')
	b.WriteByte("012345"[c.Face()])
	fmt.Fprintf(&b, "/L%d/", c.Level())
	for level := 1; level <= c.Level(); level++ {
		b.WriteByte("0123"[c.ChildPosition(level)])
	}
	return b.String()
}

// S2CellID transforms the given key into the S2 CellID.
func (k Key) S2CellID() s2.CellID {
	return s2.CellID(k)
}

// KeySpan represents a range of Keys.
type KeySpan struct {
	// Both Start and End are inclusive, i.e., [Start, End].
	Start, End Key
}

// UnionKeySpans is the set of indexed spans to retrieve and combine via set
// union. The spans are guaranteed to be non-overlapping and sorted in
// increasing order. Duplicate primary keys will not be retrieved by any
// individual key, but they may be present if more than one key is retrieved
// (including duplicates in a single span where End - Start > 1).
type UnionKeySpans []KeySpan

func (s UnionKeySpans) String() string {
	return s.toString(math.MaxInt32)
}

func (s UnionKeySpans) toString(wrap int) string {
	b := newStringBuilderWithWrap(&strings.Builder{}, wrap)
	for i, span := range s {
		if span.Start == span.End {
			fmt.Fprintf(b, "%s", span.Start)
		} else {
			fmt.Fprintf(b, "[%s, %s]", span.Start, span.End)
		}
		if i != len(s)-1 {
			b.WriteString(", ")
		}
		b.tryWrap()
	}
	return b.String()
}

// RPExprElement is an element in the Reverse Polish notation expression.
// It is implemented by Key and RPSetOperator.
type RPExprElement interface {
	rpExprElement()
}

// RPSetOperator is a set operator in the Reverse Polish notation expression.
type RPSetOperator int

const (
	// RPSetUnion is the union operator.
	RPSetUnion RPSetOperator = iota + 1

	// RPSetIntersection is the intersection operator.
	RPSetIntersection
)

// rpExprElement implements the RPExprElement interface.
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
			elements = append(elements, elem.String())
		case RPSetOperator:
			switch elem {
			case RPSetUnion:
				elements = append(elements, `\U`)
			case RPSetIntersection:
				elements = append(elements, `\I`)
			}
		}
	}
	return strings.Join(elements, " ")
}

// Helper functions for index implementations that use the S2 geometry
// library.

// covererInterface provides a covering for a set of regions.
type covererInterface interface {
	// covering returns a normalized CellUnion, i.e., it is sorted, and does not
	// contain redundancy.
	covering(regions []s2.Region) s2.CellUnion
}

// simpleCovererImpl is an implementation of covererInterface that delegates
// to s2.RegionCoverer.
type simpleCovererImpl struct {
	rc *s2.RegionCoverer
}

var _ covererInterface = simpleCovererImpl{}

func (rc simpleCovererImpl) covering(regions []s2.Region) s2.CellUnion {
	// TODO(sumeer): Add a max cells constraint for the whole covering,
	// to respect the index configuration.
	var u s2.CellUnion
	for _, r := range regions {
		u = append(u, rc.rc.Covering(r)...)
	}
	// Ensure the cells are non-overlapping.
	u.Normalize()
	return u
}

// The "inner covering", for shape s, represented by the regions parameter, is
// used to find shapes that contain shape s. A regular covering that includes
// a cell c not completely covered by shape s could result in false negatives,
// since shape x that covers shape s could use a finer cell covering (using
// cells below c). For example, consider a portion of the cell quad-tree
// below:
//
//                      c0
//                      |
//                      c3
//                      |
//                  +---+---+
//                  |       |
//                 c13      c15
//                  |       |
//                 c53   +--+--+
//                       |     |
//                      c61    c64
//
// Shape s could have a regular covering c15, c53, where c15 has 4 child cells
// c61..c64, and shape s only intersects wit c61, c64. A different shape x
// that covers shape s may have a covering c61, c64, c53. That is, it has used
// the finer cells c61, c64. If we used both regular coverings it is hard to
// know that x covers g. Hence, we compute the "inner covering" of g (defined
// below).
//
// The interior covering of shape s includes only cells covered by s. This is
// computed by RegionCoverer.InteriorCovering() and is intuitively what we
// need. But the interior covering is naturally empty for points and lines
// (and can be empty for polygons too), and an empty covering is not useful
// for constraining index lookups. We observe that leaf cells that intersect
// shape s can be used in the covering, since the covering of shape x must
// also cover these cells. This allows us to compute non-empty coverings for
// all shapes. Since this is not technically the interior covering, we use the
// term "inner covering".
func innerCovering(rc *s2.RegionCoverer, regions []s2.Region) s2.CellUnion {
	var u s2.CellUnion
	for _, r := range regions {
		switch region := r.(type) {
		case s2.Point:
			cellID := cellIDCoveringPoint(region, rc.MaxLevel)
			u = append(u, cellID)
		case *s2.Polyline:
			// TODO(sumeer): for long lines could also pick some intermediate
			// points along the line. Decide based on experiments.
			for _, p := range *region {
				cellID := cellIDCoveringPoint(p, rc.MaxLevel)
				u = append(u, cellID)
			}
		case *s2.Polygon:
			// Iterate over all exterior points
			if region.NumLoops() > 0 {
				loop := region.Loop(0)
				for _, p := range loop.Vertices() {
					cellID := cellIDCoveringPoint(p, rc.MaxLevel)
					u = append(u, cellID)
				}
				// Arbitrary threshold value. This is to avoid computing an expensive
				// region covering for regions with small area.
				// TODO(sumeer): Improve this heuristic:
				// - Area() may be expensive.
				// - For large area regions, put an upper bound on the
				//   level used for cells.
				// Decide based on experiments.
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

func cellIDCoveringPoint(point s2.Point, level int) s2.CellID {
	cellID := s2.CellFromPoint(point).ID()
	if !cellID.IsLeaf() {
		panic("bug in S2")
	}
	return cellID.Parent(level)
}

// ancestorCells returns the set of cells containing these cells, not
// including the given cells.
//
// TODO(sumeer): use the MinLevel and LevelMod of the RegionCoverer used
// for the index to constrain the ancestors set.
func ancestorCells(cells []s2.CellID) []s2.CellID {
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
				seen[p] = struct{}{}
			}
			ancestors = append(ancestors, p)
		}
	}
	return ancestors
}

// Helper for InvertedIndexKeys.
func invertedIndexKeys(_ context.Context, rc covererInterface, r []s2.Region) []Key {
	covering := rc.covering(r)
	keys := make([]Key, len(covering))
	for i, cid := range covering {
		keys[i] = Key(cid)
	}
	return keys
}

// TODO(sumeer): examine RegionCoverer carefully to see if we can strengthen
// the covering invariant, which would increase the efficiency of covers() and
// remove the need for TestingInnerCovering().
//
// Helper for Covers.
func covers(c context.Context, rc covererInterface, r []s2.Region) UnionKeySpans {
	// We use intersects since geometries covered by r may have been indexed
	// using cells that are ancestors of the covering of r. We could avoid
	// reading ancestors if we had a stronger covering invariant, such as by
	// indexing inner coverings.
	return intersects(c, rc, r)
}

// Helper for Intersects. Returns spans in sorted order for convenience of
// scans.
func intersects(_ context.Context, rc covererInterface, r []s2.Region) UnionKeySpans {
	covering := rc.covering(r)
	return intersectsUsingCovering(covering)
}

func intersectsUsingCovering(covering s2.CellUnion) UnionKeySpans {
	querySpans := make([]KeySpan, len(covering))
	for i, cid := range covering {
		querySpans[i] = KeySpan{Start: Key(cid.RangeMin()), End: Key(cid.RangeMax())}
	}
	for _, cid := range ancestorCells(covering) {
		querySpans = append(querySpans, KeySpan{Start: Key(cid), End: Key(cid)})
	}
	sort.Slice(querySpans, func(i, j int) bool { return querySpans[i].Start < querySpans[j].Start })
	return querySpans
}

// Helper for CoveredBy.
func coveredBy(_ context.Context, rc *s2.RegionCoverer, r []s2.Region) RPKeyExpr {
	covering := innerCovering(rc, r)
	ancestors := ancestorCells(covering)

	// The covering is normalized so no 2 cells are such that one is an ancestor
	// of another. Arrange these cells and their ancestors in a quad-tree. Any cell
	// with more than one child needs to be unioned with the intersection of the
	// expressions corresponding to each child. See the detailed comment in
	// generateRPExprForTree().

	// It is sufficient to represent the tree(s) using presentCells since the ids
	// of all possible 4 children of a cell can be computed and checked for
	// presence in the map.
	presentCells := make(map[s2.CellID]struct{}, len(covering)+len(ancestors))
	for _, c := range covering {
		presentCells[c] = struct{}{}
	}
	for _, c := range ancestors {
		presentCells[c] = struct{}{}
	}

	// Construct the reverse polish expression. Note that there are up to 6
	// trees corresponding to the 6 faces in S2. The expressions for the
	// trees need to be intersected with each other.
	expr := make([]RPExprElement, 0, len(presentCells)*2)
	numFaces := 0
	for face := 0; face < 6; face++ {
		rootID := s2.CellIDFromFace(face)
		if _, ok := presentCells[rootID]; !ok {
			continue
		}
		expr = generateRPExprForTree(rootID, presentCells, expr)
		numFaces++
		if numFaces > 1 {
			expr = append(expr, RPSetIntersection)
		}
	}
	return expr
}

// The quad-trees stored in presentCells together represent a set expression.
// This expression specifies:
// - the path for each leaf to the root of that quad-tree. The index entries
//   on each such path represent the shapes that cover that leaf. Hence these
//   index entries for a single path need to be unioned to give the shapes
//   that cover the leaf.
// - The full expression specifies the shapes that cover all the leaves, so
//   the union expressions for the paths must be intersected with each other.
//
// Reusing an example from earlier in this file, say the quad-tree is:
//                      c0
//                      |
//                      c3
//                      |
//                  +---+---+
//                  |       |
//                 c13      c15
//                  |       |
//                 c53   +--+--+
//                       |     |
//                      c61    c64
//
// This tree represents the following expression (where I(c) are the index
// entries stored at cell c):
//  (I(c64) \union I(c15) \union I(c3) \union I(c0)) \intersection
//  (I(c61) \union I(c15) \union I(c3) \union I(c0)) \intersection
//  (I(c53) \union I(c13) \union I(c3) \union I(c0))
// In this example all the union sub-expressions have the same number of terms
// but that does not need to be true.
//
// The above expression can be factored to eliminate repetition of the
// same cell. The factored expression for this example is:
//   I(c0) \union I(c3) \union
//    ((I(c13) \union I(c53)) \intersection
//     (I(c15) \union (I(c61) \intersection I(c64)))
//
// This function generates this factored expression represented in reverse
// polish notation.
//
// One can generate the factored expression in reverse polish notation using
// a post-order traversal of this tree:
// Step A. append the expression for the subtree rooted at c3
// Step B. append c0 and the union operator
// For Step A:
// - append the expression for the subtree rooted at c13
// - append the expression for the subtree rooted at c15
// - append the intersection operator
// - append c13
// - append the union operator
func generateRPExprForTree(
	rootID s2.CellID, presentCells map[s2.CellID]struct{}, expr []RPExprElement,
) []RPExprElement {
	expr = append(expr, Key(rootID))
	if rootID.IsLeaf() {
		return expr
	}
	numChildren := 0
	for _, childCellID := range rootID.Children() {
		if _, ok := presentCells[childCellID]; !ok {
			continue
		}
		expr = generateRPExprForTree(childCellID, presentCells, expr)
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

// stringBuilderWithWrap is a strings.Builder that approximately wraps at a
// certain number of characters. Newline characters should only be
// written using tryWrap and doWrap.
type stringBuilderWithWrap struct {
	*strings.Builder
	wrap     int
	lastWrap int
}

func newStringBuilderWithWrap(b *strings.Builder, wrap int) *stringBuilderWithWrap {
	return &stringBuilderWithWrap{
		Builder:  b,
		wrap:     wrap,
		lastWrap: b.Len(),
	}
}

func (b *stringBuilderWithWrap) tryWrap() {
	if b.Len()-b.lastWrap > b.wrap {
		b.doWrap()
	}
}

func (b *stringBuilderWithWrap) doWrap() {
	fmt.Fprintln(b)
	b.lastWrap = b.Len()
}

// DefaultS2Config returns the default S2Config to initialize.
func DefaultS2Config() *S2Config {
	return &S2Config{
		MinLevel: 0,
		MaxLevel: 30,
		LevelMod: 1,
		MaxCells: 4,
	}
}

func makeGeomTFromKeys(
	keys []Key, srid geopb.SRID, xyFromS2Point func(s2.Point) (float64, float64),
) (geom.T, error) {
	t := geom.NewMultiPolygon(geom.XY).SetSRID(int(srid))
	for _, key := range keys {
		cell := s2.CellFromCellID(key.S2CellID())
		flatCoords := make([]float64, 0, 10)
		for i := 0; i < 4; i++ {
			x, y := xyFromS2Point(cell.Vertex(i))
			flatCoords = append(flatCoords, x, y)
		}
		// The last point is the same as the first point.
		flatCoords = append(flatCoords, flatCoords[0:2]...)
		err := t.Push(geom.NewPolygonFlat(geom.XY, flatCoords, []int{10}))
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}
