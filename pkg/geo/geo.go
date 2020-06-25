// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geo contains the base types for spatial data type operations.
package geo

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

// DefaultEWKBEncodingFormat is the default encoding format for EWKB.
var DefaultEWKBEncodingFormat = binary.LittleEndian

// EmptyBehavior is the behavior to adopt when an empty Geometry is encountered.
type EmptyBehavior uint8

const (
	// EmptyBehaviorError will error with EmptyGeometryError when an empty geometry
	// is encountered.
	EmptyBehaviorError EmptyBehavior = 0
	// EmptyBehaviorOmit will omit an entry when an empty geometry is encountered.
	EmptyBehaviorOmit EmptyBehavior = 1
)

//
// Geospatial Type
//

// GeospatialType are functions that are common between all Geospatial types.
type GeospatialType interface {
	// SRID returns the SRID of the given type.
	SRID() geopb.SRID
	// Shape returns the Shape of the given type.
	ShapeType() geopb.ShapeType
}

var _ GeospatialType = (*Geometry)(nil)
var _ GeospatialType = (*Geography)(nil)

// GeospatialTypeFitsColumnMetadata determines whether a GeospatialType is compatible with the
// given SRID and Shape.
// Returns an error if the types does not fit.
func GeospatialTypeFitsColumnMetadata(
	t GeospatialType, srid geopb.SRID, shapeType geopb.ShapeType,
) error {
	// SRID 0 can take in any SRID. Otherwise SRIDs must match.
	if srid != 0 && t.SRID() != srid {
		return errors.Newf("object SRID %d does not match column SRID %d", t.SRID(), srid)
	}
	// Shape_Geometry/Shape_Unset can take in any kind of shape.
	// Otherwise, shapes must match.
	if shapeType != geopb.ShapeType_Unset && shapeType != geopb.ShapeType_Geometry && shapeType != t.ShapeType() {
		return errors.Newf("object type %s does not match column type %s", t.ShapeType(), shapeType)
	}
	return nil
}

//
// Geometry
//

// Geometry is planar spatial object.
type Geometry struct {
	spatialObject geopb.SpatialObject
}

// NewGeometry returns a new Geometry. Assumes the input EWKB is validated and in little endian.
func NewGeometry(spatialObject geopb.SpatialObject) (*Geometry, error) {
	if spatialObject.SRID != 0 {
		if _, ok := geoprojbase.Projection(spatialObject.SRID); !ok {
			return nil, errors.Newf("unknown SRID for Geometry: %d", spatialObject.SRID)
		}
	}
	return &Geometry{spatialObject: spatialObject}, nil
}

// NewGeometryUnsafe creates a geometry object that assumes spatialObject is from the DB.
// It assumes the spatialObject underneath is safe.
func NewGeometryUnsafe(spatialObject geopb.SpatialObject) *Geometry {
	return &Geometry{spatialObject: spatialObject}
}

// NewGeometryFromPointCoords makes a point from x, y coordinates.
func NewGeometryFromPointCoords(x, y float64) (*Geometry, error) {
	s, err := spatialObjectFromGeomT(geom.NewPointFlat(geom.XY, []float64{x, y}))
	if err != nil {
		return nil, err
	}
	return NewGeometry(s)
}

// NewGeometryFromGeom creates a new Geometry object from a geom.T object.
func NewGeometryFromGeom(g geom.T) (*Geometry, error) {
	spatialObject, err := spatialObjectFromGeomT(g)
	if err != nil {
		return nil, err
	}
	return NewGeometry(spatialObject)
}

// ParseGeometry parses a Geometry from a given text.
func ParseGeometry(str string) (*Geometry, error) {
	spatialObject, err := parseAmbiguousText(str, geopb.DefaultGeometrySRID)
	if err != nil {
		return nil, err
	}
	return NewGeometry(spatialObject)
}

// MustParseGeometry behaves as ParseGeometry, but panics if there is an error.
func MustParseGeometry(str string) *Geometry {
	g, err := ParseGeometry(str)
	if err != nil {
		panic(err)
	}
	return g
}

// ParseGeometryFromEWKT parses the EWKT into a Geometry.
func ParseGeometryFromEWKT(
	ewkt geopb.EWKT, srid geopb.SRID, defaultSRIDOverwriteSetting defaultSRIDOverwriteSetting,
) (*Geometry, error) {
	g, err := parseEWKT(ewkt, srid, defaultSRIDOverwriteSetting)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g)
}

// ParseGeometryFromEWKB parses the EWKB into a Geometry.
func ParseGeometryFromEWKB(ewkb geopb.EWKB) (*Geometry, error) {
	g, err := parseEWKB(ewkb, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g)
}

// ParseGeometryFromEWKBAndSRID parses the EWKB into a given Geometry with the given
// SRID set.
func ParseGeometryFromEWKBAndSRID(ewkb geopb.EWKB, srid geopb.SRID) (*Geometry, error) {
	g, err := parseEWKB(ewkb, srid, DefaultSRIDShouldOverwrite)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g)
}

// MustParseGeometryFromEWKB behaves as ParseGeometryFromEWKB, but panics if an error occurs.
func MustParseGeometryFromEWKB(ewkb geopb.EWKB) *Geometry {
	ret, err := ParseGeometryFromEWKB(ewkb)
	if err != nil {
		panic(err)
	}
	return ret
}

// ParseGeometryFromGeoJSON parses the GeoJSON into a given Geometry.
func ParseGeometryFromGeoJSON(json []byte) (*Geometry, error) {
	g, err := parseGeoJSON(json, geopb.DefaultGeometrySRID)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g)
}

// ParseGeometryFromEWKBUnsafe returns a new Geometry from an EWKB, without any SRID checks.
// You should only do this if you trust the EWKB is setup correctly.
// You most likely want geo.ParseGeometryFromEWKB instead.
func ParseGeometryFromEWKBUnsafe(ewkb geopb.EWKB) (*Geometry, error) {
	base, err := parseEWKBRaw(ewkb)
	if err != nil {
		return nil, err
	}
	return NewGeometryUnsafe(base), nil
}

// AsGeography converts a given Geometry to its Geography form.
func (g *Geometry) AsGeography() (*Geography, error) {
	if g.SRID() != 0 {
		return NewGeography(g.spatialObject)
	}

	spatialObjectObj := protoutil.Clone(&g.spatialObject)
	spatialObject := *(spatialObjectObj.(*geopb.SpatialObject))
	spatialObject.SRID = geopb.DefaultGeographySRID
	return NewGeography(spatialObject)
}

// CloneWithSRID sets a given Geometry's SRID to another, without any transformations.
// Returns a new Geometry object.
func (g *Geometry) CloneWithSRID(srid geopb.SRID) (*Geometry, error) {
	spatialObjectObj := protoutil.Clone(&g.spatialObject)
	spatialObject := *(spatialObjectObj.(*geopb.SpatialObject))
	spatialObject.SRID = srid
	return NewGeometry(spatialObject)
}

// AsGeomT returns the geometry as a geom.T object.
func (g *Geometry) AsGeomT() (geom.T, error) {
	return spatialObjectToGeomT(g.spatialObject)
}

// Empty returns whether the given Geometry is empty.
func (g *Geometry) Empty() bool {
	return g.spatialObject.BoundingBox == nil
}

// EWKB returns the EWKB representation of the Geometry.
func (g *Geometry) EWKB() geopb.EWKB {
	// TODO(otan): clean this up by returning errors.
	t, err := g.AsGeomT()
	if err != nil {
		panic(err)
	}
	ret, err := ewkb.Marshal(t, DefaultEWKBEncodingFormat)
	if err != nil {
		panic(err)
	}
	return geopb.EWKB(ret)
}

// SpatialObject returns the SpatialObject representation of the Geometry.
func (g *Geometry) SpatialObject() geopb.SpatialObject {
	return g.spatialObject
}

// EWKBHex returns the EWKBHex representation of the Geometry.
func (g *Geometry) EWKBHex() string {
	// TODO(otan): clean this up by returning errors.
	t, err := g.AsGeomT()
	if err != nil {
		panic(err)
	}
	ret, err := ewkb.Marshal(t, DefaultEWKBEncodingFormat)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%X", ret)
}

// SRID returns the SRID representation of the Geometry.
func (g *Geometry) SRID() geopb.SRID {
	return g.spatialObject.SRID
}

// ShapeType returns the shape of the Geometry.
func (g *Geometry) ShapeType() geopb.ShapeType {
	switch shape := g.spatialObject.Shape.(type) {
	case *geopb.SpatialObject_GeometryCollectionShape:
		return geopb.ShapeType_GeometryCollection
	case *geopb.SpatialObject_SingleShape:
		return shape.SingleShape.ShapeType
	default:
		return geopb.ShapeType(0)
	}
}

// BoundingBoxIntersects returns whether the bounding box of the given geometry
// intersects with the other.
func (g *Geometry) BoundingBoxIntersects(o *Geometry) bool {
	return g.spatialObject.BoundingBox.Intersects(o.spatialObject.BoundingBox)
}

//
// Geography
//

// Geography is a spherical spatial object.
type Geography struct {
	spatialObject geopb.SpatialObject
}

// NewGeography returns a new Geography. Assumes the input EWKB is validated and in little endian.
func NewGeography(spatialObject geopb.SpatialObject) (*Geography, error) {
	projection, ok := geoprojbase.Projection(spatialObject.SRID)
	if !ok {
		return nil, errors.Newf("unknown SRID for Geography: %d", spatialObject.SRID)
	}
	if !projection.IsLatLng {
		return nil, errors.Newf(
			"SRID %d cannot be used for geography as it is not in a lon/lat coordinate system",
			spatialObject.SRID,
		)
	}
	return &Geography{spatialObject: spatialObject}, nil
}

// NewGeographyUnsafe creates a geometry object that assumes spatialObject is from the DB.
// It assumes the spatialObject underneath is safe.
func NewGeographyUnsafe(spatialObject geopb.SpatialObject) *Geography {
	return &Geography{spatialObject: spatialObject}
}

// NewGeographyFromGeom creates a new Geography from a geom.T object.
func NewGeographyFromGeom(g geom.T) (*Geography, error) {
	spatialObject, err := spatialObjectFromGeomT(g)
	if err != nil {
		return nil, err
	}
	return NewGeography(spatialObject)
}

// MustNewGeographyFromGeom enforces no error from NewGeographyFromGeom.
func MustNewGeographyFromGeom(g geom.T) *Geography {
	ret, err := NewGeographyFromGeom(g)
	if err != nil {
		panic(err)
	}
	return ret
}

// ParseGeography parses a Geography from a given text.
func ParseGeography(str string) (*Geography, error) {
	spatialObject, err := parseAmbiguousText(str, geopb.DefaultGeographySRID)
	if err != nil {
		return nil, err
	}
	return NewGeography(spatialObject)
}

// MustParseGeography behaves as ParseGeography, but panics if there is an error.
func MustParseGeography(str string) *Geography {
	g, err := ParseGeography(str)
	if err != nil {
		panic(err)
	}
	return g
}

// ParseGeographyFromEWKT parses the EWKT into a Geography.
func ParseGeographyFromEWKT(
	ewkt geopb.EWKT, srid geopb.SRID, defaultSRIDOverwriteSetting defaultSRIDOverwriteSetting,
) (*Geography, error) {
	g, err := parseEWKT(ewkt, srid, defaultSRIDOverwriteSetting)
	if err != nil {
		return nil, err
	}
	return NewGeography(g)
}

// ParseGeographyFromEWKB parses the EWKB into a Geography.
func ParseGeographyFromEWKB(ewkb geopb.EWKB) (*Geography, error) {
	g, err := parseEWKB(ewkb, geopb.DefaultGeographySRID, DefaultSRIDIsHint)
	if err != nil {
		return nil, err
	}
	return NewGeography(g)
}

// ParseGeographyFromEWKBAndSRID parses the EWKB into a given Geography with the
// given SRID set.
func ParseGeographyFromEWKBAndSRID(ewkb geopb.EWKB, srid geopb.SRID) (*Geography, error) {
	g, err := parseEWKB(ewkb, srid, DefaultSRIDShouldOverwrite)
	if err != nil {
		return nil, err
	}
	return NewGeography(g)
}

// MustParseGeographyFromEWKB behaves as ParseGeographyFromEWKB, but panics if an error occurs.
func MustParseGeographyFromEWKB(ewkb geopb.EWKB) *Geography {
	ret, err := ParseGeographyFromEWKB(ewkb)
	if err != nil {
		panic(err)
	}
	return ret
}

// ParseGeographyFromGeoJSON parses the GeoJSON into a given Geography.
func ParseGeographyFromGeoJSON(json []byte) (*Geography, error) {
	g, err := parseGeoJSON(json, geopb.DefaultGeographySRID)
	if err != nil {
		return nil, err
	}
	return NewGeography(g)
}

// ParseGeographyFromEWKBUnsafe returns a new Geography from an EWKB, without any SRID checks.
// You should only do this if you trust the EWKB is setup correctly.
// You most likely want ParseGeographyFromEWKB instead.
func ParseGeographyFromEWKBUnsafe(ewkb geopb.EWKB) (*Geography, error) {
	base, err := parseEWKBRaw(ewkb)
	if err != nil {
		return nil, err
	}
	return NewGeographyUnsafe(base), nil
}

// CloneWithSRID sets a given Geography's SRID to another, without any transformations.
// Returns a new Geography object.
func (g *Geography) CloneWithSRID(srid geopb.SRID) (*Geography, error) {
	spatialObjectObj := protoutil.Clone(&g.spatialObject)
	spatialObject := *(spatialObjectObj.(*geopb.SpatialObject))
	spatialObject.SRID = srid
	return NewGeography(spatialObject)
}

// AsGeometry converts a given Geography to its Geometry form.
func (g *Geography) AsGeometry() (*Geometry, error) {
	return NewGeometry(g.spatialObject)
}

// AsGeomT returns the Geography as a geom.T object.
func (g *Geography) AsGeomT() (geom.T, error) {
	return spatialObjectToGeomT(g.spatialObject)
}

// EWKB returns the EWKB representation of the Geography.
func (g *Geography) EWKB() geopb.EWKB {
	// TODO(otan): clean this up by returning errors.
	t, err := g.AsGeomT()
	if err != nil {
		panic(err)
	}
	ret, err := ewkb.Marshal(t, DefaultEWKBEncodingFormat)
	if err != nil {
		panic(err)
	}
	return geopb.EWKB(ret)
}

// SpatialObject returns the SpatialObject representation of the Geography.
func (g *Geography) SpatialObject() geopb.SpatialObject {
	return g.spatialObject
}

// EWKBHex returns the EWKBHex representation of the Geography.
func (g *Geography) EWKBHex() string {
	// TODO(otan): clean this up by returning errors.
	t, err := g.AsGeomT()
	if err != nil {
		panic(err)
	}
	ret, err := ewkb.Marshal(t, DefaultEWKBEncodingFormat)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%X", ret)
}

// SRID returns the SRID representation of the Geography.
func (g *Geography) SRID() geopb.SRID {
	return g.spatialObject.SRID
}

// ShapeType returns the shape of the Geography.
func (g *Geography) ShapeType() geopb.ShapeType {
	switch shape := g.spatialObject.Shape.(type) {
	case *geopb.SpatialObject_GeometryCollectionShape:
		return geopb.ShapeType_GeometryCollection
	case *geopb.SpatialObject_SingleShape:
		return shape.SingleShape.ShapeType
	default:
		return geopb.ShapeType(0)
	}
}

// Spheroid returns the spheroid represented by the given Geography.
func (g *Geography) Spheroid() (*geographiclib.Spheroid, error) {
	proj, ok := geoprojbase.Projection(g.SRID())
	if !ok {
		return nil, errors.Newf("expected spheroid for SRID %d", g.SRID())
	}
	return proj.Spheroid, nil
}

// AsS2 converts a given Geography into it's S2 form.
func (g *Geography) AsS2(emptyBehavior EmptyBehavior) ([]s2.Region, error) {
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return nil, err
	}
	// TODO(otan): convert by reading from SpatialObject to S2 directly.
	return S2RegionsFromGeom(geomRepr, emptyBehavior)
}

// BoundingBoxIntersects returns whether the bounding box of the given geography
// intersects with the other.
func (g *Geography) BoundingBoxIntersects(o *Geography) bool {
	return g.spatialObject.BoundingBox.Intersects(o.spatialObject.BoundingBox)
}

// isLinearRingCCW returns whether a given linear ring is counter clock wise.
// See 2.07 of http://www.faqs.org/faqs/graphics/algorithms-faq/.
// "Find the lowest vertex (or, if  there is more than one vertex with the same lowest coordinate,
//  the rightmost of those vertices) and then take the cross product of the edges fore and aft of it."
func isLinearRingCCW(linearRing *geom.LinearRing) bool {
	smallestIdx := 0
	smallest := linearRing.Coord(0)

	for pointIdx := 1; pointIdx < linearRing.NumCoords()-1; pointIdx++ {
		curr := linearRing.Coord(pointIdx)
		if curr.Y() < smallest.Y() || (curr.Y() == smallest.Y() && curr.X() > smallest.X()) {
			smallestIdx = pointIdx
			smallest = curr
		}
	}

	// prevIdx is the previous point. If we are at the 0th point, the last coordinate
	// is also the 0th point, so take the second last point.
	// Note we don't have to apply this for "nextIdx" as we cap the search above at the
	// second last vertex.
	prevIdx := smallestIdx - 1
	if smallestIdx == 0 {
		prevIdx = linearRing.NumCoords() - 2
	}
	a := linearRing.Coord(prevIdx)
	b := smallest
	c := linearRing.Coord(smallestIdx + 1)

	// We could do the cross product, but we are only interested in the sign.
	// To find the sign, reorganize into the orientation matrix:
	//  1 x_a y_a
	//  1 x_b y_b
	//  1 x_c y_c
	// and find the determinant.
	// https://en.wikipedia.org/wiki/Curve_orientation#Orientation_of_a_simple_polygon
	areaSign := a.X()*b.Y() - a.Y()*b.X() +
		a.Y()*c.X() - a.X()*c.Y() +
		b.X()*c.Y() - c.X()*b.Y()
	// Note having an area sign of 0 means it is a flat polygon, which is invalid.
	return areaSign > 0
}

// S2RegionsFromGeom converts an geom representation of an object
// to s2 regions.
// As S2 does not really handle empty geometries well, we need to ingest emptyBehavior and
// react appropriately.
func S2RegionsFromGeom(geomRepr geom.T, emptyBehavior EmptyBehavior) ([]s2.Region, error) {
	var regions []s2.Region
	if geomRepr.Empty() {
		switch emptyBehavior {
		case EmptyBehaviorOmit:
			return nil, nil
		case EmptyBehaviorError:
			return nil, NewEmptyGeometryError()
		default:
			return nil, errors.Newf("programmer error: unknown behavior")
		}
	}
	switch repr := geomRepr.(type) {
	case *geom.Point:
		regions = []s2.Region{
			s2.PointFromLatLng(s2.LatLngFromDegrees(repr.Y(), repr.X())),
		}
	case *geom.LineString:
		latLngs := make([]s2.LatLng, repr.NumCoords())
		for i := 0; i < repr.NumCoords(); i++ {
			p := repr.Coord(i)
			latLngs[i] = s2.LatLngFromDegrees(p.Y(), p.X())
		}
		regions = []s2.Region{
			s2.PolylineFromLatLngs(latLngs),
		}
	case *geom.Polygon:
		loops := make([]*s2.Loop, repr.NumLinearRings())
		// All loops must be oriented CCW for S2.
		for ringIdx := 0; ringIdx < repr.NumLinearRings(); ringIdx++ {
			linearRing := repr.LinearRing(ringIdx)
			points := make([]s2.Point, linearRing.NumCoords())
			isCCW := isLinearRingCCW(linearRing)
			for pointIdx := 0; pointIdx < linearRing.NumCoords(); pointIdx++ {
				p := linearRing.Coord(pointIdx)
				pt := s2.PointFromLatLng(s2.LatLngFromDegrees(p.Y(), p.X()))
				if isCCW {
					points[pointIdx] = pt
				} else {
					points[len(points)-pointIdx-1] = pt
				}
			}
			loops[ringIdx] = s2.LoopFromPoints(points)
		}
		regions = []s2.Region{
			s2.PolygonFromLoops(loops),
		}
	case *geom.GeometryCollection:
		for _, geom := range repr.Geoms() {
			subRegions, err := S2RegionsFromGeom(geom, emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	case *geom.MultiPoint:
		for i := 0; i < repr.NumPoints(); i++ {
			subRegions, err := S2RegionsFromGeom(repr.Point(i), emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	case *geom.MultiLineString:
		for i := 0; i < repr.NumLineStrings(); i++ {
			subRegions, err := S2RegionsFromGeom(repr.LineString(i), emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	case *geom.MultiPolygon:
		for i := 0; i < repr.NumPolygons(); i++ {
			subRegions, err := S2RegionsFromGeom(repr.Polygon(i), emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	}
	return regions, nil
}

//
// Common
//

// spatialObjectFromGeomT creates a geopb.SpatialObject from a geom.T.
func spatialObjectFromGeomT(t geom.T) (geopb.SpatialObject, error) {
	switch t.Layout() {
	case geom.XY:
	case geom.NoLayout:
		if gc, ok := t.(*geom.GeometryCollection); !ok || !gc.Empty() {
			return geopb.SpatialObject{}, errors.Newf("no layout found on object")
		}
	default:
		return geopb.SpatialObject{}, errors.Newf("only 2D objects are currently supported")
	}
	bbox := boundingBoxFromGeom(t)

	var shape geopb.SpatialObjectShape
	switch t := t.(type) {
	case *geom.GeometryCollection:
		shapes := make([]geopb.Shape, len(t.Geoms()))
		for i, g := range t.Geoms() {
			shapeElem, err := geopbShapeFromGeomT(g)
			if err != nil {
				return geopb.SpatialObject{}, err
			}
			shapes[i] = shapeElem
		}
		shape = geopb.MakeGeometryCollectionSpatialObjectShape(shapes)
	default:
		shapeElem, err := geopbShapeFromGeomT(t)
		if err != nil {
			return geopb.SpatialObject{}, err
		}
		shape = geopb.MakeSingleSpatialObjectShape(shapeElem)
	}
	return geopb.SpatialObject{
		SRID:        geopb.SRID(t.SRID()),
		Shape:       shape,
		BoundingBox: bbox,
	}, nil
}

// spatialObjectToGeomT converts a spatial object to a geom.T.
func spatialObjectToGeomT(so geopb.SpatialObject) (geom.T, error) {
	switch shape := so.Shape.(type) {
	case *geopb.SpatialObject_GeometryCollectionShape:
		ret := geom.NewGeometryCollection().SetSRID(int(so.SRID))
		for _, s := range shape.GeometryCollectionShape.Shapes {
			toPush, err := geopbShapeToGeomT(s, so.SRID)
			if err != nil {
				return nil, err
			}
			if err := ret.Push(toPush); err != nil {
				return nil, err
			}
		}
		return ret, nil
	case *geopb.SpatialObject_SingleShape:
		return geopbShapeToGeomT(*shape.SingleShape, so.SRID)
	default:
		return nil, errors.Newf("unknown shape: %T", shape)
	}
}

// geopbShapeToGeomT converts a geopb.Shape to a geom.T.
func geopbShapeToGeomT(shape geopb.Shape, srid geopb.SRID) (geom.T, error) {
	switch shape.ShapeType {
	case geopb.ShapeType_Point:
		return geom.NewPointFlat(geom.XY, shape.Coords).SetSRID(int(srid)), nil
	case geopb.ShapeType_LineString:
		return geom.NewLineStringFlat(geom.XY, shape.Coords).SetSRID(int(srid)), nil
	case geopb.ShapeType_Polygon:
		return geom.NewPolygonFlat(geom.XY, shape.Coords, int64ArrayToIntArray(shape.Ends)).SetSRID(int(srid)), nil
	case geopb.ShapeType_MultiPoint:
		return geom.NewMultiPointFlat(
			geom.XY,
			shape.Coords,
			geom.NewMultiPointFlatOptionWithEnds(int64ArrayToIntArray(shape.Ends)),
		).SetSRID(int(srid)), nil
	case geopb.ShapeType_MultiLineString:
		return geom.NewMultiLineStringFlat(geom.XY, shape.Coords, int64ArrayToIntArray(shape.Ends)).SetSRID(int(srid)), nil
	case geopb.ShapeType_MultiPolygon:
		endss := make([][]int, len(shape.Endss))
		prevIdx := int64(0)
		for i, endIdx := range shape.Endss {
			endss[i] = int64ArrayToIntArray(shape.Ends[int(prevIdx):int(endIdx)])
			prevIdx = endIdx
		}
		return geom.NewMultiPolygonFlat(geom.XY, shape.Coords, endss).SetSRID(int(srid)), nil
	default:
		return nil, errors.Newf("unknown shape type: %s", shape.ShapeType)
	}
}

// geopbShapeFromGeomT creates a geopb.Shape from a geom object.
func geopbShapeFromGeomT(t geom.T) (geopb.Shape, error) {
	switch t := t.(type) {
	case *geom.Point:
		return geopb.Shape{
			ShapeType: geopb.ShapeType_Point,
			Coords:    t.FlatCoords(),
		}, nil
	case *geom.LineString:
		return geopb.Shape{
			ShapeType: geopb.ShapeType_LineString,
			Coords:    t.FlatCoords(),
		}, nil
	case *geom.Polygon:
		return geopb.Shape{
			ShapeType: geopb.ShapeType_Polygon,
			Coords:    t.FlatCoords(),
			Ends:      intArrayToInt64Array(t.Ends()),
		}, nil
	case *geom.MultiPoint:
		return geopb.Shape{
			ShapeType: geopb.ShapeType_MultiPoint,
			Coords:    t.FlatCoords(),
			Ends:      intArrayToInt64Array(t.Ends()),
		}, nil
	case *geom.MultiLineString:
		return geopb.Shape{
			ShapeType: geopb.ShapeType_MultiLineString,
			Coords:    t.FlatCoords(),
			Ends:      intArrayToInt64Array(t.Ends()),
		}, nil
	case *geom.MultiPolygon:
		ends := []int64{}
		endss := make([]int64, len(t.Endss()))
		for i, endsIn := range t.Endss() {
			ends = append(ends, intArrayToInt64Array(endsIn)...)
			endss[i] = int64(len(ends))
		}
		return geopb.Shape{
			ShapeType: geopb.ShapeType_MultiPolygon,
			Coords:    t.FlatCoords(),
			Ends:      ends,
			Endss:     endss,
		}, nil
	default:
		return geopb.Shape{}, errors.Newf("unknown shape: %T", t)
	}
}

// geopbShapeTypeFromGeomT infers a geopb.ShapeType from a geom object.
func geopbShapeTypeFromGeomT(t geom.T) (geopb.ShapeType, error) {
	switch t := t.(type) {
	case *geom.Point:
		return geopb.ShapeType_Point, nil
	case *geom.LineString:
		return geopb.ShapeType_LineString, nil
	case *geom.Polygon:
		return geopb.ShapeType_Polygon, nil
	case *geom.MultiPoint:
		return geopb.ShapeType_MultiPoint, nil
	case *geom.MultiLineString:
		return geopb.ShapeType_MultiLineString, nil
	case *geom.MultiPolygon:
		return geopb.ShapeType_MultiPolygon, nil
	case *geom.GeometryCollection:
		return geopb.ShapeType_GeometryCollection, nil
	default:
		return geopb.ShapeType_Unset, errors.Newf("unknown shape type: %T", t)
	}
}

// int64ArrayToIntArray converts an int64 array to an int array.
func int64ArrayToIntArray(arr []int64) []int {
	ret := make([]int, len(arr))
	for i := range arr {
		ret[i] = int(arr[i])
	}
	return ret
}

// intArrayToInt64Array converts an int array to an int64 array.
func intArrayToInt64Array(arr []int) []int64 {
	ret := make([]int64, len(arr))
	for i := range arr {
		ret[i] = int64(arr[i])
	}
	return ret
}
