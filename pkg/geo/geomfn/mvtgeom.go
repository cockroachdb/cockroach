// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geomfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// AsMVTGeometry returns a geometry converted into Mapbox Vector Tile coordinate
// space.
func AsMVTGeometry(
	g geo.Geometry, bounds geo.CartesianBoundingBox, extent int, buffer int, clipGeometry bool,
) (geo.Geometry, error) {
	bbox := bounds.BoundingBox
	err := validateInputParams(bbox, extent, buffer)
	if err != nil {
		return geo.Geometry{}, err
	}
	gt, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to convert geometry to geom.T")
	}
	if geometrySmallerThanHalfBoundingBox(gt, bbox, extent) {
		return geo.Geometry{}, nil
	}

	return transformToMVTGeom(gt, g, bbox, extent, buffer, clipGeometry)
}

func validateInputParams(bbox geopb.BoundingBox, extent int, buffer int) error {
	if bbox.HiX-bbox.LoX <= 0 || bbox.HiY-bbox.LoY <= 0 {
		return pgerror.New(pgcode.InvalidParameterValue, "geometric bounds are too small")
	}
	if extent <= 0 {
		return pgerror.New(pgcode.InvalidParameterValue, "extent must be greater than 0")
	}
	if buffer < 0 {
		return pgerror.New(pgcode.InvalidParameterValue, "buffer value cannot be negative")
	}
	return nil
}

func geometrySmallerThanHalfBoundingBox(gt geom.T, bbox geopb.BoundingBox, extent int) bool {
	switch gt := gt.(type) {
	// Don't check GeometryCollection so that we don't discard a collection of points.
	case *geom.LineString, *geom.MultiLineString, *geom.Polygon, *geom.MultiPolygon:
		geometryBoundingBox := gt.Bounds()
		geomWidth := geometryBoundingBox.Max(0) - geometryBoundingBox.Min(0)
		geomHeight := geometryBoundingBox.Max(1) - geometryBoundingBox.Min(1)
		// We use half of the square height and width as limit instead of area so it works properly with lines.
		halfBoundsWidth := ((bbox.HiX - bbox.LoX) / float64(extent)) / 2
		halfBoundsHeight := ((bbox.HiY - bbox.LoY) / float64(extent)) / 2
		if geomWidth < halfBoundsWidth && geomHeight < halfBoundsHeight {
			return true
		}
	}
	return false
}

// transformToMVTGeom transforms a geometry into vector tile coordinate space.
func transformToMVTGeom(
	gt geom.T, g geo.Geometry, bbox geopb.BoundingBox, extent int, buffer int, clipGeometry bool,
) (geo.Geometry, error) {
	basicType, err := getBasicType(gt)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to get geom.T basic type")
	}
	basicTypeGeometry, err := convertToBasicType(g, basicType)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to convert geometry to basic type")
	}
	if basicTypeGeometry.Empty() {
		return geo.Geometry{}, nil
	}

	removeRepeatedPointsGeometry, err := RemoveRepeatedPoints(basicTypeGeometry, 0)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to remove repeated points")
	}
	// Remove points on straight lines
	simplifiedGeometry, empty, err := Simplify(removeRepeatedPointsGeometry, 0, false)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to simplify geometry")
	}
	if empty {
		return geo.Geometry{}, nil
	}

	affinedGeometry, err := transformToTileCoordinateSpace(simplifiedGeometry, bbox, extent)
	if err != nil {
		return geo.Geometry{}, err
	}

	snappedGeometry, err := snapToIntegersGrid(affinedGeometry)
	if err != nil {
		return geo.Geometry{}, err
	}

	out, err := clipAndValidateMVTOutput(snappedGeometry, basicType, extent, buffer, clipGeometry)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to clip and validate")
	}
	if out.Empty() {
		return geo.Geometry{}, nil
	}
	return out, nil
}

func transformToTileCoordinateSpace(
	g geo.Geometry, bbox geopb.BoundingBox, extent int,
) (geo.Geometry, error) {
	width := bbox.HiX - bbox.LoX
	height := bbox.HiY - bbox.LoY
	fx := float64(extent) / width
	fy := -float64(extent) / height
	affineMatrix := AffineMatrix{
		{fx, 0, 0, -bbox.LoX * fx},
		{0, fy, 0, -bbox.HiY * fy},
		{0, 0, 1, 0},
		{0, 0, 0, 1},
	}
	affinedGeometry, err := Affine(g, affineMatrix)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to affine geometry")
	}
	return affinedGeometry, nil
}

// clipAndValidateMVTOutput prepares the geometry so that it's clipped (if needed), valid, snapped to grid and it's basic type is the same as the input geometry.
func clipAndValidateMVTOutput(
	g geo.Geometry, inputBasicType geopb.ShapeType, extent int, buffer int, clipGeometry bool,
) (geo.Geometry, error) {
	var err error
	if clipGeometry && !g.Empty() {
		bbox := *g.CartesianBoundingBox()
		bbox.HiX = overwriteMinusZero(float64(extent) + float64(buffer))
		bbox.HiY = overwriteMinusZero(float64(extent) + float64(buffer))
		bbox.LoX = overwriteMinusZero(-float64(buffer))
		bbox.LoY = overwriteMinusZero(-float64(buffer))
		g, err = ClipByRect(g, bbox)
		if err != nil {
			return geo.Geometry{}, errors.Wrap(err, "failed to clip geometry")
		}
	}
	g, err = snapToGridAndValidate(g, inputBasicType)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to grid and validate")
	}
	outputGt, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to convert geometry into geom.T")
	}
	outputBasicType, err := getBasicType(outputGt)
	if err != nil {
		return geo.Geometry{}, err
	}
	if inputBasicType != outputBasicType {
		return geo.Geometry{}, errors.New("geometry dropped because of output type change")
	}
	return g, nil
}

func snapToGridAndValidate(g geo.Geometry, basicType geopb.ShapeType) (geo.Geometry, error) {
	var err error
	g, err = convertToBasicType(g, basicType)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to convert to basic type")
	}
	if basicType != geopb.ShapeType_Polygon {
		snappedGeometry, err := snapToIntegersGrid(g)
		if err != nil {
			return geo.Geometry{}, err
		}
		return snappedGeometry, nil
	}
	return snapToGridAndValidateBasicPolygon(g, basicType)
}

// snapToGridAndValidateBasicPolygon snaps to the integer grid and force validation.
// Since snapping to the grid can create an invalid geometry and making it valid can create float values,
// we iterate up to 3 times to generate a valid geometry with integer coordinates.
func snapToGridAndValidateBasicPolygon(
	g geo.Geometry, basicType geopb.ShapeType,
) (geo.Geometry, error) {
	iterations := 0
	const maxIterations = 3
	valid := false

	var err error
	g, err = snapToIntegersGrid(g)
	if err != nil {
		return geo.Geometry{}, err
	}
	valid, err = IsValid(g)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to check validity")
	}
	for !valid && iterations < maxIterations {
		g, err = MakeValid(g)
		if err != nil {
			return geo.Geometry{}, errors.Wrap(err, "failed to make valid")
		}

		g, err = snapToIntegersGrid(g)
		if err != nil {
			return geo.Geometry{}, err
		}
		converted, err := convertToBasicType(g, basicType)
		if err != nil {
			return geo.Geometry{}, errors.Wrap(err, "failed to convert to basic type")
		}
		g = converted
		valid, err = IsValid(g)
		if err != nil {
			return geo.Geometry{}, errors.Wrap(err, "failed to check validity")
		}
		iterations++
	}
	if !valid {
		return geo.Geometry{}, nil
	}

	gt, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to convert to geom.T")
	}
	err = forcePolygonOrientation(gt, OrientationCCW)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to force polygon orientation")
	}
	g, err = geo.MakeGeometryFromGeomT(gt)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to make geometry from geom.T")
	}
	return g, nil
}

func snapToIntegersGrid(g geo.Geometry) (geo.Geometry, error) {
	offset := geom.Coord{0, 0, 0, 0}
	grid := geom.Coord{1, 1, 0, 0}
	g, err := SnapToGrid(g, offset, grid)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to snap to grid")
	}
	return g, nil
}

func overwriteMinusZero(val float64) float64 {
	if val == -0 {
		return 0
	}
	return val
}

// For a given geometry, look for the highest dimensional basic type: point, line or polygon.
func getBasicType(gt geom.T) (geopb.ShapeType, error) {
	switch gt := gt.(type) {
	case *geom.Point:
		return geopb.ShapeType_Point, nil
	case *geom.LineString:
		return geopb.ShapeType_LineString, nil
	case *geom.Polygon:
		return geopb.ShapeType_Polygon, nil
	case *geom.MultiPoint:
		return geopb.ShapeType_Point, nil
	case *geom.MultiLineString:
		return geopb.ShapeType_LineString, nil
	case *geom.MultiPolygon:
		return geopb.ShapeType_Polygon, nil
	case *geom.GeometryCollection:
		maxShape := geopb.ShapeType_Unset
		for _, subGt := range gt.Geoms() {
			shapeType, err := getBasicType(subGt)
			if err != nil {
				return geopb.ShapeType_Unset, err
			}
			if shapeType > maxShape {
				maxShape = shapeType
			}
		}
		return maxShape, nil
	default:
		return geopb.ShapeType_Unset, geom.ErrUnsupportedType{Value: gt}
	}
}

func convertToBasicType(g geo.Geometry, basicType geopb.ShapeType) (geo.Geometry, error) {
	var err error
	if g.ShapeType() == geopb.ShapeType_GeometryCollection {
		g, err = CollectionExtract(g, basicType)
		if err != nil {
			return geo.Geometry{}, errors.Wrap(err, "failed to extract collection")
		}
	}
	// If the extracted geometry is a MultiPoint, MultiLineString or MultiPolygon but includes only one sub geometry, we want to return that subgeometry.
	out, err := CollectionHomogenize(g)
	if err != nil {
		return geo.Geometry{}, errors.Wrap(err, "failed to homogenize collection")
	}
	return out, nil
}
