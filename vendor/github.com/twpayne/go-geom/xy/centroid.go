package xy

import (
	"fmt"

	"github.com/twpayne/go-geom"
)

// Centroid calculates the centroid of the geometry.  The centroid may be outside of the geometry depending
// on the topology of the geometry
func Centroid(geometry geom.T) (centroid geom.Coord, err error) {
	switch t := geometry.(type) {
	case *geom.Point:
		centroid = PointsCentroid(t)
	case *geom.MultiPoint:
		centroid = MultiPointCentroid(t)
	case *geom.LineString:
		centroid = LinesCentroid(t)
	case *geom.LinearRing:
		centroid = LinearRingsCentroid(t)
	case *geom.MultiLineString:
		centroid = MultiLineCentroid(t)
	case *geom.Polygon:
		centroid = PolygonsCentroid(t)
	case *geom.MultiPolygon:
		centroid = MultiPolygonCentroid(t)
	default:
		err = fmt.Errorf("%v is not a supported type for centroid calculation", t)
	}

	return centroid, err
}
