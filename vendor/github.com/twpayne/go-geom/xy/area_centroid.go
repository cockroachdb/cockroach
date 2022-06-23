package xy

import (
	"math"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/internal"
)

// PolygonsCentroid computes the centroid of an area geometry. (Polygon)
//
// Algorithm
// Based on the usual algorithm for calculating the centroid as a weighted sum of the centroids
// of a decomposition of the area into (possibly overlapping) triangles.
//
// The algorithm has been extended to handle holes and multi-polygons.
//
// See http://www.faqs.org/faqs/graphics/algorithms-faq/ for further details of the basic approach.
//
// The code has also be extended to handle degenerate (zero-area) polygons.
//
// In this case, the centroid of the line segments in the polygon will be returned.
func PolygonsCentroid(polygon *geom.Polygon, extraPolys ...*geom.Polygon) (centroid geom.Coord) {
	calc := NewAreaCentroidCalculator(polygon.Layout())
	calc.AddPolygon(polygon)
	for _, p := range extraPolys {
		calc.AddPolygon(p)
	}
	return calc.GetCentroid()
}

// MultiPolygonCentroid computes the centroid of an area geometry. (MultiPolygon)
//
// Algorithm
// Based on the usual algorithm for calculating the centroid as a weighted sum of the centroids
// of a decomposition of the area into (possibly overlapping) triangles.
//
// The algorithm has been extended to handle holes and multi-polygons.
//
// See http://www.faqs.org/faqs/graphics/algorithms-faq/ for further details of the basic approach.
//
// The code has also be extended to handle degenerate (zero-area) polygons.
//
// In this case, the centroid of the line segments in the polygon will be returned.
func MultiPolygonCentroid(polygon *geom.MultiPolygon) (centroid geom.Coord) {
	calc := NewAreaCentroidCalculator(polygon.Layout())
	for i := 0; i < polygon.NumPolygons(); i++ {
		calc.AddPolygon(polygon.Polygon(i))
	}
	return calc.GetCentroid()
}

// AreaCentroidCalculator is the data structure that contains the centroid calculation
// data.  This type cannot be used using its 0 values, it must be created
// using NewAreaCentroid
type AreaCentroidCalculator struct {
	layout        geom.Layout
	stride        int
	basePt        geom.Coord
	triangleCent3 geom.Coord // temporary variable to hold centroid of triangle
	areasum2      float64    // Partial area sum
	cg3           geom.Coord // partial centroid sum

	centSum     geom.Coord // data for linear centroid computation, if needed
	totalLength float64
}

// NewAreaCentroidCalculator creates a new instance of the calculator.
// Once a calculator is created polygons can be added to it and the
// GetCentroid method can be used at any point to get the current centroid
// the centroid will naturally change each time a polygon is added
func NewAreaCentroidCalculator(layout geom.Layout) *AreaCentroidCalculator {
	return &AreaCentroidCalculator{
		layout:        layout,
		stride:        layout.Stride(),
		centSum:       geom.Coord(make([]float64, layout.Stride())),
		triangleCent3: geom.Coord(make([]float64, layout.Stride())),
		cg3:           geom.Coord(make([]float64, layout.Stride())),
	}
}

// GetCentroid obtains centroid currently calculated.  Returns a 0 coord if no geometries have been added
func (calc *AreaCentroidCalculator) GetCentroid() geom.Coord {
	cent := geom.Coord(make([]float64, calc.stride))

	if calc.centSum == nil {
		return cent
	}

	if math.Abs(calc.areasum2) > 0.0 {
		cent[0] = calc.cg3[0] / 3 / calc.areasum2
		cent[1] = calc.cg3[1] / 3 / calc.areasum2
	} else {
		// if polygon was degenerate, compute linear centroid instead
		cent[0] = calc.centSum[0] / calc.totalLength
		cent[1] = calc.centSum[1] / calc.totalLength
	}
	return cent
}

// AddPolygon adds a polygon to the calculation.
func (calc *AreaCentroidCalculator) AddPolygon(polygon *geom.Polygon) {
	calc.setBasePoint(polygon.Coord(0))

	calc.addShell(polygon.LinearRing(0).FlatCoords())
	for i := 1; i < polygon.NumLinearRings(); i++ {
		calc.addHole(polygon.LinearRing(i).FlatCoords())
	}
}

func (calc *AreaCentroidCalculator) setBasePoint(basePt geom.Coord) {
	if calc.basePt == nil {
		calc.basePt = basePt
	}
}

func (calc *AreaCentroidCalculator) addShell(pts []float64) {
	stride := calc.stride

	isPositiveArea := !IsRingCounterClockwise(calc.layout, pts)
	p1 := geom.Coord{0, 0}
	p2 := geom.Coord{0, 0}

	for i := 0; i < len(pts)-stride; i += stride {
		p1[0] = pts[i]
		p1[1] = pts[i+1]
		p2[0] = pts[i+stride]
		p2[1] = pts[i+stride+1]
		calc.addTriangle(calc.basePt, p1, p2, isPositiveArea)
	}
	calc.addLinearSegments(pts)
}

func (calc *AreaCentroidCalculator) addHole(pts []float64) {
	stride := calc.stride

	isPositiveArea := IsRingCounterClockwise(calc.layout, pts)
	p1 := geom.Coord{0, 0}
	p2 := geom.Coord{0, 0}

	for i := 0; i < len(pts)-stride; i += stride {
		p1[0] = pts[i]
		p1[1] = pts[i+1]
		p2[0] = pts[i+stride]
		p2[1] = pts[i+stride+1]
		calc.addTriangle(calc.basePt, p1, p2, isPositiveArea)
	}
	calc.addLinearSegments(pts)
}

func (calc *AreaCentroidCalculator) addTriangle(p0, p1, p2 geom.Coord, isPositiveArea bool) {
	sign := float64(1.0)
	if isPositiveArea {
		sign = -1.0
	}
	centroid3(p0, p1, p2, calc.triangleCent3)
	area2 := area2(p0, p1, p2)
	calc.cg3[0] += float64(sign * area2 * calc.triangleCent3[0]) // nolint:unconvert
	calc.cg3[1] += float64(sign * area2 * calc.triangleCent3[1]) // nolint:unconvert
	calc.areasum2 += float64(sign * area2)                       // nolint:unconvert
}

// Returns three times the centroid of the triangle p1-p2-p3.
// The factor of 3 is left in to permit division to be avoided until later.
func centroid3(p1, p2, p3, c geom.Coord) {
	c[0] = p1[0] + p2[0] + p3[0]
	c[1] = p1[1] + p2[1] + p3[1]
}

// Returns twice the signed area of the triangle p1-p2-p3,
// positive if a,b,c are oriented ccw, and negative if cw.
func area2(p1, p2, p3 geom.Coord) float64 {
	return float64((p2[0]-p1[0])*(p3[1]-p1[1])) - float64((p3[0]-p1[0])*(p2[1]-p1[1])) // nolint:unconvert
}

// Adds the linear segments defined by an array of coordinates
// to the linear centroid accumulators.
// This is done in case the polygon(s) have zero-area,
// in which case the linear centroid is computed instead.
//
// Param pts - an array of Coords
func (calc *AreaCentroidCalculator) addLinearSegments(pts []float64) {
	stride := calc.stride
	for i := 0; i < len(pts)-stride; i += stride {
		segmentLen := internal.Distance2D(geom.Coord(pts[i:i+2]), pts[i+stride:i+stride+2])
		calc.totalLength += segmentLen

		midx := (pts[i] + pts[i+stride]) / 2
		calc.centSum[0] += float64(segmentLen * midx) // nolint:unconvert
		midy := (pts[i+1] + pts[i+stride+1]) / 2
		calc.centSum[1] += float64(segmentLen * midy) // nolint:unconvert
	}
}
