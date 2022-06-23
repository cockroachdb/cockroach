package xy

import (
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/internal"
)

// LinesCentroid computes the centroid of all the LineStrings provided as arguments.
//
// Algorithm: Compute the average of the midpoints of all line segments weighted by the segment length.
func LinesCentroid(line *geom.LineString, extraLines ...*geom.LineString) (centroid geom.Coord) {
	calculator := NewLineCentroidCalculator(line.Layout())
	calculator.AddLine(line)

	for _, l := range extraLines {
		calculator.AddLine(l)
	}

	return calculator.GetCentroid()
}

// LinearRingsCentroid computes the centroid of all the LinearRings provided as arguments.
//
// Algorithm: Compute the average of the midpoints of all line segments weighted by the segment length.
func LinearRingsCentroid(line *geom.LinearRing, extraLines ...*geom.LinearRing) (centroid geom.Coord) {
	calculator := NewLineCentroidCalculator(line.Layout())
	calculator.AddLinearRing(line)

	for _, l := range extraLines {
		calculator.AddLinearRing(l)
	}

	return calculator.GetCentroid()
}

// MultiLineCentroid computes the centroid of the MultiLineString string
//
// Algorithm: Compute the average of the midpoints of all line segments weighted by the segment length.
func MultiLineCentroid(line *geom.MultiLineString) (centroid geom.Coord) {
	calculator := NewLineCentroidCalculator(line.Layout())
	start := 0
	for _, end := range line.Ends() {
		calculator.addLine(line.FlatCoords(), start, end)
		start = end
	}

	return calculator.GetCentroid()
}

// LineCentroidCalculator is the data structure that contains the centroid calculation
// data.  This type cannot be used using its 0 values, it must be created
// using NewLineCentroid
type LineCentroidCalculator struct {
	layout      geom.Layout
	stride      int
	centSum     geom.Coord
	totalLength float64
}

// NewLineCentroidCalculator creates a new instance of the calculator.
// Once a calculator is created polygons, linestrings or linear rings can be added and the
// GetCentroid method can be used at any point to get the current centroid
// the centroid will naturally change each time a geometry is added
func NewLineCentroidCalculator(layout geom.Layout) *LineCentroidCalculator {
	return &LineCentroidCalculator{
		layout:  layout,
		stride:  layout.Stride(),
		centSum: geom.Coord(make([]float64, layout.Stride())),
	}
}

// GetCentroid obtains centroid currently calculated.  Returns a 0 coord if no geometries have been added
func (calc *LineCentroidCalculator) GetCentroid() geom.Coord {
	cent := geom.Coord(make([]float64, calc.layout.Stride()))
	cent[0] = calc.centSum[0] / calc.totalLength
	cent[1] = calc.centSum[1] / calc.totalLength
	return cent
}

// AddPolygon adds a Polygon to the calculation.
func (calc *LineCentroidCalculator) AddPolygon(polygon *geom.Polygon) *LineCentroidCalculator {
	for i := 0; i < polygon.NumLinearRings(); i++ {
		calc.AddLinearRing(polygon.LinearRing(i))
	}

	return calc
}

// AddLine adds a LineString to the current calculation
func (calc *LineCentroidCalculator) AddLine(line *geom.LineString) *LineCentroidCalculator {
	coords := line.FlatCoords()
	calc.addLine(coords, 0, len(coords))
	return calc
}

// AddLinearRing adds a LinearRing to the current calculation
func (calc *LineCentroidCalculator) AddLinearRing(line *geom.LinearRing) *LineCentroidCalculator {
	coords := line.FlatCoords()
	calc.addLine(coords, 0, len(coords))
	return calc
}

func (calc *LineCentroidCalculator) addLine(line []float64, startLine, endLine int) {
	lineMinusLastPoint := endLine - calc.stride
	for i := startLine; i < lineMinusLastPoint; i += calc.stride {
		segmentLen := internal.Distance2D(geom.Coord(line[i:i+2]), geom.Coord(line[i+calc.stride:i+calc.stride+2]))
		calc.totalLength += segmentLen

		midx := (line[i] + line[i+calc.stride]) / 2
		calc.centSum[0] += float64(segmentLen * midx) // nolint:unconvert
		midy := (line[i+1] + line[i+calc.stride+1]) / 2
		calc.centSum[1] += float64(segmentLen * midy) // nolint:unconvert
	}
}
