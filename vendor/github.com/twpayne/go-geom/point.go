package geom

import "math"

// PointEmptyCoordHex is the hex representation of a NaN that represents
// an empty coord in a shape.
const PointEmptyCoordHex = 0x7FF8000000000000

// PointEmptyCoord is the NaN float64 representation of the empty coordinate.
func PointEmptyCoord() float64 {
	return math.Float64frombits(PointEmptyCoordHex)
}

// A Point represents a single point.
type Point struct {
	geom0
}

// NewPoint allocates a new Point with layout l and all values zero.
func NewPoint(l Layout) *Point {
	return NewPointFlat(l, make([]float64, l.Stride()))
}

// NewPointEmpty allocates a new Point with no coordinates.
func NewPointEmpty(l Layout) *Point {
	return NewPointFlat(l, nil)
}

// NewPointFlat allocates a new Point with layout l and flat coordinates flatCoords.
func NewPointFlat(l Layout, flatCoords []float64) *Point {
	g := new(Point)
	g.layout = l
	g.stride = l.Stride()
	g.flatCoords = flatCoords
	return g
}

// NewPointFlatMaybeEmpty returns a new point, checking whether the point may be empty
// by checking wther all the points are NaN.
func NewPointFlatMaybeEmpty(layout Layout, flatCoords []float64) *Point {
	isEmpty := true
	for _, coord := range flatCoords {
		if math.Float64bits(coord) != PointEmptyCoordHex {
			isEmpty = false
			break
		}
	}
	if isEmpty {
		return NewPointEmpty(layout)
	}
	return NewPointFlat(layout, flatCoords)
}

// Area returns g's area, i.e. zero.
func (g *Point) Area() float64 {
	return 0
}

// Clone returns a copy of g that does not alias g.
func (g *Point) Clone() *Point {
	return deriveClonePoint(g)
}

// Length returns the length of g, i.e. zero.
func (g *Point) Length() float64 {
	return 0
}

// MustSetCoords is like SetCoords but panics on any error.
func (g *Point) MustSetCoords(coords Coord) *Point {
	Must(g.SetCoords(coords))
	return g
}

// SetCoords sets the coordinates of g.
func (g *Point) SetCoords(coords Coord) (*Point, error) {
	if err := g.setCoords(coords); err != nil {
		return nil, err
	}
	return g, nil
}

// SetSRID sets the SRID of g.
func (g *Point) SetSRID(srid int) *Point {
	g.srid = srid
	return g
}

// Swap swaps the values of g and g2.
func (g *Point) Swap(g2 *Point) {
	*g, *g2 = *g2, *g
}

// X returns g's X-coordinate.
func (g *Point) X() float64 {
	return g.flatCoords[0]
}

// Y returns g's Y-coordinate.
func (g *Point) Y() float64 {
	return g.flatCoords[1]
}

// Z returns g's Z-coordinate, or zero if g has no Z-coordinate.
func (g *Point) Z() float64 {
	zIndex := g.layout.ZIndex()
	if zIndex == -1 {
		return 0
	}
	return g.flatCoords[zIndex]
}

// M returns g's M-coordinate, or zero if g has no M-coordinate.
func (g *Point) M() float64 {
	mIndex := g.layout.MIndex()
	if mIndex == -1 {
		return 0
	}
	return g.flatCoords[mIndex]
}
