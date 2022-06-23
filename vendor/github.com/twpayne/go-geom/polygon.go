package geom

// A Polygon represents a polygon as a collection of LinearRings. The first
// LinearRing is the outer boundary. Subsequent LinearRings are inner
// boundaries (holes).
type Polygon struct {
	geom2
}

// NewPolygon returns a new, empty, Polygon.
func NewPolygon(layout Layout) *Polygon {
	return NewPolygonFlat(layout, nil, nil)
}

// NewPolygonFlat returns a new Polygon with the given flat coordinates.
func NewPolygonFlat(layout Layout, flatCoords []float64, ends []int) *Polygon {
	g := new(Polygon)
	g.layout = layout
	g.stride = layout.Stride()
	g.flatCoords = flatCoords
	g.ends = ends
	return g
}

// Area returns the area.
func (g *Polygon) Area() float64 {
	return doubleArea2(g.flatCoords, 0, g.ends, g.stride) / 2
}

// Clone returns a deep copy.
func (g *Polygon) Clone() *Polygon {
	return deriveClonePolygon(g)
}

// Length returns the perimter.
func (g *Polygon) Length() float64 {
	return length2(g.flatCoords, 0, g.ends, g.stride)
}

// LinearRing returns the ith LinearRing.
func (g *Polygon) LinearRing(i int) *LinearRing {
	offset := 0
	if i > 0 {
		offset = g.ends[i-1]
	}
	return NewLinearRingFlat(g.layout, g.flatCoords[offset:g.ends[i]])
}

// MustSetCoords sets the coordinates and panics on any error.
func (g *Polygon) MustSetCoords(coords [][]Coord) *Polygon {
	Must(g.SetCoords(coords))
	return g
}

// NumLinearRings returns the number of LinearRings.
func (g *Polygon) NumLinearRings() int {
	return len(g.ends)
}

// Push appends a LinearRing.
func (g *Polygon) Push(lr *LinearRing) error {
	if lr.layout != g.layout {
		return ErrLayoutMismatch{Got: lr.layout, Want: g.layout}
	}
	g.flatCoords = append(g.flatCoords, lr.flatCoords...)
	g.ends = append(g.ends, len(g.flatCoords))
	return nil
}

// SetCoords sets the coordinates.
func (g *Polygon) SetCoords(coords [][]Coord) (*Polygon, error) {
	if err := g.setCoords(coords); err != nil {
		return nil, err
	}
	return g, nil
}

// SetSRID sets the SRID of g.
func (g *Polygon) SetSRID(srid int) *Polygon {
	g.srid = srid
	return g
}

// Swap swaps the values of g and g2.
func (g *Polygon) Swap(g2 *Polygon) {
	*g, *g2 = *g2, *g
}
