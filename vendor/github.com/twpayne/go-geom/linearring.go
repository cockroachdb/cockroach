package geom

// A LinearRing is a linear ring.
type LinearRing struct {
	geom1
}

// NewLinearRing returns a new LinearRing with no coordinates.
func NewLinearRing(layout Layout) *LinearRing {
	return NewLinearRingFlat(layout, nil)
}

// NewLinearRingFlat returns a new LinearRing with the given flat coordinates.
func NewLinearRingFlat(layout Layout, flatCoords []float64) *LinearRing {
	g := new(LinearRing)
	g.layout = layout
	g.stride = layout.Stride()
	g.flatCoords = flatCoords
	return g
}

// Area returns the the area.
func (g *LinearRing) Area() float64 {
	return doubleArea1(g.flatCoords, 0, len(g.flatCoords), g.stride) / 2
}

// Clone returns a deep copy.
func (g *LinearRing) Clone() *LinearRing {
	return deriveCloneLinearRing(g)
}

// Length returns the length of the perimeter.
func (g *LinearRing) Length() float64 {
	return length1(g.flatCoords, 0, len(g.flatCoords), g.stride)
}

// MustSetCoords sets the coordinates and panics if there is any error.
func (g *LinearRing) MustSetCoords(coords []Coord) *LinearRing {
	Must(g.SetCoords(coords))
	return g
}

// SetCoords sets the coordinates.
func (g *LinearRing) SetCoords(coords []Coord) (*LinearRing, error) {
	if err := g.setCoords(coords); err != nil {
		return nil, err
	}
	return g, nil
}

// SetSRID sets the SRID of g.
func (g *LinearRing) SetSRID(srid int) *LinearRing {
	g.srid = srid
	return g
}

// Swap swaps the values of g and g2.
func (g *LinearRing) Swap(g2 *LinearRing) {
	*g, *g2 = *g2, *g
}
