package geom

// A MultiLineString is a collection of LineStrings.
type MultiLineString struct {
	geom2
}

// NewMultiLineString returns a new MultiLineString with no LineStrings.
func NewMultiLineString(layout Layout) *MultiLineString {
	return NewMultiLineStringFlat(layout, nil, nil)
}

// NewMultiLineStringFlat returns a new MultiLineString with the given flat coordinates.
func NewMultiLineStringFlat(layout Layout, flatCoords []float64, ends []int) *MultiLineString {
	g := new(MultiLineString)
	g.layout = layout
	g.stride = layout.Stride()
	g.flatCoords = flatCoords
	g.ends = ends
	return g
}

// Area returns the area of g, i.e. 0.
func (g *MultiLineString) Area() float64 {
	return 0
}

// Clone returns a deep copy.
func (g *MultiLineString) Clone() *MultiLineString {
	return deriveCloneMultiLineString(g)
}

// Length returns the sum of the length of the LineStrings.
func (g *MultiLineString) Length() float64 {
	return length2(g.flatCoords, 0, g.ends, g.stride)
}

// LineString returns the ith LineString.
func (g *MultiLineString) LineString(i int) *LineString {
	offset := 0
	if i > 0 {
		offset = g.ends[i-1]
	}
	if offset == g.ends[i] {
		return NewLineString(g.layout)
	}
	return NewLineStringFlat(g.layout, g.flatCoords[offset:g.ends[i]])
}

// MustSetCoords sets the coordinates and panics on any error.
func (g *MultiLineString) MustSetCoords(coords [][]Coord) *MultiLineString {
	Must(g.SetCoords(coords))
	return g
}

// NumLineStrings returns the number of LineStrings.
func (g *MultiLineString) NumLineStrings() int {
	return len(g.ends)
}

// Push appends a LineString.
func (g *MultiLineString) Push(ls *LineString) error {
	if ls.layout != g.layout {
		return ErrLayoutMismatch{Got: ls.layout, Want: g.layout}
	}
	g.flatCoords = append(g.flatCoords, ls.flatCoords...)
	g.ends = append(g.ends, len(g.flatCoords))
	return nil
}

// SetCoords sets the coordinates.
func (g *MultiLineString) SetCoords(coords [][]Coord) (*MultiLineString, error) {
	if err := g.setCoords(coords); err != nil {
		return nil, err
	}
	return g, nil
}

// SetSRID sets the SRID of g.
func (g *MultiLineString) SetSRID(srid int) *MultiLineString {
	g.srid = srid
	return g
}

// Swap swaps the values of g and g2.
func (g *MultiLineString) Swap(g2 *MultiLineString) {
	*g, *g2 = *g2, *g
}
