package geom

// A MultiPolygon is a collection of Polygons.
type MultiPolygon struct {
	geom3
}

// NewMultiPolygon returns a new MultiPolygon with no Polygons.
func NewMultiPolygon(layout Layout) *MultiPolygon {
	return NewMultiPolygonFlat(layout, nil, nil)
}

// NewMultiPolygonFlat returns a new MultiPolygon with the given flat coordinates.
func NewMultiPolygonFlat(layout Layout, flatCoords []float64, endss [][]int) *MultiPolygon {
	g := new(MultiPolygon)
	g.layout = layout
	g.stride = layout.Stride()
	g.flatCoords = flatCoords
	g.endss = endss
	return g
}

// Area returns the sum of the area of the individual Polygons.
func (g *MultiPolygon) Area() float64 {
	return doubleArea3(g.flatCoords, 0, g.endss, g.stride) / 2
}

// Clone returns a deep copy.
func (g *MultiPolygon) Clone() *MultiPolygon {
	return deriveCloneMultiPolygon(g)
}

// Length returns the sum of the perimeters of the Polygons.
func (g *MultiPolygon) Length() float64 {
	return length3(g.flatCoords, 0, g.endss, g.stride)
}

// MustSetCoords sets the coordinates and panics on any error.
func (g *MultiPolygon) MustSetCoords(coords [][][]Coord) *MultiPolygon {
	Must(g.SetCoords(coords))
	return g
}

// NumPolygons returns the number of Polygons.
func (g *MultiPolygon) NumPolygons() int {
	return len(g.endss)
}

// Polygon returns the ith Polygon.
func (g *MultiPolygon) Polygon(i int) *Polygon {
	if len(g.endss[i]) == 0 {
		return NewPolygon(g.layout)
	}
	// Find the offset from the previous non-empty polygon element.
	offset := 0
	lastNonEmptyIdx := i - 1
	for lastNonEmptyIdx >= 0 {
		ends := g.endss[lastNonEmptyIdx]
		if len(ends) > 0 {
			offset = ends[len(ends)-1]
			break
		}
		lastNonEmptyIdx--
	}
	ends := make([]int, len(g.endss[i]))
	if offset == 0 {
		copy(ends, g.endss[i])
	} else {
		for j, end := range g.endss[i] {
			ends[j] = end - offset
		}
	}
	return NewPolygonFlat(g.layout, g.flatCoords[offset:g.endss[i][len(g.endss[i])-1]], ends)
}

// Push appends a Polygon.
func (g *MultiPolygon) Push(p *Polygon) error {
	if p.layout != g.layout {
		return ErrLayoutMismatch{Got: p.layout, Want: g.layout}
	}
	offset := len(g.flatCoords)
	var ends []int
	if len(p.ends) > 0 {
		ends = make([]int, len(p.ends))
		if offset == 0 {
			copy(ends, p.ends)
		} else {
			for i, end := range p.ends {
				ends[i] = end + offset
			}
		}
	}
	g.flatCoords = append(g.flatCoords, p.flatCoords...)
	g.endss = append(g.endss, ends)
	return nil
}

// SetCoords sets the coordinates.
func (g *MultiPolygon) SetCoords(coords [][][]Coord) (*MultiPolygon, error) {
	if err := g.setCoords(coords); err != nil {
		return nil, err
	}
	return g, nil
}

// SetSRID sets the SRID of g.
func (g *MultiPolygon) SetSRID(srid int) *MultiPolygon {
	g.srid = srid
	return g
}

// Swap swaps the values of g and g2.
func (g *MultiPolygon) Swap(g2 *MultiPolygon) {
	*g, *g2 = *g2, *g
}
