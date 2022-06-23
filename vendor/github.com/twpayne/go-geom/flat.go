package geom

import "math"

type geom0 struct {
	layout     Layout
	stride     int
	flatCoords []float64
	srid       int
}

type geom1 struct {
	geom0
}

type geom2 struct {
	geom1
	ends []int
}

type geom3 struct {
	geom1
	endss [][]int
}

// Bounds returns the bounds of g.
func (g *geom0) Bounds() *Bounds {
	return NewBounds(g.layout).extendFlatCoords(g.flatCoords, 0, len(g.flatCoords), g.stride)
}

// Coords returns all the coordinates in g, i.e. a single coordinate.
func (g *geom0) Coords() Coord {
	return inflate0(g.flatCoords, 0, len(g.flatCoords), g.stride)
}

// Empty returns true if g contains no coordinates.
func (g *geom0) Empty() bool {
	return len(g.flatCoords) == 0
}

// Ends returns the end indexes of sub-structures of g, i.e. an empty slice.
func (g *geom0) Ends() []int {
	return nil
}

// Endss returns the end indexes of sub-sub-structures of g, i.e. an empty
// slice.
func (g *geom0) Endss() [][]int {
	return nil
}

// FlatCoords returns the flat coordinates of g.
func (g *geom0) FlatCoords() []float64 {
	return g.flatCoords
}

// Layout returns g's layout.
func (g *geom0) Layout() Layout {
	return g.layout
}

// NumCoords returns the number of coordinates in g, i.e. 1.
func (g *geom0) NumCoords() int {
	return 1
}

// Reserve reserves space in g for n coordinates.
func (g *geom0) Reserve(n int) {
	if cap(g.flatCoords) < n*g.stride {
		fcs := make([]float64, len(g.flatCoords), n*g.stride)
		copy(fcs, g.flatCoords)
		g.flatCoords = fcs
	}
}

// SRID returns g's SRID.
func (g *geom0) SRID() int {
	return g.srid
}

func (g *geom0) setCoords(coords0 []float64) error {
	var err error
	g.flatCoords, err = deflate0(nil, coords0, g.stride)
	return err
}

// Stride returns g's stride.
func (g *geom0) Stride() int {
	return g.stride
}

func (g *geom0) verify() error {
	if g.stride != g.layout.Stride() {
		return errStrideLayoutMismatch
	}
	if g.stride == 0 {
		if len(g.flatCoords) != 0 {
			return errNonEmptyFlatCoords
		}
		return nil
	}
	if len(g.flatCoords) != g.stride {
		return errLengthStrideMismatch
	}
	return nil
}

// Coord returns the ith coord of g.
func (g *geom1) Coord(i int) Coord {
	return g.flatCoords[i*g.stride : (i+1)*g.stride]
}

// Coords unpacks and returns all of g's coordinates.
func (g *geom1) Coords() []Coord {
	return inflate1(g.flatCoords, 0, len(g.flatCoords), g.stride)
}

// NumCoords returns the number of coordinates in g.
func (g *geom1) NumCoords() int {
	return len(g.flatCoords) / g.stride
}

// Reverse reverses the order of g's coordinates.
func (g *geom1) Reverse() {
	reverse1(g.flatCoords, 0, len(g.flatCoords), g.stride)
}

func (g *geom1) setCoords(coords1 []Coord) error {
	var err error
	g.flatCoords, err = deflate1(nil, coords1, g.stride)
	return err
}

func (g *geom1) verify() error {
	if g.stride != g.layout.Stride() {
		return errStrideLayoutMismatch
	}
	if g.stride == 0 {
		if len(g.flatCoords) != 0 {
			return errNonEmptyFlatCoords
		}
	} else {
		if len(g.flatCoords)%g.stride != 0 {
			return errLengthStrideMismatch
		}
	}
	return nil
}

// Coords returns all of g's coordinates.
func (g *geom2) Coords() [][]Coord {
	return inflate2(g.flatCoords, 0, g.ends, g.stride)
}

// Ends returns the end indexes of all sub-structures in g.
func (g *geom2) Ends() []int {
	return g.ends
}

// Reverse reverses the order of coordinates for each sub-structure in g.
func (g *geom2) Reverse() {
	reverse2(g.flatCoords, 0, g.ends, g.stride)
}

func (g *geom2) setCoords(coords2 [][]Coord) error {
	var err error
	g.flatCoords, g.ends, err = deflate2(nil, nil, coords2, g.stride)
	return err
}

func (g *geom2) verify() error {
	if g.stride != g.layout.Stride() {
		return errStrideLayoutMismatch
	}
	if g.stride == 0 {
		if len(g.flatCoords) != 0 {
			return errNonEmptyFlatCoords
		}
		if len(g.ends) != 0 {
			return errNonEmptyEnds
		}
		return nil
	}
	if len(g.flatCoords)%g.stride != 0 {
		return errLengthStrideMismatch
	}
	offset := 0
	for _, end := range g.ends {
		if end%g.stride != 0 {
			return errMisalignedEnd
		}
		if end < offset {
			return errOutOfOrderEnd
		}
		offset = end
	}
	if offset != len(g.flatCoords) {
		return errIncorrectEnd
	}
	return nil
}

// Coords returns all the coordinates in g.
func (g *geom3) Coords() [][][]Coord {
	return inflate3(g.flatCoords, 0, g.endss, g.stride)
}

// Endss returns a list of all the sub-sub-structures in g.
func (g *geom3) Endss() [][]int {
	return g.endss
}

// Reverse reverses the order of coordinates for each sub-sub-structure in g.
func (g *geom3) Reverse() {
	reverse3(g.flatCoords, 0, g.endss, g.stride)
}

func (g *geom3) setCoords(coords3 [][][]Coord) error {
	var err error
	g.flatCoords, g.endss, err = deflate3(nil, nil, coords3, g.stride)
	return err
}

func (g *geom3) verify() error {
	if g.stride != g.layout.Stride() {
		return errStrideLayoutMismatch
	}
	if g.stride == 0 {
		if len(g.flatCoords) != 0 {
			return errNonEmptyFlatCoords
		}
		if len(g.endss) != 0 {
			return errNonEmptyEndss
		}
		return nil
	}
	if len(g.flatCoords)%g.stride != 0 {
		return errLengthStrideMismatch
	}
	offset := 0
	for _, ends := range g.endss {
		for _, end := range ends {
			if end%g.stride != 0 {
				return errMisalignedEnd
			}
			if end < offset {
				return errOutOfOrderEnd
			}
			offset = end
		}
	}
	if offset != len(g.flatCoords) {
		return errIncorrectEnd
	}
	return nil
}

func doubleArea1(flatCoords []float64, offset, end, stride int) float64 {
	var doubleArea float64
	for i := offset + stride; i < end; i += stride {
		doubleArea += (flatCoords[i+1] - flatCoords[i+1-stride]) * (flatCoords[i] + flatCoords[i-stride])
	}
	return doubleArea
}

func doubleArea2(flatCoords []float64, offset int, ends []int, stride int) float64 {
	var doubleArea float64
	for i, end := range ends {
		da := doubleArea1(flatCoords, offset, end, stride)
		if i == 0 {
			doubleArea = da
		} else {
			doubleArea -= da
		}
		offset = end
	}
	return doubleArea
}

func doubleArea3(flatCoords []float64, offset int, endss [][]int, stride int) float64 {
	var doubleArea float64
	for _, ends := range endss {
		doubleArea += doubleArea2(flatCoords, offset, ends, stride)
		offset = ends[len(ends)-1]
	}
	return doubleArea
}

func deflate0(flatCoords []float64, c Coord, stride int) ([]float64, error) {
	if len(c) != stride {
		return nil, ErrStrideMismatch{Got: len(c), Want: stride}
	}
	flatCoords = append(flatCoords, c...)
	return flatCoords, nil
}

func deflate1(flatCoords []float64, coords1 []Coord, stride int) ([]float64, error) {
	for _, c := range coords1 {
		var err error
		flatCoords, err = deflate0(flatCoords, c, stride)
		if err != nil {
			return nil, err
		}
	}
	return flatCoords, nil
}

func deflate2(
	flatCoords []float64, ends []int, coords2 [][]Coord, stride int,
) ([]float64, []int, error) {
	for _, coords1 := range coords2 {
		var err error
		flatCoords, err = deflate1(flatCoords, coords1, stride)
		if err != nil {
			return nil, nil, err
		}
		ends = append(ends, len(flatCoords))
	}
	return flatCoords, ends, nil
}

func deflate3(
	flatCoords []float64, endss [][]int, coords3 [][][]Coord, stride int,
) ([]float64, [][]int, error) {
	for _, coords2 := range coords3 {
		var err error
		var ends []int
		flatCoords, ends, err = deflate2(flatCoords, ends, coords2, stride)
		if err != nil {
			return nil, nil, err
		}
		endss = append(endss, ends)
	}
	return flatCoords, endss, nil
}

func inflate0(flatCoords []float64, offset, end, stride int) Coord {
	if offset+stride != end {
		panic("geom: stride mismatch")
	}
	c := make([]float64, stride)
	copy(c, flatCoords[offset:end])
	return c
}

func inflate1(flatCoords []float64, offset, end, stride int) []Coord {
	coords1 := make([]Coord, (end-offset)/stride)
	for i := range coords1 {
		coords1[i] = inflate0(flatCoords, offset, offset+stride, stride)
		offset += stride
	}
	return coords1
}

func inflate2(flatCoords []float64, offset int, ends []int, stride int) [][]Coord {
	coords2 := make([][]Coord, len(ends))
	for i := range coords2 {
		end := ends[i]
		coords2[i] = inflate1(flatCoords, offset, end, stride)
		offset = end
	}
	return coords2
}

func inflate3(flatCoords []float64, offset int, endss [][]int, stride int) [][][]Coord {
	coords3 := make([][][]Coord, len(endss))
	for i := range coords3 {
		ends := endss[i]
		coords3[i] = inflate2(flatCoords, offset, ends, stride)
		if len(ends) > 0 {
			offset = ends[len(ends)-1]
		}
	}
	return coords3
}

func length1(flatCoords []float64, offset, end, stride int) float64 {
	var length float64
	for i := offset + stride; i < end; i += stride {
		dx := flatCoords[i] - flatCoords[i-stride]
		dy := flatCoords[i+1] - flatCoords[i+1-stride]
		length += math.Sqrt(dx*dx + dy*dy)
	}
	return length
}

func length2(flatCoords []float64, offset int, ends []int, stride int) float64 {
	var length float64
	for _, end := range ends {
		length += length1(flatCoords, offset, end, stride)
		offset = end
	}
	return length
}

func length3(flatCoords []float64, offset int, endss [][]int, stride int) float64 {
	var length float64
	for _, ends := range endss {
		length += length2(flatCoords, offset, ends, stride)
		offset = ends[len(ends)-1]
	}
	return length
}

func reverse1(flatCoords []float64, offset, end, stride int) {
	for i, j := offset+stride, end; i <= j; i, j = i+stride, j-stride {
		for k := 0; k < stride; k++ {
			flatCoords[i-stride+k], flatCoords[j-stride+k] = flatCoords[j-stride+k], flatCoords[i-stride+k]
		}
	}
}

func reverse2(flatCoords []float64, offset int, ends []int, stride int) {
	for _, end := range ends {
		reverse1(flatCoords, offset, end, stride)
		offset = end
	}
}

func reverse3(flatCoords []float64, offset int, endss [][]int, stride int) {
	for _, ends := range endss {
		if len(ends) == 0 {
			continue
		}
		reverse2(flatCoords, offset, ends, stride)
		offset = ends[len(ends)-1]
	}
}
