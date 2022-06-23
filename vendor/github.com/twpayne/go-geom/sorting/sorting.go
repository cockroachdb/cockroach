package sorting

import "github.com/twpayne/go-geom"

// FlatCoord is a sort.Interface implementation that will result in sorting the wrapped coords based on the
// the comparator function
//
// Note: this data struct cannot be used with its 0 values.  it must be constructed using NewFlatCoordSorting
type FlatCoord struct {
	isLess IsLess
	coords []float64
	layout geom.Layout
	stride int
}

// IsLess the function used by FlatCoord to sort the coordinate array
// returns true is v1 is less than v2
type IsLess func(v1, v2 []float64) bool

// IsLess2D is a comparator that compares based on the size of the x and y coords.
//
// First the x coordinates are compared.
// if x coords are equal then the y coords are compared
func IsLess2D(v1, v2 []float64) bool {
	if v1[0] < v2[0] {
		return true
	}
	if v1[0] > v2[0] {
		return false
	}
	if v1[1] < v2[1] {
		return true
	}

	return false
}

// NewFlatCoordSorting2D creates a Compare2D based sort.Interface implementation
func NewFlatCoordSorting2D(layout geom.Layout, coordData []float64) FlatCoord {
	return NewFlatCoordSorting(layout, coordData, IsLess2D)
}

// NewFlatCoordSorting creates a sort.Interface implementation based on the Comparator function
func NewFlatCoordSorting(layout geom.Layout, coordData []float64, comparator IsLess) FlatCoord {
	return FlatCoord{
		isLess: comparator,
		coords: coordData,
		layout: layout,
		stride: layout.Stride(),
	}
}

func (s FlatCoord) Len() int {
	return len(s.coords) / s.stride
}

func (s FlatCoord) Swap(i, j int) {
	for k := 0; k < s.stride; k++ {
		s.coords[i*s.stride+k], s.coords[j*s.stride+k] = s.coords[j*s.stride+k], s.coords[i*s.stride+k]
	}
}

func (s FlatCoord) Less(i, j int) bool {
	is, js := i*s.stride, j*s.stride
	return s.isLess(s.coords[is:is+s.stride], s.coords[js:js+s.stride])
}
