package location

import "fmt"

// Type enumerates the different topological locations which can occur in a {@link Geometry}.
// The constants are also used as the row and column indices of DE-9IM {@link IntersectionMatrix}es.
type Type int

const (
	// Interior is the location value for the interior of a geometry.
	// Also, DE-9IM row index of the interior of the first geometry and column index of
	// the interior of the second geometry.
	Interior Type = iota
	// Boundary is the location value for the boundary of a geometry.
	// Also, DE-9IM row index of the boundary of the first geometry and column index of
	// the boundary of the second geometry.
	Boundary
	// Exterior is the location value for the exterior of a geometry.
	// Also, DE-9IM row index of the exterior of the first geometry and column index of
	// the exterior of the second geometry.
	Exterior
	// None is used for uninitialized location values.
	None
)

func (t Type) String() string {
	switch t {
	case Exterior:
		return "Exterior"
	case Boundary:
		return "Boundary"
	case Interior:
		return "Interior"
	case None:
		return "None"
	}

	panic(fmt.Sprintf("Unknown location value: %v", int(t)))
}

// Symbol converts the location value to a location symbol, for example, Exterior => 'e'
// locationValue
// Returns either 'e', 'b', 'i' or '-'
func (t Type) Symbol() rune {
	switch t {
	case Exterior:
		return 'e'
	case Boundary:
		return 'b'
	case Interior:
		return 'i'
	case None:
		return '-'
	}
	panic(fmt.Sprintf("Unknown location value: %v", int(t)))
}
