package orientation

import "fmt"

// Type enumerates the angular relationship between points and vectors.
type Type int

const (
	// Clockwise indicates that the vector or point is clockwise relative to the base vector
	Clockwise Type = iota - 1
	// Collinear indicates that the vector or point is along the same vector as the base vector
	Collinear
	// CounterClockwise indicates that the vector or point is clockwise relative to the base vector
	CounterClockwise
)

var orientationLabels = [3]string{"Clockwise", "Collinear", "CounterClockwise"}

func (o Type) String() string {
	if o > 1 || o < -1 {
		return fmt.Sprintf("Unsafe to calculate: %v", int(o))
	}
	return orientationLabels[int(o+1)]
}
