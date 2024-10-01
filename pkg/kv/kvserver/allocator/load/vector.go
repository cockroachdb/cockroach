// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"fmt"

	"github.com/cockroachdb/redact"
)

// Vector is a static container which implements the Load interface.
type Vector [nDimensions]float64

var _ Load = Vector{}

// Dim returns the value of the Dimension given.
func (v Vector) Dim(dim Dimension) float64 {
	if int(dim) > len(v) || dim < 0 {
		panic(fmt.Sprintf("Unknown load dimension access, %d", dim))
	}
	return v[dim]
}

// String returns a string representation of Load.
func (v Vector) String() string {
	return redact.StringWithoutMarkers(v)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (v Vector) SafeFormat(w redact.SafePrinter, _ rune) {
	var buf redact.StringBuilder

	buf.SafeRune('(')
	for i, val := range v {
		if i > 0 {
			buf.SafeRune(' ')
		}
		dim := Dimension(i)
		buf.Printf("%v=%v", dim, dim.format(val))
	}
	buf.SafeRune(')')
	w.Print(buf)
}
