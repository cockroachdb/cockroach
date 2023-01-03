// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package load

import (
	"fmt"
	"strings"
)

// Vector is a static container which implements the Load interface.
type Vector [nDimensions]float64

var _ Load = Vector{}

// Dim returns the value of the Dimension given.
func (s Vector) Dim(dim Dimension) float64 {
	if int(dim) > len(s) || dim < 0 {
		panic(fmt.Sprintf("Unknown load dimension access, %d", dim))
	}
	return s[dim]
}

// String returns a string representation of Load.
func (s Vector) String() string {
	var buf strings.Builder

	fmt.Fprint(&buf, "(")
	for i, val := range s {
		if i > 0 {
			fmt.Fprint(&buf, " ")
		}
		dim := Dimension(i)
		fmt.Fprintf(&buf, "%s=%s", dim.String(), dim.Format(val))
	}
	fmt.Fprint(&buf, ")")
	return buf.String()
}
