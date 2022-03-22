// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/twpayne/go-geom"
)

// SwapOrdinates returns a version of the given geometry with given ordinates swapped.
// The ords parameter is a 2-characters string naming the ordinates to swap. Valid names are: x,y,z and m.
func SwapOrdinates(geometry geo.Geometry, ords string) (geo.Geometry, error) {
	if geometry.Empty() {
		return geometry, nil
	}

	t, err := geometry.AsGeomT()
	if err != nil {
		return geometry, err
	}

	newT, err := applyOnCoordsForGeomT(t, func(l geom.Layout, dst, src []float64) error {
		if len(ords) != 2 {
			return pgerror.Newf(pgcode.InvalidParameterValue, "invalid ordinate specification. need two letters from the set (x, y, z and m)")
		}
		ordsIndices, err := getOrdsIndices(l, ords)
		if err != nil {
			return err
		}

		dst[ordsIndices[0]], dst[ordsIndices[1]] = src[ordsIndices[1]], src[ordsIndices[0]]
		return nil
	})
	if err != nil {
		return geometry, err
	}

	return geo.MakeGeometryFromGeomT(newT)
}

// getOrdsIndices get the indices position of ordinates string
func getOrdsIndices(l geom.Layout, ords string) ([2]int, error) {
	ords = strings.ToUpper(ords)
	var ordIndices [2]int
	for i := 0; i < len(ords); i++ {
		oi := findOrdIndex(ords[i], l)
		if oi == -2 {
			return ordIndices, pgerror.Newf(pgcode.InvalidParameterValue, "invalid ordinate specification. need two letters from the set (x, y, z and m)")
		}
		if oi == -1 {
			return ordIndices, pgerror.Newf(pgcode.InvalidParameterValue, "geometry does not have a %s ordinate", string(ords[i]))
		}
		ordIndices[i] = oi
	}

	return ordIndices, nil
}

func findOrdIndex(ordString uint8, l geom.Layout) int {
	switch ordString {
	case 'X':
		return 0
	case 'Y':
		return 1
	case 'Z':
		return l.ZIndex()
	case 'M':
		return l.MIndex()
	default:
		return -2
	}
}
