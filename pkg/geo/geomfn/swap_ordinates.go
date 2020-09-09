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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/errors"
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
			return errInvalidOrdinate
		}
		ordIndex, err := getOrdsIndex(l, src, ords)
		if err != nil {
			return err
		}

		dst[ordIndex[0]], dst[ordIndex[1]] = src[ordIndex[1]], src[ordIndex[0]]
		return nil
	})
	if err != nil {
		return geometry, err
	}

	return geo.MakeGeometryFromGeomT(newT)
}

var ordsMap = map[uint8]int{
	'x': 0,
	'y': 1,
	'z': 2,
	'm': 3,
}

var errInvalidOrdinate = errors.New("invalid ordinate specification. need two letters from the set (x,y,z and m)")

func getOrdsIndex(l geom.Layout, src []float64, ords string) ([2]int, error) {
	ords = strings.ToLower(ords)
	var ordsIndex [2]int
	for i := 0; i < len(ords); i++ {
		o, ok := ordsMap[ords[i]]
		if !ok {
			return ordsIndex, errInvalidOrdinate
		}
		if o > len(src)/l.Stride() {
			return ordsIndex, fmt.Errorf("geometry does not have an %s ordinate", string(ords[i]))
		}
		ordsIndex[i] = o
	}

	return ordsIndex, nil
}
