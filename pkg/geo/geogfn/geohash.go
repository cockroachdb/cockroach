// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geogfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/fmtsafe/testdata/src/github.com/cockroachdb/errors"
)

// GeoHash returns the string representing the geohash from the feature a based on
// the precision provided p. If no precision is given (0 or -1) it is calculated from
// the bounding box size. For point features, a default precision of 20 is used.
func GeoHash(a *geo.Geography, p int32) (string, error) {

	bb := a.SpatialObject().BoundingBox
	if bb == nil {
		return "", nil
	}
	// Geohash needs decimal degrees:
	if bb.MinX < -180.0 || bb.MinY < -90.0 || bb.MaxX > 180.0 || bb.MaxY > 90.0 {
		return "", errors.Newf("geohash requires inputs in decimal degrees, got (%f, %f, %f, %f)", bb.MinX, bb.MinY, bb.MaxX, bb.MaxY)
	}

	// It uses the size of the bounding box to calculate the precision
	if p <= 0 {
		p = getPrecisionByBBox(bb)
	}

	// returns the center from the bounding box to calculate the geohash
	bbCenterLng := bb.MinX + (bb.MaxX-bb.MinX)/2.0
	bbCenterLat := bb.MinY + (bb.MaxY-bb.MinY)/2.0

	geohash := geohash_point(bbCenterLng, bbCenterLat, p)
	return geohash, nil
}

// getPrecisionByBB is a function from PostGIS that iterates to get a
//geohash precision that wraps the whole feature. Larger features = less precision.
func getPrecisionByBBox(box *geopb.BoundingBox) int32 {
	minx := box.MinX
	miny := box.MinY
	maxx := box.MaxX
	maxy := box.MaxY
	var precision int32 = 0

	// this is a point, returns the full precision
	if minx == maxx && miny == maxy {
		return 20
	}

	// Starts from a world bounding box:
	lonmin := -180.0
	latmin := -90.0
	lonmax := 180.0
	latmax := 90.0

	// Each iteration shrinks  the world bounding box more and more
	// until it intersects with the feature bbox.
	for {
		lonwidth := lonmax - lonmin
		latwidth := latmax - latmin
		latmaxadjust, lonmaxadjust, latminadjust, lonminadjust := 0.0, 0.0, 0.0, 0.0
		if minx > lonmin+lonwidth/2.0 {
			lonminadjust = lonwidth / 2.0
		} else if maxx < lonmax-lonwidth/2.0 {
			lonmaxadjust = -1 * lonwidth / 2.0
		}
		if lonminadjust != 0.0 || lonmaxadjust != 0.0 {
			lonmin += lonminadjust
			lonmax += lonmaxadjust
			precision++
		} else {
			break
		}

		if miny > latmin+latwidth/2.0 {
			latminadjust = latwidth / 2.0
		} else if maxy < latmax-latwidth/2.0 {
			latmaxadjust = -1 * latwidth / 2.0
		}
		if latminadjust != 0.0 || latmaxadjust != 0.0 {
			latmin += latminadjust
			latmax += latmaxadjust
			precision++
		} else {
			break
		}
	}
	// the precision (number of characters) is divided by 5
	//since each geohash character is encoded as base32 (5 bits).
	return precision / 5
}

// geohash_point encodes the given latitude and longitude
// by iterating downwards, gaining precision each iteration
// until reaching the provided precision
func geohash_point(longitude float64, latitude float64, precision int32) string {
	isEven := true
	i := int32(0)
	bitIdx := 0
	bit := 0
	lat := [2]float64{-90.0, 90.0}
	lon := [2]float64{-180.0, 180.0}

	base32 := "0123456789bcdefghjkmnpqrstuvwxyz"

	geohash := ""

	for i < precision {
		if isEven {
			mid := (lon[0] + lon[1]) / 2
			if longitude >= mid {
				bitIdx = bitIdx*2 + 1
				lon[0] = mid
			} else {
				bitIdx = bitIdx * 2
				lon[1] = mid
			}
		} else {
			mid := (lat[0] + lat[1]) / 2
			if latitude >= mid {
				bitIdx = bitIdx*2 + 1
				lat[0] = mid
			} else {
				bitIdx = bitIdx * 2
				lat[1] = mid
			}
		}
		isEven = !isEven
		if bit < 4 {
			bit++
		} else {
			geohash += string(base32[bitIdx])
			i++
			bit = 0
			bitIdx = 0
		}
	}
	return geohash
}
