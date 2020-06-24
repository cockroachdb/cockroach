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
	"github.com/cockroachdb/errors"
)

// GeoHashNoPrecision is used to calculate the precision based on
// the size of the feature bounding box.
const GeoHashNoPrecision = 0

// GeoHash returns the string representing the geohash from the feature a based on
// the precision provided p. If no precision is given, it is calculated from
// the bounding box size: larger features have less precision; smaller features have more.
// For point features, a default precision of 20 is used.
func GeoHash(a *geo.Geography, p int) (string, error) {

	bb := a.SpatialObject().BoundingBox
	if bb == nil {
		return "", nil
	}

	// GeoHash needs decimal degrees:
	if bb.MinX < -180.0 || bb.MinY < -90.0 || bb.MaxX > 180.0 || bb.MaxY > 90.0 {
		return "", errors.Newf("GeoHash requires inputs in decimal degrees, got (%f, %f, %f, %f)", bb.MinX, bb.MinY, bb.MaxX, bb.MaxY)
	}

	// It uses the size of the bounding box to calculate the precision
	if p <= GeoHashNoPrecision {
		p = getPrecisionByBBox(bb)
	}

	// returns the center from the bounding box to calculate the GeoHash
	bbCenterLng := bb.MinX + (bb.MaxX-bb.MinX)/2.0
	bbCenterLat := bb.MinY + (bb.MaxY-bb.MinY)/2.0

	geohash := geohashPoint(bbCenterLng, bbCenterLat, p)
	return geohash, nil
}

// getPrecisionByBBox is a function from PostGIS that iterates downward from a
// world bounding box halving it each time until it intersects with the
// feature bounding box, to get the geohash precision number that wraps
// the whole feature. Larger features means less precision.
func getPrecisionByBBox(box *geopb.BoundingBox) int {
	minx := box.MinX
	miny := box.MinY
	maxx := box.MaxX
	maxy := box.MaxY
	precision := 0

	// this is a point, for points we use the full precision
	if minx == maxx && miny == maxy {
		return 20
	}

	// Starts from a world bounding box:
	lonmin := -180.0
	latmin := -90.0
	lonmax := 180.0
	latmax := 90.0

	// Each iteration shrinks the world bounding box by half, making
	// adjustments each iteration until it intersects with
	// the feature bbox.
	for {
		lonwidth := lonmax - lonmin
		latwidth := latmax - latmin
		latmaxadjust, lonmaxadjust, latminadjust, lonminadjust := 0.0, 0.0, 0.0, 0.0

		// looks whether the longitudes of the bbox are to the left or
		// the right of the world bbox longitudes, shrinks it and makes adjustments
		// for the next iteration
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
			// we stop until the adjustment is 0, meaning the longitude
			// intersected with the feature bounding box
			break
		}

		// looks whether the latitudes of the bbox are to the left or
		// the right of the world bbox latitudes, shrinks it and makes adjustments
		// for the next iteration
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
			// we stop until the adjustment is 0, meaning the latitude
			// intersected with the feature bounding box
			break
		}
	}
	// the precision (number of characters) is divided by 5
	// since each geohash character is encoded as base32 (5 bits).
	return precision / 5
}

// geohashPoint encodes the given latitude and longitude
// by iterating downwards, gaining precision each iteration
// until reaching the provided precision.
// It encodes the latitude and the longitude into a binary string
// The latitude is encoded as the odd bits and the longitude as the
// even ones.
// Every iteration (stopping until reaching the provided precision)
// a range is used (starting from {-90.0, 90.0} for latitudes and
// {-180.0, 180.0} for longitudes) and each bit represents whether the
// latitude or longitude is to the left or to the right of that range.
// Reaching 5 bits means we find the encoding character for that
// precision level.
// More info on: https://en.wikipedia.org/wiki/Geohash#Algorithm_and_example
func geohashPoint(longitude float64, latitude float64, precision int) string {
	isEven := true
	i := 0
	bitIdx := 0
	bit := 0

	// Initial lookup ranges
	lat := [2]float64{-90.0, 90.0}
	lon := [2]float64{-180.0, 180.0}

	// Special base32 alphabet for geohashing
	base32 := "0123456789bcdefghjkmnpqrstuvwxyz"

	geohash := ""

	// iterates downward calculating the hashing bits for both the
	// latitude and the longitude until reaching the precision.
	for i < precision {
		// even bits will hash the longitude. It will check if the
		// longitude is to the right (1) or the left (0) of the range
		// and will adjust it for the next iteration
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
			// odd bits will hash the latitude. It will check if the
			// latitude is to the right (1) or the left (0) of the range
			// and will adjust it for the next iteration
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

		// We stop until reaching 5 bits and then we look that index in
		// the base32 alphabet
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
