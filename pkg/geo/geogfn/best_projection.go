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
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// BestGeomProjection translates roughly to the ST_BestSRID function in PostGIS.
// It attempts to find the best projection for a bounding box into an accurate
// geometry-type projection.
//
// The algorithm is described by ST_Buffer documentation:
//   It first determines the best SRID that fits the bounding box of the 2
//   geography objects (if geography objects are within one half zone UTM
//   but not same UTM will pick one of those) (favoring UTM or Lambert Azimuthal
//   Equal Area (LAEA) north/south pole, and falling back on mercator in worst
//   case scenario)...and retransforms back to WGS84 geography.
func BestGeomProjection(boundingRect s2.Rect) (geoprojbase.Proj4Text, error) {
	center := boundingRect.Center()

	latWidth := s1.Angle(boundingRect.Lat.Length())
	lngWidth := s1.Angle(boundingRect.Lng.Length())

	// Check if these fit either the North Pole or South Pole areas.
	// If the center of each is greater than 70, and it is within 45 degrees, we can use
	// the Lambert Azimuthal Equal Area projections.
	if latWidth.Degrees() < 45 {
		// North pole.
		if center.Lat.Degrees() > 70 {
			return getGeomProjection(3574)
		}
		// South pole.
		if center.Lat.Degrees() < -70 {
			return getGeomProjection(3409)
		}
	}

	// Attempt to fit the geometries into one UTM zone, which is 6 degrees wide each.
	if lngWidth.Degrees() < 6 {
		// Determine the offset of the projection.
		// Offset longitude -180 to 0 and divide by 6 to get the zone.
		// Note that we treat 180 degree longtitudes as offset 59.
		sridOffset := geopb.SRID(math.Min(math.Floor((center.Lng.Degrees()+180)/6), 59))
		if center.Lat.Degrees() >= 0 {
			// Start at the north UTM SRID.
			return getGeomProjection(32601 + sridOffset)
		}
		// Start at the south UTM SRID.
		return getGeomProjection(32701 + sridOffset)
	}

	// Attempt to fit into LAEA areas if the width is less than 25 degrees (we can go up to 30
	// but want to leave some room for precision issues).
	//
	// LAEA areas are separated into 3 latitude zones between 0 and 90 and 3 latitude zones
	// between -90 and 0. Within each latitude zones, they have different longitude bands:
	// * The bands closest to the equator have 12x30 degree longitude zones.
	// * The bands in the temperate area 8x45 degree longitude zones.
	// * The bands near the poles have 4x90 degree longitude zones.
	//
	// For each of these bands, we custom define a LAEA area with the center of the LAEA area
	// as the lat/lon offset.
	//
	// See also: https://en.wikipedia.org/wiki/Lambert_azimuthal_equal-area_projection.
	if latWidth.Degrees() < 25 {
		// Convert lat to a known 30 degree zone..
		// -3 represents [-90, -60), -2 represents [-60, -30) ... and 2 represents [60, 90].
		// (note: 90 is inclusive at the end).
		// Treat a 90 degree latitude as band 2.
		latZone := math.Min(math.Floor(center.Lat.Degrees()/30), 2)
		latZoneCenterDegrees := (latZone * 30) + 15
		// Equator bands - 30 degree zones.
		if (latZone == 0 || latZone == -1) && lngWidth.Degrees() <= 30 {
			lngZone := math.Floor(center.Lng.Degrees() / 30)
			return geoprojbase.MakeProj4Text(
				fmt.Sprintf(
					"+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=%g +lon_0=%g +units=m +no_defs",
					latZoneCenterDegrees,
					(lngZone*30)+15,
				),
			), nil
		}
		// Temperate bands - 45 degree zones.
		if (latZone == -2 || latZone == 1) && lngWidth.Degrees() <= 45 {
			lngZone := math.Floor(center.Lng.Degrees() / 45)
			return geoprojbase.MakeProj4Text(
				fmt.Sprintf(
					"+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=%g +lon_0=%g +units=m +no_defs",
					latZoneCenterDegrees,
					(lngZone*45)+22.5,
				),
			), nil
		}
		// Polar bands -- 90 degree zones.
		if (latZone == -3 || latZone == 2) && lngWidth.Degrees() <= 90 {
			lngZone := math.Floor(center.Lng.Degrees() / 90)
			return geoprojbase.MakeProj4Text(
				fmt.Sprintf(
					"+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=%g +lon_0=%g +units=m +no_defs",
					latZoneCenterDegrees,
					(lngZone*90)+45,
				),
			), nil
		}
	}

	// Default to Web Mercator.
	return getGeomProjection(3857)
}

// getGeomProjection returns the Proj4Text associated with an SRID.
func getGeomProjection(srid geopb.SRID) (geoprojbase.Proj4Text, error) {
	proj, ok := geoprojbase.Projection(srid)
	if !ok {
		return geoprojbase.Proj4Text{}, errors.Newf("unexpected SRID %d", srid)
	}
	return proj.Proj4Text, nil
}
