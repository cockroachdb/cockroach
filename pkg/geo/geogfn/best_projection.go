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
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// BestGeomProjection translates roughly to the ST_BestSRID function in PostGIS.
// It attempts to find the best projection for a bounding box into an accurate
// geometry-type projection.
//
// The algorithm is described by ST_Buffer/ST_Intersection documentation (paraphrased):
//   It first determines the best SRID that fits the bounding box of the 2 geography objects (ST_Intersection only).
//   It favors a north/south pole projection, then UTM, then LAEA for smaller zones, otherwise falling back
//   to web mercator.
//   If geography objects are within one half zone UTM but not the same UTM it will pick one of those.
//   After the calculation is complete, it will fall back to WGS84 Geography.
func BestGeomProjection(boundingRect s2.Rect) (geoprojbase.Proj4Text, error) {
	center := boundingRect.Center()

	latWidth := s1.Angle(boundingRect.Lat.Length())
	lngWidth := s1.Angle(boundingRect.Lng.Length())

	// Check if these fit either the North Pole or South Pole areas.
	// If the center has latitude greater than 70 (an arbitrary polar threshold), and it is
	// within the polar ranges, return that.
	if center.Lat.Degrees() > 70 && boundingRect.Lo().Lat.Degrees() > 45 {
		// See: https://epsg.io/3574.
		return getGeomProjection(3574)
	}
	// Same for south pole.
	if center.Lat.Degrees() < -70 && boundingRect.Hi().Lat.Degrees() < -45 {
		// See: https://epsg.io/3409
		return getGeomProjection(3409)
	}

	// Each UTM zone is 6 degrees wide and distortion is low for geometries that fit within the zone. We use
	// UTM if the width is lower than the UTM zone width, even though the geometry may span 2 zones -- using
	// the geometry center to pick the UTM zone should result in most of the geometry being in the picked zone.
	if lngWidth.Degrees() < 6 {
		// Determine the offset of the projection.
		// Offset longitude -180 to 0 and divide by 6 to get the zone.
		// Note that we treat 180 degree longtitudes as offset 59.
		// TODO(#geo): do we care about https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system#Exceptions?
		// PostGIS's _ST_BestSRID function doesn't seem to care: .
		sridOffset := geopb.SRID(math.Min(math.Floor((center.Lng.Degrees()+180)/6), 59))
		if center.Lat.Degrees() >= 0 {
			// Start at the north UTM SRID.
			return getGeomProjection(32601 + sridOffset)
		}
		// Start at the south UTM SRID.
		// This should make no difference in end result compared to using the north UTMs,
		// but for completeness we do it.
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
	proj, err := geoprojbase.Projection(srid)
	if err != nil {
		return geoprojbase.Proj4Text{}, err
	}
	return proj.Proj4Text, nil
}
