// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoprojbase

import "github.com/cockroachdb/cockroach/pkg/geo/geopb"

// projections is a mapping of SRID to projections.
// This file is not spell checked.
var projections = map[geopb.SRID]ProjInfo{
	4326: {
		SRID:      4326,
		AuthName:  "EPSG",
		AuthSRID:  4326,
		SRText:    `GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]`,
		Proj4Text: MakeProj4Text("+proj=longlat +datum=WGS84 +no_defs"),
		IsLatLng:  true,
	},
	4004: {
		SRID:      4004,
		AuthName:  "EPSG",
		AuthSRID:  4004,
		SRText:    `GEOGCS["Unknown datum based upon the Bessel 1841 ellipsoid",DATUM["Not_specified_based_on_Bessel_1841_ellipsoid",SPHEROID["Bessel 1841",6377397.155,299.1528128,AUTHORITY["EPSG","7004"]],AUTHORITY["EPSG","6004"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4004"]]`,
		Proj4Text: MakeProj4Text("+proj=longlat +ellps=bessel +no_defs"),
		IsLatLng:  true,
	},
	3857: {
		SRID:      3857,
		AuthName:  "EPSG",
		AuthSRID:  3857,
		SRText:    `PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Mercator_1SP"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"],AUTHORITY["EPSG","3857"]]`,
		Proj4Text: MakeProj4Text("+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"),
		IsLatLng:  false,
	},
}
