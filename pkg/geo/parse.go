// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geo

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/ewkbhex"
	"github.com/twpayne/go-geom/encoding/geojson"
)

// parseAmbiguousTextToEWKB parses a text as a number of different options
// that is available in the geospatial world using the first character as
// a heuristic.
// This matches the PostGIS direct cast from a string to GEOGRAPHY/GEOMETRY.
func parseAmbiguousTextToEWKB(str string, defaultSRID geopb.SRID) (geopb.EWKB, error) {
	if len(str) == 0 {
		return nil, fmt.Errorf("geo: parsing empty string to geo type")
	}

	switch str[0] {
	case '0':
		return ParseEWKBHex(str, defaultSRID)
	case 0x00, 0x01:
		return ParseEWKB([]byte(str), defaultSRID)
	case '{':
		return ParseGeoJSON([]byte(str), defaultSRID)
	}

	// TODO(otan): parse GeoJSON by checking if character starts with '{'
	return ParseEWKT(geopb.EWKT(str), defaultSRID)
}

// ParseEWKBHex takes a given str assumed to be in EWKB hex and transforms it
// into an EWKB.
func ParseEWKBHex(str string, defaultSRID geopb.SRID) (geopb.EWKB, error) {
	t, err := ewkbhex.Decode(str)
	if err != nil {
		return nil, err
	}
	// TODO(otan): check SRID is valid against spatial_ref_sys.
	if defaultSRID != 0 && t.SRID() == 0 {
		adjustGeomSRID(t, defaultSRID)
	}
	return ewkb.Marshal(t, ewkbEncodingFormat)
}

// ParseEWKB takes given bytes assumed to be EWKB and transforms it into
// an EWKB in the proper format.
func ParseEWKB(b []byte, defaultSRID geopb.SRID) (geopb.EWKB, error) {
	t, err := ewkb.Unmarshal(b)
	if err != nil {
		return nil, err
	}
	// TODO(otan): check SRID is valid against spatial_ref_sys.
	if defaultSRID != 0 && t.SRID() == 0 {
		adjustGeomSRID(t, defaultSRID)
	}
	return ewkb.Marshal(t, ewkbEncodingFormat)
}

// ParseGeoJSON takes given bytes assumed to be GeoJSON and transforms it to
// an EWKB in the proper format.
func ParseGeoJSON(b []byte, defaultSRID geopb.SRID) (geopb.EWKB, error) {
	var f geojson.Feature
	if err := f.UnmarshalJSON(b); err != nil {
		return nil, err
	}
	t := f.Geometry
	// TODO(otan): check SRID from properties.
	if defaultSRID != 0 && t.SRID() == 0 {
		adjustGeomSRID(t, defaultSRID)
	}
	return ewkb.Marshal(t, ewkbEncodingFormat)
}

// adjustGeomSRID adjusts the SRID of a given geom.T.
// Ideally SetSRID is an interface of geom.T, but that is not the case.
func adjustGeomSRID(t geom.T, srid geopb.SRID) {
	switch t := t.(type) {
	case *geom.Point:
		t.SetSRID(int(srid))
	case *geom.LineString:
		t.SetSRID(int(srid))
	case *geom.Polygon:
		t.SetSRID(int(srid))
	case *geom.GeometryCollection:
		t.SetSRID(int(srid))
	case *geom.MultiPoint:
		t.SetSRID(int(srid))
	case *geom.MultiLineString:
		t.SetSRID(int(srid))
	case *geom.MultiPolygon:
		t.SetSRID(int(srid))
	default:
		panic(fmt.Errorf("geo: unknown geom type: %v", t))
	}
}

const sridPrefix = "SRID="
const sridPrefixLen = len(sridPrefix)

// ParseEWKT decodes a WKT string.
func ParseEWKT(str geopb.EWKT, defaultSRID geopb.SRID) (geopb.EWKB, error) {
	srid := defaultSRID
	if strings.HasPrefix(string(str), sridPrefix) {
		end := strings.Index(string(str[sridPrefixLen:]), ";")
		if end != -1 {
			sridInt64, err := strconv.ParseInt(string(str[sridPrefixLen:sridPrefixLen+end]), 10, 32)
			if err != nil {
				return nil, err
			}
			// Only override the SRID if the SRID is not zero.
			// This is in line with observed PostGIS behavior, where Geography still uses
			// SRID 4326 if a 0 SRID was explicitly made at the beginning.
			if sridInt64 != 0 {
				srid = geopb.SRID(sridInt64)
			}
			str = str[sridPrefixLen+end+1:]
		} else {
			return nil, fmt.Errorf(
				"geo: failed to find ; character with SRID declaration during EWKT decode: %q",
				str,
			)
		}
	}

	ewkb, err := geos.WKTToEWKB(geopb.WKT(str), srid)
	if err != nil {
		return nil, err
	}
	return ewkb, nil
}
