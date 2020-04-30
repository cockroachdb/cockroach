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
	"github.com/twpayne/go-geom/encoding/wkb"
)

// parseAmbiguousText parses a text as a number of different options
// that is available in the geospatial world using the first character as
// a heuristic.
// This matches the PostGIS direct cast from a string to GEOGRAPHY/GEOMETRY.
func parseAmbiguousText(str string, defaultSRID geopb.SRID) (geopb.SpatialObject, error) {
	if len(str) == 0 {
		return geopb.SpatialObject{}, fmt.Errorf("geo: parsing empty string to geo type")
	}

	switch str[0] {
	case '0':
		return SpatialObjectFromEWKBHex(str, defaultSRID)
	case 0x00, 0x01:
		return SpatialObjectFromEWKB([]byte(str), defaultSRID, DefaultSRIDIsHint)
	case '{':
		return SpatialObjectFromGeoJSON([]byte(str), defaultSRID)
	}

	return SpatialObjectFromEWKT(geopb.EWKT(str), defaultSRID, DefaultSRIDIsHint)
}

// SpatialObjectFromEWKBHex takes a given str assumed to be in EWKB hex and transforms it
// into a SpatialObject.
func SpatialObjectFromEWKBHex(str string, defaultSRID geopb.SRID) (geopb.SpatialObject, error) {
	t, err := ewkbhex.Decode(str)
	if err != nil {
		return geopb.SpatialObject{}, err
	}
	// TODO(otan): check SRID is valid against spatial_ref_sys.
	if defaultSRID != 0 && t.SRID() == 0 {
		adjustGeomSRID(t, defaultSRID)
	}
	return SpatialObjectFromGeom(t)
}

// SpatialObjectFromEWKB takes given bytes assumed to be EWKB and transforms it into a SpatialObject.
// The defaultSRID will overwrite any SRID set in the EWKB if overwrite is true.
func SpatialObjectFromEWKB(
	b []byte, defaultSRID geopb.SRID, overwrite defaultSRIDOverwriteSetting,
) (geopb.SpatialObject, error) {
	t, err := ewkb.Unmarshal(b)
	if err != nil {
		return geopb.SpatialObject{}, err
	}
	// TODO(otan): check SRID is valid against spatial_ref_sys.
	if overwrite == DefaultSRIDShouldOverwrite || (defaultSRID != 0 && t.SRID() == 0) {
		adjustGeomSRID(t, defaultSRID)
	}
	return SpatialObjectFromGeom(t)
}

// SpatialObjectFromWKB takes given bytes assumed to be WKB and transforms it into a SpatialObject.
func SpatialObjectFromWKB(b []byte, defaultSRID geopb.SRID) (geopb.SpatialObject, error) {
	t, err := wkb.Unmarshal(b)
	if err != nil {
		return geopb.SpatialObject{}, err
	}
	adjustGeomSRID(t, defaultSRID)
	return SpatialObjectFromGeom(t)
}

// SpatialObjectFromGeoJSON takes given bytes assumed to be GeoJSON and transforms it into a SpatialObject.
func SpatialObjectFromGeoJSON(b []byte, defaultSRID geopb.SRID) (geopb.SpatialObject, error) {
	var f geojson.Feature
	if err := f.UnmarshalJSON(b); err != nil {
		return geopb.SpatialObject{}, err
	}
	t := f.Geometry
	// TODO(otan): check SRID from properties.
	if defaultSRID != 0 && t.SRID() == 0 {
		adjustGeomSRID(t, defaultSRID)
	}
	return SpatialObjectFromGeom(t)
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

type defaultSRIDOverwriteSetting bool

const (
	// DefaultSRIDShouldOverwrite implies the parsing function should overwrite
	// the SRID with the defaultSRID.
	DefaultSRIDShouldOverwrite defaultSRIDOverwriteSetting = true
	// DefaultSRIDIsHint implies that the default SRID is only a hint
	// and if the SRID is provided by the given EWKT/EWKB, it should be
	// used instead.
	DefaultSRIDIsHint defaultSRIDOverwriteSetting = false
)

// SpatialObjectFromEWKT decodes a WKT string and transforms it into a SpatialObject.
// The defaultSRID will overwrite any SRID set in the EWKT if overwrite is true.
func SpatialObjectFromEWKT(
	str geopb.EWKT, defaultSRID geopb.SRID, overwrite defaultSRIDOverwriteSetting,
) (geopb.SpatialObject, error) {
	srid := defaultSRID
	if strings.HasPrefix(string(str), sridPrefix) {
		end := strings.Index(string(str[sridPrefixLen:]), ";")
		if end != -1 {
			if overwrite != DefaultSRIDShouldOverwrite {
				sridInt64, err := strconv.ParseInt(string(str[sridPrefixLen:sridPrefixLen+end]), 10, 32)
				if err != nil {
					return geopb.SpatialObject{}, err
				}
				// Only use the parsed SRID if the parsed SRID is not zero and it was not
				// to be overwritten by the DefaultSRID parameter.
				if sridInt64 != 0 {
					srid = geopb.SRID(sridInt64)
				}
			}
			str = str[sridPrefixLen+end+1:]
		} else {
			return geopb.SpatialObject{}, fmt.Errorf(
				"geo: failed to find ; character with SRID declaration during EWKT decode: %q",
				str,
			)
		}
	}

	ewkb, err := geos.WKTToEWKB(geopb.WKT(str), srid)
	if err != nil {
		return geopb.SpatialObject{}, err
	}
	return SpatialObjectFromEWKBRaw(ewkb)
}
