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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/pierrre/geohash"
	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/ewkbhex"
	"github.com/twpayne/go-geom/encoding/geojson"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkbcommon"
	"github.com/twpayne/go-geom/encoding/wkt"
)

// parseEWKBRaw creates a geopb.SpatialObject from an EWKB
// without doing any SRID based checks.
// You most likely want parseEWKB instead.
func parseEWKBRaw(soType geopb.SpatialObjectType, in geopb.EWKB) (geopb.SpatialObject, error) {
	t, err := ewkb.Unmarshal(in)
	if err != nil {
		return geopb.SpatialObject{}, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "error parsing EWKB")
	}
	return spatialObjectFromGeomT(t, soType)
}

// parseAmbiguousText parses a text as a number of different options
// that is available in the geospatial world using the first character as
// a heuristic.
// This matches the PostGIS direct cast from a string to GEOGRAPHY/GEOMETRY.
func parseAmbiguousText(
	soType geopb.SpatialObjectType, str string, defaultSRID geopb.SRID,
) (geopb.SpatialObject, error) {
	if len(str) == 0 {
		return geopb.SpatialObject{}, pgerror.Newf(pgcode.InvalidParameterValue, "geo: parsing empty string to geo type")
	}

	switch str[0] {
	case '0':
		return parseEWKBHex(soType, str, defaultSRID)
	case 0x00, 0x01:
		return parseEWKB(soType, []byte(str), defaultSRID, DefaultSRIDIsHint)
	case '{':
		return parseGeoJSON(soType, []byte(str), defaultSRID)
	}

	return parseEWKT(soType, geopb.EWKT(str), defaultSRID, DefaultSRIDIsHint)
}

// parseEWKBHex takes a given str assumed to be in EWKB hex and transforms it
// into a SpatialObject.
func parseEWKBHex(
	soType geopb.SpatialObjectType, str string, defaultSRID geopb.SRID,
) (geopb.SpatialObject, error) {
	t, err := ewkbhex.Decode(str)
	if err != nil {
		return geopb.SpatialObject{}, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "error parsing EWKB hex")
	}
	if (defaultSRID != 0 && t.SRID() == 0) || int32(t.SRID()) < 0 {
		AdjustGeomTSRID(t, defaultSRID)
	}
	return spatialObjectFromGeomT(t, soType)
}

// parseEWKB takes given bytes assumed to be EWKB and transforms it into a SpatialObject.
// The defaultSRID will overwrite any SRID set in the EWKB if overwrite is true.
func parseEWKB(
	soType geopb.SpatialObjectType,
	b []byte,
	defaultSRID geopb.SRID,
	overwrite defaultSRIDOverwriteSetting,
) (geopb.SpatialObject, error) {
	t, err := ewkb.Unmarshal(b)
	if err != nil {
		return geopb.SpatialObject{}, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "error parsing EWKB")
	}
	if overwrite == DefaultSRIDShouldOverwrite || (defaultSRID != 0 && t.SRID() == 0) || int32(t.SRID()) < 0 {
		AdjustGeomTSRID(t, defaultSRID)
	}
	return spatialObjectFromGeomT(t, soType)
}

// parseWKB takes given bytes assumed to be WKB and transforms it into a SpatialObject.
func parseWKB(
	soType geopb.SpatialObjectType, b []byte, defaultSRID geopb.SRID,
) (geopb.SpatialObject, error) {
	t, err := wkb.Unmarshal(b, wkbcommon.WKBOptionEmptyPointHandling(wkbcommon.EmptyPointHandlingNaN))
	if err != nil {
		return geopb.SpatialObject{}, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "error parsing WKB")
	}
	AdjustGeomTSRID(t, defaultSRID)
	return spatialObjectFromGeomT(t, soType)
}

// parseGeoJSON takes given bytes assumed to be GeoJSON and transforms it into a SpatialObject.
func parseGeoJSON(
	soType geopb.SpatialObjectType, b []byte, defaultSRID geopb.SRID,
) (geopb.SpatialObject, error) {
	var t geom.T
	if err := geojson.Unmarshal(b, &t); err != nil {
		return geopb.SpatialObject{}, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "error parsing GeoJSON")
	}
	if t == nil {
		return geopb.SpatialObject{}, pgerror.Newf(pgcode.InvalidParameterValue, "invalid GeoJSON input")
	}
	if defaultSRID != 0 && t.SRID() == 0 {
		AdjustGeomTSRID(t, defaultSRID)
	}
	return spatialObjectFromGeomT(t, soType)
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

// parseEWKT decodes a WKT string and transforms it into a SpatialObject.
// The defaultSRID will overwrite any SRID set in the EWKT if overwrite is true.
func parseEWKT(
	soType geopb.SpatialObjectType,
	str geopb.EWKT,
	defaultSRID geopb.SRID,
	overwrite defaultSRIDOverwriteSetting,
) (geopb.SpatialObject, error) {
	srid := defaultSRID
	if hasPrefixIgnoreCase(string(str), sridPrefix) {
		end := strings.Index(string(str[sridPrefixLen:]), ";")
		if end != -1 {
			if overwrite != DefaultSRIDShouldOverwrite {
				sridInt64, err := strconv.ParseInt(string(str[sridPrefixLen:sridPrefixLen+end]), 10, 32)
				if err != nil {
					return geopb.SpatialObject{}, pgerror.Wrapf(
						err,
						pgcode.InvalidParameterValue,
						"error parsing SRID for EWKT",
					)
				}
				// Only use the parsed SRID if the parsed SRID is > 0 and it was not
				// to be overwritten by the DefaultSRID parameter.
				if sridInt64 > 0 {
					srid = geopb.SRID(sridInt64)
				}
			}
			str = str[sridPrefixLen+end+1:]
		} else {
			return geopb.SpatialObject{}, pgerror.Newf(
				pgcode.InvalidParameterValue,
				"geo: failed to find ; character with SRID declaration during EWKT decode: %q",
				str,
			)
		}
	}

	g, wktUnmarshalErr := wkt.Unmarshal(string(str))
	if wktUnmarshalErr != nil {
		return geopb.SpatialObject{}, pgerror.Wrap(
			wktUnmarshalErr,
			pgcode.InvalidParameterValue,
			"error parsing EWKT",
		)
	}
	AdjustGeomTSRID(g, srid)
	return spatialObjectFromGeomT(g, soType)
}

// hasPrefixIgnoreCase returns whether a given str begins with a prefix, ignoring case.
// It assumes that the string and prefix contains only ASCII bytes.
func hasPrefixIgnoreCase(str string, prefix string) bool {
	if len(str) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		if util.ToLowerSingleByte(str[i]) != util.ToLowerSingleByte(prefix[i]) {
			return false
		}
	}
	return true
}

// ParseGeometryPointFromGeoHash converts a GeoHash to a Geometry Point
// using a Lng/Lat Point representation of the GeoHash.
func ParseGeometryPointFromGeoHash(g string, precision int) (Geometry, error) {
	box, err := parseGeoHash(g, precision)
	if err != nil {
		return Geometry{}, err
	}
	point := box.Center()
	geom, gErr := MakeGeometryFromPointCoords(point.Lon, point.Lat)
	if gErr != nil {
		return Geometry{}, gErr
	}
	return geom, nil
}

// ParseCartesianBoundingBoxFromGeoHash converts a GeoHash to a CartesianBoundingBox.
func ParseCartesianBoundingBoxFromGeoHash(g string, precision int) (CartesianBoundingBox, error) {
	box, err := parseGeoHash(g, precision)
	if err != nil {
		return CartesianBoundingBox{}, err
	}
	return CartesianBoundingBox{
		BoundingBox: geopb.BoundingBox{
			LoX: box.Lon.Min,
			HiX: box.Lon.Max,
			LoY: box.Lat.Min,
			HiY: box.Lat.Max,
		},
	}, nil
}

func parseGeoHash(g string, precision int) (geohash.Box, error) {
	if len(g) == 0 {
		return geohash.Box{}, pgerror.Newf(pgcode.InvalidParameterValue, "length of GeoHash must be greater than 0")
	}

	// In PostGIS the parsing is case-insensitive.
	g = strings.ToLower(g)

	// If precision is more than the length of the geohash
	// or if precision is less than 0 then set
	// precision equal to length of geohash.
	if precision > len(g) || precision < 0 {
		precision = len(g)
	}
	box, err := geohash.Decode(g[:precision])
	if err != nil {
		return geohash.Box{}, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid GeoHash")
	}
	return box, nil
}

// GeometryToEncodedPolyline turns the provided geometry and precision into a Polyline ASCII
func GeometryToEncodedPolyline(g Geometry, p int) (string, error) {
	gt, err := g.AsGeomT()
	if err != nil {
		return "", errors.Wrap(err, "error parsing input geometry")
	}
	if gt.SRID() != 4326 {
		return "", pgerror.Newf(pgcode.InvalidParameterValue, "only SRID 4326 is supported")
	}

	return encodePolylinePoints(gt.FlatCoords(), p), nil
}

// ParseEncodedPolyline takes the encoded polyline ASCII and precision, decodes the points and returns them as a geometry
func ParseEncodedPolyline(encodedPolyline string, precision int) (Geometry, error) {
	flatCoords := decodePolylinePoints(encodedPolyline, precision)
	ls := geom.NewLineStringFlat(geom.XY, flatCoords).SetSRID(4326)

	g, err := MakeGeometryFromGeomT(ls)
	if err != nil {
		return Geometry{}, errors.Wrap(err, "parsing geography error")
	}
	return g, nil
}
