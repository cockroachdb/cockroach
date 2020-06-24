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
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/geojson"
	"github.com/twpayne/go-geom/encoding/kml"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkbcommon"
	"github.com/twpayne/go-geom/encoding/wkbhex"
	"github.com/twpayne/go-geom/encoding/wkt"
)

// EWKBToWKT transforms a given EWKB to WKT.
func EWKBToWKT(b geopb.EWKB, maxDecimalDigits int) (geopb.WKT, error) {
	t, err := ewkb.Unmarshal([]byte(b))
	if err != nil {
		return "", err
	}
	ret, err := wkt.Marshal(t, wkt.EncodeOptionWithMaxDecimalDigits(maxDecimalDigits))
	return geopb.WKT(ret), err
}

// EWKBToEWKT transforms a given EWKB to EWKT.
func EWKBToEWKT(b geopb.EWKB, maxDecimalDigits int) (geopb.EWKT, error) {
	t, err := ewkb.Unmarshal([]byte(b))
	if err != nil {
		return "", err
	}
	ret, err := wkt.Marshal(t, wkt.EncodeOptionWithMaxDecimalDigits(maxDecimalDigits))
	if err != nil {
		return "", err
	}
	if t.SRID() != 0 {
		ret = fmt.Sprintf("SRID=%d;%s", t.SRID(), ret)
	}
	return geopb.EWKT(ret), err
}

// EWKBToWKB transforms a given EWKB to WKB.
func EWKBToWKB(b geopb.EWKB, byteOrder binary.ByteOrder) (geopb.WKB, error) {
	t, err := ewkb.Unmarshal([]byte(b))
	if err != nil {
		return nil, err
	}
	ret, err := wkb.Marshal(t, byteOrder, wkbcommon.WKBOptionEmptyPointHandling(wkbcommon.EmptyPointHandlingNaN))
	return geopb.WKB(ret), err
}

// EWKBToGeoJSONFlag maps to the ST_AsGeoJSON flags for PostGIS.
type EWKBToGeoJSONFlag int

// These should be kept with ST_AsGeoJSON in PostGIS.
// 0: means no option
// 1: GeoJSON BBOX
// 2: GeoJSON Short CRS (e.g EPSG:4326)
// 4: GeoJSON Long CRS (e.g urn:ogc:def:crs:EPSG::4326)
// 8: GeoJSON Short CRS if not EPSG:4326 (default)
const (
	EWKBToGeoJSONFlagIncludeBBox EWKBToGeoJSONFlag = 1 << (iota)
	EWKBToGeoJSONFlagShortCRS
	EWKBToGeoJSONFlagLongCRS
	EWKBToGeoJSONFlagShortCRSIfNot4326

	EWKBToGeoJSONFlagZero = 0
)

// geomToGeoJSONCRS converts a geom to its CRS GeoJSON form.
func geomToGeoJSONCRS(t geom.T, long bool) (*geojson.CRS, error) {
	projection, ok := geoprojbase.Projection(geopb.SRID(t.SRID()))
	if !ok {
		return nil, errors.Newf("unknown SRID: %d", t.SRID())
	}
	var prop string
	if long {
		prop = fmt.Sprintf("urn:ogc:def:crs:%s::%d", projection.AuthName, projection.AuthSRID)
	} else {
		prop = fmt.Sprintf("%s:%d", projection.AuthName, projection.AuthSRID)
	}
	crs := &geojson.CRS{
		Type: "name",
		Properties: map[string]interface{}{
			"name": prop,
		},
	}
	return crs, nil
}

// EWKBToGeoJSON transforms a given EWKB to GeoJSON.
func EWKBToGeoJSON(b geopb.EWKB, maxDecimalDigits int, flag EWKBToGeoJSONFlag) ([]byte, error) {
	t, err := ewkb.Unmarshal([]byte(b))
	if err != nil {
		return nil, err
	}
	options := []geojson.EncodeGeometryOption{
		geojson.EncodeGeometryWithMaxDecimalDigits(maxDecimalDigits),
	}
	if flag&EWKBToGeoJSONFlagIncludeBBox != 0 {
		options = append(
			options,
			geojson.EncodeGeometryWithBBox(),
		)
	}
	// Take CRS flag in order of precedence.
	if t.SRID() != 0 {
		if flag&EWKBToGeoJSONFlagLongCRS != 0 {
			crs, err := geomToGeoJSONCRS(t, true /* long */)
			if err != nil {
				return nil, err
			}
			options = append(options, geojson.EncodeGeometryWithCRS(crs))
		} else if flag&EWKBToGeoJSONFlagShortCRS != 0 {
			crs, err := geomToGeoJSONCRS(t, false /* long */)
			if err != nil {
				return nil, err
			}
			options = append(options, geojson.EncodeGeometryWithCRS(crs))
		} else if flag&EWKBToGeoJSONFlagShortCRSIfNot4326 != 0 {
			if t.SRID() != 4326 {
				crs, err := geomToGeoJSONCRS(t, false /* long */)
				if err != nil {
					return nil, err
				}
				options = append(options, geojson.EncodeGeometryWithCRS(crs))
			}
		}
	}

	return geojson.Marshal(t, options...)
}

// EWKBToWKBHex transforms a given EWKB to WKBHex.
func EWKBToWKBHex(b geopb.EWKB) (string, error) {
	t, err := ewkb.Unmarshal([]byte(b))
	if err != nil {
		return "", err
	}
	ret, err := wkbhex.Encode(t, DefaultEWKBEncodingFormat)
	return strings.ToUpper(ret), err
}

// EWKBToKML transforms a given EWKB to KML.
func EWKBToKML(b geopb.EWKB) (string, error) {
	t, err := ewkb.Unmarshal([]byte(b))
	if err != nil {
		return "", err
	}
	kmlElement, err := kml.Encode(t)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := kmlElement.Write(&buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// StringToByteOrder returns the byte order of string.
func StringToByteOrder(s string) binary.ByteOrder {
	switch strings.ToLower(s) {
	case "ndr":
		return binary.LittleEndian
	case "xdr":
		return binary.BigEndian
	default:
		return DefaultEWKBEncodingFormat
	}
}
