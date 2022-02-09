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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/golang/geo/s1"
	"github.com/pierrre/geohash"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/geojson"
	"github.com/twpayne/go-geom/encoding/kml"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkbcommon"
	"github.com/twpayne/go-geom/encoding/wkbhex"
	"github.com/twpayne/go-geom/encoding/wkt"
)

// DefaultGeoJSONDecimalDigits is the default number of digits coordinates in GeoJSON.
const DefaultGeoJSONDecimalDigits = 9

// SpatialObjectToWKT transforms a given SpatialObject to WKT.
func SpatialObjectToWKT(so geopb.SpatialObject, maxDecimalDigits int) (geopb.WKT, error) {
	t, err := ewkb.Unmarshal([]byte(so.EWKB))
	if err != nil {
		return "", err
	}
	ret, err := wkt.Marshal(t, wkt.EncodeOptionWithMaxDecimalDigits(maxDecimalDigits))
	return geopb.WKT(ret), err
}

// SpatialObjectToEWKT transforms a given SpatialObject to EWKT.
func SpatialObjectToEWKT(so geopb.SpatialObject, maxDecimalDigits int) (geopb.EWKT, error) {
	t, err := ewkb.Unmarshal([]byte(so.EWKB))
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

// SpatialObjectToWKB transforms a given SpatialObject to WKB.
func SpatialObjectToWKB(so geopb.SpatialObject, byteOrder binary.ByteOrder) (geopb.WKB, error) {
	t, err := ewkb.Unmarshal([]byte(so.EWKB))
	if err != nil {
		return nil, err
	}
	ret, err := wkb.Marshal(t, byteOrder, wkbcommon.WKBOptionEmptyPointHandling(wkbcommon.EmptyPointHandlingNaN))
	return geopb.WKB(ret), err
}

// SpatialObjectToGeoJSONFlag maps to the ST_AsGeoJSON flags for PostGIS.
type SpatialObjectToGeoJSONFlag int

// These should be kept with ST_AsGeoJSON in PostGIS.
// 0: means no option
// 1: GeoJSON BBOX
// 2: GeoJSON Short CRS (e.g EPSG:4326)
// 4: GeoJSON Long CRS (e.g urn:ogc:def:crs:EPSG::4326)
// 8: GeoJSON Short CRS if not EPSG:4326 (default)
const (
	SpatialObjectToGeoJSONFlagIncludeBBox SpatialObjectToGeoJSONFlag = 1 << (iota)
	SpatialObjectToGeoJSONFlagShortCRS
	SpatialObjectToGeoJSONFlagLongCRS
	SpatialObjectToGeoJSONFlagShortCRSIfNot4326

	SpatialObjectToGeoJSONFlagZero = 0
)

// geomToGeoJSONCRS converts a geom to its CRS GeoJSON form.
func geomToGeoJSONCRS(t geom.T, long bool) (*geojson.CRS, error) {
	projection, err := geoprojbase.Projection(geopb.SRID(t.SRID()))
	if err != nil {
		return nil, err
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

// SpatialObjectToGeoJSON transforms a given SpatialObject to GeoJSON.
func SpatialObjectToGeoJSON(
	so geopb.SpatialObject, maxDecimalDigits int, flag SpatialObjectToGeoJSONFlag,
) ([]byte, error) {
	t, err := ewkb.Unmarshal([]byte(so.EWKB))
	if err != nil {
		return nil, err
	}
	options := []geojson.EncodeGeometryOption{
		geojson.EncodeGeometryWithMaxDecimalDigits(maxDecimalDigits),
	}
	if flag&SpatialObjectToGeoJSONFlagIncludeBBox != 0 {
		// Do not encoding empty bounding boxes.
		if so.BoundingBox != nil {
			options = append(
				options,
				geojson.EncodeGeometryWithBBox(),
			)
		}
	}
	// Take CRS flag in order of precedence.
	if t.SRID() != 0 {
		if flag&SpatialObjectToGeoJSONFlagLongCRS != 0 {
			crs, err := geomToGeoJSONCRS(t, true /* long */)
			if err != nil {
				return nil, err
			}
			options = append(options, geojson.EncodeGeometryWithCRS(crs))
		} else if flag&SpatialObjectToGeoJSONFlagShortCRS != 0 {
			crs, err := geomToGeoJSONCRS(t, false /* long */)
			if err != nil {
				return nil, err
			}
			options = append(options, geojson.EncodeGeometryWithCRS(crs))
		} else if flag&SpatialObjectToGeoJSONFlagShortCRSIfNot4326 != 0 {
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

// SpatialObjectToWKBHex transforms a given SpatialObject to WKBHex.
func SpatialObjectToWKBHex(so geopb.SpatialObject) (string, error) {
	t, err := ewkb.Unmarshal([]byte(so.EWKB))
	if err != nil {
		return "", err
	}
	ret, err := wkbhex.Encode(t, DefaultEWKBEncodingFormat, wkbcommon.WKBOptionEmptyPointHandling(wkbcommon.EmptyPointHandlingNaN))
	return strings.ToUpper(ret), err
}

// SpatialObjectToKML transforms a given SpatialObject to KML.
func SpatialObjectToKML(so geopb.SpatialObject) (string, error) {
	t, err := ewkb.Unmarshal([]byte(so.EWKB))
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

// GeoHashAutoPrecision means to calculate the precision of SpatialObjectToGeoHash
// based on input, up to 32 characters.
const GeoHashAutoPrecision = 0

// GeoHashMaxPrecision is the maximum precision for GeoHashes.
// 20 is picked as doubles have 51 decimals of precision, and each base32 position
// can contain 5 bits of data. As we have two points, we use floor((2 * 51) / 5) = 20.
const GeoHashMaxPrecision = 20

// SpatialObjectToGeoHash transforms a given SpatialObject to a GeoHash.
func SpatialObjectToGeoHash(so geopb.SpatialObject, p int) (string, error) {
	if so.BoundingBox == nil {
		return "", nil
	}
	bbox := so.BoundingBox
	if so.Type == geopb.SpatialObjectType_GeographyType {
		// Convert bounding box back to degrees.
		bbox = &geopb.BoundingBox{
			LoX: s1.Angle(bbox.LoX).Degrees(),
			HiX: s1.Angle(bbox.HiX).Degrees(),
			LoY: s1.Angle(bbox.LoY).Degrees(),
			HiY: s1.Angle(bbox.HiY).Degrees(),
		}
	}
	if bbox.LoX < -180 || bbox.HiX > 180 || bbox.LoY < -90 || bbox.HiY > 90 {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue,
			"object has bounds greater than the bounds of lat/lng, got (%f %f, %f %f)",
			bbox.LoX, bbox.LoY,
			bbox.HiX, bbox.HiY,
		)
	}

	// Get precision using the bounding box if required.
	if p <= GeoHashAutoPrecision {
		p = getPrecisionForBBox(bbox)
	}

	// Support up to 20, which is the same as PostGIS.
	if p > GeoHashMaxPrecision {
		p = GeoHashMaxPrecision
	}

	bbCenterLng := bbox.LoX + (bbox.HiX-bbox.LoX)/2.0
	bbCenterLat := bbox.LoY + (bbox.HiY-bbox.LoY)/2.0

	return geohash.Encode(bbCenterLat, bbCenterLng, p), nil
}

// getPrecisionForBBox is a function imitating PostGIS's ability to go from
// a world bounding box and truncating a GeoHash to fit the given bounding box.
// The algorithm halves the world bounding box until it intersects with the
// feature bounding box to get a precision that will encompass the entire
// bounding box.
func getPrecisionForBBox(bbox *geopb.BoundingBox) int {
	bitPrecision := 0

	// This is a point, for points we use the full bitPrecision.
	if bbox.LoX == bbox.HiX && bbox.LoY == bbox.HiY {
		return GeoHashMaxPrecision
	}

	// Starts from a world bounding box:
	lonMin := -180.0
	lonMax := 180.0
	latMin := -90.0
	latMax := 90.0

	// Each iteration shrinks the world bounding box by half in the dimension that
	// does not fit, making adjustments each iteration until it intersects with
	// the object bbox.
	for {
		lonWidth := lonMax - lonMin
		latWidth := latMax - latMin
		latMaxDelta, lonMaxDelta, latMinDelta, lonMinDelta := 0.0, 0.0, 0.0, 0.0

		// Look at whether the longitudes of the bbox are to the left or
		// the right of the world bbox longitudes, shrinks it and makes adjustments
		// for the next iteration.
		if bbox.LoX > lonMin+lonWidth/2.0 {
			lonMinDelta = lonWidth / 2.0
		} else if bbox.HiX < lonMax-lonWidth/2.0 {
			lonMaxDelta = lonWidth / -2.0
		}
		// Look at whether the latitudes of the bbox are to the left or
		// the right of the world bbox latitudes, shrinks it and makes adjustments
		// for the next iteration.
		if bbox.LoY > latMin+latWidth/2.0 {
			latMinDelta = latWidth / 2.0
		} else if bbox.HiY < latMax-latWidth/2.0 {
			latMaxDelta = latWidth / -2.0
		}

		// Every change we make that splits the box up adds precision.
		// If we detect no change, we've intersected a box and so must exit.
		precisionDelta := 0
		if lonMinDelta != 0.0 || lonMaxDelta != 0.0 {
			lonMin += lonMinDelta
			lonMax += lonMaxDelta
			precisionDelta++
		} else {
			break
		}
		if latMinDelta != 0.0 || latMaxDelta != 0.0 {
			latMin += latMinDelta
			latMax += latMaxDelta
			precisionDelta++
		} else {
			break
		}
		bitPrecision += precisionDelta
	}
	// Each character can represent 5 bits of bitPrecision.
	// As such, divide by 5 to get GeoHash precision.
	return bitPrecision / 5
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
