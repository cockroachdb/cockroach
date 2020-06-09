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
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/geojson"
	"github.com/twpayne/go-geom/encoding/kml"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkbhex"
	"github.com/twpayne/go-geom/encoding/wkt"
)

// EWKBToWKT transforms a given EWKB to WKT.
func EWKBToWKT(b geopb.EWKB) (geopb.WKT, error) {
	t, err := ewkb.Unmarshal([]byte(b))
	if err != nil {
		return "", err
	}
	ret, err := wkt.Marshal(t)
	return geopb.WKT(ret), err
}

// EWKBToEWKT transforms a given EWKB to EWKT.
func EWKBToEWKT(b geopb.EWKB) (geopb.EWKT, error) {
	t, err := ewkb.Unmarshal([]byte(b))
	if err != nil {
		return "", err
	}
	ret, err := wkt.Marshal(t)
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
	ret, err := wkb.Marshal(t, byteOrder)
	return geopb.WKB(ret), err
}

// EWKBToGeoJSON transforms a given EWKB to GeoJSON.
func EWKBToGeoJSON(b geopb.EWKB) ([]byte, error) {
	t, err := ewkb.Unmarshal([]byte(b))
	if err != nil {
		return nil, err
	}
	f := geojson.Feature{
		// TODO(otan): add features once we have spatial_ref_sys.
		Geometry: t,
	}
	return f.MarshalJSON()
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
