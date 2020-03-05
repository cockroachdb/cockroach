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

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"

	// Force these into vendor until they're used.
	"github.com/otan-cockroach/gogeos/geos"
	_ "github.com/otan-cockroach/gogeos/geos"
	"github.com/twpayne/go-geom/encoding/ewkb"
	_ "github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/ewkbhex"
	_ "github.com/twpayne/go-geom/encoding/ewkbhex"
	_ "github.com/twpayne/go-geom/encoding/wkbhex"
)

var featureEncodingEndianness = binary.LittleEndian

// Feature is planar or spherical spatial object.
type Feature struct {
	// Exactly one of these two will be set.
	gm geom.T
	gg s2.Region

	SRID geopb.SRID
}

// MarshalFeature and UnmarshalFeature round trip a Feature through a byte
// format designed for long-term storage.
func MarshalFeature(f *Feature) ([]byte, error) {
	if f.gm != nil {
		return ewkb.Marshal(f.gm, featureEncodingEndianness)
	}
	return nil, errors.Errorf(`unhandled geog type %T: %s`, f.gg, f.gg)
}

// UnmarshalFeature and MarshalFeature round trip a Feature through a byte
// format designed for long-term storage.
func UnmarshalFeature(buf []byte) (*Feature, error) {
	gm, err := ewkb.Unmarshal(buf)
	if err != nil {
		return nil, err
	}
	return &Feature{gm: gm}, nil
}

// ParseGeometry instantiates a Feature from any of the string formats we
// accept.
func ParseGeometry(str string) (*Feature, error) {
	// Try parse it as wkbhex first, if we can do so.
	if '0' <= str[0] && str[0] <= '9' {
		gm, err := ewkbhex.Decode(str)
		if err == nil {
			return &Feature{gm: gm}, nil
		}
	}

	var wkbBytes []byte
	{
		// The geom library doesn't have WKT decoding, so roundtrip to WKB through
		// the other library for now.
		//
		// TODO(otan): SRID= at the front... doesn't work in the geos library!
		g, err := geos.FromWKT(str)
		if err != nil {
			return nil, err
		}
		wkbBytes, err = g.WKB()
		if err != nil {
			return nil, err
		}
	}
	gm, err := ewkb.Read(bytes.NewReader(wkbBytes))
	if err != nil {
		return nil, err
	}
	return &Feature{gm: gm}, nil
}

// AsGeometry returns a (possibly new) Feature with the internal representation
// necessary for geometrical computations.
func (f *Feature) AsGeometry() (*Feature, error) {
	if f.gm != nil {
		return f, nil
	}
	gg, err := geomToGeog(f.SRID, f.gm)
	if err != nil {
		return nil, err
	}
	return &Feature{gg: gg, SRID: f.SRID}, nil
}

// AsGeography returns a (possibly new) Feature with the internal representation
// necessary for geographical computations. The SRID must be lat/lng based.
func (f *Feature) AsGeography() (*Feature, error) {
	if f.gg != nil {
		return f, nil
	}
	gm, err := geogToGeom(f.gg)
	if err != nil {
		return nil, err
	}
	return &Feature{gm: gm, SRID: f.SRID}, nil
}

func geomToGeog(srid geopb.SRID, gm geom.T) (s2.Region, error) {
	if srid != geopb.DefaultGeographySRID {
		// TODO(dan): Structured error so SQL can turn it into something with a
		// pgcode.
		return nil, errors.Errorf(`invalid geography SRID: %d`, srid)
	}
	switch s := gm.(type) {
	case *geom.Point:
		lat, lng := s.X(), s.Y()
		return s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng)), nil
	default:
		return nil, errors.Errorf(`unhandled geom type %T: %s`, s, s)
	}
}

func geogToGeom(gg s2.Region) (geom.T, error) {
	switch s := gg.(type) {
	case s2.Point:
		ll := s2.LatLngFromPoint(s)
		gm := geom.NewPointFlat(geom.XY, []float64{ll.Lat.Degrees(), ll.Lng.Degrees()})
		return gm, nil
	default:
		return nil, errors.Errorf(`unhandled geog type %T: %s`, s, s)
	}
}
