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
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/ewkbhex"
	_ "github.com/twpayne/go-geom/encoding/wkbhex"
)

var featureEncodingEndianness = binary.LittleEndian

// Geometry is planar spatial object.
type Geometry struct {
	// TODO(dan): This should be a geos.Geometry.
	Figure geom.T
	SRID   geopb.SRID
}

// Geography is a spherical spatial object.
type Geography struct {
	Figure []s2.Region
	SRID   geopb.SRID
}

// MarshalGeometry and UnmarshalGeometry roundtrip a Geometry through a byte
// format designed for long-term storage.
func MarshalGeometry(g *Geometry) ([]byte, error) {
	return ewkb.Marshal(g.Figure, featureEncodingEndianness)
}

// UnmarshalGeometry and MarshalGeometry roundtrip a Geometry through a byte
// format designed for long-term storage.
func UnmarshalGeometry(buf []byte) (*Geometry, error) {
	figure, err := ewkb.Unmarshal(buf)
	if err != nil {
		return nil, err
	}
	// TODO(dan): Roundtrip the SRID too
	return &Geometry{Figure: figure}, nil
}

// ParseGeometry instantiates a Geometry from any of the string formats we
// accept.
func ParseGeometry(str string) (*Geometry, error) {
	// Try parse it as wkbhex first, if we can do so.
	if '0' <= str[0] && str[0] <= '9' {
		figure, err := ewkbhex.Decode(str)
		if err == nil {
			return &Geometry{Figure: figure}, nil
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
	figure, err := ewkb.Read(bytes.NewReader(wkbBytes))
	if err != nil {
		return nil, err
	}
	return &Geometry{Figure: figure}, nil
}

// AsGeometry converts the underlying geography figure into a geometry one.
func (f *Geography) AsGeometry() (*Geometry, error) {
	figure, err := geogToGeom(f.Figure)
	if err != nil {
		return nil, err
	}
	return &Geometry{Figure: figure, SRID: f.SRID}, nil
}

// AsGeography returns a (possibly new) Feature with the internal representation
// necessary for geographical computations. The SRID must be lat/lng based.
func (f *Geometry) AsGeography() (*Geography, error) {
	figure, err := geomToGeog(f.SRID, f.Figure)
	if err != nil {
		return nil, err
	}
	return &Geography{Figure: figure, SRID: f.SRID}, nil
}

func geomToGeog(srid geopb.SRID, figure geom.T) ([]s2.Region, error) {
	if srid != geopb.DefaultGeographySRID {
		// TODO(dan): Structured error so SQL can turn it into something with a
		// pgcode.
		return nil, errors.Errorf(`invalid geography SRID: %d`, srid)
	}
	switch f := figure.(type) {
	case *geom.Point:
		lat, lng := f.X(), f.Y()
		ll := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
		return []s2.Region{ll}, nil
	default:
		return nil, errors.Errorf(`unhandled geom type %T: %s`, f, f)
	}
}

func geogToGeom(figure []s2.Region) (geom.T, error) {
	r, err := assertSingleFigure(figure)
	if err != nil {
		return nil, err
	}
	switch f := r.(type) {
	case s2.Point:
		ll := s2.LatLngFromPoint(f)
		gm := geom.NewPointFlat(geom.XY, []float64{ll.Lat.Degrees(), ll.Lng.Degrees()})
		return gm, nil
	default:
		return nil, errors.Errorf(`unhandled geog type %T: %s`, f, f)
	}
}

func assertSingleFigure(figure []s2.Region) (s2.Region, error) {
	if len(figure) != 1 {
		return nil, errors.Errorf(`Multi* geographies are not yet supported: %v`, figure)
	}
	return figure[0], nil
}
