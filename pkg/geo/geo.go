// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geo contains the base types for spatial data type operations.
package geo

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	// Force these into vendor until they're used.
	_ "github.com/golang/geo/s2"
	_ "github.com/twpayne/go-geom"
	_ "github.com/twpayne/go-geom/encoding/ewkb"
	_ "github.com/twpayne/go-geom/encoding/ewkbhex"
	_ "github.com/twpayne/go-geom/encoding/geojson"
	_ "github.com/twpayne/go-geom/encoding/kml"
	_ "github.com/twpayne/go-geom/encoding/wkb"
	_ "github.com/twpayne/go-geom/encoding/wkbhex"
	_ "github.com/twpayne/go-geom/encoding/wkt"
)

// spatialObjectBase is the base for spatial objects.
type spatialObjectBase struct {
	ewkb geopb.EWKB
	// TODO: denormalize SRID from EWKB.
}

// Geometry is planar spatial object.
type Geometry struct {
	spatialObjectBase
}

// NewGeometry returns a new Geometry.
func NewGeometry(ewkb geopb.EWKB) *Geometry {
	return &Geometry{spatialObjectBase{ewkb: ewkb}}
}

// ParseGeometry parses a Geometry from a given text.
func ParseGeometry(str geopb.WKT) (*Geometry, error) {
	wkb, err := geos.WKTToWKB(str)
	if err != nil {
		return nil, err
	}
	return NewGeometry(geopb.EWKB(wkb)), nil
}

// Geography is a spherical spatial object.
type Geography struct {
	spatialObjectBase
}

// NewGeography returns a new Geography.
func NewGeography(ewkb geopb.EWKB) *Geography {
	return &Geography{spatialObjectBase{ewkb: ewkb}}
}

// ParseGeography parses a Geography from a given text.
// TODO(otan): when we have our own WKT parser, move this to geo.
func ParseGeography(str geopb.WKT) (*Geography, error) {
	// TODO(otan): set SRID of EWKB to 4326.
	wkb, err := geos.WKTToWKB(str)
	if err != nil {
		return nil, err
	}
	return NewGeography(geopb.EWKB(wkb)), nil
}
